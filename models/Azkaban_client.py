# !/usr/bin/env python
# !-*- coding: utf-8 -*-

import json
import requests


import logging
import logging.handlers
import _mysql
from FlowGraph import FlowGraph
from config import config
from utils import logger
DEBUG = config["debug"]


def debug_print(s):
    if DEBUG:
        print s


class Azkaban(object):
    sid = None

    def __init__(self):
        """
            初始化会自动登陆
        """
        self.login()
        self.projects = self.get_projects()

    @staticmethod
    def login():
        """
            利用config中的userame和password登陆
            返回是否成功
        """
        logger.info("login")
        if Azkaban.sid:
            return True
        ok = False
        for i in range(config["retry"]):
            try:
                params = {
                    'action': "login",
                    'username': config["username"],
                    'password': config["password"]
                }
                r = requests.post(config["manager"], params)
                content = r.text
                debug_print(content)
                Azkaban.sid = str(json.loads(content)["session.id"])
                ok = True
                break
            except Exception as e:
                logger.exception("login exception: {e}".format(e=e))

        logger.info("login update sid {0}".format(Azkaban.sid))
        return ok

    @staticmethod
    def get_projects():
        db = _mysql.connect(host=config["mysql"],
                            user=config["username"],
                            passwd=config["password"],
                            db=config["password"])
        db.query("""SELECT name FROM projects """)
        r = db.store_result()
        projects = r.fetch_row(0)
        # each record is a tuple,map each record into project name only
        projects = map(lambda e: e[0], projects)
        logger.info("get_project: {}".format(projects))

        return projects

    @staticmethod
    def get_ajax(method, url, payload):
        """
            入参:
            method,string : GET or POST
            url,string : ajax url
            payload,dict : 参数字典

            返回:
            content,string : 请求回复
            ok,bool : 是否成功
        """
        content = "\{\}"
        ok = False
        for i in range(config["retry"]):
            try:
                if Azkaban.sid:
                    payload["session.id"] = Azkaban.sid
                    if method == "POST":
                        r = requests.post(url, data=payload)
                    else:
                        r = requests.get(url, params=payload)
                    logging.info("{method} {url}, {params}".format(
                        url=url, method=method, params=payload))
                    content = r.text
                    ok = True
                    break
                else:
                    login_success = self.login()
                    if login_success:
                        return self.get_ajax(method, url, payload)

            except Exception as e:
                logger.error(
                    "get_ajax Exception {e}: {method} {url}, {params}".format(
                     e=e, url=url, method=method, params=payload))

        return content, ok

    def fetch_project_flows(self, project):
        """
            获取project的flowid
            返回 flowid list
        """
        method = "GET"
        url = config["manager"]
        params = {
            "ajax": "fetchprojectflows",
            "project": project
        }
        content, ok = self.get_ajax(method, url, params)
        flowids = []
        if ok:
            try:
                res = json.loads(content)
                flowids = map(lambda e: e["flowId"], res["flows"])
            except Exception as e:
                logger.exception(e)
                return []

            logger.info(
                "fetch_project_flows {0} : {1} SUCCESS".format(
                    project, flowids))
            return flowids
        else:
            logger.info("fetch_project_flows {0} FAILED".format(project))

        return flowids

    def fetch_all_execution(self, project, flowid, length):
        """
            获取project下某个flow最近length个execution
            返回 execIds list
        """
        logger.info("fetching flow {} from {}.".format(flowid, project))
        url = config["manager"]
        method = "GET"
        params = {
            'project': project,
            'flow': flowid,
            'ajax': 'fetchFlowExecutions',
            'start': 0,
            'length': length
        }

        content, ok = self.get_ajax(method, url, params)
        exes = []
        total = 0
        if ok:
            try:
                res = json.loads(content)
                exes = res['executions']
                total = res['total']
            except Exception as e:
                logging.exception(e)

        return exes, total

    def fetch_execution(self, execid):
        method = "GET"
        url = config["executor"]
        params = {
            "ajax": "fetchexecflow",
            "execid": execid
        }

        content, ok = self.get_ajax(method, url, params)
        ret = {}
        if ok:
            debug_print(content)
            logger.info("fetch execution {} success.".format(execid))
            try:
                ret = json.loads(content)
            except Exception as e:
                logger.exception(e)
        else:
            logger.info("fetch execution {} failed.".format(execid))

        return ret

    def fetch_running_execution(self, project, flow):
        method = "GET"
        url = config["executor"]
        params = {
            "ajax": "getRunning",
            "project": project,
            "flow": flow
        }

        content, ok = self.get_ajax(method, url, params)
        ret = []
        if ok:
            debug_print(content)
            logger.info("fetch running execution from {} {} success: {}.".format(project, flow, content))
            try:
                ret = json.loads(content)
            except Exception as e:
                logger.exception(e)
        else:
            logger.info("fetch running execution from {} {} failed: {}.".format(project, flow))

        return ret, ok

    def fetch_flow_graph(self, project, flow):
        method = "GET"
        url = config["manager"]
        params = {
            "ajax": "fetchflowgraph",
            "project": project,
            "flow": flow
        }

        content, ok = self.get_ajax(method, url, params)

        if ok:
            debug_print(content)
            logger.info("fetch graph {} {} success.".format(project, flow))
            nodes_list = json.loads(content)['nodes']
            g = FlowGraph(project, flow, nodes_list)
            return g.sort()

        else:
            logger.info("fetch graph {} {} failed.".format(project, flow))

azkaban = Azkaban()
