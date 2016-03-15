# !/usr/bin/env python
# !-*- coding: utf-8 -*-
import logging
import logging.handlers
from config import config
import pymongo
from pymongo import MongoClient
import datetime

LOG_FILE = "azkaban-monitor-log"
handler = logging.handlers.RotatingFileHandler(LOG_FILE)    # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'

formatter = logging.Formatter(fmt)                # 实例化formatter
handler.setFormatter(formatter)                   # 为handler添加formatter

logger = logging.getLogger('azkaban-monitor')     # 获取名为azkaban-monitor的logger
logger.addHandler(handler)                        # 为logger添加handler
logger.setLevel(logging.DEBUG)

client = MongoClient(config["mongo_addr"], config["mongo_port"])
jobinfo_db = client['jobinfo']


def beatify_time(ts):
    ts = str(ts)[0:10]
    if ts == "-1":
        return "-1(unknown)"
    ret = datetime.datetime.fromtimestamp(
            int(ts)
        ).strftime('%Y-%m-%d %H:%M:%S')
    return ret


class URLBuilder(object):
    @staticmethod
    def buildexecutionurl(eid):
        return config["execution_url_format"].format(
            Azkaban_addr=config["Azkaban_addr"], eid=eid)

    @staticmethod
    def buildjoblogurl(eid, nodename):
        return config["nodelog_url_format"].format(
            Azkaban_addr=config["Azkaban_addr"], eid=eid, nodename=nodename)

    @staticmethod
    def buildjobinfourl(project, nodename):
        return config["nodeinfo_url_format"].format(
            Azkaban_addr=config["Azkaban_addr"],
            project=project, nodename=nodename)


def merge_node(nodes):
    # node status :READY SKIPPED RUNNING SUCCEEDED KILLED CANCELLED FAILED
    # merge node logic:
    # 如果nodes有成功(SUCCEEDED)的节点,取最后一次的成功节点作为成功的代表节点
    # 如果nodes有失败(FAILED)的有节点, 需要显示故障时间，故障时间计算规则如下：
    #   如果该失败节点后有成功的节点，则故障时间为该失败节点的结束时间到该节点后第一个成功节点的开始时间
    #   否则故障时间直到当前
    # 如果没有成功,也没有失败,则显示待定
    merge_info = {}
    success_found = False
    if len(nodes) > 0:
        index_marks = set()
        for ind, n in enumerate(reversed(nodes)):
            if n["status"] == "SUCCEEDED":
                start = datetime.datetime.strptime(n["startTime"], '%Y-%m-%d %H:%M:%S')
                end = datetime.datetime.strptime(n["endTime"], '%Y-%m-%d %H:%M:%S')
                deltatime = end - start
                n["delta"] = str(deltatime)
                merge_info["SUCCEED"] = n
                index_marks.add(len(nodes) - 1 - ind)
                success_found = True
                break
        ind = 0
        l = len(nodes)
        down_ind = 1
        while ind < l:
            n = nodes[ind]
            if n["status"] == "FAILED":
                start = datetime.datetime.strptime(n["startTime"], '%Y-%m-%d %H:%M:%S')
                end = datetime.datetime.strptime(n["endTime"], '%Y-%m-%d %H:%M:%S')
                deltatime = end - start
                n["delta"] = str(deltatime)
                merge_info["FAILED{}".format(down_ind)] = n  # first failed
                index_marks.add(ind)
                i = ind + 1
                failedend = datetime.datetime.now()
                isfixed = False
                while i < l:
                    if nodes[i]["status"] == "SUCCEEDED":
                        failedend = datetime.datetime.strptime(nodes[i]["startTime"], '%Y-%m-%d %H:%M:%S')
                        isfixed = True
                        ind = i + 1
                        break
                    i += 1
                failed_time = failedend - end
                merge_info["DOWN{}".format(down_ind)] = {
                        "startTime": end.strftime('%Y-%m-%d %H:%M:%S'),
                        "endTime": failedend.strftime('%Y-%m-%d %H:%M:%S'),
                        "delta": str(failed_time),
                        "execId": "",
                        "exeurl": "",
                        "nodelogurl": "",
                        "fixed": isfixed,
                        "status": "FIXED" if isfixed else "UNFIXED"
                    }
                down_ind += 1
                break
            ind += 1
        if len(nodes) - 1 not in index_marks and (not success_found or nodes[-1]["status"] == "RUNNING"):
            recent_node = nodes[len(nodes) - 1]
            if recent_node["endTime"] != "-1(unknown)":
                start = datetime.datetime.strptime(recent_node["startTime"], '%Y-%m-%d %H:%M:%S')
                end = datetime.datetime.strptime(recent_node["endTime"], '%Y-%m-%d %H:%M:%S')
                deltatime = end - start
                recent_node["delta"] = str(deltatime)
            merge_info["RECENT"] = recent_node
        print merge_info
    return merge_info
