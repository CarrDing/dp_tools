# !/usr/bin/env python
# !-*- coding: utf-8 -*-
from tornado.web import RequestHandler
from models.check_conflict import Conflict
from models.Azkaban_client import azkaban
from models.config import config

__all__ = (
    'IndexHandler',
)


class IndexHandler(RequestHandler):
    def get(self):
        projects = azkaban.get_projects()
        project_flow = {}
        # 获取每个project的flow list
        for pname in projects:
            project_flow[pname] = azkaban.fetch_project_flows(pname)

        flows_executions = {}
        flow_graph = {}
        # 获取每个project下每个flow进行处理
        daily_flows_merge = {}
        pf_list = []
        for p in project_flow:
            if p in config["exclusive_projects"]:
                continue
            for f in project_flow[p]:
                pf_list.append((p,f))

        self.render('index.html',pf_list = pf_list)
