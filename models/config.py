#!/usr/bin/env python
# -*- coding: utf-8 -*-
Azkaban_addr = "http://ip:port"

config = {
    "Azkaban_addr": "http://ip:port",
    "manager": Azkaban_addr + "/manager",
    "executor": Azkaban_addr + "/executor",
    "mysql": "",
    "mongo_addr": "",
    "mongo_port": 27017,
    "username": "azkaban",
    "password": "azkaban",
    "db": "azkaban",
    "retry": 5,
    "execution_url_format": "{Azkaban_addr}/executor?execid={eid}#jobslist",
    "nodelog_url_format": "{Azkaban_addr}/executor?execid={eid}&job={nodename}&attempt=0",
    "nodeinfo_url_format": "{Azkaban_addr}/manager?project={project}&job={nodename}&history",
    "dateformat": "%Y-%m-%d %H:%M:%S",
    "debug": False,
    "logfile": "azkaban-monitor-log",
    "exclusive_projects" : ["test"]
}
