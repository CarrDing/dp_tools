#!/usr/bin/env python
# -*- coding: utf-8 -*-
Azkaban_addr = "http://10.0.2.112:8085"

config = {
    "Azkaban_addr": "http://10.0.2.112:8085",
    "manager": Azkaban_addr + "/manager",
    "executor": Azkaban_addr + "/executor",
    "mysql": "10.0.0.227",
    "mongo_addr": "10.0.0.198",
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
