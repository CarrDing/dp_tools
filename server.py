#!/usr/bin/env python
#!-*- coding: utf-8 -*-


import os.path
from tornado.options import options, define
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.web import URLSpec as U
import tornado.web
from handlers.conflict import ConflictListHandler
from handlers.jobinfo import JobInfoHandler,JobSummaryHandler,NodeSummaryHandler,FlowSummaryHandler,NodeSummaryDateRange
from handlers.index import IndexHandler

define('port', default=9900, help='', type=int)
define('mongo_host', default='127.0.0.1', help='', type=str)


class Application(tornado.web.Application):
    def __init__(self):
        base_path = os.path.dirname(__file__)
        settings = {
            'template_path': os.path.join(base_path, 'templates'),
            'static_path': os.path.join(base_path, 'static'),
            'debug': True,
        }

        handlers = [
            U('/conflict', ConflictListHandler, name='Conflict'),
            U('/jobinfo/all', JobInfoHandler, name='allJobInfo'),
            U(r'/jobinfo/pf.*', JobInfoHandler, name='eachJobInfo'),
            U('/summary', JobSummaryHandler, name='summary'),
            U('/index', IndexHandler, name="index"),
            U('/nodesummary/all', NodeSummaryHandler, name="allnodesummary"),
            U(r'/nodesummary/pf.*', NodeSummaryHandler, name="eachnodesummary"),
            U('/flowsummary/all', FlowSummaryHandler, name="allflowsummary"),
            U(r'/flowsummary/pf.*', FlowSummaryHandler, name="eachflowsummary"),
            U(r'/nodesummary_range.*', NodeSummaryDateRange, name="nodesummary_range"),



            # U('/summery/update', SummeryEditHandler, name='summery_update'),
            # U('/problem/delete', ProblemDeleteHandler, name='problem_delete'),
            # U('/problem/update', ProblemUpdateHandler, name='problem_update'),
            # U('/extraction/update', ExtractionEditHandler, name='extraction_update'),
            # U('/problem/export', ProblemExportHandler, name='problem_export'),
            # U('/summery/export', SummeryExportHandler, name='summery_export'),
        ]

        tornado.web.Application.__init__(self, handlers, **settings)

def main():
    tornado.options.parse_command_line()
    http_server = HTTPServer(Application())
    http_server.listen(options.port)
    IOLoop.instance().start()

if __name__ == '__main__':
    main()
