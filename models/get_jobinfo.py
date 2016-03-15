# !/usr/bin/env python
# !-*- coding: utf-8 -*-

import datetime
from collections import defaultdict
from Azkaban_client import azkaban
from config import config
from utils import URLBuilder, beatify_time, merge_node
from utils import jobinfo_db
import pymongo



class JobInfo(object):

    def getSavedFlowExesCount(self):
        SavedFlowExesCount = defaultdict(lambda: 0)
        docs = jobinfo_db["flowSaveExesCount"].find()
        for d in docs:
            SavedFlowExesCount[d["flow"]] = d["count"]

        return SavedFlowExesCount

    def updateSavedFlowExesCount(self, NewFlowExesCount):
        for pf in NewFlowExesCount:
            # print pf, NewFlowExesCount[pf]
            jobinfo_db["flowSaveExesCount"].update(
                {"flow": pf},
                {
                    "$set": {
                        "count": NewFlowExesCount[pf]
                    }
                },
                True)
        return

    def get_flow_executions(self, project, flow, length = -1):
        p = project
        f = flow
        if length == -1:
            exes, total = azkaban.fetch_all_execution(p, f, 1)
            exes, total = azkaban.fetch_all_execution(p, f, total)
        else:
            exes, total = azkaban.fetch_all_execution(p, f, length)
        for e in exes:
            e['startTime'] = beatify_time(e['startTime'])
            e['endTime'] = beatify_time(e['endTime'])
            eid = e['execId']
            cur_exe = azkaban.fetch_execution(eid)
            einfo = cur_exe.get("nodes", [])
            for n in einfo:
                n['startTime'] = beatify_time(n['startTime'])
                n['endTime'] = beatify_time(n['endTime'])
            e["einfo"] = {}
            e["startDate"] = e["startTime"].split()[0]
            print e["startDate"]
            # e["einfo"] 储存了该execution每个job的运行情况
            for n in einfo:
                name = n["id"]
                e["einfo"][name] = n
        # uncessary
        # exes.sort(key=lambda exe: exe["execId"], reverse=True)
        # exes = filter(lambda exe: exe["startDate"] != "unknown", exes)
        return exes, total

    def merge_execution_daily(self, flows_executions, graph):
        # print flows_executions
        flows_executions = filter(lambda exe: exe["startDate"] != "-1(unknown)", flows_executions)
        daily_flows = defaultdict(lambda: [])
        for execution in flows_executions:
            # print execution
            execution_start_date = execution["startTime"].split()[0]
            daily_flows[execution_start_date].append(execution)
        days_exe = {}
        # days of flow loop
        for day in daily_flows:
            # in place sort, execId could imply execution order
            daily_flows[day].sort(key=lambda e: e["execId"])
            one_day_exe = {}
            # nodes of flow loop
            for nodename in graph:
                nodes = []
                # 根据时间先后顺序，把当天该node的所有executions添加到nodes
                exes = daily_flows[day]
                for exe in exes:
                    if nodename in exe["einfo"]:
                        nodes.append((exe["einfo"][nodename], exe["execId"]))
                # 对nodes中的数据进行变换，nodes里的每个元素由list转换为字典
                nodes = map(lambda n: {"execId": n[1],
                                        "startTime": n[0]["startTime"],
                                        "endTime": n[0]["endTime"],
                                        "status": n[0]["status"],
                                        "nodename": n[0]["id"],
                                        "exeurl": URLBuilder.buildexecutionurl(n[1]),
                                        "nodelogurl": URLBuilder.buildjoblogurl(n[1], n[0]["id"])}, nodes)
                # 对nodes进行合并
                node_state = merge_node(nodes)
                # 保存到单天execution
                one_day_exe[nodename] = node_state
            # end node loop
            days_exe[day] = one_day_exe
        return days_exe

    def gen_merge_tables(self):
        # 获取所有project name
        projects = azkaban.get_projects()
        project_flow = {}
        # 获取每个project的flow list
        for pname in projects:
            project_flow[pname] = azkaban.fetch_project_flows(pname)

        flows_executions = {}
        flow_graph = {}
        # 获取每个project下每个flow进行处理
        daily_flows_merge = {}
        for p in project_flow:
            if p in config["exclusive_projects"]:
                continue
            for f in project_flow[p]:
                flowname = p + "//" + f
                # 获取运行情况和拓扑图
                flows_executions[flowname],total = self.get_flow_executions(p, f)
                flow_graph[flowname] = azkaban.fetch_flow_graph(p, f)
                days_exe = self.merge_execution_daily(flows_executions[flowname],
                                                        flow_graph[flowname])
                # end day loop
                daily_flows_merge[flowname] = days_exe

        tables = {}
        for flowname in daily_flows_merge:
            days_exe_dict = daily_flows_merge[flowname]
            data = []
            for day in days_exe_dict:
                data.append((day, days_exe_dict[day]))
            # 按时间倒序排列
            data.sort(key=lambda e: e[0], reverse=True)
            # 表头
            head = map(lambda e: e[0], data)
            head = ["node/exec_id"] + head
            # 行数据，每一行为一个job在各天的执行情况
            rows = []
            # flow_graph[flowname] 中的node是已经过拓扑排序的
            for nodename in flow_graph[flowname]:
                cur = [(nodename, URLBuilder.buildjobinfourl(flowname.split("//")[0], nodename))]
                for exe in data:
                    cur.append(exe[1][nodename])
                rows.append(cur)
            tables[flowname] = [head, rows]
        return tables


    def gen_merge_tables_db(self, p = None, f = None):
        mongo_tables = jobinfo_db["flows_executions"]

        savedFlowExesCount = self.getSavedFlowExesCount()
        # 获取所有project name
        project_flow = {}
        if p:
            projects = [p]
            project_flow[p] = [f]
        else:
            projects = azkaban.get_projects()
            # 获取每个project的flow list
            for pname in projects:
                project_flow[pname] = azkaban.fetch_project_flows(pname)

        flows_executions = {}
        flow_graph = {}
        # 获取每个project下每个flow进行处理
        daily_flows_merge = {}
        for p in project_flow:
            if p in config["exclusive_projects"]:
                continue
            for f in project_flow[p]:
                flowname = p + "//" + f
                # 获取运行情况和拓扑图(还没有保存到db的)
                _, total = self.get_flow_executions(p, f, 1)
                save_length = savedFlowExesCount[flowname]
                length = total - save_length
                flow_graph[flowname] = azkaban.fetch_flow_graph(p, f)
                days_exe = {}
                if length > 0:
                    flows_executions[flowname], _ = self.get_flow_executions(p, f, length)
                    days_exe = self.merge_execution_daily(flows_executions[flowname],
                                                            flow_graph[flowname])

                day_length = len(days_exe)

                if day_length < 7:
                    docs = mongo_tables[flowname].find(
                        {},
                        {'_id': False},
                        sort=[('date', pymongo.DESCENDING)],
                        limit=7 - day_length)
                    for doc in docs:
                        print doc
                        days_exe[doc['date']] = doc['execution']
                # end day loop
                daily_flows_merge[flowname] = days_exe

        tables = {}
        for flowname in daily_flows_merge:
            days_exe_dict = daily_flows_merge[flowname]
            data = []
            for day in days_exe_dict:
                data.append((day, days_exe_dict[day]))
            # 按时间倒序排列
            data.sort(key=lambda e: e[0], reverse=True)
            # 表头
            head = map(lambda e: e[0], data)
            head = ["node/exec_id"] + head
            # 行数据，每一行为一个job在各天的执行情况
            rows = []
            # flow_graph[flowname] 中的node是已经过拓扑排序的
            for nodename in flow_graph[flowname]:
                cur = [(nodename, URLBuilder.buildjobinfourl(flowname.split("//")[0], nodename))]
                for exe in data:
                    cur.append(exe[1][nodename])
                rows.append(cur)
            tables[flowname] = [head, rows]
        return tables



    def gen_nodesummary(self, p = None, f = None, start_date = "-1", end_date = "2999-01-01"):
        print p,f,start_date,end_date
        headers = ["nodename","fixed","unfixed","total_down","down_time","recent_run"]
        ret = {}
        mongo_tables = jobinfo_db["flows_executions"]
        savedFlowExesCount = self.getSavedFlowExesCount()
        project_flow = {}
        if p:
            projects = [p]
            project_flow[p] = [f]
        else:
            projects = azkaban.get_projects()
            # 获取每个project的flow list
            for pname in projects:
                project_flow[pname] = azkaban.fetch_project_flows(pname)

        flows_executions = {}
        flow_graph = {}
        # 获取每个project下每个flow进行处理
        daily_flows_merge = {}
        match = "FIXED"
        for p in project_flow:
            if p in config["exclusive_projects"]:
                continue
            for f in project_flow[p]:
                flowname = p + "//" + f
                summary_rows = []
                flow_graph[flowname] = azkaban.fetch_flow_graph(p, f)
                fixed_cnt_sum = 0
                unfixed_cnt_sum = 0
                down_time_sum = datetime.timedelta()
                down_cnt_sum = 0
                for nodename in flow_graph[flowname]:
                    cursor = mongo_tables[flowname].find({"execution.{}".format(nodename) :{ "$exists": True }, "date": { "$gte": start_date, "$lte": end_date }}, limit = 1, sort=[('date',pymongo.DESCENDING)])
                    if cursor.count() > 0:
                        recent_run = cursor[0]["date"]
                    else:
                        recent_run = None
                    cursor = mongo_tables[flowname].find({"execution.{}.DOWN{}.status".format(nodename,1) :{ "$exists": True }, "date": { "$gte": start_date, "$lte": end_date }})
                    fixed_cnt = 0
                    unfixed_cnt = 0
                    down_time = datetime.timedelta()
                    down_cnt = cursor.count()
                    for doc in cursor:
                        for state in doc["execution"][nodename]:
                            if state.startswith("DOWN"):
                                # print doc["execution"][nodename][state]
                                if doc["execution"][nodename][state]["status"] == "FIXED":
                                    fixed_cnt += 1
                                    delta_time = datetime.datetime.strptime(doc["execution"][nodename][state]["delta"],"%H:%M:%S")
                                    dt = datetime.timedelta(hours=delta_time.hour, minutes=delta_time.minute, seconds=delta_time.second)
                                    down_time += dt
                                else:
                                    unfixed_cnt += 1
                    fixed_cnt_sum += fixed_cnt
                    unfixed_cnt_sum += unfixed_cnt
                    down_time_sum += down_time
                    down_cnt_sum += down_cnt
                    print "-------------"

                    print nodename,": ",fixed_cnt,unfixed_cnt,down_cnt,down_time
                    print "-------------"

                    summary_rows.append(map(str,[nodename,fixed_cnt,unfixed_cnt,down_cnt,down_time,recent_run]))
                summary_rows.append(map(str,["汇总",fixed_cnt_sum,unfixed_cnt_sum,down_cnt_sum,down_time_sum,""]))
                ret[flowname] = [headers, summary_rows]
        return ret

    def gen_failed_rate(self, p = None, f = None):
        mongo_tables = jobinfo_db["flows_executions"]
        project_flow = {}
        if p:
            projects = [p]
            project_flow[p] = [f]
        else:
            projects = azkaban.get_projects()
            # 获取每个project的flow list
            for pname in projects:
                project_flow[pname] = azkaban.fetch_project_flows(pname)

        flow_graph = {}

        ret = defaultdict(lambda : defaultdict(lambda : {}))
        for p in project_flow:
            if p in config["exclusive_projects"]:
                continue
            for f in project_flow[p]:
                flowname = p + "//" + f
                print flowname
                # flow_graph[flowname] = azkaban.fetch_flow_graph(p, f)
                summary_rows = []
                years_months = defaultdict(lambda : defaultdict(lambda : []))
                cursor = mongo_tables[flowname].find({},  sort=[('date',pymongo.DESCENDING)])
                for doc in cursor:
                    #print "-----------------"
                    #print doc
                    exe_date = datetime.datetime.strptime(doc["date"],"%Y-%m-%d")
                    year = exe_date.year
                    month = exe_date.month
                    day = exe_date.day
                    years_months[year][month].append(doc)
                    print year,month,day
                    # print doc
                    #print "-----------------"

                for year in years_months:
                    for month in years_months[year]:
                        day_cnt = 0
                        success_time = datetime.timedelta()
                        down_time = datetime.timedelta()
                        fail_time = datetime.timedelta()
                        down_cnt = 0
                        down_fixed = 0
                        down_unfixed = 0
                        for doc in years_months[year][month]:
                            day_cnt += 1
                            for nodename in doc["execution"]:
                                for state in doc["execution"][nodename]:
                                    # todo
                                    try:
                                        delta_time = datetime.datetime.strptime(doc["execution"][nodename][state]["delta"],"%H:%M:%S")
                                        dt = datetime.timedelta(hours=delta_time.hour, minutes=delta_time.minute, seconds=delta_time.second)
                                    except:
                                        print "except"
                                        dt = datetime.timedelta()
                                    if state == "SUCCEED":
                                        success_time += dt
                                    if state.startswith("FAIL"):
                                        fail_time += dt
                                    if state.startswith("DOWN"):
                                        down_cnt += 1
                                        if doc["execution"][nodename][state]["status"] == "FIXED":
                                            down_time += dt
                                            down_fixed += 1
                                        else:
                                            down_unfixed += 1
                        success_seconds = success_time.total_seconds()
                        fail_seconds = fail_time.total_seconds()
                        down_seconds = down_time.total_seconds()
                        total_seconds = success_seconds + fail_seconds
                        if total_seconds > 0:
                            success_rate = success_seconds / total_seconds * 100
                            success_rate = "%.2f%%" % success_rate
                        else:
                            success_rate = None
                        usable_rate = None
                        if day_cnt > 0:
                            total_seconds = day_cnt*24*3600
                            down_rate = down_seconds / total_seconds * 100
                            usable_rate = 100 - down_rate
                            down_rate = "%.2f%%" % down_rate
                            usable_rate = "%.2f%%" % usable_rate

                        else:
                            down_rate = None
                        ret[flowname][year][month] = map(str,[day_cnt,success_time,fail_time,down_time,success_rate,down_rate,usable_rate,down_cnt,down_fixed, down_unfixed])
        return ret

# t = JobInfo()
# ret = t.gen_failed_rate()
#
# for flowname in ret:
#     print flowname
#     for year in ret[flowname]:
#         for month in ret[flowname][year]:
#             print year,month
#             print ret[flowname][year][month]
