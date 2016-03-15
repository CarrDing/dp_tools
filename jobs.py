#!/usr/bin/python
# -*- coding: utf-8 -*-

# 定时更新数据库，数据库中每个flow存储着目前以确定的日期，executions数，数据库中储存者
# 每天最终的merge excution(已确定)
# 每天凌晨执行
from models.Azkaban_client import azkaban
import logging
import json
from models.utils import beatify_time, jobinfo_db
import datetime
import copy
from models.get_jobinfo import JobInfo
LOG_FILE = "syncdb-log"
handler = logging.handlers.RotatingFileHandler(LOG_FILE)    # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'

formatter = logging.Formatter(fmt)                # 实例化formatter
handler.setFormatter(formatter)                   # 为handler添加formatter

logger = logging.getLogger('syncdb')     # 获取名为azkaban-monitor的logger
logger.addHandler(handler)                        # 为logger添加handler
#logger.setLevel(logging.DEBUG)
t = JobInfo()


def updatedb(p, f, running_eids, save_data_length):
    # print "-----updatedb----"
    flowname = p + "//" + f
    running_eids.sort(reverse=True)
    #print running_eids
    start_date = datetime.datetime.today().strftime("%Y-%m-%d")
    if len(running_eids) > 0:
        exe = azkaban.fetch_execution(running_eids[-1])
        start_date = beatify_time(exe["startTime"]).split()[0]
    #print start_date

    save_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    save_date_str = save_date.strftime("%Y-%m-%d")
    # print save_date_str
    exes, total = t.get_flow_executions(p, f)
    # print p, f, "： save_data_length : ",save_data_length.get(flowname, 0),"total: ", total
    if total > 0:
        # print exes
        start = 0
        end = len(exes) - 1
        if exes[end]["startDate"] >= save_date_str:
            ind = end + 1
        else:
            while start < end:
                mid = (start + end)/2
                curdate = exes[mid]["startDate"]
                if curdate < save_date_str:
                    if mid - 1 >= 0:
                        if exes[mid - 1]["startTime"] >= save_date_str:
                            end = mid
                            break
                        else:
                            end = mid - 1
                    else:
                        end = mid
                        break
                else:
                    start = mid + 1
            ind = end

        # this should merge for realtime display
        for exe in exes[0:ind]:
            pass

        # this should be merged then save into databaese

        save_length = save_data_length.get(flowname, 0)
        start = ind
        end = total - save_length
        days_exe = []
        # print "start : end",start,end
        if end > start:
            graph = azkaban.fetch_flow_graph(p, f)
            days_exe = t.merge_execution_daily(exes[start:end], graph)
            save_data_length[flowname] = total - start
        # save to mongo_tables

        mongo_tables = jobinfo_db["flows_executions"]
        for day in days_exe:
            mongo_tables[flowname].update(
                {"date": day},
                {
                    "$set": {
                        "date": day,
                        "execution": copy.copy(days_exe[day])
                    }
                }, True)




def sync_db():

    logger.info("sync_db start")
    projects = azkaban.get_projects()
    SavedFlowExesCount = t.getSavedFlowExesCount()
    for project in projects:
        flows = azkaban.fetch_project_flows(project)
        for flow in flows:
            # 获取当天正在运行的executions
            ret, ok = azkaban.fetch_running_execution(project, flow)
            running = []
            if ok:
                running = ret.get("execIds", [])
            updatedb(project, flow, running, SavedFlowExesCount)
            # 计算所有正在运行execution的最小开始时间,没有则最小开始时间为当前
    t.updateSavedFlowExesCount(SavedFlowExesCount)

sync_db()
