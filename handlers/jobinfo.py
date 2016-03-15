from tornado.web import RequestHandler
from models.get_jobinfo import JobInfo
from models.Get_jobinfo_job import JobSummary
from models.stats import get_succeed, gen_success_table
import json
__all__ = (
    'JobInfoHandler',
)

                            #   <th>type</th>
                            #   <th>start</th>
                            #   <th>end</th>
                            #   <th>duration</th>
                            #   <th>exec</th>
                            #   <th>log</th>

def easy_get(d, key1, key2):
    try:
        ret = d[key1][key2]
    except:
        ret = "None"
    return ret


class JobInfoHandler(RequestHandler):
    def get(self):
        t = JobInfo()
        # tables = t.gen_merge_tables()
        p = self.get_argument("project", None)
        f = self.get_argument("flow", None)
        tables = t.gen_merge_tables_db(p,f)
        for tname in tables:
            rows = tables[tname][1]
            new_rows = []
            for r in rows:
                new_r = [r[0]]
                for node_status in r[1:]:
                    has_keys = []
                    nodes_arrays = []
                    ind = 1
                    while True:
                        k1 = "FAILED{}".format(ind)
                        k2 = "DOWN{}".format(ind)
                        if k1 in node_status and k2 in node_status:
                            has_keys.append(k1)
                            nodes_arrays.append(node_status[k1])
                            has_keys.append(k2)
                            nodes_arrays.append(node_status[k2])
                        else:
                            break
                        ind += 1
                    for k in ["SUCCEED","RECENT"]:
                        if k in node_status:
                            has_keys.append(k)
                            nodes_arrays.append(node_status[k])
                    # nodes_arrays = [node_status.get("recent_successed",{}),node_status.get("first_failed",{}),
                    #                 node_status.get("breakdown",{}), node_status.get("recent_status",{})]
                    small_table_head = [""] + has_keys
                    r1 = ["start"] + map(lambda n : n.get("startTime","None") if n else "None", nodes_arrays)
                    r2 = ["end"] + map(lambda n : n.get("endTime","None") if n else "None", nodes_arrays)
                    r3 = ["duration"] + map(lambda n : n.get("delta","None") if n else "None", nodes_arrays)
                    r4 = ["execid"] + map(lambda n : (n.get("execId","None"),n.get("exeurl","None")) if n else "None", nodes_arrays)
                    r5 = ["log"] + map(lambda n : n.get("nodelogurl","None") if n else "None", nodes_arrays)
                    r6 = ["status"] + map(lambda n : n.get("status","None") if n else "None", nodes_arrays)
                    small_table = [small_table_head, r1,r2,r3,r4,r5,r6]
                    new_r.append(small_table)
                new_rows.append(new_r)
            tables[tname][1] = new_rows
        self.render('jobinfo.html', tables=tables)

class JobSummaryHandler(RequestHandler):
    def get(self):
        t = JobInfo()
        p = self.get_argument("project", None)
        f = self.get_argument("flow", None)
        tables = t.gen_merge_tables_db(p,f)
        for tname in tables:
            rows = tables[tname][1]
            new_rows = []
            for r in rows:
                new_r = [r[0]]
                for node_status in r[1:]:
                    has_keys = []
                    nodes_arrays = []
                    ind = 1
                    while True:
                        k1 = "FAILED{}".format(ind)
                        k2 = "DOWN{}".format(ind)
                        if k1 in node_status and k2 in node_status:
                            has_keys.append(k1)
                            nodes_arrays.append(node_status[k1])
                            has_keys.append(k2)
                            nodes_arrays.append(node_status[k2])
                        else:
                            break
                        ind += 1
                    for k in ["SUCCEED","RECENT"]:
                        if k in node_status:
                            has_keys.append(k)
                            nodes_arrays.append(node_status[k])
                    # nodes_arrays = [node_status.get("recent_successed",{}),node_status.get("first_failed",{}),
                    #                 node_status.get("breakdown",{}), node_status.get("recent_status",{})]
                    small_table_head = [""] + has_keys
                    r1 = ["start"] + map(lambda n : n.get("startTime","None") if n else "None", nodes_arrays)
                    r2 = ["end"] + map(lambda n : n.get("endTime","None") if n else "None", nodes_arrays)
                    r3 = ["duration"] + map(lambda n : n.get("delta","None") if n else "None", nodes_arrays)
                    r4 = ["execid"] + map(lambda n : (n.get("execId","None"),n.get("exeurl","None")) if n else "None", nodes_arrays)
                    r5 = ["log"] + map(lambda n : n.get("nodelogurl","None") if n else "None", nodes_arrays)
                    r6 = ["status"] + map(lambda n : n.get("status","None") if n else "None", nodes_arrays)
                    small_table = [small_table_head, r1,r2,r3,r4,r5,r6]
                    new_r.append(small_table)
                new_rows.append(new_r)
            tables[tname][1] = new_rows

            # flow_succeed,flow_date_range = get_succeed()
            # summary_tables = gen_success_table(flow_succeed, flow_date_range)

        self.render('jobsummary.html', summary=tables)


class NodeSummaryHandler(RequestHandler):
    def get(self):
        t = JobInfo()
        p = self.get_argument("project", None)
        f = self.get_argument("flow", None)
        tables = t.gen_nodesummary(p, f)
        self.render('nodesummary.html', nodesummary=tables)

class FlowSummaryHandler(RequestHandler):
    def get(self):
        t = JobInfo()
        p = self.get_argument("project", None)
        f = self.get_argument("flow", None)
        ret = t.gen_failed_rate(p,f)

        self.render('flowsummary.html', flowsummary=ret)

class NodeSummaryDateRange(RequestHandler):
    def get(self):
        t = JobInfo()
        start = self.get_argument("start", None)
        end = self.get_argument("end", None)
        project = self.get_argument("project", None)
        flow = self.get_argument("flow", None)
        print start,end
        tables = t.gen_nodesummary(project, flow, start, end)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(tables))
