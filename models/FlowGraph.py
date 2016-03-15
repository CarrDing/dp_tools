import copy


class Node(object):

    def __init__(self, job_id, job_type, job_parent):
        self.job_id = job_id
        self.job_type = job_type
        self.childs = []
        self.parents = set(job_parent)
        self.indegree = len(self.parents)

    def show(self, level):
        prefix = '\t'*level
        print prefix, self.job_id, self.job_type


class FlowGraph(object):
    def __init__(self, project, flow, graph_list):
        # graph_list is a list parsed from return of fetchflowgraph api in azkaban
        self.project = project
        self.flow = flow
        self.node_map = {}
        for node in graph_list:
            assert "id" in node
            assert "type" in node
            n = Node(node["id"], node["type"], node.get("in", []))
            self.node_map[n.job_id] = n
        for job_id in self.node_map:
            n = self.node_map[job_id]
            for pid in n.parents:
                self.node_map[pid].childs.append(job_id)

    def sort(self):
        node_map = copy.deepcopy(self.node_map)
        sort_nodes = []
        while len(node_map) > 0:
            for job_id in node_map.keys():
                n = node_map[job_id]
                if n.indegree == 0:
                    sort_nodes.append(job_id)
                    for c in n.childs:
                        node_map[c].indegree -= 1
                    del node_map[job_id]

        return sort_nodes
