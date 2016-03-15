#!/usr/bin/env python
#!-*- coding: utf-8 -*-
import urllib
import json
from collections import defaultdict
import urllib

class Conflict(object):
    def get(self):
        def dfs(node, prefix, conflict_info):
            for field in node:
                if "type" in node[field]:
                    field_type = node[field][u'type']
                    conflict_info[field][0].add(field_type)
                    conflict_info[field][1][field_type].append(prefix + "." + field)
                elif "properties" in node[field]:
                    dfs(node[field]["properties"], table + "." + field, conflict_info)



        response = urllib.urlopen('http://10.0.0.227:9200/_mappings')
        mapping_data = response.read()
        mapping_json = json.loads(mapping_data)
        print 'RESPONSE:', mapping_data
        result = {}

        for index in mapping_json:
            conflict_info = defaultdict(lambda : [set(), defaultdict(lambda : [])])
            ### key fieldname, value is  [typeset, type_table_dict], type_table_dict is a dict whose key is type and value is a list of table name
            for table in mapping_json[index]["mappings"]:
                prop = mapping_json[index]["mappings"][table]["properties"]
                dfs(prop, table, conflict_info)

            result[index] = conflict_info
        conflict_list = []
        success_index = []
        fail_index = []
        for index in result:
            # print "------------------"
            # print "[" + index + "]"
            # print "confilct info:"
            if not index.startswith("."):
                err = 0
                for fieldname in result[index]:
                    #print fieldname
                    type_set = result[index][fieldname][0]
                    type_table_dict = result[index][fieldname][1]
                    if len(type_set) > 1:
                        # calculate the max length of type
                        err += 1
                        max_len = -1
                        for t in type_set:
                            if len(type_table_dict[t]) > max_len:
                                max_len = len(type_table_dict[t])

                        correct_set = set()
                        for t in type_set:
                            if len(type_table_dict[t]) == max_len:
                                correct_set.add(t)

                        conflict_list.append({"index":index,"field":fieldname,"type_list":list(type_set), "type_table":type_table_dict,"correct_types" : correct_set})
                if err > 0:
                    fail_index.append((index,err))

                else:
                    success_index.append(index)

        #print {"info":conflict_list,"success_list":success_index,"fail_list":fail_index}
        return {"info":conflict_list,"success_list":success_index,"fail_list":fail_index}
