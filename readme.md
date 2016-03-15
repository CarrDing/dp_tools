## estype_conflict_check


es数据库同名字段不同类型在聚合时会出现问题,必须确保同名字段同类型. 提供web服务,检查10.0.0.227上的es,检查的依据是[http://10.0.0.227:9200/_mappings](http://10.0.0.227:9200/_mappings)

## 示例

![alt text](Screenshot.png "screenshot")


## azkaban flows 天级别运行统计

外部依赖：

- azkaban backend db(mysql) 数据库访问权限
- 正在运行的azkaban server（提供ajax api 调用）
- mongodb (储存)
- crontab job.py（每天需要运行一次job.py并将结果保存到mongodb)

目前统计示例：

### flow最近7天运行

以天为单位，描述了flow每天execution的汇总情况。列为时间天，行为flow中job节点的详细情况。flow中的每个节点可能包含succeed，failed，down，recent的信息。
一个flow单天可以执行多次executions，所以涉及到executions的合并。executions的合并逻辑在 (models/utils/merge_node)[models/utils.py]

- 一个node即为一次executions里的一个job
- node status :READY SKIPPED RUNNING SUCCEEDED KILLED CANCELLED FAILED
- merge node logic:

    把同一天（同一天开始的execution进行合并）在不同executions执行的node进行合并。

    如果nodes有成功(SUCCEEDED)的节点,取最后一次的成功节点作为成功的代表节点

    如果nodes有失败(FAILED)的有节点，需要显示故障时间，故障时间计算规则如下：
    如果该失败节点后有成功的节点，则故障时间（DOWN）为该失败节点的结束时间到该节点后第一个成功节点的开始时间，否则故障时间（DOWN）直到当前

    如果既没有成功,也没有失败,则显示待定（RECENT）

example: http://10.0.2.112:9900/jobinfo/pf?project=datawarehouse&flow=datawarehouse

### flow summary

按月份总结flow的执行情况。包括如下信息

- 运行日数

    该月该flow执行的天数
- 成功时长

    flow中各天所有执行成功的节点时间累加和
- 失败时长

    flow中各天所有执行失败的节点时间累加和
- DOWN时长

    flow中各天所有Down时间累加和
- 成功时间比例

    计算方法：成功时长 / （成功时长 + 失败时长），刻画我们机器在做有效计算的比例。
- DOWN时长比例

    计算方法： DOWN时长 / 运行日数， 刻画flow在正常运行日子中的down机比例



example: http://10.0.2.112:9900/flowsummary/pf?project=datawarehouse&flow=datawarehouse

### node summary
