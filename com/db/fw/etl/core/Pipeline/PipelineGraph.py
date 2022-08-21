import datetime
import logging
import uuid
import time
from datetime import date, datetime
import json
from json import JSONEncoder

from time import sleep
from threading import Thread

from com.db.fw.etl.core.Exception.EtlExceptions import PipelineException
from com.db.fw.etl.core.common.Commons import Commons

from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS as Constants
import traceback


class PipelineNodeStatus:
    NONE = "none"
    INIT = "init"
    STARTED = "started"
    FINISHED = "finished"
    ERROR = "error"
    DEAD_END = "dead_end"
    RUNNING = "running"
    EVENT_TODO = "event_todo"
    TODO = "todo"


class PipelineNode:

    def __init__(self, name, task):
        self.out_edges = []
        self.in_edges = []
        self.status = None
        self.start_time = None
        self.end_time = None
        self.name = name
        self.task = task

    def getTask(self):
        return self.task

    def setStatus(self, new_status):
        self.status = new_status

    def reprJSON(self):
        return dict(status=self.status, name=self.name)


class PipelineEdge():
    def __init__(self, edge_id, from_node, to_node):
        self.edge_id = edge_id
        self.from_node = from_node
        self.to_node = to_node

    def reprJSON(self):
        return dict(edge_id=self.edge_id,
                    from_node_name=self.from_node.name, to_node_name=self.to_node.name)


class PipelineGraph:
    def __init__(self):
        self.logger = None;
        self.nodes = {}
        self.edges = {}
        self.status = "Active"
        self.rerunPipeline = False
        self.pipeLineUid = None
        self.pipeLineName = None
        self.spark = None

    def __str__(self):
        return str(self.__dict__)

    def reprJSON(self):
        return dict(pipeLineName=self.pipeLineName,
                    pipeLineUid=self.pipeLineUid)

    def add_node(self, node):
        if node.name in self.nodes:
            node = self.nodes.get(node.name)
        else:
            self.nodes[node.name] = node
        (node)

    def all_nodes_done(self):
        status = True
        for v in self.nodes.values():
            if v.status == Constants.ERROR:
                # logger.info("Got error , Shutting down the system "+v.getTask().taskName)
                Commons.printInfoMessage(" all_nodes_done - Got error , Shutting down the system , got error is Task {} ".format(v.getTask().task_name)  )
                status = True

                raise PipelineException(
                    "Got error , Shutting down the system {} - {}".format(v.getTask().task_name, v.getTask().error_msg))

            if v.status != PipelineNodeStatus.FINISHED:
                status = False
        return status

    def add_edge(self, node1, node2):
        edge_id = str(uuid.uuid1())
        edge = PipelineEdge(edge_id, node1, node2)
        node1.out_edges.append(edge)
        node2.in_edges.append(edge)
        self.edges[edge_id] = edge

    def get_node(self, name):
        return self.nodes.get(name)

    def get_edge(self, name):
        return self.edges.get(name)

    def get_level1_node(self, status):
        tmpnodes = []
        for v in self.nodes:
            if len(v.in_edges) < 1:
                tmpnodes.append(v)
        (tmpnodes)

    # this is returning all previous nodes , irrespecgive of done or not
    def get_done_nodes(self, node):
        tmpnodes = []
        in_edges = node.in_edges
        if len(in_edges) < 1:
            return tmpnodes

        for e in in_edges:
            tmpnodes.append(e.from_node)

        return tmpnodes

    def updateNodeStatus(self, v, taskStatus):
        pass

    def get_nodes_for_execution(self):
        tmp_nodes = []

        for v in self.nodes.values():
            self.updateNodeStatus(v, v.getTask().task_status)

        for v in self.nodes.values():
            if v.status == PipelineNodeStatus.NONE:
                if len(v.in_edges) < 1:
                    tmp_nodes.append(v)

                elif self.is_all_previous_done_nodes(v) == True:
                    tmp_nodes.append(v)
        return tmp_nodes

    def marked_finished(self, job_node):
        job_node.end_time = time.local_time
        self.updateNodeStatus(job_node, PipelineNodeStatus.FINISHED)

    def marked_error(self, job_node):
        job_node.end_time = time.local_time
        self.updateNodeStatus(job_node, PipelineNodeStatus.ERROR)

    def marked_started(self, job_node):
        job_node.end_time = time.local_time
        self.updateNodeStatus(job_node, PipelineNodeStatus.STARTED)

    def updateNodeStatus(self, job_node, nodeStatus):
        job_node.status = nodeStatus

    def is_all_previous_done_nodes(self, job_node):
        in_edges = job_node.in_edges
        if len(in_edges) < 1:
            return False

        for e in in_edges:
            if e.from_node.status == PipelineNodeStatus.NONE or e.from_node.status == PipelineNodeStatus.STARTED:
                return False

            if e.from_node.status == PipelineNodeStatus.DEAD_END:
                job_node.status = PipelineNodeStatus.DEAD_END
                return False

        return True


class Pipeline:

    def __init__(self, logger, spark, pipelineGraph, name, pipid, io_service):
        self.logger = logger
        self.spark = spark
        self.pipelineGraph = pipelineGraph
        self.name = name
        self.pipelineid = pipid
        self.io_service = io_service

    def getPipelineGraph(self):
        return self.pipelineGraph

    def reprJSON(self):
        return dict(name=self.name,
                    pipelineid=self.pipelineid)

    def start(self):
        print("{} , {} ".format(self.pipelineid, str(datetime.now())))
        self.logger.info("Pipeline started with Id : {} on Dated {}".format(self.pipelineid, str(datetime.now())))

        status_time = Commons.get_curreny_time()
        self.io_service.store_pipeline_status(self.name, self.pipelineid, Constants.STARTED, status_time, None)

        run = True;
        try:

            nodes = self.pipelineGraph.get_nodes_for_execution()
            while self.pipelineGraph.all_nodes_done() == False and run:
                for n in nodes:
                    task = n.getTask()
                    self.logger.info("Task in hand " + n.name)
                    task.spark = self.spark
                    task.task_name = n.name
                    task.logger = self.logger
                    task.pipeline_name = self.name
                    task.io_service = self.io_service
                    task.pipeline_uid = self.pipelineid
                    previousNodes = self.pipelineGraph.get_done_nodes(n)
                    inputResultMap = {}
                    if len(previousNodes) > 0:
                        for pn in previousNodes:
                            if pn.getTask().output_dataframes != None:
                                inputResultMap[pn.name] = pn.getTask().output_dataframes

                            if pn.getTask().output_dataframe != None:
                                task.input_dataframe = pn.getTask().output_dataframe

                        task.inputDataframe = inputResultMap
                    task.start()
                    time.sleep(2)

                nodes = self.pipelineGraph.get_nodes_for_execution()

            status_time = Commons.get_curreny_time()
            self.io_service.store_pipeline_status(self.name, self.pipelineid, Constants.FINISHED, status_time, None)
        except Exception as e:
            status_time = Commons.get_curreny_time()
            run = False
            Commons.printErrorMessage("Exception occurred in Pipeline {} ".format(str(e)))
            Commons.printErrorMessage("Exception Source " + traceback.format_exc())
            self.io_service.store_pipeline_status(self.name, self.pipelineid, Constants.ERROR_FINISHED, status_time, e)
            raise PipelineException("Pipeline STOPPED due to Error !!!")
