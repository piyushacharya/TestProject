import json

from com.db.fw.etl.core.Exception.EtlExceptions import EtlBuilderException
from com.db.fw.etl.core.Pipeline.PipelineGraph import PipelineGraph, Pipeline, PipelineNode, PipelineNodeStatus
from com.db.fw.etl.core.Dummy.DummyTasks import *



class PipelineBuilder:
    def __init__(self, spark, name, logger, pip_id,io_service):
        self.pipelineGraph = PipelineGraph()
        self.logger = logger
        self.pipeLineName = name
        self.spark = spark
        self.pipid = pip_id
        self.io_service = io_service

    def add_node(self, node):
        self.pipelineGraph.add_node(node)
        return self

    def add_node_after(self, after_node_name, node):
        self.pipelineGraph.add_node(node)
        node_after = self.pipelineGraph.get_node(after_node_name)
        self.pipelineGraph.add_edge(node_after, node)
        return self

    def build(self):
        pipeline = Pipeline(self.logger, self.spark, self.pipelineGraph, self.pipeLineName, self.pipid,self.io_service)
        return pipeline



    def get_json_dump(pipeline):
        nodes = pipeline.pipelineGraph.nodes
        pip_id = pipeline.pipelineid
        name= pipeline.name
        meta_jsons = []

        for n in nodes.values():
            node_metadata= {}

            in_edges_coll = []
            node_metadata["in_edges_coll"] = in_edges_coll
            in_edges = n.in_edges
            for edge in in_edges:
                in_edges_coll.append(edge.reprJSON())

            out_edges_coll = []
            node_metadata["out_edges_coll"] = out_edges_coll
            out_edges = n.out_edges
            for edge in out_edges:
                out_edges_coll.append(edge.reprJSON())

            node_metadata["task"] = n.task.reprJSON()
            node_metadata["node"] = n.reprJSON()

            meta_jsons.append(node_metadata)

        return (pip_id,name,meta_jsons)


    def build_pipeline(pip_id,name,jsons):
        print (jsons)







class PipelineNodeBuilder:
    DUMMY_READER = "dummy_reader"
    DUMMY_WRITER = "dummy_writer"
    DUMMY_PROCESSOR = "dummy_processor"

    EVENT_HUBS_READER = "event_hubs_reader"
    DELTA_WRITER = "delta_writer"

    def __init__(self):
        self.name = None
        self.input_options = {}
        self.out_options = {}
        self.task_output_type = None;
        self.type = None

    def set_type(self, type):
        self.type = type
        return self

    def add_input_option(self, key, value):
        self.input_options[key] = value
        return self

    def add_input_options(self, option_pairs):
        for x in option_pairs:
            self.input_options[x] = option_pairs[x]
        return self

    def add_output_option(self, key, value):
        self.output_options[key] = value
        return self

    def add_output_options(self, option_pairs):
        for x in option_pairs:
            self.output_options[x] = option_pairs[x]
        return self

    def set_name(self, name):
        self.name = name
        return self;





    def build(self):
        if self.name is None or self.type is None:
            raise EtlBuilderException(" Insufficient Param while building the node Name and Type are mandatory")

        task = None
        if self.type == PipelineNodeBuilder.DUMMY_READER:
            task = DummyReader(self.name,self.type)
        elif self.type == PipelineNodeBuilder.DUMMY_WRITER:
            task = DummyWritter(self.name,self.type)
        elif self.type == PipelineNodeBuilder.DUMMY_PROCESSOR:
            task = DummyProcessor(self.name,self.type)
        else:
            raise EtlBuilderException(" Invalid Param {}".format(str(self.type)))

        task.add_option_values(self.input_options)
        task.task_status = PipelineNodeStatus.NONE

        node = PipelineNode(self.name, task)
        return node


