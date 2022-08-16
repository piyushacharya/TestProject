import uuid

from com.db.fw.etl.core.Pipeline.PipelineBuilder import  *
from pyspark.sql import SparkSession
from pyspark import *
import logging
from com.db.fw.etl.core.common.DeltaStatLogger import IOService





spark = SparkSession.builder.getOrCreate()
# spark.sql("optimize tata_poc.task_status")
# spark.sql("optimize tata_poc.operational_stats")
# spark.sql("optimize tata_poc.pipeline_status")



reader = PipelineNodeBuilder()\
    .set_name("dummy_reader")\
    .set_type(PipelineNodeBuilder.DUMMY_READER)\
    .build()


processor = PipelineNodeBuilder()\
    .set_name("dummy_processor")\
    .set_type(PipelineNodeBuilder.DUMMY_PROCESSOR)\
    .build()

writer = PipelineNodeBuilder()\
    .set_name("dummy_writer")\
    .set_type(PipelineNodeBuilder.DUMMY_WRITER)\
    .build()


pipeline_name = "my_dummy_graph"
pip_id = str(uuid.uuid1())
io_service =  IOService()
logger = logging.getLogger(__name__)



pipeline =  PipelineBuilder(spark,"my_dummy_graph", logger,pip_id,io_service)\
    .add_node(reader)\
    .add_node_after(reader.name,processor)\
    .add_node_after(processor.name,writer).build()

# pip_id,name,meta_jsons = PipelineBuilder.get_json_dump(pipeline)
# print(pip_id)
# print(name)
# print(meta_jsons)
#


pipeline.start()







