import uuid

from com.db.fw.etl.TataDigital.TataProcessors import TDBaseEventParseProcessor
from com.db.fw.etl.core.Pipeline.PipelineBuilder import *
from pyspark.sql import SparkSession
from pyspark import *
import logging

from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
from com.db.fw.etl.TataDigital.TataUtilities import *

spark = SparkSession.builder.getOrCreate()

# schema = get_poc_payload_schema(spark)


df = spark.read.option("multiLine", True).json("dbfs:/tmp/td.json")
print(df.schema)

events_reader = PipelineNodeBuilder() \
    .set_name("event_reader") \
    .set_type(PipelineNodeBuilder.EVENT_HUBS_READER) \
    .add_input_option("YOUR.CONNECTION.STRING",
                      "Endpoint=sb://pvnevntdbns.servicebus.windows.net/;SharedAccessKeyName=DBManageSharedAccessKey;SharedAccessKey=LHtOrPKZR7OWnGDtUJ8krCruazpxw7E5l+EOnBs5/kE=;") \
    .add_input_option("EVENT_HUBS_NAME", "tata_event_hub") \
    .build()

base_event_parse_processor = TDBaseEventParseProcessor("base_event_parse_processor",
                                                       PipelineNodeBuilder.CUSTOM_PROCESSOR)


master_table_tcp_payment_writer = PipelineNodeBuilder.set_name("master_table_tcp_payment_writer")\
    .set_type(PipelineNodeBuilder.DELTA_WRITER)\
    .add_input_option("mode","append")\
    .add_input_option(COMMON_CONSTANTS.DB_NAME,"tata_poc")\
    .add_input_option(COMMON_CONSTANTS.TABLE_NAME,"master_table_tcp_payment")\
    .add_input_option(COMMON_CONSTANTS.CHECK_POINT_LOCATION,"/tmp/tata_poc/check_points/master")\
    .add_input_option(COMMON_CONSTANTS.TRIGGER_TIME,"5 seconds")\
    .build()




pipeline_name = "tata_poc_pipeline"
pip_id = str(uuid.uuid1())
io_service =  IOService()
logger = logging.getLogger(__name__)

pipeline =  PipelineBuilder(spark,pipeline_name, logger,pip_id,io_service)\
    .add_node(events_reader)\
    .add_node_after(events_reader.name,base_event_parse_processor)\
    .add_node_after(base_event_parse_processor.name,master_table_tcp_payment_writer).build()

