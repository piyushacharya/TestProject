from pyspark.sql import SparkSession
import uuid
from com.db.fw.etl.core.Exception.EtlExceptions import InvalidParamsException


class IOService:

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()


    def cleanup_msg(self,message):
        if message is not None:
            message = message.replace("'","")
        return message


    def store_pipeline_status(self, pipeline_name, pipeline_instance_id, status, time, error):
        tmp_uuid = str(uuid.uuid1())
        sql_stmt = "INSERT INTO  tata_poc.pipeline_status VALUES('{}', '{}', '{}','{}','{}','{}')".format(tmp_uuid,
                                                                                                           pipeline_name,
                                                                                                           pipeline_instance_id,
                                                                                                           status, time,
                                                                                                           self.cleanup_msg(str(error)))
        self.spark.sql(sql_stmt)

    def store_task_status(self,  pipeline_name, pipeline_instance_id, name, status, time, error):
        tmp_uuid =  str(uuid.uuid1())
        sql_stmt = "INSERT INTO  tata_poc.task_status VALUES('{}', '{}', '{}','{}','{}','{}','{}')".format(tmp_uuid,
                                                                                                                   pipeline_name,
                                                                                                                   pipeline_instance_id,
                                                                                                                   name,
                                                                                                                   status,
                                                                                                                   time,
                                                                                                                   self.cleanup_msg(str(error)))
        self.spark.sql(sql_stmt)

    def store_operational_stats(self, pipeline_instance_id, pipeline_name, task_name, stats ,time):
        for stat_name , state_value in stats.items():
            sql_stmt = "INSERT INTO  tata_poc.operational_stats  VALUES( '{}', '{}','{}','{}','{}','{}')".format(
                pipeline_instance_id, pipeline_name, task_name, stat_name, state_value, time)
            self.spark.sql(sql_stmt)


    def store_pipeline_metadata(self, out_pip_id, out_name, out_jsons):
        sql_stmt = "INSERT INTO  tata_poc.pipeline_metadata  VALUES('{}', '{}', '{}')".format(out_pip_id, out_name,
                                                                                              out_jsons)
        self.spark.sql(sql_stmt)

    def get_pipeline_metadata(self, pipeline_name, task_name):
        sql_stmt = "select * from tata_poc.pipeline_metadata  where pipeline_name == '{}'".format(pipeline_name)
        meta_values = self.spark.sql(sql_stmt).collect()
        if len(meta_values) <= 0:
            raise InvalidParamsException(task_name, pipeline_name, {})

        pipeline_id = meta_values[0].pipeline_id
        pipeline_name = meta_values[0].pipeline_name
        node_defination = meta_values[0].node_defination
        return pipeline_id, pipeline_name, node_defination
