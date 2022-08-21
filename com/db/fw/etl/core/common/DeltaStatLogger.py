from pyspark.sql import SparkSession
import uuid
from com.db.fw.etl.core.Exception.EtlExceptions import InvalidParamsException


class IOService:

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    IO_SERVICE_STATUS= True


    def cleanup_msg(self,message):
        if message is not None:
            message = message.replace("'","")
        return message


    def store_pipeline_status(self, pipeline_name, pipeline_instance_id, status, time, error):
        if IOService.IO_SERVICE_STATUS == False :
            return None;

        tmp_uuid = str(uuid.uuid1())
        sql_stmt = "INSERT INTO  tata_poc.pipeline_status VALUES('{}', '{}', '{}','{}','{}','{}')".format(tmp_uuid,
                                                                                                           pipeline_name,
                                                                                                           pipeline_instance_id,
                                                                                                           status, time,
                                                                                                           self.cleanup_msg(str(error)))
        self.spark.sql(sql_stmt)

    def store_task_status(self,  pipeline_name, pipeline_instance_id, name, status, time, error):
        if IOService.IO_SERVICE_STATUS == False :
            return None;
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
        # if IOService.IO_SERVICE_STATUS == False :
        #     return None;
        print("store_operation_stats I am here inside")
        INSERT_STMT = "INSERT INTO  tata_poc.operational_stats VALUES "
        comma_sept_for_values =""
        for stat_name , state_value in stats.items():

            values_stmt  = "( '{}', '{}','{}','{}','{}','{}') {} ".format(
                pipeline_instance_id, pipeline_name, task_name, stat_name, state_value, time,comma_sept_for_values)
            comma_sept_for_values = ","

            INSERT_STMT = INSERT_STMT + values_stmt

        self.spark.sql(INSERT_STMT)


    def store_pipeline_metadata(self, out_pip_id, out_name, out_jsons):
        if IOService.IO_SERVICE_STATUS == False :
            return None;
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
