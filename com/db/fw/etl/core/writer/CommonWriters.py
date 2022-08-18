import threading
from abc import ABC
from pyspark.sql import SparkSession
from  com.db.fw.etl.core.writer.CommonWritingUtils import *
from com.db.fw.etl.core.Exception.EtlExceptions import *

from com.db.fw.etl.core.common.Task import Task


class BaseWriter(Task):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)


class DeltaWriter(BaseWriter):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)

    def execute(self):

        mode = self.get_option_value("mode")
        inputDf = self.get_input_dataframe();


        if mode == "append":
            delta_insert(self.spark, inputDf, self.input_options,COMMON_CONSTANTS.APPEND)
        elif mode == "overwrite":
            delta_insert(self.spark, inputDf, self.input_options,COMMON_CONSTANTS.OVERWRITE)
        elif mode == "delete":
            delta_delete(self.spark, self.input_options)
        elif mode == "update":
            delta_update(self.spark, inputDf, self.input_options)
        elif mode == "merge":
            delta_merge(self.spark, inputDf, self.input_options)
        else:
            raise InsufficientParamsException(
                self.task_name, self.pipeline_name, str(self.input_options))



class ConsoleWriter(BaseWriter):
    def __init__(self, task_name, type):
        Task.__init__(self, task_name, type)

    def execute(self):
        final_df = self.get_input_dataframe()


        final_df.show(10)

            # .writeStream.format("console")\
            # .outputMode("append").start()
