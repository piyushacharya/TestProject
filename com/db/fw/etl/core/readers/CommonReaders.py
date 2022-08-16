import threading
from abc import ABC
from pyspark.sql import SparkSession

from com.db.fw.etl.core.common.Task import Task
import json



class BaseReader(Task):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)




class EvenHubsReader(BaseReader):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)

    def execute(self):
        ehConf = {}
        connectionString = self.get_option_value("YOUR.CONNECTION.STRING")
        ehConf['eventhubs.connectionString'] = self.spark.sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            connectionString)

        df = self.spark\
            .readStream\
            .format("eventhubs")\
            .options(**ehConf)\
            .load()

        self.set_output_dataframe(df)

