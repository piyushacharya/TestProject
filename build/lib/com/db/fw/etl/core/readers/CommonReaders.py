import threading
from abc import ABC
from pyspark.sql import SparkSession

from com.db.fw.etl.core.common.Task import Task
import json



class BaseReader(Task):
    def __init__(self, task_name,type):
        Task.__init__(self, task_name,type)




