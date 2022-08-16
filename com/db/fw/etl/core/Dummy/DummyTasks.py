from pyspark.sql import SparkSession

from com.db.fw.etl.core.Exception.EtlExceptions import InsufficientParamsException
from com.db.fw.etl.core.common.Task import Task
from com.db.fw.etl.core.readers.CommonReaders import BaseReader
from com.db.fw.etl.core.writer.CommonWriters import BaseWriter
from pyspark.sql.functions import lit
import json


class DummyReader(BaseReader):
    def __init__(self, name, type):
        BaseReader.__init__(self, name, type)

    def execute(self):
        df = self.spark.range(20)
        self.set_output_dataframe(df)
        print("I am in {}".format(self.task_name))

        count_stats = df.count ()
        self.add_facts("input_row_count",count_stats)




class DummyWritter(BaseWriter):
    def __init__(self, name, type):
        BaseWriter.__init__(self, name, type)

    def execute(self):
        df = self.get_input_dataframe()
        df.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable("tata_poc.dummy_table1")

        print("I am in {}".format(self.task_name))


class DummyProcessor(Task):
    def __init__(self, name, type):
        Task.__init__(self, name, type)

    def execute(self):
        print("In Dummy Process : Task Name {}".format(self.task_name))
        df = self.get_input_dataframe()

        df = df.withColumn("dummy_col", lit(0.3))

        self.set_output_dataframe(df)
        print("I am in {}".format(self.task_name))

        # fact calc
        count_stats = df.count()
        self.add_facts("process_row_count", count_stats)
