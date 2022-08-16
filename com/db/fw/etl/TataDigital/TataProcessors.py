from pyspark.sql.functions import from_json

from com.db.fw.etl.TataDigital.TataUtilities import get_poc_payload_schema
from com.db.fw.etl.core.common.Task import Task


class TDBaseEventParseProcessor(Task):
    def __init__(self, name, type):
        Task.__init__(self, name, type)

    def execute(self):
        schema = get_poc_payload_schema(self.spark)
        df = self.get_input_dataframe()
        df = df.selectExpr("cast(body AS String)", "enqueuedTime").withColumn("payload",from_json(df["body"], schema))
        df.select("payload.*", "body", "enqueuedTime").withColumn("dataSize", len(df["data"]))
        payloadErrorDf = df.filter("dataSize < 0")
        self.set_output_dataframe(payloadErrorDf)
