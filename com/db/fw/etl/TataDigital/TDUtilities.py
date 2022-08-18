from pyspark.sql import SparkSession

def get_poc_payload_schema(spark):
    df = spark.read.option("multiLine", True).json("dbfs:/tmp/td.json")
    return df.schema