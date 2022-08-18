from pyspark.sql import SparkSession
from pyspark import *

# spark = SparkSession.builder.config("spark.jars.packages","com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.22").getOrCreate()


spark = SparkSession.builder.getOrCreate()
schemaDF = spark.read.json("/FileStore/piyush/jsons/piyush.json");
schema = schemaDF.schema
schemaDF.show(10)



df = spark.readStream.schema(schema).json("/FileStore/piyush/jsons/")
wordCounts = df.groupBy("name").count()
wordCounts = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# df = spark.readStream.schema(schema).json("dbfs:/FileStore/piyush/jsons/")
# df.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint/t1").toTable("tata_poc.json_table")