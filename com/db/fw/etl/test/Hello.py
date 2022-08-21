from pyspark.sql import SparkSession
from pyspark import *

# spark = SparkSession.builder.config("spark.jars.packages","com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.22").getOrCreate()


spark = SparkSession.builder.getOrCreate()

schemaDF = spark.read.json("/FileStore/piyush/jsons/piyush.json");
check_point = None
tataFactStreamingQuery = schemaDF.writeStream.option("checkpointLocation", check_point)\
    .queryName("Tata Fact Pipeline Events")\
    .outputMode("append")\
    .trigger(processingTime='60 seconds')\
    .foreachBatch(factForEachBatchWriter)\
    .start()


