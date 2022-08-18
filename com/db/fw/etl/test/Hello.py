from pyspark.sql import SparkSession
from pyspark import *


spark = SparkSession.builder.getOrCreate()


schemaDF = spark.read.json("/FileStore/piyush/jsons/piyush.json");
schema = schemaDF.schema
schemaDF.show(10)
df= spark.readStream.option("maxFilesPerTrigger", 1).schema(schema).json("/FileStore/piyush/jsons/")
df.createTempView("sample")

spark.sql("select * from sample").show(10)



# df = spark.range(100).show()
# df.select.write.save("namesAndAges.parquet", format="parquet")


