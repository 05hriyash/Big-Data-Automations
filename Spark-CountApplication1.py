
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

file_path = "constitution.txt"

baseRDD =sc.textFile(file_path).flatMap(lambda x :x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
print(baseRDD.take(20))
