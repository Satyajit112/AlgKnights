from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestSpark") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("✅ Spark Session Created Successfully")
