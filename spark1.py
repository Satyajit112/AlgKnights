from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("SparkApp") \
    .getOrCreate()

# Example DataFrame
data = [("John", 30), ("Jane", 25), ("Alice", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()
