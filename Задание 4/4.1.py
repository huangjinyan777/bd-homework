from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# initialization Spark Session
spark = SparkSession.builder \
    .appName("Temperature Data Processing") \
    .getOrCreate()

# Reading CSV files
df = spark.read.csv("D:\Задание 4\GlobalLandTemperaturesByMajorCity.csv", header=True, inferSchema=True)

# Show raw data structure
df.printSchema()

# Filter out data after 1950-01-01
filtered_df = df.filter(col("dt") >= "1950-01-01")

# View Results
filtered_df.show()

# If desired, you can save a new DataFrame
# filtered_df.write.csv("FilteredTemperatureData.csv")

# cloture Spark Session
spark.stop()


