from pyspark.sql import SparkSession
from pyspark.sql.functions import var_samp

# initialization Spark Session
spark = SparkSession.builder \
    .appName("Max Variance City in Temperature Data") \
    .getOrCreate()

# Reading CSV files
df = spark.read.csv("D:\Задание 4\Data after 1950 GlobalLandTemperaturesByMajorCity.csv", header=True, inferSchema=True)

# Calculate the sample variance of temperature for each city
variance_df = df.groupBy("City").agg(var_samp("AverageTemperature").alias("Variance"))

# Find the city with the highest sample variance
max_variance_city = variance_df.orderBy("Variance", ascending=False).first()

# Print results
print(f"The city with the maximum temperature variance is {max_variance_city['City']} with a variance of {max_variance_city['Variance']}.")

# cloture Spark Session
spark.stop()
