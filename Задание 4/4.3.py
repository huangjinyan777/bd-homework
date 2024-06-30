from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, col, lead, lag
from pyspark.sql.window import Window

# initialization Spark Session
spark = SparkSession.builder \
    .appName("St. Petersburg Average Temperature Analysis") \
    .getOrCreate()

# Reading CSV files
df = spark.read.csv("D:\Задание 4\Data after 1950 GlobalLandTemperaturesByMajorCity.csv", header=True, inferSchema=True)

# Filtering out St. Petersburg and extracting the year
stp_df = df.filter(col("City") == "Saint Petersburg").withColumn("Year", year(col("dt")))

# Calculate the average temperature for each year
yearly_avg_temp = stp_df.groupBy("Year").agg(avg("AverageTemperature").alias("AvgTemp"))

# Use the window function to compare average temperatures of neighboring years
windowSpec = Window.orderBy("Year")
yearly_avg_temp = yearly_avg_temp.withColumn("PrevYearTemp", lag("AvgTemp", 1).over(windowSpec))
yearly_avg_temp = yearly_avg_temp.withColumn("NextYearTemp", lead("AvgTemp", 1).over(windowSpec))

# Identify years with higher average temperatures than the previous and subsequent years
result = yearly_avg_temp.filter((col("AvgTemp") > col("PrevYearTemp")) & (col("AvgTemp") > col("NextYearTemp")))

# output result
result.select("Year").show()

# cloture Spark Session
spark.stop()
