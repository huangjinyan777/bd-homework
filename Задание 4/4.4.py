from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, abs, to_date

# initialization Spark Session
spark = SparkSession.builder \
    .appName("City Temperature Analysis") \
    .getOrCreate()

# Reading CSV files
df = spark.read.csv("D:\Задание 4\Data after 1950 GlobalLandTemperaturesByMajorCity.csv", header=True, inferSchema=True)

# Convert a date string to a date type and extract the year and month
df = df.withColumn("Date", to_date(col("dt"), "yyyy-MM-dd")) \
       .withColumn("Year", year(col("Date"))) \
       .withColumn("Month", month(col("Date")))

# Registering a DataFrame as a SQL Queryable View
df.createOrReplaceTempView("temperature_data")

# SQL Query 1: Cities with the largest difference between the maximum and minimum annual average temperature
query1 = """
SELECT City, MAX(YearlyAvgTemp) - MIN(YearlyAvgTemp) AS MaxDiff
FROM (
    SELECT City, Year, AVG(AverageTemperature) AS YearlyAvgTemp
    FROM temperature_data
    GROUP BY City, Year
)
GROUP BY City
ORDER BY MaxDiff DESC
LIMIT 1;
"""

# SQL Query 2: Cities with the largest difference between the mean temperature in January and the mean temperature in July
query2 = """
SELECT City, ABS(JanTemp - JulTemp) AS MaxDifference
FROM (
    SELECT City,
           AVG(CASE WHEN Month = 1 THEN AverageTemperature ELSE NULL END) AS JanTemp,
           AVG(CASE WHEN Month = 7 THEN AverageTemperature ELSE NULL END) AS JulTemp
    FROM temperature_data
    GROUP BY City
)
ORDER BY MaxDifference DESC
LIMIT 1;
"""

# SQL Query 3: Cities with the highest number of months with negative average annual temperatures
query3 = """
SELECT City, COUNT(*) AS NegativeMonths
FROM (
    SELECT City, Month, AVG(AverageTemperature) AS MonthlyAvgTemp
    FROM temperature_data
    GROUP BY City, Month
    HAVING MonthlyAvgTemp < 0
)
GROUP BY City
ORDER BY NegativeMonths DESC
LIMIT 1;
"""

# Execute a SQL query and print the results
result1 = spark.sql(query1)
result2 = spark.sql(query2)
result3 = spark.sql(query3)

print("The city with the largest difference between the maximum and minimum annual average temperature of the city:")
result1.show()

print("Cities with the largest difference between the average temperature in January and the average temperature in July:")
result2.show()

print("Cities with the highest number of months with negative average temperatures throughout the year:")
result3.show()

# cloture Spark Session
spark.stop()
