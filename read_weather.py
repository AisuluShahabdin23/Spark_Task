from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Weather Data Processing") \
    .getOrCreate()

# List of all weather data paths
weather_data_paths = [
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/1_October_1-4/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/2_October_5-8/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/3_October_9-12/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/4_October_13-16/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/5_October_17-20/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/6_October_21-24/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/7_October_25-28/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/8_October_29-31/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/9_August_1-4/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/10_August_5-8/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/11_August_9-12/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/12_August_13-16/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/13_August_17-20/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/14_August_21-24/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/15_August_25-28/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/16_August_29-31/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/17_September_1-5/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/18_September_6-10/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/19_September_11-15/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/20_September_16-20/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/21_September_21-25/weather/",
    r"C:/PycharmProjects/SPARK_TASK/data/weather_data/22_September_26-30/weather/"
]

# Initialize an empty DataFrame to hold the weather data
weather_df = None

# Iterate through all paths and read each directory separately
for weather_data_path in weather_data_paths:
    # Read each partitioned Parquet directory with basePath
    df = spark.read.option("basePath", r"C:/PycharmProjects/SPARK_TASK/data/weather_data/") \
        .parquet(weather_data_path)

    # Union the data with the existing DataFrame
    if weather_df is None:
        weather_df = df
    else:
        weather_df = weather_df.union(df)

# Show the schema to check partition columns (year, month, day)
weather_df.printSchema()

# Example: Filter for weather data from October 1st, 2024
weather_df_filtered = weather_df.filter(
    (weather_df.year == 2016) & (weather_df.month == 10) & (weather_df.day == 1)
)

count_parquet = weather_df.count()
print(count_parquet)

# Show the filtered data
weather_df_filtered.show()

# Stop the Spark session
spark.stop()
