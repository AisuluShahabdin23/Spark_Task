from pyspark.sql import SparkSession, functions
from dotenv import load_dotenv
import os
from pyspark.sql.functions import col, when, udf
import requests
from pyspark.sql.types import DoubleType, StringType
import geohash2


spark = SparkSession.builder \
    .appName("Restaurant and Weather Data Processing") \
    .getOrCreate()

csv_file_path = r'C:/PycharmProjects/SPARK_TASK/data/restaurants_data/*.csv'

# Read CSV files
restaurants_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
count_csv = restaurants_df.count()
print(count_csv)

restaurants_df.printSchema()
restaurants_df.show()

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

count_weather = weather_df.count()
print(count_weather)

# Show the schema to check partition columns (year, month, day)
weather_df.printSchema()
weather_df.show()


# Check for null latitudes and longitudes in the restaurant dataset
restaurant_df = restaurants_df.withColumn("correct_latitude",
    when(col("lat").isNull(), True).otherwise(False)  # Use 'lat' instead of 'latitude'
).withColumn(
    "correct_longitude",
    when(col("lng").isNull(), True).otherwise(False)  # Use 'lng' instead of 'longitude'
)

# Check for invalid values
correct_restaurants_df = restaurants_df.filter((col("lat").isNotNull()) & (col("lng").isNotNull()))
incorrect_restaurants_df = restaurants_df.filter((col("lat").isNull()) | (col("lng").isNull()))

incorrect_restaurants_df.show()

# Updating coordinates via OpenCage API

# Loading variables from .env
load_dotenv()

API_KEY = os.getenv("API_KEY")


def get_coordinates(franchise_name, city, country):
    query = f"{franchise_name}, {city}, {country}"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={query}&key={API_KEY}&limit=2"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        for result in data['results']:
            category = result.get('components', {}).get('_category', '')
            if category == 'place':
                latitude = result['geometry']['lat']
                longitude = result['geometry']['lng']
                return latitude, longitude
        return None, None


# Register UDF for latitude and longitude
@udf(DoubleType())
def get_latitude_udf(franchise_name, city, country):
    return get_coordinates(franchise_name, city, country)[0]


@udf(DoubleType())
def get_longitude_udf(franchise_name, city, country):
    return get_coordinates(franchise_name, city, country)[1]


# Applying UDF to incorrect_restaurants_df
updated_restaurants_df = incorrect_restaurants_df \
    .withColumn("lat", get_latitude_udf('franchise_name', 'city', 'country')) \
    .withColumn("lng", get_longitude_udf('franchise_name', 'city', 'country'))

# Show the result
updated_restaurants_df.show()

# Merging correct and updated data
final_restaurant_df = correct_restaurants_df.union(updated_restaurants_df)
final_restaurant_df.show()

# Check for null latitudes and longitudes in the final_restaurant_df
incorrect_restaurants_df_1 = final_restaurant_df.filter((col("lat").isNull()) | (col("lng").isNull()))
incorrect_restaurants_df_1.show()


# Geohash generation
print('Geohash generation')


@udf(StringType())
def generate_geohash_udf(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)
    return None


new_final_restaurant_df = final_restaurant_df.withColumn("geohash", generate_geohash_udf(col("lat"), col("lng")))
new_weather_df = weather_df.withColumn("geohash", generate_geohash_udf(col("lat"), col("lng")))

# Rename columns lat and lng in weather dataset
new_weather_df = new_weather_df.withColumnRenamed("lat", "weather_lat")
new_weather_df = new_weather_df.withColumnRenamed("lng", "weather_lng")

# Count the number of records in the new_final_restaurant_df and show new_final_restaurant_df
count_new_final_restaurant_df = new_final_restaurant_df.count()
new_final_restaurant_df.show()
print(f'Number of records in new_final_restaurant_df =', count_new_final_restaurant_df)

# Count the number of records in the new_weather_df and show new_weather_df
count_new_weather_df = new_weather_df.count()
new_weather_df.show()
print(f'Number of records in new_weather_df =', count_new_weather_df)


# Aggregating weather data to avoid duplicates
aggregated_weather_df = new_weather_df.groupBy("geohash", "wthr_date", "year", "month", "day").agg(
    functions.round(functions.avg("avg_tmpr_f"), 2).alias("avg_avg_tmpr_f"),
    functions.round(functions.avg("avg_tmpr_c"), 2).alias("avg_avg_tmpr_c")
)

aggregated_weather_df.show()

# Count the number of records in the aggregated_weather_df
count_aggregated_weather_df = aggregated_weather_df.count()
print(f'Number of records in aggregated_weather_df =', count_aggregated_weather_df)

# Join new_final_restaurant_df with aggregated_weather_df
merged_df = new_final_restaurant_df.join(aggregated_weather_df, on="geohash", how="left")
merged_df.show()
print('new_final_restaurant_df and aggregated_weather_df joined')

# Check the number of records for 1 restaurant for 1 month (to check for duplicates)
filtered_df = merged_df.filter((col("franchise_name") == "Savoria") & (col("country") == "US") & (col("city") == "Dillon") & (col("month") == 10) & (col("year") == 2016))
filtered_df.show(50)


# Saving data in Parquet format
output_path = "C:/PycharmProjects/SPARK_TASK/result/enriched_data.parquet"
merged_df.write.mode("overwrite").parquet(output_path)

print("Data successfully saved in Parquet format with partitioning.")

# Stop the Spark session
spark.stop()
