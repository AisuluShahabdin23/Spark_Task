from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Restaurant Data Processing") \
    .getOrCreate()

csv_file_path = r'C:/PycharmProjects/SPARK_TASK/data/restaurants_data/*.csv'

# Read CSV files
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
# count_csv = csv_df.count()
# print(count_csv)
csv_df.show()
