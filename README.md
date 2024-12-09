## Restaurant and Weather Data Processing
This project processes restaurant and weather data using Apache Spark. 
The main tasks include reading CSV files and Parquet data, handling missing values,
updating coordinates using the OpenCage API, generating geohashes, and aggregating weather data. 
Finally, it joins the restaurant and weather data and saves the results in Parquet format.

# Setup
Apache Spark Setup:
Download and install Apache Spark from Spark Official Site.
Set up the Spark environment variables (SPARK_HOME, HADOOP_HOME, etc.) as per your system configuration.
Environment Variables: Create a .env file in the project root to store your API key for the OpenCage API. 
Example: API_KEY=your_api_key_here

# Data Files:
Ensure that the dataset for restaurants and weather data is available in the correct paths 
(as mentioned in the code). The restaurant data should be in CSV format, and weather data 
should be in Parquet format.

# Code Overview
A Spark session is created to process the data using pyspark.sql.SparkSession.

# Reading Data:
The restaurant data is read from CSV files using spark.read.csv().
Weather data is read from multiple Parquet directories, and all partitions are merged into 
a single DataFrame using union().

# Handling Missing Data:
The code checks for missing latitude and longitude values in the restaurant dataset.
Missing coordinates are updated by calling the OpenCage API using a requests.get() call.

# Geohash Generation:
Geohashes are generated for both the restaurant and weather datasets using the geohash2.encode() method.

# Weather Data Aggregation:
The weather data is aggregated by calculating the average temperature for each geohash and date.

# Data Merging:
The restaurant and weather data are merged based on geohash.
The final dataset is filtered for specific conditions franchise_name = Savoria and country = US 
and city = Dillon and year = 2016 and month = 10 for check number of records in new dataset.

# Saving Output:
The final merged dataset is saved in Parquet format, making it suitable for further analysis or use in downstream applications.

# How to Run
Ensure you have all the required dependencies and the .env file with the OpenCage API key.
Place the restaurant and weather data in the specified paths.

Run the script:
python script_name.py

This will process the data, merge it, and save the output in Parquet format.

# Output
The final processed data will be saved in the specified output_path as a Parquet file.
