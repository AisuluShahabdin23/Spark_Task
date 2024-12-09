import unittest
from unittest.mock import patch, MagicMock
import os
import requests
import geohash2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from dotenv import load_dotenv

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


@udf(DoubleType())
def get_latitude_udf(franchise_name, city, country):
    return get_coordinates(franchise_name, city, country)[0]


@udf(DoubleType())
def get_longitude_udf(franchise_name, city, country):
    return get_coordinates(franchise_name, city, country)[1]


def generate_geohash_udf(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)
    return None


class TestRestaurantAndWeatherProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create SparkSession for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestRestaurantAndWeatherDataProcessing") \
            .master("local[2]") \
            .getOrCreate()

        cls.sample_data = [
            ("Burger King", "Los Angeles", "US", 34.052235, -118.243683),
            ("Pizza Hut", "Chicago", "US", 41.878113, -87.629799),
            ("Starbucks", "San Francisco", "US", None, None)
        ]

        cls.schema = StructType([
            StructField("franchise_name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True)
        ])

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after tests are completed."""
        cls.spark.stop()

    def test_get_coordinates(self):
        """Testing the get_coordinates function."""
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                'results': [{
                    'components': {'_category': 'place'},
                    'geometry': {'lat': 41.878113, 'lng': -87.629799}
                }]
            }
            mock_get.return_value = mock_response

            lat, lng = get_coordinates("Pizza Hut", "Chicago", "US")
            self.assertEqual(lat, 41.878113)
            self.assertEqual(lng, -87.629799)

    def test_fetch_lat_udf(self):
        """Testing the get_latitude_udf function."""
        input_data = self.spark.createDataFrame(self.sample_data, self.schema)
        result = input_data.withColumn("calculated_latitude",
                                       get_latitude_udf(col("franchise_name"), col("city"), col("country")))
        result.show()
        self.assertTrue("calculated_latitude" in result.columns)

    def test_get_longitude_udf(self):
        """Testing the get_longitude_udf function."""
        input_data = self.spark.createDataFrame(self.sample_data, self.schema)
        result = input_data.withColumn("calculated_longitude",
                                       get_longitude_udf(col("franchise_name"), col("city"), col("country")))
        result.show()
        self.assertTrue("calculated_longitude" in result.columns)

    def test_generate_geohash_udf(self):
        test_case = [
            {"lat": 40.7128, "lng": -74.0060, "expected_geohash": "dr5r"},  # New York
            {"lat": 48.8566, "lng": 2.3522, "expected_geohash": "u09t"},  # Paris
            {"lat": 34.0522, "lng": -118.2437, "expected_geohash": "9q5c"},  # Los Angeles
        ]

        for case in test_case:
            with self.subTest(lat=case["lat"], lng=case["lng"]):
                result = generate_geohash_udf(case["lat"], case["lng"])
                self.assertEqual(result, case["expected_geohash"], f"Failed for lat={case['lat']}, lng={case['lng']}")


if __name__ == "__main__":
    unittest.main()
