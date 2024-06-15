import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, year, month, sum, row_number, avg, dayofmonth, regexp_replace
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from graphframes import *
from pyspark.sql.functions import concat_ws, col

#SPARK Configuration
if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("NYC")\
        .getOrCreate()
    
    def good_ride_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            #int(fields[0])
            return True
        except:
            return False
            
    def good_taxi_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=4:
                return False
            #int(fields[0])
            return True
        except:
            return False
  
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
# Load rideshare_data.csv 
    ride_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    clean_ride_lines = ride_lines.filter(good_ride_line)
    ride_features = clean_ride_lines.map(lambda l: tuple(l.split(',')))
    header_ride = ride_features.first()
    ride_data_RDD = ride_features.filter(lambda row: row!=header_ride)
    column_names = ["business", "pickup_location", "dropoff_location", "trip_length", "request_to_pickup", "total_ride_time", 
        "on_scene_to_pickup", "on_scene_to_dropoff", "time_of_day", "date", "passenger_fare", "driver_total_pay", "rideshare_profit", 
         "hourly_rate", "dollars_per_mile"]
    ride_df = ride_data_RDD.toDF(column_names)

# Load taxi_zone_lookup.csv.
    taxi_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")
    clean_taxi_lines = taxi_lines.filter(good_taxi_line)
    taxi_features = clean_taxi_lines.map(lambda l: tuple(l.split(',')))
    header_taxi = taxi_features.first()
    taxi_data_RDD = taxi_features.filter(lambda row: row!=header_taxi)
    column_names_taxi = ["LocationID", "Borough", "Zone", "service_zone"]
    taxi_df = taxi_data_RDD.toDF(column_names_taxi)

    # Removing double quotes from the taxi zone look up table values
    for column in column_names_taxi:
        taxi_df = taxi_df.withColumn(column, regexp_replace(col(column), '\"', ''))

# Joining the datasets
    # Performing join on pickup_location and LocationID
    joined_df1 = ride_df.join(taxi_df, ride_df.pickup_location == taxi_df.LocationID)
    joined_df_final = joined_df1.withColumnRenamed("Borough", "Pickup_Borough")\
                                 .withColumnRenamed("Zone", "Pickup_Zone")\
                                 .withColumnRenamed("service_zone", "Pickup_service_zone")\
                                 .drop("LocationID")

    # Performing join on dropoff_location and LocationID
    joined_df2 = joined_df_final.join(taxi_df, joined_df_final.dropoff_location == taxi_df.LocationID)
    nyc_df = joined_df2.withColumnRenamed("Borough", "Dropoff_Borough")\
                                 .withColumnRenamed("Zone", "Dropoff_Zone")\
                                 .withColumnRenamed("service_zone", "Dropoff_service_zone")\
                                 .drop("LocationID")
    
    #convert the UNIX timestamp to the "yyyy-MM-dd" format
    nyc_df= nyc_df.withColumn("date", date_format(from_unixtime("date"), "yyyy-MM-dd"))

    # Make sure, the format of date is in "date" data type
    nyc_df = nyc_df.withColumn("date", to_date(nyc_df.date, "yyyy-MM-dd"))
    
    # Extract month from the date    
    nyc_df1 = nyc_df.withColumn("month", month("date"))

# TASK 6 - Filtering Data

#1} Finding trip counts greater than 0 and less than 1000 for different 'Pickup_Borough' at different 'time_of_day'
    
    # Grouping by 'Pickup_Borough' and 'time_of_day' and count the trips
    borough_time_of_day_trip_counts = nyc_df1 .groupBy("Pickup_Borough", "time_of_day") \
                                    .agg(count("*").alias("trip_count"))

    # Filter for trip counts greater than 0 and less than 1000
    filtered_trip_counts = borough_time_of_day_trip_counts \
                           .filter((col("trip_count") > 0) & (col("trip_count") < 1000))
    
    # result
    filtered_trip_counts.show(truncate=False)

#2} Calculate the number of trips for each 'Pickup_Borough' in the evening time

     # Filter for trip counts where time of day is evening
    filtered_evening_trip_counts = borough_time_of_day_trip_counts \
                                    .filter(col("time_of_day")== "evening")
    # result
    filtered_evening_trip_counts.show(truncate=False)

#3} The number of trips that started in Brooklyn (Pickup_Borough field) and ended in Staten Island (Dropoff_Borough field)

    # Filtering the data based on brooklyn pickup and staten island dropoff
    selected_trips_df =nyc_df1.filter((col("Pickup_Borough")=="Brooklyn") & (col("Dropoff_Borough") == "Staten Island"))

    # Selecting the required columns after filtering for pick up zone
    selected_trips_df_final = selected_trips_df.select("Pickup_Borough","Dropoff_Borough","Pickup_Zone")
                        
    # result
    selected_trips_df_final.show(10)

    # Number of trips count from Brooklyn to Staten Island
    print("Count of trips from Brooklyn to Staten Island: ", selected_trips_df_final.count())

    spark.stop()


    

