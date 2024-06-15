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
from pyspark.sql.functions import to_date, count, col, year, month, sum, row_number
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from graphframes import *
from pyspark.sql.functions import concat_ws, col, regexp_replace

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

    #Load rideshare_data.csv 
    ride_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    clean_ride_lines = ride_lines.filter(good_ride_line)
    ride_features = clean_ride_lines.map(lambda l: tuple(l.split(',')))
    header_ride = ride_features.first()
    ride_data_RDD = ride_features.filter(lambda row: row!=header_ride)
    column_names = ["business", "pickup_location", "dropoff_location", "trip_length", "request_to_pickup", "total_ride_time", 
        "on_scene_to_pickup", "on_scene_to_dropoff", "time_of_day", "date", "passenger_fare", "driver_total_pay", "rideshare_profit", 
         "hourly_rate", "dollars_per_mile"]
    ride_df = ride_data_RDD.toDF(column_names)
  
  
    #Load taxi_zone_lookup.csv.
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

#1} Define the StructType of vertexSchema and edgeSchema.   
    
    # Schema for vertices dataframe based on the taxi zone lookup data
    vertexSchema = StructType([
        StructField("id", IntegerType(), False),  # False for nullable indicates this field cannot be null
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    # Schema for edges dataframe based on the rideshare data
    edgeSchema = StructType([
        StructField("src", IntegerType(), False),
        StructField("dst", IntegerType(), False)
    ])
    
    
#2} Construct edges dataframe, and vertices dataframe.
     
    # Creating the vertices DataFrame by selecting required columns from the taxi zone DataFrame.
    vertices = taxi_df.select(col("LocationID").alias("id"), col("Borough"), col("Zone"), col("service_zone"))
    
    # Creating the edges DataFrame by selecting the pickup and dropoff locations from the rideshare DataFrame.
    edges = ride_df.select(col("pickup_location").alias("src"), 
                           col("dropoff_location").alias("dst"))
    
    # Create a GraphFrame from the vertices and edges DataFrames.
    graph = GraphFrame(vertices, edges)
    
    # Result of 10 samples of the vertices and edges DataFrame
    graph.vertices.show(10)
    graph.edges.show(10)

# 3} Create a graph using the vertices and edges. Print 10 samples of the graph DataFrame with columns ‘src’, ‘edge’, and ‘dst’
    
    #printing the graph using the show() command on "triplets" properties which return DataFrame with columns ‘src’, ‘edge’, and ‘dst’
    graph.triplets.show(10, truncate=False)

    #distinct() method to give unique triplet combinations are displayed, removing duplicate entries and useful for analyzing unique routes without repetition in the sample data.
    graph.triplets.distinct().show(10, truncate=False)


#4) Count connected vertices with the same Borough and same service_zone. And, select 10 samples from your result. 
    
    # Finding motifs in the graph where two connected vertices belong to the same Borough and service zone.
    same_borough_service = graph.find("(a)-[e]->(b)") \
        .filter("a.Borough = b.Borough AND a.service_zone = b.service_zone")
    
    # Selecting and renaming the columns for clarity
    same_borough_service_connected_vertices = same_borough_service.select(
        col("a.id"),
        col("b.id"),
        col("a.Borough"),
        col("a.service_zone")
    )
    
    # Result of 10 samples
    same_borough_service_connected_vertices.show(10, truncate=False)

    # distinct() method to view unique connections only, ensuring the sample reflects different pairings.
    same_borough_service_connected_vertices.distinct().show(10, truncate=False)
    
    # Counting the total number of such connections
    connected_vertices_total = same_borough_service_connected_vertices.count()
    print(f"Total connected vertices with the same Borough and service zone: {connected_vertices_total}")
 


#5} Perform page ranking on the graph dataframe. 
    
    # Running the PageRank algorthm
    page_rank = graph.pageRank(resetProbability=0.17, tol=0.01).vertices.sort('pagerank', ascending=False)
    page_rank.select("id", "pagerank").show(5, truncate=False)

    
    spark.stop()