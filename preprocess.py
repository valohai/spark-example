#!/usr/bin/env python3
"""
Sample Spark job in Python to demonstrate reading data from a CSV file,
performing a simple transformation, and writing the output to disk.

Usage:
    spark-submit sample_spark_script.py [input_csv_path] [output_path]
"""

import sys
from pyspark.sql import SparkSession
import valohai

def preprocess(input_path, output_path):
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("SampleSparkScript") \
        .getOrCreate()
    
    # Read CSV data
    df = spark.read.option("header", "true") \
                   .option("inferSchema", "true") \
                   .csv(input_path)
    
    # A simple transformation: group by a column and count
    grouped_df = df.groupBy("Age").count()
    
    # Show the results in the console
    grouped_df.show()
    
    # Write the results to the specified output path
    grouped_df.write.mode("overwrite").csv(output_path)

    
    # Setup a temporary table called titanic
    spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path) \
                .createOrReplaceTempView("titanic")
    
    sql_df = spark.sql("""
        SELECT 
            Survived,
            Pclass,
            COUNT(*) AS count
        FROM titanic
        GROUP BY Survived, Pclass
        ORDER BY Survived, Pclass
    """)
    
    # Show the results in the console
    sql_df.show()
    sql_df.write.mode("overwrite").csv("/valohai/outputs/sql-output")
    
    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    input_csv_path = valohai.inputs("train", "train.csv").path()
    output_path = "/valohai/outputs/processed-spark"
    preprocess(input_csv_path, output_path)
