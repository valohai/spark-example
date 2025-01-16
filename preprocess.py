#!/usr/bin/env python3
"""
Sample Spark job in Python to demonstrate reading data from a CSV file,
performing a simple transformation, and writing the output to disk.

Usage:
    spark-submit sample_spark_script.py [input_csv_path] [output_path]
"""

import sys
from pyspark.sql import SparkSession

def main(input_path: str, output_path: str):
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
    
    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    # Expect two arguments: input CSV path and output path
    if len(sys.argv) != 3:
        print("Usage: spark-submit sample_spark_script.py [input_csv_path] [output_path]")
        sys.exit(-1)

    input_csv_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_csv_path, output_path)
