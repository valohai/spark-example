#!/usr/bin/env python3
"""
This Python script is a Spark job that demonstrates how to read data from a CSV file, perform basic data transformations, 
and optionally execute SQL queries.

Usage:
    python preprocess.py [input_csv_path] [output_path]
"""

import sys
from pyspark.sql import SparkSession
import valohai


def preprocess(input_path, output_path):
    # Initialize a SparkSession
    spark = SparkSession.builder.appName("SampleSparkScript").getOrCreate()

    # Read CSV data
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # A simple transformation: group by a column and count
    grouped_df = df.groupBy("Age").count()

    # Show the results in the console
    grouped_df.show()

    # Write the results to the specified output path
    grouped_df.write.mode("overwrite").csv(output_path)

    sql_query = valohai.parameters("sql", "").value
    if not (sql_query is None or sql_query == ""):
        # Setup a temporary table called titanic
        spark.read.option("header", "true").option("inferSchema", "true") \
                                            .csv(input_path) \
                                            .createOrReplaceTempView("titanic")
        sql_df = spark.sql(sql_query)
        print("Running sql query...")
        print(sql_query)
        # Show the results in the console
        sql_df.show()
        sql_df.write.mode("overwrite").csv("/valohai/outputs/sql-output")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    input_csv_path = valohai.inputs("train", "train.csv").path()
    output_path = "/valohai/outputs/processed-spark"
    preprocess(input_csv_path, output_path)
