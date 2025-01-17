import requests
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql import DataFrame
from typing import Optional

class SilverLayer:
    def __init__(self, spark: SparkSession, bronze_dir: str = "bronze_layer", silver_dir: str = "silver_layer") -> None:
        """
        Initializes the class to store data in the Silver layer using PySpark.

        :param spark: Spark session.
        :param bronze_dir: Directory where raw data (Bronze) is stored.
        :param silver_dir: Directory where transformed data (Silver) will be stored.
        """
        self.spark = spark
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir

    def read_from_bronze(self, file_name: str) -> DataFrame:
        """
        Reads data from the Bronze layer (Parquet or CSV).

        :param file_name: Name of the file to be read (without extension).
        :return: DataFrame containing the read data.
        """
        file_path = f"{self.bronze_dir}/{file_name}.json"
        df = self.spark.read.option("multiLine", "true").json(file_path)
        return df

    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Performs data transformations such as cleaning and renaming columns.

        :param df: DataFrame containing the data to be transformed.
        :return: Transformed DataFrame.
        """
        # Example of a simple transformation: renaming columns
        df_transformed = df.withColumnRenamed("id", "brewery_id") \
                           .withColumnRenamed("name", "brewery_name")
        return df_transformed

    def save_to_silver(self, df: DataFrame, file_name: str, partition_col: str = "country", format: str = "parquet") -> None:
        """
        Saves the transformed data to the Silver layer in the specified format and partitions by a column.

        :param df: DataFrame containing the transformed data.
        :param file_name: Name of the file (without extension).
        :param partition_col: Name of the column to partition by (default is 'location').
        :param format: Storage format (can be 'parquet' or 'delta').
        """
        file_path = f"{self.silver_dir}/{file_name}"

        print(df)

        if format == "parquet":
            # Saving as Parquet, partitioned by the location column
            df.write.partitionBy(partition_col).parquet(file_path, mode="overwrite")
            print(f"Data saved to the Silver layer at: {file_path} in Parquet format.")
        else:
            print("Error")
