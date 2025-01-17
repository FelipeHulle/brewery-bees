import requests
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql import DataFrame
from typing import Optional

class GoldLayer:
    def __init__(self, spark: SparkSession, silver_dir: str = "silver_layer", gold_dir: str = "gold_layer") -> None:
        """
        Initializes the class to store data in the Gold layer using PySpark.

        :param spark: Spark session.
        :param silver_dir: Directory where Silver layer data is stored.
        :param gold_dir: Directory where aggregated Gold layer data will be stored.
        """
        self.spark = spark
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir

    def read_from_silver(self, file_name: str) -> DataFrame:
        """
        Reads data from the Silver layer (Parquet or Delta).

        :param file_name: Name of the file to be read from the Silver layer.
        :return: DataFrame containing the data read.
        """
        file_path = f"{self.silver_dir}/{file_name}"
        df = self.spark.read.parquet(file_path)
        return df

    def create_aggregated_view(self, df: DataFrame) -> DataFrame:
        """
        Creates an aggregated view with the number of breweries by type and location.

        :param df: DataFrame containing Silver layer data.
        :return: Aggregated DataFrame.
        """
        return (
            df.groupBy("brewery_type", "country")
              .agg(count("*").alias("brewery_count"))
              .orderBy("country", "brewery_type")
        )

    def save_to_gold(self, df: DataFrame, file_name: str, format: str = "parquet") -> None:
        """
        Saves aggregated data to the Gold layer.

        :param df: DataFrame with aggregated data.
        :param file_name: Name of the file to be saved.
        :param format: Storage format (can be 'parquet' or 'delta').
        """
        file_path = f"{self.gold_dir}/{file_name}"

        if format == "parquet":
            # Saving as Parquet
            df.write.mode("overwrite").parquet(file_path)
            print(f"Data saved to the Gold layer at: {file_path} in Parquet format.")
        else:
            print("Invalid format. Use 'parquet' or 'delta'.")
