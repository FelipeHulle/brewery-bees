import requests
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from typing import Optional, Dict, Any


class BronzeLayer:
    def __init__(self, output_dir: str = "bronze_layer") -> None:
        """
        Initializes the class to store data in the Bronze layer.

        :param output_dir: Directory where data will be stored.
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Creates the directory if it does not exist

    def save_to_file(self, data: list[Dict[str, Any]], file_name: str) -> None:
        """
        Saves the data to a JSON file.

        :param data: Data to be saved (as a list of dictionaries).
        :param file_name: File name (without extension).
        """
        file_path = os.path.join(self.output_dir, f"{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data saved to: {file_path}")
