from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from include.scripts import APIClient, BronzeLayer, SilverLayer, GoldLayer

def fetch_data_from_api():
    base_url = "https://api.openbrewerydb.org/breweries"
    client = APIClient(base_url)

    # Parâmetros de paginação
    per_page = 200
    page = 0
    all_data = []  # Lista para armazenar todos os resultados

    while True:
        print(f"Fetching page {page}...")
        params = {"per_page": per_page, "page": page}
        response = client.get("", params=params)

        # Verifique se há dados na resposta
        if not response or len(response) == 0:
            print("No more data to fetch.")
            break  # Sai do loop se não houver mais dados

        # Adiciona os dados retornados à lista total
        all_data.extend(response)
        page += 1  # Incrementa para a próxima página

    bronze = BronzeLayer(output_dir="bronze_layer")
    bronze.save_to_file(all_data, "breweries_raw")

def transform_bronze_to_silver():
    spark = SparkSession.builder \
        .appName("Bronze to Silver Layer with Partitioning") \
        .getOrCreate()

    silver = SilverLayer(spark, bronze_dir="bronze_data", silver_dir="silver_data")

    bronze_data = silver.read_from_bronze("breweries_raw")
    transformed_data = silver.transform_data(bronze_data)
    silver.save_to_silver(transformed_data, file_name="breweries_silver", partition_col="country", format="parquet")

    spark.stop()

def aggregate_silver_to_gold():
    spark = SparkSession.builder \
        .appName("Silver to Gold Layer Aggregation") \
        .getOrCreate()

    gold_layer = GoldLayer(spark, silver_dir="silver_data", gold_dir="gold_data")
    silver_data = gold_layer.read_from_silver("breweries_silver")
    aggregated_data = gold_layer.create_aggregated_view(silver_data)
    gold_layer.save_to_gold(aggregated_data, file_name="aggregated_breweries", format="parquet")

    spark.stop()

with DAG(
    dag_id="dag_brewery",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Ou defina um cron para agendamento
    catchup=False,
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data_from_api,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_bronze_to_silver,
    )

    aggregate_data_task = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_silver_to_gold,
    )

    fetch_data_task >> transform_data_task >> aggregate_data_task
