#%%
import requests
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count


class APIClient:
    def __init__(self, base_url, headers=None, timeout=10):
        """
        Inicializa o cliente da API.
        
        :param base_url: URL base da API.
        :param headers: Dicionário com os headers padrão para todas as requisições.
        :param timeout: Tempo limite para as requisições, em segundos.
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout

    def get(self, endpoint, params=None):
        """
        Faz uma requisição GET.
        
        :param endpoint: Endpoint da API.
        :param params: Dicionário de parâmetros da query string.
        :return: Resposta da API.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None):
        """
        Faz uma requisição POST.
        
        :param endpoint: Endpoint da API.
        :param data: Dados para enviar no corpo da requisição (formato padrão).
        :param json: Dados em formato JSON para enviar no corpo da requisição.
        :return: Resposta da API.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, headers=self.headers, data=data, json=json, timeout=self.timeout)
        return self._handle_response(response)

    def put(self, endpoint, data=None, json=None):
        """
        Faz uma requisição PUT.
        
        :param endpoint: Endpoint da API.
        :param data: Dados para enviar no corpo da requisição (formato padrão).
        :param json: Dados em formato JSON para enviar no corpo da requisição.
        :return: Resposta da API.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.put(url, headers=self.headers, data=data, json=json, timeout=self.timeout)
        return self._handle_response(response)

    def delete(self, endpoint):
        """
        Faz uma requisição DELETE.
        
        :param endpoint: Endpoint da API.
        :return: Resposta da API.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.delete(url, headers=self.headers, timeout=self.timeout)
        return self._handle_response(response)

    def _handle_response(self, response):
        """
        Lida com a resposta da API, verificando erros e retornando os dados.
        
        :param response: Objeto de resposta da API.
        :return: Conteúdo da resposta, se bem-sucedido.
        """
        try:
            response.raise_for_status()  # Levanta um erro se o status não for 2xx
        except requests.exceptions.HTTPError as e:
            return {"error": str(e), "status_code": response.status_code}
        try:
            return response.json()  # Tenta retornar a resposta como JSON
        except ValueError:
            return response.text  # Retorna como texto se não for JSON
        


class BronzeLayer:
    def __init__(self, output_dir="bronze_layer"):
        """
        Inicializa a classe para armazenar dados na camada Bronze.
        
        :param output_dir: Diretório onde os dados serão armazenados.
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Cria o diretório se ele não existir

    def save_to_file(self, data, file_name):
        """
        Salva os dados em um arquivo JSON.
        
        :param data: Dados a serem salvos (em formato de lista de dicionários).
        :param file_name: Nome do arquivo (sem extensão).
        """
        file_path = os.path.join(self.output_dir, f"{file_name}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Dados salvos em: {file_path}")

class SilverLayer:
    def __init__(self, spark, bronze_dir="bronze_layer", silver_dir="silver_layer"):
        """
        Inicializa a classe para armazenar dados na camada Silver usando PySpark.
        
        :param spark: Sessão do Spark.
        :param bronze_dir: Diretório onde os dados brutos (Bronze) estão armazenados.
        :param silver_dir: Diretório onde os dados transformados (Silver) serão armazenados.
        """
        self.spark = spark
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir

    def read_from_bronze(self, file_name):
        """
        Lê os dados da camada Bronze (Parquet ou CSV).
        
        :param file_name: Nome do arquivo a ser lido (sem extensão).
        :return: DataFrame com os dados lidos.
        """
        file_path = f"{self.bronze_dir}/{file_name}.json"
        df = self.spark.read.option("multiLine", "true").json(file_path)
        return df

    def transform_data(self, df):
        """
        Realiza transformações nos dados, como limpeza e renomeação de colunas.
        
        :param df: DataFrame que contém os dados a serem transformados.
        :return: DataFrame transformado.
        """
        # Exemplo de transformação simples: renomear colunas
        df_transformed = df.withColumnRenamed("id", "brewery_id") \
                           .withColumnRenamed("name", "brewery_name")
        return df_transformed

    def save_to_silver(self, df, file_name, partition_col="country", format="parquet"):
        """
        Salva os dados transformados na camada Silver, no formato especificado, e particiona por uma coluna.
        
        :param df: DataFrame com os dados transformados.
        :param file_name: Nome do arquivo (sem extensão).
        :param partition_col: Nome da coluna para particionamento (default é 'location').
        :param format: Formato de armazenamento (pode ser 'parquet' ou 'delta').
        """
        file_path = f"{self.silver_dir}/{file_name}"

        print(df)
        
        if format == "parquet":
            # Salvando em Parquet, particionado pela coluna de localização
            df.write.partitionBy(partition_col).parquet(file_path, mode="overwrite")
            print(f"Dados salvos na camada Silver em: {file_path} no formato Parquet.")
        
        else:
            print("Error")

class GoldLayer:
    def __init__(self, spark, silver_dir="silver_layer", gold_dir="gold_layer"):
        """
        Inicializa a classe para armazenar dados na camada Gold usando PySpark.

        :param spark: Sessão Spark.
        :param silver_dir: Diretório onde os dados da camada Silver estão armazenados.
        :param gold_dir: Diretório onde os dados agregados da camada Gold serão armazenados.
        """
        self.spark = spark
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir

    def read_from_silver(self, file_name):
        """
        Lê os dados da camada Silver (Parquet ou Delta).

        :param file_name: Nome do arquivo a ser lido da camada Silver.
        :return: DataFrame com os dados lidos.
        """
        file_path = f"{self.silver_dir}/{file_name}"
        df = self.spark.read.parquet(file_path)
        return df

    def create_aggregated_view(self, df):
        """
        Cria uma visão agregada com a quantidade de cervejarias por tipo e localização.

        :param df: DataFrame contendo os dados da camada Silver.
        :return: DataFrame agregado.
        """
        return (
            df.groupBy("brewery_type", "country")
              .agg(count("*").alias("brewery_count"))
              .orderBy("country", "brewery_type")
        )

    def save_to_gold(self, df, file_name, format="parquet"):
        """
        Salva os dados agregados na camada Gold.

        :param df: DataFrame com os dados agregados.
        :param file_name: Nome do arquivo a ser salvo.
        :param format: Formato de armazenamento (pode ser 'parquet' ou 'delta').
        """
        file_path = f"{self.gold_dir}/{file_name}"

        if format == "parquet":
            # Salvando em Parquet
            df.write.mode("overwrite").parquet(file_path)
            print(f"Dados salvos na camada Gold em: {file_path} no formato Parquet.")
        else:
            print("Formato inválido. Use 'parquet' ou 'delta'.")
#%%
# Exemplo de uso:
if __name__ == "__main__":
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
        
    print(f"Total items fetched: {len(all_data)}")
#%%

    bronze = BronzeLayer(output_dir="bronze_data")
    bronze.save_to_file(all_data, "breweries_raw")

#%%

    spark = SparkSession.builder \
        .appName("Bronze to Silver Layer with Partitioning") \
        .getOrCreate()

    # Instanciando a classe SilverLayer
    silver = SilverLayer(spark, bronze_dir="bronze_data", silver_dir="silver_data")

    # Lendo dados da camada Bronze
    bronze_data = silver.read_from_bronze("breweries_raw")

    # Realizando transformações (exemplo)
    transformed_data = silver.transform_data(bronze_data)

    # Salvando os dados na camada Silver em Parquet, particionados por 'location'
    silver.save_to_silver(transformed_data, file_name="breweries_silver", partition_col="country", format="parquet")

#%%

    gold_layer = GoldLayer(spark, silver_dir="silver_data", gold_dir="gold_data")

    # Lendo dados da camada Silver
    silver_data = gold_layer.read_from_silver("breweries_silver")

    # Criando a visão agregada
    aggregated_data = gold_layer.create_aggregated_view(silver_data)

    # Salvando os dados na camada Gold em Parquet
    gold_layer.save_to_gold(aggregated_data, file_name="aggregated_breweries", format="parquet")
#%%
    # Encerramento da sessão Spark
    spark.stop()
