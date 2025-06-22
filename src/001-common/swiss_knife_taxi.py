# Databricks notebook source
#Função para manutenção do Delta Table

import os
import requests
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import date_format, current_timestamp, col, row_number

# COMMAND ----------

## Funções para downlaod dos arquivos

def download_file_to_dbfs(url, dbfs_path):
    """
    Função para baixar um arquivo de uma URL e mover para o DBFS do Databricks.
    """
    try:
        # Caminho temporário local
        tmp_local_path = "/tmp/" + os.path.basename(dbfs_path)

        # Faz o download via requests
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Escreve o arquivo localmente
        with open(tmp_local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Arquivo baixado localmente em {tmp_local_path}")

        # Limpa o prefixo "dbfs:" e move para o DBFS com dbutils
        dbutils.fs.mkdirs(os.path.dirname(dbfs_path.replace("dbfs:", "")))
        dbutils.fs.cp("file://" + tmp_local_path, dbfs_path, True)

        print(f"Arquivo movido para {dbfs_path}")
        return True

    except Exception as e:
        print(f"Erro ao baixar ou mover arquivo: {e}")
        return False

def url_exists(url):
    """
    Função para verificar se a URL está acessível
    """
    try:
        r = requests.head(url)
        return r.status_code == 200
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return False
    

def ingest_taxi_data(types, years, months, base_url="https://d37ci6vzurychx.cloudfront.net/trip-data/"):
    """
    Função para cada combinação de tipo, ano e mês, verifica e baixa os arquivos para o DBFS
    """
    for t in types:
        for year in years:
            for month in months:
                month_str = f"{int(month):02d}"
                file_name = f"{t}_tripdata_{year}-{month_str}.parquet"
                url = base_url + file_name
                print(f"Verificando URL: {url}")

                if url_exists(url):
                    start = datetime.now()
                    print(f"Iniciando download de {file_name} às {start.strftime('%Y-%m-%d %H:%M:%S')}")

                    dbfs_path = f"dbfs:/FileStore/raw/TLC_NYC/{t}/{year}/{month_str}/{file_name}"

                    success = download_file_to_dbfs(url, dbfs_path)
                    if success:
                        end = datetime.now()
                        dur = (end - start).total_seconds()
                        print(f"{file_name} baixado com sucesso em {dur:.2f} segundos.")
                    else:
                        print(f"Falha ao baixar {file_name}")
                else:
                    print(f"Arquivo não encontrado: {url}")


# COMMAND ----------

#Função para salvar em Delta camada na Silver

def raw_to_silver(types, years, months, raw_base, silver_base):
    for t in types:
        for year in years:
            for month in months:
                month_str = f"{int(month):02d}"
                raw_path = f"{raw_base}{t}/{year}/{month_str}/"
                silver_path = f"{silver_base}{t}/{year}/{month_str}/"

                try:
                    df = spark.read.parquet(raw_path)

                    #adição de coluna de controle de escrita na silver
                    df = df.withColumn("dt_ingestion_utc", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

                    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

                    print(f"Dados convertidos para Silver com sucesso: {silver_path}")

                except Exception as e:
                    print(f"Erro ao processar {raw_path}: {e}")

# COMMAND ----------

#Função para agrupar os dados dos meses em um único path separado por tipo de taxi

def silver_to_gold(types, years, months, silver_base, gold_base):
    for t in types:
        if t == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"
        elif t == "green":
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"
        else:
            pickup_col = "pickup_datetime"
            dropoff_col = "dropoff_datetime"
        
        selected_columns = ["VendorID","passenger_count","total_amount",pickup_col,dropoff_col]

        for year in years:
            for month in months:
                month_str = f"{int(month):02d}"
                silver_path = f"{silver_base}{t}/{year}/{month_str}/"
                gold_path = f"{gold_base}{t}/"

                try:
                    df = spark.read.format("delta").load(silver_path)

                    #rename para padronizar as colunas
                    if pickup_col != "pickup_datetime":
                        df = df.withColumnRenamed(pickup_col , "pickup_datetime")
                    if dropoff_col != "dropoff_datetime":
                        df = df.withColumnRenamed(dropoff_col, "dropoff_datetime")
                    
                    df_gold = (
                        df.select(
                            "VendorID",
                            "passenger_count",
                            "total_amount",
                            date_format("pickup_datetime", "yyyy-MM-dd HH:mm:ss").alias("pickup_datetime"),
                            date_format("dropoff_datetime", "yyyy-MM-dd HH:mm:ss").alias("dropoff_datetime")
                        )
                        .withColumn("dt_update_table_utc", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
                    )

                    df_gold = df_gold.filter(
                        (col("pickup_datetime") >= "2023-01-01") &
                        (col("pickup_datetime") < "2023-06-01")
                    )

                    #remoção de duplicatas para manter sempre o registro mais recente
                    window = Window.partitionBy("VendorID", "pickup_datetime", "dropoff_datetime").orderBy(col("dt_update_table_utc").desc())

                    df_gold = df_gold.withColumn("rn", row_number().over(window))
                    df_gold = df_gold.filter(col("rn") == 1).drop("rn")

                    if DeltaTable.isDeltaTable(spark, gold_path):
                        delta_table = DeltaTable.forPath(spark, gold_path)
                        (
                            delta_table.alias("target")
                            .merge(
                                df_gold.alias("source"),
                                "target.VendorID = source.VendorID AND target.pickup_datetime = source.pickup_datetime and target.dropoff_datetime = source.dropoff_datetime"
                            )
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute()
                        )
                        print(f"MERGE executado com sucesso: {gold_path}")
                    else:
                        #primeira carga na gold
                        df_gold.write.format("delta").mode("overwrite").save(gold_path)
 
                except Exception as e:
                    print(f"Erro ao processar {silver_path}: {e}")
