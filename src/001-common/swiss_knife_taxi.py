# Databricks notebook source
#Função para manutenção do Delta Table

from delta.tables import DeltaTable
import datetime

def delta_maintenance(silver_path="dbfs:/FileStore/raw/TLC_NYC/"):
    print(f"Executando manutenção Delta em {datetime.datetime.now()}")

    types = [f.name.strip("/") for f in dbutils.fs.ls(silver_path) if f.isDir]

    for t in types:
        type_path = f"{silver_path}{t}/"
        years = [f.name.strip("/") for f in dbutils.fs.ls(type_path) if f.isDir]

        for year in years:
            year_path = f"{type_path}{year}/"
            months = [f.name.strip("/") for f in dbutils.fs.ls(year_path) if f.isDir]

            for month in months:
                delta_path = f"{year_path}{month}/"
                try:
                    delta_table = DeltaTable.forPath(spark, delta_path)
                    delta_table.optimize().executeCompaction()
                    print(f"OPTIMIZE executado com sucesso em {delta_path}")

                    delta_table.vacuum(168)  # 7 dias
                    print(f"VACUUM executado com sucesso em {delta_path}")
                except Exception as e:
                    print(f"Erro na manutenção em {delta_path}: {e}")

# COMMAND ----------

## Funções para downlaod dos arquivos

import os
import requests
from datetime import datetime

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
from pyspark.sql.functions import current_timestamp, date_format

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