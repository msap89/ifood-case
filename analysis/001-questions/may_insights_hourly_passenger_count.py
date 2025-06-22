# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Análise da Média de Passageiros por Hora em Maio/2023
# MAGIC
# MAGIC ## Enunciado do Desafio
# MAGIC > Qual a média de passageiros por cada hora do dia no mês de maio, considerando todos os táxis da frota?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW all_taxis AS
# MAGIC SELECT * FROM delta.`dbfs:/FileStore/gold/TLC_NYC/yellow/`
# MAGIC UNION ALL
# MAGIC SELECT * FROM delta.`dbfs:/FileStore/gold/TLC_NYC/green/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     HOUR(pickup_datetime) AS HOUR
# MAGIC     ,round(AVG(passenger_count), 2) AS AVG_PASSENGERS
# MAGIC FROM all_taxis
# MAGIC WHERE MONTH(pickup_datetime) = 5
# MAGIC GROUP BY HOUR(pickup_datetime)
# MAGIC ORDER BY AVG_PASSENGERS DESC;