# Databricks notebook source
# MAGIC %md
# MAGIC # Análise da Média do Valor Total por Mês dos Yellow Taxis
# MAGIC
# MAGIC ## Enunciado do Desafio
# MAGIC > Qual a média do valor total (total_amount) recebido em cada mês, considerando todas as corridas dos táxis amarelos (yellow taxi)?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW yellow_taxi AS
# MAGIC SELECT * FROM delta.`dbfs:/FileStore/gold/TLC_NYC/yellow/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   MONTH(pickup_datetime) AS MONTH
# MAGIC   ,ROUND(AVG(total_amount), 2) AS AVG_TOTAL_AMOUNT
# MAGIC FROM yellow_taxi
# MAGIC GROUP BY MONTH(pickup_datetime)
# MAGIC ORDER BY MONTH;