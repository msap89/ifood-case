# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Análise Exploratória Complementar
# MAGIC
# MAGIC ## Enunciado do Desafio
# MAGIC > Processo de análise exploratória.
# MAGIC
# MAGIC Foi adicionado consultas que são interessantes para análise de comportamento dos Yellow Táxis

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW yellow_taxi AS
# MAGIC SELECT * FROM delta.`dbfs:/FileStore/gold/TLC_NYC/yellow/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volume de corridas por mês
# MAGIC SELECT
# MAGIC   YEAR(pickup_datetime) AS ano,
# MAGIC   MONTH(pickup_datetime) AS mes,
# MAGIC   COUNT(*) AS quantidade_corridas
# MAGIC FROM yellow_taxi
# MAGIC GROUP BY ano, mes
# MAGIC ORDER BY ano, mes;

# COMMAND ----------

# MAGIC %md
# MAGIC Este gráfico mostra o comportamento mensal da demanda por táxis entre janeiro e maio de 2023, útil para identificar sazonalidade ou picos de atividade.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 corridas com os maiores valores pagos
# MAGIC SELECT
# MAGIC   pickup_datetime,
# MAGIC   dropoff_datetime,
# MAGIC   passenger_count,
# MAGIC   total_amount
# MAGIC FROM yellow_taxi
# MAGIC ORDER BY total_amount DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Essas corridas apresentam valores de total_amount muito acima da média. Possivelmente envolvem erros de entrada ou tarifas extraordinárias.