# Databricks notebook source
# MAGIC %run ../src/002-ingestion/external_data_to_raw

# COMMAND ----------

# MAGIC %run ../src/003-raw-processing/raw_to_silver

# COMMAND ----------

# MAGIC %run ../src/004-silver-processing/silver_to_gold

# COMMAND ----------

# MAGIC %run ../analysis/001-awsers/insights_yellow_taxi

# COMMAND ----------

# MAGIC %run ../analysis/001-awsers/may_insights_hourly_passenger_count