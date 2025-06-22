# Databricks notebook source
# MAGIC %run ../../src/001-common/swiss_knife_taxi

# COMMAND ----------

dbutils.widgets.text("types", "yellow,green", "Types")
dbutils.widgets.text("months", "01,02,03,04,05", "Months")
dbutils.widgets.text("years", "2023", "Years")

# COMMAND ----------

types = dbutils.widgets.get("types").split(",")
months = dbutils.widgets.get("months").split(",")
years = dbutils.widgets.get("years").split(",")

#chamada da função
ingest_taxi_data(types, years, months)