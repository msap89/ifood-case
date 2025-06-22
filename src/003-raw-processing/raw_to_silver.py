# Databricks notebook source
# MAGIC %run ../../src/001-common/swiss_knife_taxi

# COMMAND ----------

dbutils.widgets.text("types", "yellow,green", "Types")
dbutils.widgets.text("months", "01,02,03,04,05", "Months")
dbutils.widgets.text("years", "2023", "Years")
dbutils.widgets.text("raw_base", "dbfs:/FileStore/raw/TLC_NYC/", "Raw_Base")
dbutils.widgets.text("silver_base", "dbfs:/FileStore/silver/TLC_NYC/", "Silver_Base")

# COMMAND ----------

types = dbutils.widgets.get("types").split(",")
months = dbutils.widgets.get("months").split(",")
years = dbutils.widgets.get("years").split(",")
raw_base = dbutils.widgets.get("raw_base")
silver_base = dbutils.widgets.get("silver_base")

raw_to_silver(types, years, months, raw_base, silver_base)