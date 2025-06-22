# Databricks notebook source
# MAGIC %run ../../src/001-common/swiss_knife_taxi

# COMMAND ----------

dbutils.widgets.text("types", "yellow,green", "Types")
dbutils.widgets.text("months", "01,02,03,04,05", "Months")
dbutils.widgets.text("years", "2023", "Years")
dbutils.widgets.text("silver_base", "dbfs:/FileStore/silver/TLC_NYC/", "Silver_Base")
dbutils.widgets.text("gold_base", "dbfs:/FileStore/gold/TLC_NYC/", "Gold_Base")

# COMMAND ----------

types = dbutils.widgets.get("types").split(",")
months = dbutils.widgets.get("months").split(",")
years = dbutils.widgets.get("years").split(",")
silver_base = dbutils.widgets.get("silver_base")
gold_base = dbutils.widgets.get("gold_base")

silver_to_gold(types, years, months, silver_base, gold_base)