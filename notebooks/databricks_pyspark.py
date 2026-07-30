# Databricks notebook source
# MAGIC %md
# MAGIC # CantuStore - Carrinhos abandonados com PySpark

# COMMAND ----------

dbutils.widgets.text("data_dir", "dbfs:/FileStore/cantu/doc")
dbutils.widgets.text("output_dir", "dbfs:/FileStore/cantu/output")
dbutils.widgets.text("top_limit", "50")

data_dir = dbutils.widgets.get("data_dir")
output_dir = dbutils.widgets.get("output_dir")
top_limit = int(dbutils.widgets.get("top_limit"))

# COMMAND ----------

from cantu_abandoned_carts import spark_pipeline

spark_pipeline.run(spark, data_dir=data_dir, output_dir=output_dir, top_limit=top_limit)

print(f"Relatorios gerados em {output_dir}")
