# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Transformation using filter

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_df.display()

# COMMAND ----------

# races_filter_df = races_df.filter("race_year=2019 and round<=5")

# races_filter_df = races_df.filter(races_df.race_year==2019)

races_filter_df = races_df.filter((races_df["race_year"]==2019) & (races_df["round"] <= 5))

# COMMAND ----------

races_filter_df.display()

# COMMAND ----------

