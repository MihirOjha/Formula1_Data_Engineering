# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Access data frames using SQL
# MAGIC 
# MAGIC ### 1. Creating temporary or global views of dataframe
# MAGIC ### 2. Access the view from a SQL cell
# MAGIC ### 3. Access the view from a Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

race_result_df.display()

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_result")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC Select * from v_race_result where race_year = 2020

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_result_2020_df = spark.sql(f"Select * from v_race_result where race_year = {p_race_year}")

# COMMAND ----------

race_result_2020_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## global view

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC Select * from global_temp.gv_race_result where race_year = 2020

# COMMAND ----------

