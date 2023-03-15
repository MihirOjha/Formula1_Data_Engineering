# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.filter("race_year=2019")\
.withColumnRenamed("name","race_name")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("name","circuit_name")\
.filter("circuit_id<70")

# COMMAND ----------

races_df.display()

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using inner join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"inner")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

race_circuit_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using left outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"left")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using right outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"right")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using full outer join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"full")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using semi join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"semi")

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using anti join

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df,races_df.circuit_id==circuits_df.circuit_id,"anti")

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using cross join

# COMMAND ----------

race_circuit_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

race_circuit_df.display()

# COMMAND ----------

race_circuit_df.count()

# COMMAND ----------

int(races_df.count()*circuits_df.count())

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC Select * from v_race_result where race_year = 2020

# COMMAND ----------

