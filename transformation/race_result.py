# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number","driver_number")\
.withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationality")

driver_df.display()

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","team")

constructor_df.display()

# COMMAND ----------

circuit_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

circuit_df.display()

# COMMAND ----------

race_df = spark.read.format("delta").load(f"{processed_folder_path}/races")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

race_df.display()

# COMMAND ----------

result_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
.filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed("time","race_time")\
.withColumnRenamed("race_id","result_race_id")\
.withColumnRenamed("file_date","result_file_date")

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Joining circuit to race

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df,race_df.circuit_id==circuit_df.circuit_id,"inner")\
.select(race_df.race_id,race_df.race_year,race_df.race_name,race_df.race_date,circuit_df.circuit_location)

race_circuit_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Joining result to other dataframes

# COMMAND ----------

race_result_df = result_df.join(race_circuit_df,race_circuit_df.race_id == result_df.result_race_id)\
.join(driver_df, driver_df.driver_id == result_df.driver_id)\
.join(constructor_df,constructor_df.constructor_id == result_df.constructor_id)

race_result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## creating final_df from race_result_df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name",
                                 "driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position","result_file_date")\
.withColumn("created_date",current_timestamp())\
.withColumnRenamed("result_file_date","file_date")

final_df.display()

# COMMAND ----------

display(final_df.filter("race_year=2020 and race_name='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.race_results")

# overwrite_partition(final_df, 'presentation', 'race_results', 'race_id')

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"

merge_delta_data(final_df, 'presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, driver_name
# MAGIC FROM presentation.race_results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

