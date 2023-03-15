# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Loading .json file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## step 1: defining schema

# COMMAND ----------

from pyspark.sql.functions import concat,concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType,FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(),True),
                                     StructField("grid", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(),True),
                                     StructField("fastestLap", IntegerType(), True),
                                     StructField("rank", IntegerType(), True),
                                     StructField("fastestLapTime", StringType(), True),
                                     StructField("fastestLapSpeed", FloatType(), True),
                                     StructField("statusId", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading nested .json file

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId","result_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("positionText","position_text")\
.withColumnRenamed("positionOrder","position_order")\
.withColumnRenamed("fastestLap","fastest_lap")\
.withColumnRenamed("fastestLapTime","fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
.withColumnRenamed("statusId","status_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dropping unwanted columns

# COMMAND ----------

results_final_df = results_final_df.drop("status_id")

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["driver_id","race_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("processed.results")

# overwrite_partition(results_final_df, 'processed', 'results', 'race_id')

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"

merge_delta_data(results_deduped_df, 'processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# %sql

# DROP TABLE processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, driver_id, count(1)
# MAGIC FROM processed.results
# MAGIC GROUP BY driver_id,race_id
# MAGIC HAVING count(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

