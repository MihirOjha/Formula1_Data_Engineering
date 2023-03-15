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

pitstops_schema = StructType(fields=[
                                     StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(),True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), False)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading nested multiline .json file

# COMMAND ----------

pitstops_df = spark.read\
.schema(pitstops_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pitstops_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_final_df)

# COMMAND ----------

pitstops_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# pitstops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("processed.pit_stops")

# overwrite_partition(pitstops_final_df, 'processed', 'pit_stops', 'race_id')

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop"

merge_delta_data(pitstops_final_df, 'processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

