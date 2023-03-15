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

lap_times_schema = StructType(fields=[
                                     StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(),True)])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading multiple .csv files

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_df.display()

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))


# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("processed.lap_times")

# overwrite_partition(lap_times_final_df, 'processed', 'lap_times', 'race_id')

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"

merge_delta_data(lap_times_final_df, 'processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")