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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(),True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading nested multiline .json file

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_df.display()

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

qualifying_renamed_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

qualifying_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# qualifying_renamed_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# qualifying_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("processed.qualifying")

# overwrite_partition(qualifying_renamed_df, 'processed', 'qualifying', 'race_id')


merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

merge_delta_data(qualifying_renamed_df, 'processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")