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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename",StringType(),True),StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading nested .json file

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming and adding columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

drivers_final_df = drivers_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("driverRef","driver_ref")\
.withColumn("name", concat(col("name.forename"), lit(' '), col('name.surname')))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_final_df)

# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dropping unwanted columns

# COMMAND ----------

drivers_final_df = drivers_final_df.drop("url")

# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}//drivers")

# drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("processed.drivers")

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")