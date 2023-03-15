# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Step 1: Loading the races.csv file

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

from pyspark.sql.functions import concat,concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) ])

# COMMAND ----------

races_df = spark.read\
.option("header",True)\
.schema(races_schema)\
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

races_df.display()

# COMMAND ----------

# Dropping url since it's not needed
# is this okay?

races_df = races_df.drop("url")

# COMMAND ----------

races_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Adding ingestion date and race timestamp to the data frame

# COMMAND ----------

races_df = add_ingestion_date(races_df)

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,col,concat,lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_with_timestamp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Renaming columns

# COMMAND ----------

races_df_renamed = races_with_timestamp_df.withColumnRenamed('raceId', 'race_id')\
                           .withColumnRenamed('circuitId', 'circuit_id')\
                           .withColumnRenamed('year', 'race_year')\
                           .withColumn("data_source",lit(v_data_source))\
                           .withColumn("file_date",lit(v_file_date))


# COMMAND ----------

races_df_renamed.display()

# COMMAND ----------

races_selected_df=races_df_renamed.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),col('name'),col('race_timestamp'),col('ingestion_date'))

# COMMAND ----------

races_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## write the output file into parquet format

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("processed.races")

races_selected_df.write.mode("overwrite").format("delta").saveAsTable("processed.races")

# COMMAND ----------

# MAGIC %fs 
# MAGIC 
# MAGIC ls mnt/bigdatastorage10/

# COMMAND ----------

dbutils.notebook.exit("Success")