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

constructor_schema = "constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_df = spark.read\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dropping unwanted columns

# COMMAND ----------

constructors_df = constructors_df.drop("url")

# COMMAND ----------

constructors_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## renaming columns and adding ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp,col

# COMMAND ----------

constructors_renamed_df = constructors_df.withColumnRenamed("constructorId","constructor_id")\
.withColumnRenamed("constructorRef","constructor_ref")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

constructors_renamed_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

constructors_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Adding datframe as parquet file

# COMMAND ----------

# constructors_renamed_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# constructors_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("processed.constructors")

constructors_renamed_df.write.mode("overwrite").format("delta").saveAsTable("processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")