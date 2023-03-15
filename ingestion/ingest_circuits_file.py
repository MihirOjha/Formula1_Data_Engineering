# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ingest Cicuits.csv file

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
# MAGIC ## Step 1: Reading the csv file using spark dataframe reader

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.option("inferSchema",True)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

# circuits_df = spark.read\
# .option("header",True)\
# .schema(circuits_schema)\
# .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC 
# MAGIC ls /mnt/bigdatastorage10/raw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 2: Select only required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name", "location", "country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"), col("location"), col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Renaming the columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
                                          .withColumnRenamed("circuitRef","circuit_ref")\
                                          .withColumnRenamed("lat","latitude")\
                                          .withColumnRenamed("lng","longitude")\
                                          .withColumnRenamed("alt","altitude")\
                                          .withColumn("data_source",lit(v_data_source))\
                                          .withColumn("file_date",lit(v_file_date))

                                        

# COMMAND ----------

circuits_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4: Add ingestion date to dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 5: Write data to data lake in parquet format

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("processed.circuits")

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY processed.circuits

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/bigdatastorage10/processed/

# COMMAND ----------

# df = spark.read.parquet("dbfs:/mnt/bigdatastorage10/processed/circuits/")

# COMMAND ----------

# df.display()

# COMMAND ----------

dbutils.notebook.exit("Success")