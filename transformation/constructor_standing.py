# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date= '{v_file_date}'")

# race_result_df.display()

# COMMAND ----------

race_year_list = df_column_to_list(race_result_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name","team")

constructor_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when, count

# COMMAND ----------

constructor_standing_df = race_result_df.groupBy("race_year","team").agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

constructor_standing_df.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

final_df = constructor_standing_df.withColumn("rank",rank().over(constructor_rank_spec))

final_df.display()

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# final_df.write.mode("overwrite").format("parquet").saveAsTable("presentation.constructor_standings")

# overwrite_partition(final_df, 'presentation', 'constructor_standings', 'race_year')

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
 
merge_delta_data(final_df, 'presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_year, team
# MAGIC FROM presentation.constructor_standings