# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_result_df.display()

# COMMAND ----------

demo_df = race_result_df.filter("race_year=2020")
demo_df.display()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(countDistinct("race_name").alias("race_name_distinct_count")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

# select sum(points) from demo_df where driver_name = "Lewis Hamilton"

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
.withColumnRenamed("sum(points)","total_points")\
.withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
.show()

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("driver_name","race_year").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Window functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

demo_df = race_result_df.filter("race_year in (2019,2020)")
demo_df.display()

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))

demo_grouped_df.withColumn("rank",rank().over(driver_rank_spec)).show(100)

# COMMAND ----------

