# Databricks notebook source
dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE presentation.calculated_race_results
# MAGIC -- USING parquet
# MAGIC -- AS
# MAGIC -- SELECT races.race_year,
# MAGIC --        constructors.name AS team_name,
# MAGIC --        drivers.name AS driver_name,
# MAGIC --        results.position,
# MAGIC --        results.points,
# MAGIC --        11 - results.position AS calculated_points
# MAGIC --   FROM results 
# MAGIC --   JOIN processed.drivers ON (results.driver_id = drivers.driver_id)
# MAGIC --   JOIN processed.constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC --   JOIN processed.races ON (results.race_id = races.race_id)
# MAGIC --   WHERE results.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * 
# MAGIC -- FROM presentation.calculated_race_results

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM processed.results
                JOIN processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM race_result_updated

# COMMAND ----------

