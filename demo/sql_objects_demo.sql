-- Databricks notebook source
create database demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

USE demo

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Managed table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC 
-- MAGIC race_result_df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_result_df.write.format("parquet").saveAsTable("demo.race_result_py")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_result_py

-- COMMAND ----------

SELECT * FROM race_result_py WHERE race_year=2020

-- COMMAND ----------

CREATE TABLE race_result_sql AS SELECT * FROM race_result_py WHERE race_year=2020

-- COMMAND ----------

DESC EXTENDED race_result_sql

-- COMMAND ----------

DROP TABLE race_result_sql

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## External table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC race_result_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results").saveAsTable("race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED race_results_ext_py

-- COMMAND ----------

CREATE TABLE race_result_ext_sql 
(race_year int, race_name string, race_date timestamp, circuit_location string, driver_name string,
driver_number int, driver_nationality string,team string,grid int,fastest_lap int, race_time string,
points float, position int, created_date timestamp)
USING parquet
LOCATION "/mnt/bigdatastorage10/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

INSERT INTO race_result_ext_sql 
SELECT * FROM race_results_ext_py WHERE race_year = 2020

-- COMMAND ----------

SELECT COUNT(*) FROM race_result_ext_sql

-- COMMAND ----------

DESC EXTENDED race_result_ext_sql

-- COMMAND ----------

DROP TABLE race_result_ext_sql

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Views on table

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_result
AS 
SELECT * FROM race_result_py
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_race_result

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_result
AS 
SELECT * FROM race_result_py
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_result

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_result
AS 
SELECT * FROM race_result_py
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM pv_race_result

-- COMMAND ----------

