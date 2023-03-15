-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Creating raw table 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS raw

-- COMMAND ----------

USE raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Reading csv file

-- COMMAND ----------

DROP TABLE IF EXISTS circuits;

CREATE TABLE IF NOT EXISTS circuits
(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING)
USING csv 
OPTIONS (path "/mnt/bigdatastorage10/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Reading json file

-- COMMAND ----------

DROP TABLE IF EXISTS constructors;

CREATE TABLE IF NOT EXISTS constructors
(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json 
OPTIONS (path "/mnt/bigdatastorage10/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Reading nested json file

-- COMMAND ----------

DROP TABLE IF EXISTS drivers;

CREATE TABLE IF NOT EXISTS drivers
(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json 
OPTIONS (path "/mnt/bigdatastorage10/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM drivers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## reading single line json

-- COMMAND ----------

DROP TABLE IF EXISTS results;

CREATE TABLE IF NOT EXISTS results
(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json 
OPTIONS (path "/mnt/bigdatastorage10/raw/results.json")

-- COMMAND ----------

SELECT * FROM results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## reading multi line json

-- COMMAND ----------

DROP TABLE IF EXISTS pit_stops;

CREATE TABLE IF NOT EXISTS pit_stops
(driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json 
OPTIONS (path "/mnt/bigdatastorage10/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM pit_stops

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS races;

CREATE TABLE IF NOT EXISTS races
(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv 
OPTIONS (path "/mnt/bigdatastorage10/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM races

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS qualifying;

CREATE TABLE IF NOT EXISTS qualifying
(qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING)
USING json 
OPTIONS (path "/mnt/bigdatastorage10/raw/qualifying/", multiLine true)

-- COMMAND ----------

SELECT * FROM qualifying

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS lap_times;

CREATE TABLE IF NOT EXISTS lap_times
(raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT)
USING csv 
OPTIONS (path "/mnt/bigdatastorage10/raw/lap_times/", header true)

-- COMMAND ----------

SELECT * FROM lap_times

-- COMMAND ----------

