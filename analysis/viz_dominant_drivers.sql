-- Databricks notebook source
USE presentation

-- COMMAND ----------

SELECT * 
FROM calculated_race_results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points, RANK() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
FROM calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year,driver_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points
FROM calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_drivers WHERE driver_rank <=10)
GROUP BY race_year,driver_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

