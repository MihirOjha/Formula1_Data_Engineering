-- Databricks notebook source
USE presentation

-- COMMAND ----------

SELECT * 
FROM calculated_race_results

-- COMMAND ----------

SELECT team_name, count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points
FROM calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

