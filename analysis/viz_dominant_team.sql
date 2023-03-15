-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC html = """<h1>Dominant team</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

USE presentation

-- COMMAND ----------

SELECT * 
FROM calculated_race_results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_team
AS
SELECT team_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points, RANK() OVER(ORDER BY avg(calculated_points) DESC) team_rank
FROM calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year,team_name,count(1) AS total_races, sum(calculated_points) AS total_points, avg(calculated_points) AS avg_points
FROM calculated_race_results
WHERE team_name in (SELECT team_name from v_dominant_team WHERE team_rank <=10)
GROUP BY race_year,team_name
ORDER BY race_year,avg_points DESC

-- COMMAND ----------

