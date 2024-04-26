-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center;">
-- MAGIC   <div style="display: flex; justify-content: center;">
-- MAGIC     <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRQHBG_JmpOhUxcbMLkJlZcrmEWNDLjJb1ufK7peaaJPPmDBiYBeAhIplBT-X3efpIAN5g&usqp=CAU" alt="IPL Data Analysis In Databricks Using SparkSQL" style="margin-bottom: 10px; width: 200px;">
-- MAGIC   </div>
-- MAGIC   <h2 style="margin-top: 0;">IPL Data Analysis In Databricks Using SparkSQL</h2>
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ball_by_ball 
(`ID` integer,
innings integer,
overs integer,
ballnumber integer,
batter string,
bowler string,
non_striker string,
extra_type string,
batsman_run integer,
extras_run integer,
total_run integer,
non_boundary integer,
isWicketDelivery integer,
player_out string,
kind string,
fielders_involved string,
BattingTeam string)

USING CSV OPTIONS (header = "true") LOCATION "dbfs:/FileStore/tables/IPL/IPL_Ball_by_Ball_2008_2022.csv"


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS matches
(
`ID` integer,
City string,
`Date` date,
Season string,
MatchNumber string,
Team1 string,
Team2 string,
Venue string,
TossWinner string,
TossDecision string,
SuperOver string,
WinningTeam string,
WonBy string,
Margin string,
method string,
Player_of_Match string,
Team1Players string,
Team2Players string,
Umpire1 string,
Umpire2 string  
)
USING CSV OPTIONS (header = "true") LOCATION "dbfs:/FileStore/tables/IPL/IPL_Matches_2008_2022.csv"

-- COMMAND ----------

-- Create a dataset joinning ball_by_ball and matches table to refer later.

CREATE OR REPLACE TEMP VIEW join_table AS 
SELECT b.*, 
m.City,
m.Date,
m.Season,
m.MatchNumber,
m.Team1,
m.Team2,
m.Venue,
m.TossWinner,
m.TossDecision,
m.SuperOver,
m.WinningTeam,
m.WonBy,
m.Margin,
m.method,
m.Player_of_Match,
m.Team1Players,
m.Team2Players,
m.Umpire1,
m.Umpire2
FROM ball_by_ball b INNER JOIN
matches m ON m.ID = b.ID


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1.Find out max and min wicket taken by a bowler in each season ?

-- COMMAND ----------

WITH max_wicket AS (
SELECT m.Season,b.bowler, count(*) AS total_wickets, 
dense_rank() OVER (PARTITION BY m.Season ORDER BY count(*) DESC) AS rank
FROM ball_by_ball b INNER JOIN 
matches m ON B.ID = M.ID
WHERE b.isWicketDelivery = 1
GROUP BY m.Season,b.bowler)

,min_wicket AS (
SELECT m.Season,b.bowler, count(*) AS total_wickets, 
dense_rank() OVER (PARTITION BY m.Season ORDER BY count(*)) AS rank
FROM ball_by_ball b INNER JOIN 
matches m ON B.ID = M.ID
WHERE b.isWicketDelivery = 1
GROUP BY m.Season,b.bowler)

-- SELECT * FROM max_wicket WHERE rank = 1;
SELECT * FROM min_wicket WHERE rank = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.Find out bowler taken wicket of same player more than once in same season or diff season?

-- COMMAND ----------

WITH CTE AS (
SELECT batter, bowler, count(*) AS no_of_occurances
FROM join_table
WHERE isWicketDelivery = 1 
GROUP BY batter, bowler
)

SELECT * FROM CTE WHERE no_of_occurances > 1  ORDER BY no_of_occurances 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3.Find out which bowler has given max and min runs in a season?

-- COMMAND ----------

WITH total_runs_given AS (
  SELECT Season, bowler, sum(total_run) AS total_runs
  FROM join_table
  GROUP BY Season, bowler
)
,max_run_given AS (
  SELECT * FROM total_runs_given ORDER BY total_runs DESC LIMIT(1)
)
,min_run_given AS (
  SELECT * FROM total_runs_given ORDER BY total_runs LIMIT(1)
)

SELECT * FROM min_run_given
UNION
SELECT * FROM max_run_given


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 4.Find out which bowler has given max boundary runs in each season?

-- COMMAND ----------

SELECT Season, bowler, sum(total_run) AS max_boundary_run 
FROM join_table WHERE non_boundary = 1
GROUP BY Season, bowler
ORDER BY sum(total_run) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 5.Find out which bowler has taken max wicket against each team?

-- COMMAND ----------

WITH CTE AS (
SELECT bowler,
-- CASE WHEN BattingTeam = Team1 THEN Team2 
-- WHEN BattingTeam = Team2 THEN Team1 END AS bowler_team,
BattingTeam,
sum(isWicketDelivery) AS no_of_times
FROM join_table WHERE isWicketDelivery = 1
GROUP BY bowler, BattingTeam
)
,CTE2 AS (
SELECT bowler,BattingTeam,no_of_times,
dense_rank() OVER (PARTITION BY BattingTeam ORDER BY no_of_times DESC) AS rn 
FROM CTE)

SELECT * FROM CTE2 WHERE rn = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 6.Find out which team went into finals most of the times?

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW team_into_finals AS (
WITH Team_into_finals AS (
  SELECT Team1 AS Team FROM matches WHERE MatchNumber = 'Final'
  UNION ALL
  SELECT Team2 AS Team FROM matches WHERE MatchNumber = 'Final'
)

SELECT Team, count(*) AS no_of_times 
FROM Team_into_finals
GROUP BY Team HAVING count(*) > 1
ORDER BY no_of_times DESC
);

SELECT * FROM team_into_finals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 7.Find out how many times teams have won IPL?

-- COMMAND ----------

SELECT WinningTeam, count(*) AS no_of_times 
FROM matches WHERE MatchNumber = 'Final'
GROUP BY WinningTeam
ORDER BY count(*) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 8.Find out the if a team wins a toss and choose to bat do they win the match also?

-- COMMAND ----------

SELECT DISTINCT Season, Team1, Team2, TossWinner, WinningTeam
FROM matches WHERE TossDecision = 'bat' AND TossWinner = WinningTeam

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 9.Find out city/venue where max and min matches where played in each season?

-- COMMAND ----------

SELECT Season, Venue, count(*) AS no_of_matches_played 
FROM matches
GROUP BY Season, Venue
ORDER BY no_of_matches_played DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 10.Find the strongest and weakest teams based on match wins?

-- COMMAND ----------

WITH Winning_team AS (
  SELECT WinningTeam, count(*) AS no_of_matches_won 
  FROM matches WHERE WinningTeam != 'NA'
  GROUP BY WinningTeam
)
SELECT * FROM Winning_team ORDER BY no_of_matches_won DESC LIMIT 5  -- For Strongest_teams

SELECT * FROM Winning_team ORDER BY no_of_matches_won LIMIT 5  -- For Weakest_teams


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 11.Find matches per season and plot them as a bar chart?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC df = spark.sql("""
-- MAGIC     SELECT Season, count(ID) AS total_matches
-- MAGIC     FROM matches
-- MAGIC     GROUP BY Season
-- MAGIC                """)
-- MAGIC
-- MAGIC pd_df = df.toPandas()
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(pd_df['Season'], pd_df['total_matches'], color='skyblue')
-- MAGIC plt.xlabel('Season')
-- MAGIC plt.ylabel('total_matches')
-- MAGIC plt.title('Matches Per Season')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 12.Visualize most final matches played by a team?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df1 = spark.sql("""SELECT * FROM team_into_finals""")
-- MAGIC
-- MAGIC result_pd = df1.toPandas()
-- MAGIC explode = (0.05,0.05,0.05,0.05,0.05,0.05)
-- MAGIC plt.figure(figsize=(4,4))
-- MAGIC plt.pie(result_pd['no_of_times'], labels=result_pd['Team'], autopct='%1.1f%%', startangle=140, explode=explode)
-- MAGIC plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
-- MAGIC plt.title('Team Played Most Finals')
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 13.Find out who hit the most boundaries?

-- COMMAND ----------

SELECT batter, count(*) AS no_of_boundaries 
FROM ball_by_ball where non_boundary = 1
GROUP BY batter
ORDER BY no_of_boundaries DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 14.Who won most man of the matches?

-- COMMAND ----------

SELECT Player_of_Match, count(*) AS no_of_times 
FROM matches
GROUP BY Player_of_Match
ORDER BY no_of_times DESC LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 15.Bowler who bowled most no of dot balls?

-- COMMAND ----------

SELECT bowler,count(ballnumber) AS no_of_dot_balls 
FROM ball_by_ball WHERE total_run = 0
GROUP BY bowler
ORDER BY no_of_dot_balls DESC
LIMIT 1
