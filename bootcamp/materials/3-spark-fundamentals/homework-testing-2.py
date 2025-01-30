''' PySpark Testing Homework
----------------------------------------------------------------------------------------------------------------------------------------------

Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
Create new PySpark jobs in src/jobs for these queries
Create tests in src/tests folder with fake input and expected output data

'''

'''
Create new PySpark jobs in src/jobs for these queries
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, coalesce, split, instr, when, lit, array, array_union
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NBA Stats ETL and Player Scoring Streaks") \
    .config("spark.jars", "/path/to/postgresql.jar") \
    .getOrCreate()

# Database connection properties
db_url = "jdbc:postgresql://your_db_host:your_db_port/your_db_name"
db_properties = {
    "user": "your_db_user",
    "password": "your_db_password",
    "driver": "org.postgresql.Driver"
}

# Read tables from PostgreSQL
games = spark.read.jdbc(db_url, "games", properties=db_properties)
game_details = spark.read.jdbc(db_url, "game_details", properties=db_properties)

# Define the window for deduplication
window_spec = Window.partitionBy("game_id", "team_id", "player_id").orderBy("game_date_est")

# Join tables and apply transformations for the NBA stats ETL job
deduped = game_details.join(games, "game_id") \
    .withColumn("row_num", row_number().over(window_spec))

final_df = deduped.filter(col("row_num") == 1) \
    .select(
        col("game_date_est").alias("dim_game_date"),
        col("season").alias("dim_season"),
        col("team_id").alias("dim_team_id"),
        col("player_id").alias("dim_player_id"),
        col("player_name").alias("dim_player_name"),
        col("start_position").alias("dim_start_position"),
        (col("team_id") == col("home_team_id")).alias("dim_is_playing_at_home"),
        (coalesce(instr(col("comment"), "DNP"), 0) > 0).alias("dim_did_not_play"),
        (coalesce(instr(col("comment"), "DND"), 0) > 0).alias("dim_did_not_dress"),
        (coalesce(instr(col("comment"), "NWT"), 0) > 0).alias("dim_not_with_team"),
        (split(col("min"), ":").getItem(0).cast("double") +
         split(col("min"), ":").getItem(1).cast("double") / 60).alias("m_minutes"),
        col("fgm").alias("m_fgm"),
        col("fga").alias("m_fga"),
        col("fg3m").alias("m_fg3m"),
        col("fg3a").alias("m_fg3a"),
        col("ftm").alias("m_ftm"),
        col("fta").alias("m_fta"),
        col("oreb").alias("m_oreb"),
        col("dreb").alias("m_dreb"),
        col("reb").alias("m_reb"),
        col("ast").alias("m_ast"),
        col("stl").alias("m_stl"),
        col("blk").alias("m_blk"),
        col("TO").alias("m_turnovers"),
        col("pf").alias("m_pf"),
        col("pts").alias("m_pts"),
        col("plus_minus").alias("m_plus_minus")
    )

# Register the 'players' DataFrame as a temporary SQL table for Player Scoring Streaks
players_df = spark.read.jdbc(db_url, "players", properties=db_properties)
players_df.createOrReplaceTempView("players")

# Define the SQL query for Player Scoring Streaks
query = """
WITH with_previous AS (
    SELECT
        player_name,
        current_season,
        scoring_class,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021
),
with_indicators AS (
    SELECT *,
        CASE 
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season,
       2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, start_season, end_season;
"""

# Execute the SQL query for Player Scoring Streaks
streaks_df = spark.sql(query)

# Show sample data from both transformations
final_df.show()
streaks_df.show()

# Stop Spark session
spark.stop()
