''' PySpark Testing Homework
----------------------------------------------------------------------------------------------------------------------------------------------

Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
Create new PySpark jobs in src/jobs for these queries
Create tests in src/tests folder with fake input and expected output data

'''

'''
Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
'''

SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    CASE 
        WHEN y.season_stats IS NULL THEN ARRAY(STRUCT(t.season, t.gp, t.pts, t.reb, t.ast))
        WHEN t.season IS NOT NULL THEN CONCAT(y.season_stats, ARRAY(STRUCT(t.season, t.gp, t.pts, t.reb, t.ast)))
        ELSE y.season_stats
    END AS season_stats,
    CASE 
        WHEN t.season IS NOT NULL THEN 
            CASE 
                WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
            END
        ELSE y.scoring_class
    END AS scoring_class,
    CASE 
        WHEN t.season IS NOT NULL THEN 0
        ELSE y.years_since_last_season + 1
    END AS years_since_last_season,
    COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name
;

WITH deduped AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id 
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(INSTR(comment, 'DNP'), 0) > 0 AS dim_did_not_play,
    COALESCE(INSTR(comment, 'DND'), 0) > 0 AS dim_did_not_dress,
    COALESCE(INSTR(comment, 'NWT'), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT(min, ':')[0] AS DOUBLE) + CAST(SPLIT(min, ':')[1] AS DOUBLE) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    `TO` AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1
;

'''
Create new PySpark jobs in src/jobs for these queries
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, array_union, when, lit, split, expr, instr, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("SparkSQLJobs").getOrCreate()

# Load data into DataFrames
today_df = spark.read.parquet("path/to/today.parquet")
yesterday_df = spark.read.parquet("path/to/yesterday.parquet")

game_details_df = spark.read.parquet("path/to/game_details.parquet")
games_df = spark.read.parquet("path/to/games.parquet")

# Query 1: Player Stats Aggregation
player_stats_df = today_df.alias("t").join(
    yesterday_df.alias("y"),
    col("t.player_name") == col("y.player_name"),
    "full_outer"
).select(
    coalesce(col("t.player_name"), col("y.player_name")).alias("player_name"),
    coalesce(col("t.height"), col("y.height")).alias("height"),
    coalesce(col("t.college"), col("y.college")).alias("college"),
    coalesce(col("t.country"), col("y.country")).alias("country"),
    coalesce(col("t.draft_year"), col("y.draft_year")).alias("draft_year"),
    coalesce(col("t.draft_round"), col("y.draft_round")).alias("draft_round"),
    coalesce(col("t.draft_number"), col("y.draft_number")).alias("draft_number"),
    when(col("y.season_stats").isNull(), array([col("t.season"), col("t.gp"), col("t.pts"), col("t.reb"), col("t.ast")]))
        .when(col("t.season").isNotNull(), array_union(col("y.season_stats"), array([col("t.season"), col("t.gp"), col("t.pts"), col("t.reb"), col("t.ast")]))).otherwise(col("y.season_stats")).alias("season_stats"),
    when(col("t.season").isNotNull(),
         when(col("t.pts") > 20, lit("star"))
        .when(col("t.pts") > 15, lit("good"))
        .when(col("t.pts") > 10, lit("average"))
        .otherwise(lit("bad")))
    .otherwise(col("y.scoring_class")).alias("scoring_class"),
    when(col("t.season").isNotNull(), lit(0))
    .otherwise(col("y.years_since_last_season") + 1).alias("years_since_last_season"),
    coalesce(col("t.season"), col("y.current_season") + 1).alias("current_season")
)

# Query 2: Game Details Aggregation
window_spec = Window.partitionBy("gd.game_id", "gd.team_id", "gd.player_id").orderBy("g.game_date_est")

deduped_df = game_details_df.alias("gd").join(
    games_df.alias("g"),
    col("gd.game_id") == col("g.game_id"),
    "inner"
).withColumn("row_num", row_number().over(window_spec))

final_game_details_df = deduped_df.filter(col("row_num") == 1).select(
    col("g.game_date_est").alias("dim_game_date"),
    col("g.season").alias("dim_season"),
    col("gd.team_id").alias("dim_team_id"),
    col("gd.player_id").alias("dim_player_id"),
    col("gd.player_name").alias("dim_player_name"),
    col("gd.start_position").alias("dim_start_position"),
    (col("gd.team_id") == col("g.home_team_id")).alias("dim_is_playing_at_home"),
    (instr(col("gd.comment"), "DNP") > 0).alias("dim_did_not_play"),
    (instr(col("gd.comment"), "DND") > 0).alias("dim_did_not_dress"),
    (instr(col("gd.comment"), "NWT") > 0).alias("dim_not_with_team"),
    (split(col("gd.min"), ":")[0].cast("double") + split(col("gd.min"), ":")[1].cast("double") / 60).alias("m_minutes"),
    col("gd.fgm").alias("m_fgm"),
    col("gd.fga").alias("m_fga"),
    col("gd.fg3m").alias("m_fg3m"),
    col("gd.fg3a").alias("m_fg3a"),
    col("gd.ftm").alias("m_ftm"),
    col("gd.fta").alias("m_fta"),
    col("gd.oreb").alias("m_oreb"),
    col("gd.dreb").alias("m_dreb"),
    col("gd.reb").alias("m_reb"),
    col("gd.ast").alias("m_ast"),
    col("gd.stl").alias("m_stl"),
    col("gd.blk").alias("m_blk"),
    col("gd.TO").alias("m_turnovers"),
    col("gd.pf").alias("m_pf"),
    col("gd.pts").alias("m_pts"),
    col("gd.plus_minus").alias("m_plus_minus")
)

# Save the results
player_stats_df.write.mode("overwrite").parquet("path/to/output/player_stats")
final_game_details_df.write.mode("overwrite").parquet("path/to/output/game_details")

# Stop Spark session
spark.stop()

'''
Create tests in src/tests folder with fake input and expected output data
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, array_union, when, lit, split, expr, instr, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("SparkSQLJobs").getOrCreate()

# Define schemas for fake data
today_schema = StructType([
    StructField("player_name", StringType(), True),
    StructField("height", StringType(), True),
    StructField("college", StringType(), True),
    StructField("country", StringType(), True),
    StructField("draft_year", IntegerType(), True),
    StructField("draft_round", IntegerType(), True),
    StructField("draft_number", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("gp", IntegerType(), True),
    StructField("pts", DoubleType(), True),
    StructField("reb", DoubleType(), True),
    StructField("ast", DoubleType(), True)
])

yesterday_schema = StructType([
    StructField("player_name", StringType(), True),
    StructField("height", StringType(), True),
    StructField("college", StringType(), True),
    StructField("country", StringType(), True),
    StructField("draft_year", IntegerType(), True),
    StructField("draft_round", IntegerType(), True),
    StructField("draft_number", IntegerType(), True),
    StructField("season_stats", ArrayType(ArrayType(DoubleType())), True),
    StructField("scoring_class", StringType(), True),
    StructField("years_since_last_season", IntegerType(), True),
    StructField("current_season", IntegerType(), True)
])

# Create fake input data
today_data = [("John Doe", "6'5", "Duke", "USA", 2015, 1, 5, 2024, 20, 18.5, 7.2, 5.1)]
yesterday_data = [("John Doe", "6'5", "Duke", "USA", 2015, 1, 5, [[2023, 19, 17.0, 6.5, 4.8]], "good", 2, 2023)]

# Create DataFrames
today_df = spark.createDataFrame(today_data, schema=today_schema)
yesterday_df = spark.createDataFrame(yesterday_data, schema=yesterday_schema)

# Run transformation
player_stats_df = today_df.alias("t").join(
    yesterday_df.alias("y"),
    col("t.player_name") == col("y.player_name"),
    "full_outer"
).select(
    coalesce(col("t.player_name"), col("y.player_name")).alias("player_name"),
    coalesce(col("t.height"), col("y.height")).alias("height"),
    coalesce(col("t.college"), col("y.college")).alias("college"),
    coalesce(col("t.country"), col("y.country")).alias("country"),
    coalesce(col("t.draft_year"), col("y.draft_year")).alias("draft_year"),
    coalesce(col("t.draft_round"), col("y.draft_round")).alias("draft_round"),
    coalesce(col("t.draft_number"), col("y.draft_number")).alias("draft_number"),
    when(col("y.season_stats").isNull(), array([col("t.season"), col("t.gp"), col("t.pts"), col("t.reb"), col("t.ast")]))
        .when(col("t.season").isNotNull(), array_union(col("y.season_stats"), array([col("t.season"), col("t.gp"), col("t.pts"), col("t.reb"), col("t.ast")]))).otherwise(col("y.season_stats")).alias("season_stats"),
    when(col("t.season").isNotNull(),
         when(col("t.pts") > 20, lit("star"))
        .when(col("t.pts") > 15, lit("good"))
        .when(col("t.pts") > 10, lit("average"))
        .otherwise(lit("bad")))
    .otherwise(col("y.scoring_class")).alias("scoring_class"),
    when(col("t.season").isNotNull(), lit(0))
    .otherwise(col("y.years_since_last_season") + 1).alias("years_since_last_season"),
    coalesce(col("t.season"), col("y.current_season") + 1).alias("current_season")
)

# Expected output
test_output = [("John Doe", "6'5", "Duke", "USA", 2015, 1, 5, [[2023, 19, 17.0, 6.5, 4.8], [2024, 20, 18.5, 7.2, 5.1]], "good", 0, 2024)]
test_df = spark.createDataFrame(test_output, schema=player_stats_df.schema)

# Validate results
assert player_stats_df.collect() == test_df.collect(), "Test failed!"
print("Test passed!")

# Stop Spark session
spark.stop()
