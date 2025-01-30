''' PySpark Testing Homework
----------------------------------------------------------------------------------------------------------------------------------------------

Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
Create new PySpark jobs in src/jobs for these queries
Create tests in src/tests folder with fake input and expected output data

'''

'''
Create tests in src/tests folder with fake input and expected output data
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, array_union, when, lit, split, expr, instr, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("NBA Stats ETL with Tests").getOrCreate()

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

# Define the window for deduplication
window_spec = Window.partitionBy("game_id", "team_id", "player_id").orderBy("game_date_est")

# Simulate the transformation process (first part of the job)
deduped = yesterday_df.join(today_df, "player_name", "full_outer") \
    .withColumn("row_num", row_number().over(window_spec))

# Join tables and apply transformations
final_df = deduped.filter(col("row_num") == 1) \
    .select(
        coalesce(col("player_name"), lit("")).alias("dim_player_name"),
        coalesce(col("season"), lit(0)).alias("dim_season"),
        coalesce(col("team_id"), lit(0)).alias("dim_team_id"),
        coalesce(col("player_id"), lit(0)).alias("dim_player_id"),
        coalesce(col("start_position"), lit("")).alias("dim_start_position"),
        when(col("team_id") == col("home_team_id"), lit(True)).otherwise(lit(False)).alias("dim_is_playing_at_home"),
        (coalesce(instr(col("comment"), "DNP"), 0) > 0).alias("dim_did_not_play"),
        (coalesce(instr(col("comment"), "DND"), 0) > 0).alias("dim_did_not_dress"),
        (coalesce(instr(col("comment"), "NWT"), 0) > 0).alias("dim_not_with_team"),
        (split(col("min"), ":").getItem(0).cast("double") +
         split(col("min"), ":").getItem(1).cast("double") / 60).alias("m_minutes"),
        coalesce(col("fgm"), lit(0)).alias("m_fgm"),
        coalesce(col("fga"), lit(0)).alias("m_fga"),
        coalesce(col("fg3m"), lit(0)).alias("m_fg3m"),
        coalesce(col("fg3a"), lit(0)).alias("m_fg3a"),
        coalesce(col("ftm"), lit(0)).alias("m_ftm"),
        coalesce(col("fta"), lit(0)).alias("m_fta"),
        coalesce(col("oreb"), lit(0)).alias("m_oreb"),
        coalesce(col("dreb"), lit(0)).alias("m_dreb"),
        coalesce(col("reb"), lit(0)).alias("m_reb"),
        coalesce(col("ast"), lit(0)).alias("m_ast"),
        coalesce(col("stl"), lit(0)).alias("m_stl"),
        coalesce(col("blk"), lit(0)).alias("m_blk"),
        coalesce(col("TO"), lit(0)).alias("m_turnovers"),
        coalesce(col("pf"), lit(0)).alias("m_pf"),
        coalesce(col("pts"), lit(0)).alias("m_pts"),
        coalesce(col("plus_minus"), lit(0)).alias("m_plus_minus")
    )

# Expected output after transformation
expected_output_data = [
    ("John Doe", "6'5", "Duke", "USA", 2015, 1, 5, [[2023, 19, 17.0, 6.5, 4.8], [2024, 20, 18.5, 7.2, 5.1]], "good", 0, 2024)
]

# Create expected output DataFrame
expected_df = spark.createDataFrame(expected_output_data, schema=today_df.schema)

# Validate results
assert final_df.collect() == expected_df.collect(), "Test failed!"
print("Test passed!")

# Stop Spark session
spark.stop()
