''' Spark Fundamentals Week
----------------------------------------------------------------------------------------------------------------------------------------------

match_details
    a row for every players performance in a match
matches
    a row for every match
medals_matches_players
    a row for every medal type a player gets in a match
medals
    a row for every medal type
Your goal is to make the following things happen:

Build a Spark job that
    Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    Explicitly broadcast JOINs medals and maps
    Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
    Aggregate the joined data frame to figure out questions like:
        Which player averages the most kills per game?
        Which playlist gets played the most?
        Which map gets played the most?
        Which map do players get the most Killing Spree medals on?
    With the aggregated data set
        Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)

Save these as .py files and submit them this way!

'''

import org.apache.spark.sql.functions.{broadcast, split, lit}
import org.apache.spark.sql.SparkSession

# Initialize Spark Session
val spark = SparkSession.builder()
  .appName("Spark Broadcast Join Example")
  .config("spark.sql.autoBroadcastJoinThreshold", "-1") # Disable automatic broadcast joins
  .getOrCreate()


'''
Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
Explicitly broadcast JOINs medals and maps
'''

val medals = spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/home/iceberg/data/medals.csv")  
val maps =  spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/home/iceberg/data/maps.csv")


val medalsDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medals (
    medal_id BIGINT,
    sprite_uri STRING,
    sprite_left INTEGER,
    sprite_top INTEGER,
    sprite_sheet_width INTEGER,
    sprite_sheet_height INTEGER,
    sprite_width INTEGER,
    sprite_height INTEGER,
    classification STRING,
    name STRING,
    difficulty INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, medal_id));
"""

spark.sql("""DROP TABLE IF EXISTS bootcamp.medals""")
spark.sql(medalsDDL)

# Write medals data into the table
medals.select(
    $"medal_id", $"sprite_uri", $"sprite_left", $"sprite_top", $"sprite_sheet_width", $"sprite_sheet_height", $"sprite_width", $"sprite_height", $"classification", $"name", $"difficulty")
    .write.mode("append")
    .bucketBy(16, "medal_id").saveAsTable("bootcamp.medals")


val mapsDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.maps (
    mapid STRING,
    name STRING,
    description STRING
)
USING iceberg
PARTITIONED BY (bucket(16, mapid));
"""

spark.sql("""DROP TABLE IF EXISTS bootcamp.maps""")
spark.sql(mapsDDL)

# Write maps data into the table
maps.select(
    $"mapid", $"name", $"description")
    .write.mode("append")
    .bucketBy(16, "mapid").saveAsTable("bootcamp.maps")


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Perform explicit broadcast join
#medals.createOrReplaceTempView("medals")
#maps.createOrReplaceTempView("maps")


val explicitBroadcast = maps.as("m").join(broadcast(medals).as("md"), $"m.mapid" === $"md.medal_id")

# Show the results
explicitBroadcast.show(false)


'''
Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
'''

val match_details = spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/home/iceberg/data/match_details.csv")
val medal_matches_players = spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/home/iceberg/data/medals_matches_players.csv")
val matches =  spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("/home/iceberg/data/matches.csv")

val bucketJoin = match_details.as("d")
.join(medal_matches_players.as("p"), $"d.match_id" === $"p.match_id")
.join(matches.as("m"), $"d.match_id" === $"m.match_id")
#.explain()
#.take(5)


''' 
Aggregate the joined data frame to figure out questions like:
    Which player averages the most kills per game?
    Which playlist gets played the most?
    Which map gets played the most?
    Which map do players get the most Killing Spree medals on?
'''


'''
With the aggregated data set
    Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
'''

# Stop the Spark session
spark.stop()