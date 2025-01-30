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

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, count, desc

# Initialize Spark Session
spark = SparkSession.builder\
  .appName("Spark Broadcast Join Example")\
  .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable automatic broadcast joins
  .getOrCreate()

# Load datasets
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medal_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")

# Create bucketed tables
match_details.write.mode("overwrite").bucketBy(16, "match_id").sortBy("match_id").saveAsTable("match_details_bucketed")
medal_matches_players.write.mode("overwrite").bucketBy(16, "match_id").sortBy("match_id").saveAsTable("medals_matches_players_bucketed")
matches.write.mode("overwrite").bucketBy(16, "match_id").sortBy("match_id").saveAsTable("matches_bucketed")

# Read bucketed tables
match_details = spark.read.table("match_details_bucketed")
medal_matches_players = spark.read.table("medals_matches_players_bucketed")
matches = spark.read.table("matches_bucketed")

# Perform explicit broadcast join
explicit_broadcast = broadcast(medals).alias("md")\
    .join(medal_matches_players.alias("p"), "medal_id")\
    .join(matches.alias("m"), "match_id")\
    .join(broadcast(maps).alias("mp"), "map_id")

# Aggregation: Player with the most kills per game
most_kills_per_game = explicit_broadcast.groupBy("p.player_id").agg(avg("p.kills").alias("avg_kills")).orderBy(desc("avg_kills"))

# Aggregation: Most played playlist
most_played_playlist = explicit_broadcast.groupBy("m.playlist").agg(count("m.match_id").alias("play_count")).orderBy(desc("play_count"))

# Aggregation: Most played map
most_played_map = explicit_broadcast.groupBy("mp.name").agg(count("m.match_id").alias("map_play_count")).orderBy(desc("map_play_count"))

# Aggregation: Map with most Killing Spree medals
most_killing_spree_map = explicit_broadcast.filter("md.classification == 'Killing Spree'")\
    .groupBy("mp.name").agg(count("md.medal_id").alias("killing_spree_count")).orderBy(desc("killing_spree_count"))

# Sorting optimizations
sorted_by_playlist = explicit_broadcast.sortWithinPartitions("m.playlist")
sorted_by_map = explicit_broadcast.sortWithinPartitions("mp.name")

# Show results
most_kills_per_game.show(False)
most_played_playlist.show(False)
most_played_map.show(False)
most_killing_spree_map.show(False)

# Stop the Spark session
spark.stop()
