# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast

spark = SparkSession.builder.appName("spark_homework").getOrCreate()

# Disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.debug.maxToStringFields", -1)

# %%
# Source Data

match_details = (spark.read.option('header', True)
                    .option('inferSchema', True)
                     .csv("/home/iceberg/data/match_details.csv"))
matches = (spark.read.option('header', True)
               .csv("/home/iceberg/data/matches.csv"))
medals_patches_players = (spark.read.option('header', True)
                              .csv("/home/iceberg/data/medals_matches_players.csv"))
medals = (spark.read.option('header', True)
              .csv("/home/iceberg/data/medals.csv")
              .withColumnRenamed("name", "medal_name")
              .drop('description'))
maps = (spark.read.option('header', True)
            .csv("/home/iceberg/data/maps.csv")
            .withColumnRenamed("name", "map_name")
            .drop('description'))

# %%
# Explicitly use broadcast join for matches and maps

explicit_matches_maps = matches.join(broadcast(maps), on="mapid", how="left")

# %% [markdown]
# # Create Bucketed Tables

# %%
spark.sql("CREATE DATABASE IF NOT EXISTS bootcamp")

# %%
# Bucket Tables

match_details.write.mode('overwrite').bucketBy(16, 'match_id').saveAsTable('bootcamp.bucketed_match_details')
matches.write.mode('overwrite').bucketBy(16, 'match_id').saveAsTable('bootcamp.bucketed_matches')
medals_patches_players.write.mode('overwrite').bucketBy(16, 'match_id').saveAsTable('bootcamp.bucketed_medals_patches_players')

# %%
base = matches.join(match_details, on="match_id", how="left") \
    .join(medals_patches_players, on=["match_id", "player_gamertag"], how="left") \
    .join(broadcast(medals.alias('medals')), on="medal_id", how="left") \
    .join(broadcast(maps.alias('maps')), on="mapid", how="left")

# %%
base.columns

# %% [markdown]
# # Questions

# %% [markdown]
# 1. Which player averages the most kills per game?
# 
# Answer: gimpinator14
# 

# %%
base.groupBy('player_gamertag').avg('player_total_kills').orderBy('avg(player_total_kills)', ascending=False) \
    .limit(1).show()

# %% [markdown]
# 2. Which playlist gets played the most?
# 
# Answer: Playlist f72e0ef0-7c4a-4307-af78-8e38dac3fdba

# %%
base.select("match_id", "playlist_id").distinct().groupBy("playlist_id").count() \
    .orderBy("count", ascending=False).limit(1).show()

# %% [markdown]
# 3. Which map gets played the most?
# 
# Answer: Breakout Arena (c7edbf0f-f206-11e4-aa52-24be05e24f7e)

# %%
base.select("match_id", "mapid", "map_name").distinct().groupBy("mapid", "map_name").count() \
    .orderBy("count", ascending=False).limit(1).show()

# %% [markdown]
# 4. Which map do players get the most Killing Spree medals on?
# 
# Answer: Breakout Arena (c7edbf0f-f206-11e4-aa52-24be05e24f7e)

# %%
base.filter(col('medal_name') == 'Killing Spree') \
    .select('match_id', 'player_gamertag', 'mapid', 'map_name', 'medal_id', 'medal_name') \
    .distinct() \
    .groupBy('mapid', 'map_name') \
    .count() \
    .orderBy('count', ascending=False) \
    .limit(1) \
    .show()

# %% [markdown]
# # Compression comparison with `.sortWithinPartitions` method
# 
# With the size comparison, the base table is more compressed when partitioning using playlist_id, and map_id.

# %%
base.columns

# %%
base.write.mode("overwrite").saveAsTable("bootcamp.base_unsorted")

# %%
first = base.repartition(10, col('maps.mapid'))
first_sort_df = first.sortWithinPartitions(col("maps.mapid"))
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.first_base_sorted")

# %%
second = base.repartition(10, col('playlist_id'))
second_sort_df = first.sortWithinPartitions(col("playlist_id"))
second_sort_df.write.mode("overwrite").saveAsTable("bootcamp.second_base_sorted")

# %%
%%sql

SELECT
  SUM(file_size_in_bytes) AS size,
  COUNT(1) AS num_files,
  'first_sorted'
FROM
  bootcamp.first_base_sorted.files
UNION ALL
SELECT
  SUM(file_size_in_bytes) AS size,
  COUNT(1) AS num_files,
  'second_sorted'
FROM
  bootcamp.second_base_sorted.files
UNION ALL
SELECT
  SUM(file_size_in_bytes) AS size,
  COUNT(1) AS num_files,
  'unsorted'
FROM
  bootcamp.base_unsorted.files

# %%



