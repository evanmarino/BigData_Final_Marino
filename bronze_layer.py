from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IMDB_movies").getOrCreate()

imdb = spark.read.option("header", "true").csv("../../spark_data/TMDB_movie_dataset_v11.csv")

#Outputs how many rows and columns the database has before cleaning
print("Row count:", imdb.count())
print("Column count:", len(imdb.columns))
spark.stop()

