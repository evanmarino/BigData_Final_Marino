from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IMDB_movies").getOrCreate()

imdb = spark.read.option("header", "true").csv("/Users/evanmarino/spark_data//Users/spark_data/TMDB_movie_dataset_v11.csv")

print("Row count:", df.count())
print("Column count:", len(df.columns))

spark.stop()
