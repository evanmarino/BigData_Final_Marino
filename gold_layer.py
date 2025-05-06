from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev

spark = SparkSession.builder.appName("IMDB_movies").getOrCreate()
imdb = spark.read.parquet("imdb_clean.parquet")

#Only select certain columns applicable for aggregate dataset
imdb = imdb.select(
	col("title"),
	col("release_date"),
	col("budget"),
	col("revenue"),
	col("vote_average").alias("average_score"),
	col("vote_count").alias("score_count")
)

#Filtering indie movies by making sure at least 5000 people have scored it, and making sure it has the right values to fit the dataset
imdb = imdb.filter((col("score_count") > 1000) & (col("budget") > 0) & (col("revenue") > 0)) 

#Calculating roi to see what how profitable a movie was at the time of release
imdb = imdb.withColumn("profit", col("revenue") - col("budget"))
imdb = imdb.withColumn("roi", col("profit") / col("budget"))

#Calculates the z values of the roi, score, and count
mean_roi = imdb.select(mean("roi")).collect()[0][0]
std_roi = imdb.select(stddev("roi")).collect()[0][0]
imdb = imdb.withColumn("roi_z", (col("roi") - mean_roi) / std_roi)

mean_count = imdb.select(mean("score_count")).collect()[0][0]
std_count = imdb.select(stddev("score_count")).collect()[0][0]
imdb = imdb.withColumn("count_z", (col("score_count") - mean_count) / std_count)

mean_score = imdb.select(mean("average_score")).collect()[0][0]
std_score = imdb.select(stddev("average_score")).collect()[0][0]
imdb = imdb.withColumn("score_z", (col("average_score") - mean_score) / std_score)

#makes new columns based on what kind of underrated the movies are
imdb = imdb.withColumn("underrated_score", col("roi_z") - col("count_z") / 125)
imdb = imdb.withColumn("underappreciated_score", col("roi_z") - col("score_z") / 15)
imdb = imdb.withColumn("secret_gems", col("count_z") - col("score_z") * 1.25)

'''
#prints out all 3 values with important values
imdb.orderBy(col("underrated_score")).select("title", "release_date", "average_score", "score_count", "roi", "underrated_score").show()
imdb.orderBy(col("underappreciated_score")).select("title", "release_date", "average_score", "score_count", "roi", "underappreciated_score").show()
imdb.orderBy(col("secret_gems")).select("title", "release_date", "average_score", "score_count", "roi", "secret_gems").show()
'''
imdb.write.mode("overwrite").option("header", True).csv("imdb_clean_csv")

spark.stop()
