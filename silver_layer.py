from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim, year

spark = SparkSession.builder.appName("IMDB_movies").getOrCreate()

imdb_clean = spark.read.option("header", "true").csv("../../spark_data/TMDB_movie_dataset_v11.csv")

#This sets all numerical values to their proper format instead of String, and inthe process makes values unfit for the column into null
imdb_clean = imdb_clean.withColumn("popularity", col("popularity").cast("float"))
imdb_clean = imdb_clean.withColumn("vote_average", col("vote_average").cast("float"))
imdb_clean = imdb_clean.withColumn("budget", col("budget").cast("int"))
imdb_clean = imdb_clean.withColumn("revenue", col("revenue").cast("int"))
imdb_clean = imdb_clean.withColumn("runtime", col("runtime").cast("int"))
imdb_clean = imdb_clean.withColumn("vote_count", col("vote_count").cast("int"))
imdb_clean = imdb_clean.withColumn("release_date", col("release_date").cast("date"))

#This replaces all blank or zero values with null
for column in imdb_clean.columns:
	dtype = dict(imdb_clean.dtypes)[column]
	if dtype == 'string':
		imdb_clean = imdb_clean.withColumn(column, when(trim(col(column)) == "", None).when(col(column) == "0", None).otherwise(col(column)))
	elif dtype in ['int', 'float']:
		imdb_clean = imdb_clean.withColumn(column, when(col(column) == 0, None).otherwise(col(column)))	
		
#This deletes any string column that has at least 25% null values
total_rows = imdb_clean.count()

drop_columns = []
for column in imdb_clean.columns:
	dtype = dict(imdb_clean.dtypes)[column]
	if dtype == 'string':
		null_count = imdb_clean.select(count(when(col(column).isNull(), column))).collect()[0][0]
		if null_count / total_rows >= 0.25:
			drop_columns.append(column)

imdb_clean = imdb_clean.drop(*drop_columns)

#This deletes rows that have a null title, an invalid release date, or have not been logged by more than one person
imdb_clean = imdb_clean.filter(imdb_clean.title.isNotNull())
imdb_clean = imdb_clean.filter((year("release_date") >= 1890) & (year("release_date") <= 2030))
imdb_clean = imdb_clean.filter((col("vote_count").isNotNull()) & (col("vote_count") > 1))

#This deletes rows that have at least 25% of the remaining columns set to null 
num_cols = len(imdb_clean.columns)
min_non_nulls_required = int(num_cols * 0.75)
imdb_clean = imdb_clean.na.drop(thresh=min_non_nulls_required)

#This creates the clean database to be used for gold layer
imdb_clean.write.mode("overwrite").parquet("imdb_clean.parquet")
spark.stop()
