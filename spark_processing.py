from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Initialize the Spark session
spark = SparkSession.builder.appName("AmazonReviewsAnalysis").getOrCreate()

# Load the data from HDFS
data = spark.read.csv("hdfs://namenode:8020/data/Reviews.csv", header=True, inferSchema=True)

# Data cleaning: Removing duplicates and null values
data = data.dropDuplicates()
data = data.na.drop()

# Preprocess to handle extraneous quotation marks
columns_to_clean = ["UserId", "ProfileName", "Summary", "Text"]
for col_name in columns_to_clean:
    data = data.withColumn(col_name, regexp_replace(col_name, "^\"+|\"+$", ""))

data.printSchema()
data.show(100)
# Analysis

# 1. Count of reviews per score
reviews_per_score = data.groupBy("Score").count()

# 2. Top 10 products reviewed
top_reviewed_products = data.groupBy("ProductId").count().orderBy("count", ascending=False).limit(10)

# 3. Top 10 active users
top_active_users = data.groupBy("UserId").count().orderBy("count", ascending=False).limit(10)

# Saving the results back to HDFS
reviews_per_score.write.csv("hdfs://namenode:8020/results/reviews_per_score.csv", mode="overwrite", header=True)
top_reviewed_products.write.csv("hdfs://namenode:8020/results/top_reviewed_products.csv", mode="overwrite", header=True)
top_active_users.write.csv("hdfs://namenode:8020/results/top_active_users.csv", mode="overwrite", header=True)

# Stop the Spark session
spark.stop()
