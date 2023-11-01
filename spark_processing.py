from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import to_date
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import explode, count



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

# 2. Top 10 products with most reviews
top_reviewed_products = data.groupBy("ProductId").count().orderBy("count", ascending=False).limit(10)

# 3. Top 10 active users
top_active_users = data.groupBy("ProfileName").count().orderBy("count", ascending=False).limit(10)

# 3. Group by Date 
data_with_date = data.withColumn("Date", to_date(from_unixtime(col("Time"))))

reviews_per_date = data_with_date.groupBy("Date").count().orderBy("Date")

# 4. Sentiment analysis
def sentiment_analysis(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'

sentiment_udf = udf(sentiment_analysis, StringType())
data_with_sentiment = data.withColumn("Sentiment", sentiment_udf(data["Text"]))

sentiment_distribution = data_with_sentiment.groupBy("Sentiment").count()

# 5. Word frequency

# Tokenisation des mots
tokenizer = Tokenizer(inputCol="Text", outputCol="words")
data_tokenized = tokenizer.transform(data)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
data_filtered = remover.transform(data_tokenized)

# Count the frequency of each word
word_frequency = data_filtered.select(explode("filtered_words").alias("word")).groupBy("word").agg(count("*").alias("frequency")).orderBy("frequency", ascending=False).limit(20)

# Saving the results back to HDFS
reviews_per_score.write.csv("hdfs://namenode:8020/results/reviews_per_score.csv", mode="overwrite", header=True)
top_reviewed_products.write.csv("hdfs://namenode:8020/results/top_reviewed_products.csv", mode="overwrite", header=True)
top_active_users.write.csv("hdfs://namenode:8020/results/top_active_users.csv", mode="overwrite", header=True)
reviews_per_date.write.csv("hdfs://namenode:8020/results/reviews_per_date.csv", mode="overwrite", header=True)
sentiment_distribution.write.csv("hdfs://namenode:8020/results/sentiment_distribution.csv", mode="overwrite", header=True)
word_frequency.write.csv("hdfs://namenode:8020/results/word_frequency.csv", mode="overwrite", header=True)

# Stop the Spark session
spark.stop()
