import dash
from dash import dcc, html
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import plotly.graph_objs as go

# Lecture des résultats à partir de HDFS
spark = SparkSession.builder.appName("AmazonReviewsVisualization").getOrCreate()

# Lire le DataFrame
reviews_per_score_spark = spark.read.csv("hdfs://namenode:8020/results/reviews_per_score.csv", header=True, inferSchema=True)

# Filter the Score column to only have numeric values
reviews_per_score_spark = reviews_per_score_spark.filter(col("Score").cast("int").isNotNull())


# Afficher le schéma du DataFrame Spark
reviews_per_score_spark.printSchema()
reviews_per_score_spark.show(100)

reviews_per_score = spark.read.csv("hdfs://namenode:8020/results/reviews_per_score.csv", header=True, inferSchema=True).toPandas()
top_reviewed_products = spark.read.csv("hdfs://namenode:8020/results/top_reviewed_products.csv", header=True, inferSchema=True).toPandas()
top_active_users = spark.read.csv("hdfs://namenode:8020/results/top_active_users.csv", header=True, inferSchema=True).toPandas()
reviews_per_date = spark.read.csv("hdfs://namenode:8020/results/reviews_per_date.csv", header=True, inferSchema=True).toPandas()
sentiment_distribution = spark.read.csv("hdfs://namenode:8020/results/sentiment_distribution.csv", header=True, inferSchema=True).toPandas()
word_frequency = spark.read.csv("hdfs://namenode:8020/results/word_frequency.csv", header=True).toPandas()


spark.stop()

# Application Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(
        id='reviews-per-score',
        figure={
            'data': [
                go.Bar(
                    x=reviews_per_score['Score'],
                    y=reviews_per_score['count'],
                    name='Dsitrubtion of rating'
                )
            ],
            'layout': go.Layout(title='Dsitrubtion of rating')
        }
    ),
    dcc.Graph(
        id='top-reviewed-products',
        figure={
            'data': [
                go.Bar(
                    x=top_reviewed_products['ProductId'],
                    y=top_reviewed_products['count'],
                    name='Top Reviewed Products'
                )
            ],
            'layout': go.Layout(title='Top 10 Reviewed Products')
        }
    ),
    dcc.Graph(
        id='top-active-users',
        figure={
            'data': [
                go.Bar(
                    x=top_active_users['ProfileName'],
                    y=top_active_users['count'],
                    name='Top Active Users'
                )
            ],
            'layout': go.Layout(title='Top 10 Active Users')
        }
    ),
    dcc.Graph(
    id='reviews-per-date',
    figure={
        'data': [
            go.Line(
                x=reviews_per_date['Date'],
                y=reviews_per_date['count'],
                name='Reviews per Date'
            )
        ],
        'layout': go.Layout(title='Reviews Trend Over Time')
    }
    ),
    dcc.Graph(
        id='sentiment-distribution',
        figure={
            'data': [
                go.Pie(
                    labels=sentiment_distribution['Sentiment'],
                    values=sentiment_distribution['count'],
                    name='Sentiment Distribution'
                )
            ],
            'layout': go.Layout(title='Sentiment Distribution')
        }
        ),
    dcc.Graph(
        id='word-frequency',
        figure={
            'data': [
                go.Bar(
                    x=word_frequency['word'],
                    y=word_frequency['frequency'],
                    name='Fréquence des mots'
                )
            ],
            'layout': go.Layout(title='Les 20 mots les plus fréquents dans les commentaires')
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8051)

