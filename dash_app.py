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
                    name='Reviews per Score'
                )
            ],
            'layout': go.Layout(title='Reviews per Score')
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
    )
    ,
    dcc.Graph(
        id='top-active-users',
        figure={
            'data': [
                go.Bar(
                    x=top_active_users['UserId'],
                    y=top_active_users['count'],
                    name='Top Active Users'
                )
            ],
            'layout': go.Layout(title='Top 10 Active Users')
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8051)

