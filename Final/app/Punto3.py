from flask import Flask, jsonify, request
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col
import findspark
import json
import re

# Inicialización de Flask
app = Flask("Final")

# Inicializar Spark
findspark.init()

# Configurar Spark para usar MongoDB
conf = SparkConf() \
    .setAppName("TFinal") \
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .set("spark.mongodb.input.uri", "mongodb://localhost:27017/Movielens32m.movies") \
    .set("spark.mongodb.output.uri", "mongodb://localhost:27017/Movielens32m.movies") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.cores", "2") \
    .set("spark.driver.maxResultSize", "2g") \
    .set("spark.python.worker.memory", "1g") \
    .set("spark.python.worker.reuse", "true") \
    .set("spark.python.worker.timeout", "600")

# Crear SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Movielens32m"]

# Cargar las colecciones desde MongoDB
movies_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/Movielens32m.movies") \
    .load().drop("_id")

ratings_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/Movielens32m.ratings") \
    .load().drop("_id","timestamp")
    
movies_df = movies_df.repartition(2)
ratings_df = ratings_df.repartition(2)

movies_rdd = movies_df.rdd.map(lambda row: row.asDict())
ratings_rdd = ratings_df.rdd.map(lambda row: row.asDict())

def calculate_average_ratings():
    # (movie_id, (rating, 1)) -> (movie_id, (sum(ratings), count(ratings)))
    ratings_avg_rdd = ratings_rdd \
        .map(lambda x: (x['movie_id'], (x['rating'], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: round(x[0] / x[1], 2))
    return ratings_avg_rdd

# ----------------------------------------------------------------------------
# i) ENDPOINT que acepte como parámetros “año”, y “genero”; filtra la RDD según estos parámetros y devuelve la información necesaria, tenga en cuenta que no necesariamente tienen que aparecer los dos parámetros en simultaneo.
# a. Un ENDPOINT llamado RATE_TOP20 que devuelva el top 20 de las películas con mejor rating según el filtro aplicado.
# ----------------------------------------------------------------------------

@app.route('/rate_top20', methods=['GET'])
#http://127.0.0.1:5000/rate_top20?year=1999&genre=Action

def rate_top20():
    
    year = request.args.get('year', default=None, type=int)
    genre = request.args.get('genre', default=None, type=str)

    filtered_movies = movies_rdd
    if year:
        filtered_movies = filtered_movies.filter(lambda x: x['year'] == year) 
    if genre:
        filtered_movies = filtered_movies.filter(lambda x: genre in x['genres'])

    ratings_avg_rdd = calculate_average_ratings()

    filtered_ratings = filtered_movies.map(lambda x: (x['movie_id'], x['title'])) \
        .join(ratings_avg_rdd)

    top_20 = filtered_ratings.takeOrdered(20, key=lambda x: -x[1][1])

    top_20_titles = [(movie[1][0], movie[1][1]) for movie in top_20]
    return jsonify(top_20_titles)


# ----------------------------------------------------------------------------
# i) ENDPOINT que acepte como parámetros “año”, y “genero”; filtra la RDD según estos parámetros y devuelve la información necesaria, tenga en cuenta que no necesariamente tienen que aparecer los dos parámetros en simultaneo.
# b. Un ENDPOINT llamado RATE_BOTTOM20 que devuelva las 20 películas con los peores ratings según el filtro aplicado.
# ----------------------------------------------------------------------------

@app.route('/rate_bottom20', methods=['GET'])
#http://127.0.0.1:5000/rate_bottom20?year=1999&genre=Action

def rate_bottom20():
    year = request.args.get('year', default=None, type=int)
    genre = request.args.get('genre', default=None, type=str)

    filtered_movies = movies_rdd
    if year:
        filtered_movies = filtered_movies.filter(lambda x: x['year'] == year)
    if genre:
        filtered_movies = filtered_movies.filter(lambda x: genre in x['genres'])

    ratings_avg_rdd = calculate_average_ratings()

    filtered_ratings = filtered_movies.map(lambda x: (x['movie_id'], x['title'])) \
        .join(ratings_avg_rdd)

    bottom_20 = filtered_ratings.takeOrdered(20, key=lambda x: x[1][1]) 

    bottom_20_titles = [(movie[1][0], movie[1][1]) for movie in bottom_20]
    return jsonify(bottom_20_titles)


# ----------------------------------------------------------------------------
# i) ENDPOINT que acepte como parámetros “año”, y “genero”; filtra la RDD según estos parámetros y devuelve la información necesaria, tenga en cuenta que no necesariamente tienen que aparecer los dos parámetros en simultaneo.
# c. Un ENDPOINT llamado COUNT_TOP 20 que devuelva el top 20 de las películas más vistas según el filtro aplicado.
# ----------------------------------------------------------------------------

@app.route('/count_top20', methods=['GET'])

#http://127.0.0.1:5000/count_top20?year=1999&genre=Action
def count_top20():
    year = request.args.get('year', default=None, type=int)
    genre = request.args.get('genre', default=None, type=str)

    filtered_movies = movies_rdd
    if year:
        filtered_movies = filtered_movies.filter(lambda x: x['year'] == year)
    if genre:
        filtered_movies = filtered_movies.filter(lambda x: genre in x['genres'])

    movie_count_rdd = ratings_rdd.map(lambda x: (x['movie_id'], 1)) \
                                 .reduceByKey(lambda x, y: x + y)

    filtered_count = filtered_movies.map(lambda x: (x['movie_id'], x['title'])) \
                                    .join(movie_count_rdd)

    top_20_most_viewed = filtered_count.takeOrdered(20, key=lambda x: -x[1][1])

    top_20_movies_info = [(movie[1][0], movie[1][1]) for movie in top_20_most_viewed]
    return jsonify(top_20_movies_info)



# ----------------------------------------------------------------------------
# ii) Un ENDPOINT llamado MOVIE que acepte el nombre de una película y devuelva la información de esta.
# ----------------------------------------------------------------------------


# http://127.0.0.1:5000/movie?title=sabrina
@app.route('/movie', methods=['GET'])
def movie():
    movie_id = request.args.get("movie_id", type=int)

    # 1. Filtrar ratings por movie_id
    ratings_filtered = ratings_df.filter(ratings_df['movie_id'] == movie_id)

    # 2. Calcular el rating promedio
    avg_rating = ratings_filtered.agg({"rating": "avg"}).collect()

    if not avg_rating:
        return jsonify({"error": "No ratings found for movie_id {}".format(movie_id)}), 404

    avg_rating = avg_rating[0][0]

    movie_df = spark.read.format("mongo") \
        .option("uri", "mongodb://localhost:27017/Movielens32m.movies") \
        .load().drop("_id")
    
    movie_title_df = movie_df.filter(movie_df['movie_id'] == movie_id).select('title').collect()

    if not movie_title_df:
        return jsonify({"error": "Movie not found for movie_id {}".format(movie_id)}), 404

    movie_title = movie_title_df[0]['title']

    result = {
        "movie_id": movie_id,
        "title": movie_title,
        "avg_rating": avg_rating
    }

    return jsonify(result)



# ----------------------------------------------------------------------------
# iii) Un ENDPOINT llamado LISTBYGENDER que acepte un género de película (Action, Adventure, Comedy, etc) y devuelva por genero elegido las 5 películas con más vistas y las 5 películas con mejor calificación promedio.
# ----------------------------------------------------------------------------

@app.route("/listbygender", methods=['GET'])
# http://127.0.0.1:5000/listbygender?genre=Action

def listbygender():

    genre = request.args.get('genre', default=None, type=str)

    if not genre:
        return jsonify({"error": "Missing 'genre' parameter"}), 400

    filtered_movies = movies_rdd.filter(lambda x: genre.lower() in [g.lower() for g in x['genres']])

    filtered_movies_list = filtered_movies.collect()

    if not filtered_movies_list:
        return jsonify({"error": f"No movies found for the genre {genre}"}), 404

    movie_count_rdd = ratings_rdd.map(lambda x: (x['movie_id'], 1)) \
                                 .reduceByKey(lambda x, y: x + y)
   
    ratings_avg_rdd = calculate_average_ratings()
    movie_count_dict = movie_count_rdd.collectAsMap()
    ratings_avg_dict = ratings_avg_rdd.collectAsMap()

    #RDD (titulo, vistas, genero, calificacion)
    movie_data = filtered_movies.flatMap(lambda movie: [
        (movie['title'],
         movie_count_dict.get(movie['movie_id'], 0),
         genre,
         ratings_avg_dict.get(movie['movie_id'], 0))
    ])

    movie_data_list = movie_data.collect()

    top_5_viewed = sorted(movie_data_list, key=lambda x: x[1], reverse=True)[:5]
    top_5_rated = sorted(movie_data_list, key=lambda x: x[3], reverse=True)[:5]

    return jsonify({
        "5 películas con más vistas": top_5_viewed,
        "5 películas con mejor calificación promedio": top_5_rated
    })

    
if __name__ == "__main__":
    app.run(debug=True)