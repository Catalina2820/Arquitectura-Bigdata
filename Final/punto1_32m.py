# ------------------------------------------------------------------------
# i) En una base de datos de Mongo cree tres colecciones llamadas ratings, movies y users. Cada una contendrá los datos de los archivos con mismo nombre.
# ------------------------------------------------------------------------

from pymongo import MongoClient
import os, re
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from flask import Flask, jsonify, request

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Movielens32m"]

# Crear colecciones
links_collection = db["links"]
movies_collection = db["movies"]
ratings_collection = db["ratings"]
tags_collection = db["tags"]


print("Paso 1/3")
print(client.list_database_names())


# ------------------------------------------------------------------------
# ii) Cree un script en python que sea capaz conectarse a mongo, lea cada archivo y lo ingeste línea a línea (cada línea como un documento) a su respectiva colección en Mongo. Eg: en movies, tenga en cuenta que cada fila del archivo se considera como un único documento dentro de mongo, por lo tanto, si el archivo movies tiene n elementos (filas) se van a ingestar n documentos a la colección movies.
# ------------------------------------------------------------------------
import csv

def load_data_to_mongo():

    # Carga de movies
    with open('./DataLake/Raw/Fuente2_ml-32m/movies.csv', mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        movies_batch = []
        for row in csv_reader:

            genres = row[2].split('|') if row[2] else []
            
            # Extraer el año del título y limpiar el título
            title = row[1]
            match = re.search(r'\((\d{4})\)', title)
            year = int(match.group(1)) if match else None
            title_without_year = re.sub(r'\s?\(\d{4}\)$', '', title)

            record = {
                "movie_id": int(row[0]),
                "title": title_without_year,
                "genres": genres,
                "year": year
            }
            
            movies_batch.append(record)

            if len(movies_batch) == 1000:
                movies_collection.insert_many(movies_batch)
                movies_batch = []

        if movies_batch:
            movies_collection.insert_many(movies_batch)
    
    
    #Carga de ratings
    with open('./DataLake/Raw/Fuente2_ml-32m/ratings.csv', mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        ratings_batch = []
        for row in csv_reader:
            record = {
                "user_id": int(row[0]),
                "movie_id": int(row[1]),
                "rating": float(row[2]),
                "timestamp": int(row[3])
            }
            ratings_batch.append(record)

            if len(ratings_batch) == 1000:
                ratings_collection.insert_many(ratings_batch)
                ratings_batch = []

        if ratings_batch:
            ratings_collection.insert_many(ratings_batch)
         
            
    #Carga de tags
    with open('./DataLake/Raw/Fuente2_ml-32m/tags.csv', mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        tags_batch = []
        for row in csv_reader:
            record = {
                "user_id": int(row[0]),
                "movie_id": int(row[1]),
                "tag": row[2],
                "timestamp": int(row[3])
            }
            tags_batch.append(record)

            if len(tags_batch) == 1000:
                tags_collection.insert_many(tags_batch)
                tags_batch = []

        if tags_batch:
            tags_collection.insert_many(tags_batch)
    
    
    # links
    with open('./DataLake/Raw/Fuente2_ml-32m/links.csv', mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        links_batch = []
        for row in csv_reader:
            record = {
                "movie_id": int(row[0]),
                "imdb_id": row[1],
                "tmdb_id": row[2]
            }
            links_batch.append(record)

            if len(links_batch) == 1000:
                links_collection.insert_many(links_batch)
                links_batch = []

        if links_batch:
            links_collection.insert_many(links_batch)
        
    print("Paso 2/3")
    print("Datos cargados exitosamente en MongoDB.")

load_data_to_mongo()


# ------------------------------------------------------------------------
# iii) Investigue como consultar las colecciones creadas desde pyspark
# ------------------------------------------------------------------------

from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from pymongo import MongoClient
import findspark
import json

# Iniciar la sesión de Spark
findspark.init()

# Configurar Spark para usar MongoDB
spark = SparkSession.builder \
    .appName("TFinal") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/Movielens32m.movies') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/Movielens32m.movies') \
    .getOrCreate()

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Movielens32M"]

# Verificar y listar las colecciones existentes en la base de datos de MongoDB
print("Colecciones disponibles en la base de datos 'Movielens':")
print(db.list_collection_names())

# Cargar las colecciones desde MongoDB

links_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/Movielens32m.links") \
    .load().drop("_id")

movies_df = spark.read.format("mongo")\
    .option("uri", "mongodb://localhost:27017/Movielens32m.movies")\
    .load().drop("_id")

ratings_df = spark.read.format("mongo")\
    .option("uri", "mongodb://localhost:27017/Movielens32m.ratings")\
    .load().drop("_id")

tags_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/Movielens32m.tags") \
    .load()

links_rdd = links_df.rdd
movies_rdd = movies_df.rdd
ratings_rdd = ratings_df.rdd
tags_rdd = tags_df.rdd

print("Paso 3/3")
print(f"Primeras 3 filas de 'links':\n{links_rdd.take(3)}")
print(f"Primeras 3 filas de 'movies':\n{movies_rdd.take(3)}")
print(f"Primeras 3 filas de 'ratings':\n{ratings_rdd.take(3)}")
print(f"Primeras 3 filas de 'tags':\n{tags_rdd.take(3)}")
print("Ejecución exitosa")

spark.stop()