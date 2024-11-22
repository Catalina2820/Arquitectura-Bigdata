# ------------------------------------------------------------------------
# i) En una base de datos de Mongo cree tres colecciones llamadas ratings, movies y users. Cada una contendrá los datos de los archivos con mismo nombre.
# ------------------------------------------------------------------------

from pymongo import MongoClient
import os, re
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from flask import Flask, jsonify, request

# importing MongoClient from pymongo
from pymongo import MongoClient 

# Hacer la conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Crear base de datos
db = client["Movielens"]

# Crear colecciones
ratings_collection = db["ratings"]
movies_collection = db["movies"]
users_collection = db["users"]

print("Paso 1/3")
print(client.list_database_names())


# ------------------------------------------------------------------------
# ii) Cree un script en python que sea capaz conectarse a mongo, lea cada archivo y lo ingeste línea a línea (cada línea como un documento) a su respectiva colección en Mongo. Eg: en movies, tenga en cuenta que cada fila del archivo se considera como un único documento dentro de mongo, por lo tanto, si el archivo movies tiene n elementos (filas) se van a ingestar n documentos a la colección movies.
# ------------------------------------------------------------------------

def load_data_to_mongo():

    # Mapeo de ocupaciones
     
    user_occupation_mapper = {
        '0': "other or not specified",
        '1': "academic/educator",
        '2': "artist",
        '3': "clerical/admin",
        '4': "college/grad student",
        '5': "customer service",
        '6': "doctor/health care",
        '7': "executive/managerial",
        '8': "farmer",
        '9': "homemaker",
        '10': "K-12 student",
        '11': "lawyer",
        '12': "programmer",
        '13': "retired",
        '14': "sales/marketing",
        '15': "scientist",
        '16': "self-employed",
        '17': "technician/engineer",
        '18': "tradesman/craftsman",
        '19': "unemployed",
        '20': "writer"
    }
    
   
    # 1. Cargar ratings.dat
    # -----------------------
    with open('./DataLake/Raw/Fuente1_ml-1m/ratings.dat') as file:
        ratings_batch = []
        
        for line in file.readlines():
            item = line.strip().split("::")
            
            record = {
                "user_id": int(item[0]),
                "movie_id": int(item[1]),
                "rating": int(item[2])
            }
            
            ratings_batch.append(record)
            
            # Insertar por lotes de 1000 documentos
            if len(ratings_batch) == 1000:
                ratings_collection.insert_many(ratings_batch)
                ratings_batch = []  # Limpiar el lote
        
        # Insertar los datos restantes si el número de registros no es múltiplo de 1000
        if ratings_batch:
            ratings_collection.insert_many(ratings_batch)

    
    # 2. Cargar users.dat
    # -----------------------
    with open('./DataLake/Raw/Fuente1_ml-1m/users.dat') as file:
        users_batch = []
        
        for line in file.readlines():
            item = line.strip().split("::")
            
            record = {
                "user_id": int(item[0]),
                "gender": item[1],
                "age": int(item[2]),
                "occupation": user_occupation_mapper.get(item[3], "Unknown")
            }
            
            users_batch.append(record)
            
            # Insertar por lotes de 1000 documentos
            if len(users_batch) == 1000:
                users_collection.insert_many(users_batch)
                users_batch = []  # Limpiar el lote
        
        # Insertar los datos restantes si el número de registros no es múltiplo de 1000
        if users_batch:
            users_collection.insert_many(users_batch)

    
    # 3. Cargar movies.dat
    # -----------------------
    # with open('./DataLake/Raw/Fuente1_ml-1m/movies.dat') as file:
    #     movies_batch = []
        
    #     for line in file.readlines():
    #         item = line.strip().split("::")
            
    #         # Extraer el año del título
    #         match = re.search(r'\((\d{4})\)', item[1])  # Buscar un número de 4 dígitos entre paréntesis
    #         year = int(match.group(1)) if match else None  # Si encuentra el año, lo extrae
            
    #         record = {
    #             "movie_id": int(item[0]),
    #             "title": item[1],
    #             "genres": item[2].split("|"),
    #             "year": year
    #         }
            
    #         movies_batch.append(record)
    
    with open('./DataLake/Raw/Fuente1_ml-1m/movies.dat') as file:
        movies_batch = []
    
        for line in file.readlines():
            item = line.strip().split("::")
            
            # Extraer el año del título
            match = re.search(r'\((\d{4})\)', item[1])  # Buscar un número de 4 dígitos entre paréntesis
            year = int(match.group(1)) if match else None 
            
            # Eliminar el año (y el espacio anterior) del título
            title_without_year = re.sub(r'\s?\(\d{4}\)$', '', item[1])
            
            record = {
                "movie_id": int(item[0]),
                "title": title_without_year,
                "genres": item[2].split("|"),
                "year": year
            }
            
            movies_batch.append(record)
            
            # Insertar por lotes de 1000 documentos
            if len(movies_batch) == 1000:
                movies_collection.insert_many(movies_batch)
                movies_batch = []  # Limpiar el lote
        
        # Insertar los datos restantes si el número de registros no es múltiplo de 1000
        if movies_batch:
            movies_collection.insert_many(movies_batch)

    print("Paso 2/3")
    print("Datos cargados exitosamente a MongoDB.")

load_data_to_mongo()

# ------------------------------------------------------------------------
# iii) Investigue como consultar las colecciones creadas desde pyspark
# ------------------------------------------------------------------------

from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from pymongo import MongoClient
import findspark
import json
import re

# Iniciar la sesión de Spark
findspark.init()

# Configurar Spark para usar MongoDB
spark = SparkSession.builder \
    .appName("TFinal") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/Movielens.movies') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/Movielens.movies') \
    .getOrCreate()

# Conectar a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Movielens"]

# Verificar y listar las colecciones existentes en la base de datos de MongoDB
print("Colecciones disponibles en la base de datos 'Movielens':")
print(db.list_collection_names())

# Cargar las colecciones desde MongoDB
movies_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/") \
    .option("database", "Movielens") \
    .option("collection", "movies") \
    .load().drop("_id")

ratings_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/") \
    .option("database", "Movielens") \
    .option("collection", "ratings") \
    .load().drop("_id")

users_df = spark.read.format("mongo") \
    .option("uri", "mongodb://localhost:27017/") \
    .option("database", "Movielens") \
    .option("collection", "users") \
    .load().drop("_id")

# Convertir el DataFrame a JSON (como un RDD de Strings en formato JSON)
movies_rdd = movies_df.toJSON()
ratings_rdd = ratings_df.toJSON()
users_rdd = users_df.toJSON()

print("Paso 3/3")
print(f"Primeras 3 filas de 'movies':\n{movies_rdd.take(3)}")
print(f"Primeras 3 filas de 'ratings':\n{ratings_rdd.take(3)}")
print(f"Primeras 3 filas de 'users':\n{users_rdd.take(3)}")
print("Ejecución exitosa")

spark.stop()