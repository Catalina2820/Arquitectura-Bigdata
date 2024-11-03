# .\.venv\Scripts\activate #para activar el env
# python .\app.py #Para correr el archivo
# deactivate #para parar la env
# pip install flask pyspark findspark
from pyspark import SparkConf, SparkContext
import findspark
from flask import Flask, request  
# request es para poder pasar parámetros
# termino el proceso con contro + C
# Para testear una sección del código solo debo iniciar el ambiente + iluminar la fracción + shift Enter
# .\.venv\Scripts\

app = Flask("MiPrimeraApp")

findspark.init()
conf = SparkConf().setMaster("local[*]").setAppName("MiSpark")
sc = SparkContext(conf = conf)

def parseLine(x):
  x = x.split("::")[0:3]
  x[2] = float(x[2])
  return x

movilens = sc.textFile("./ml-1m/ratings.dat")
movilens = movilens.map(parseLine)

def get_movies_info():
  movieNames = {}

  with open("./ml-1m/movies.dat", "r", encoding = "latin1") as f:
    for line in f:
      x = line.split("::")
      movieNames[x[0]] = {"name": x[1][:-7],
                          "year": x[1][-5:-1],
                          "genres": x[2].replace("\n", "").split("|")}
  return movieNames

movieDictio = sc.broadcast(get_movies_info())

def get_users_info():
  usersInfo = {}

  with open("./ml-1m/users.dat", "r", encoding = "latin1") as f:
    for line in f:
      x = line.split("::")
      usersInfo[x[0]] = {"gender": x[1],
                          "Age": x[2],
                         "Occupation": x[3]}
  return usersInfo

userDictio = sc.broadcast(get_users_info())


@app.route("/")
def ruta_raiz():
    return("Bienvenido a su sistema de recomendación")


@app.route("/recomendaciones")
def get_movie_recomendation():
# def get_movie_recomendation(Age = None, gender = None, Occupation = None, year = None, genre = None):
  '''
  Esta función genera recomendaciones de peliculasa basadas en los ratings de usuarios del datase Movilens-1M,
  la función opera para todo el dataset o filtrado según la información de usuarios y la información de peliculas.

  Parametros:
    - Age: str
        Edad de los usuarios, tenga en cuenta las etiquetas del README.
    - gender: str
        Género de los usuarios, M o F.
    - Occupation: str
        Ocupación de los usuarios, tenga en cuenta las etiquetas del README.
    - year: str
        Año de lanzamiento de la película.
    - genre: str
        Género de la película.
  
  Return:
    recomendation: RDD
      Las recomendaciones de las películas, ordenadas de mayor a menor, según el filtro deseado.
  '''
  
  Age = request.args.get("userAge")
  gender = request.args.get("userGender")
  Occupation = request.args.get("userOccupation")
  year = request.args.get("movieYear") 
  genre = request.args.get("movieGenre") 
  #http://127.0.0.1:5000/recomendaciones?userAge=25&movieYear=1999

  ################## Definimos los filtros de usuarios ###########################
  # return movilens.take(5)

  if Age != None:
    ratings = movilens.filter(lambda x: userDictio.value[x[0]]["Age"] == Age)

  if gender != None:
    ratings = movilens.filter(lambda x: userDictio.value[x[0]]["gender"] == gender)

  if Occupation != None:
    ratings = movilens.filter(lambda x: userDictio.value[x[0]]["Occupation"] == Occupation)

  ################# Definimos Filtros de pelicula ################################
  if year != None:
    ratings = movilens.filter(lambda x: movieDictio.value[x[1]]["year"] == year)

  if genre != None:
    ratings = movilens.filter(lambda x: genre in movieDictio.value[x[1]]["genres"])

  if Age == None and genre == None and Occupation == None and year == None and gender == None:
    ratings = movilens

  ##################### Filtramos nuestros ratings ##############################

  recomendation = ratings.map(lambda x: (x[1], x[2]))\
    .mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .mapValues(lambda x: x[0]/x[1])\
    .sortBy(lambda x: -x[1])\
    .map(lambda x: (movieDictio.value[x[0]]["name"], x[1]))

  return(recomendation.collect())

# return movilens.take(5)

# app.run()

@app.route('/get_ratings_distro')
def get_ratings_distro():
  Age = request.args.get("userAge")
  gender = request.args.get("userGender")
  Occupation = request.args.get("userOccupation")
  year = request.args.get("movieYear") 
  genre = request.args.get("movieGenre") 
  
    ################## Definimos los filtros de usuarios ###########################
  if Age != None:
    ratings_distro = movilens.filter(lambda x: userDictio.value[x[0]]["Age"] == Age)

  if gender != None:
    ratings_distro = movilens.filter(lambda x: userDictio.value[x[0]]["gender"] == gender)

  if Occupation != None:
    ratings_distro = movilens.filter(lambda x: userDictio.value[x[0]]["Occupation"] == Occupation)

  ################# Definimos Filtros de pelicula ################################
  if year != None:
    ratings_distro = movilens.filter(lambda x: movieDictio.value[x[1]]["year"] == year)

  if genre != None:
    ratings_distro = movilens.filter(lambda x: genre in movieDictio.value[x[1]]["genres"])

  if Age == None and genre == None and Occupation == None and year == None and gender == None:
    ratings_distro = movilens
    
  ratings_distro = ratings_distro.map(lambda x: x[2]).countByValue()
  
  return(ratings_distro)

app.run(debug = True) #Cada que guardo el archivo me actualiza la ejecución