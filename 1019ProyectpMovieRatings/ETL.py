
##Codigo para cambiar los '::' por ',' y crear un csv con la info de ratings

ruta1 = "./DataLake/Raw/Fuente1_ml-1m/ratings.dat"
ruta2 = "./DataLake/Clean/Fuente1_ml-1m/ratings.dat"

import os

files = os.listdir('./DataLake/Raw/Fuente1_ml-1m')

for filename in files:
    if '.dat' in filename:
        nombre = filename.replace('.dat', '')
        with open(f'./DataLake/Raw/Fuente1_ml-1m/{nombre}.dat', encoding="latin-1") as fileR, open('./DataLake/Clean/Fuente1_ml-1m/{}.csv'.format(nombre), 'a') as fileC:   
            for line in fileR.readlines():
                fileC.write(line.replace('::', ','))