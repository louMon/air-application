
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import itertools
import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
import random
import time


BASE_URL = 'https://qairamapnapi.qairadrones.com/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL + 'api/air_quality_measurements_period/'
LAST_HOURS = 15
QHAWAX_ARRAY = [37,38,39,40,41,43,45] #IOAR QHAWAXS
QHAWAX_LOCATION = [[-12.045286,-77.030902],[-12.050278,-77.026111],[-12.041025,-77.043454],[-12.0466667,-77.08027778],[-12.044182,-77.050756],[-12.0450749,-77.0278449],[-12.047538,-77.035366]]
array_columns = ["CO","H2S","NO2","O3","PM10","PM25","SO2","alt","lat","lon"]
DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0, 'H2S': 1, 'NO2': 2, 'O3': 3, 'PM10': 4, 'PM25': 5, 'SO2': 6, 'alt': 7, 'lat': 8, 'lon': 9}
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
coordenada_x_prediccion = -9 # a pesar de que le ponga diferente ubicacion, siempre bota la misma info
coordenada_y_prediccion = -77

INDICE_PM1 = 4

def getListOfMeasurementOfAllModules():
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=LAST_HOURS)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(QHAWAX_ARRAY)):
        json_params = {'name': 'qH0'+str(QHAWAX_ARRAY[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        json_measurement = json.loads(response.text)
        for j in range(len(json_measurement)):
            json_measurement[j]['lat'] =  QHAWAX_LOCATION[i][0]
            json_measurement[j]['lon'] =  QHAWAX_LOCATION[i][1]
        list_of_hours.append(json_measurement)
    return list_of_hours

def convertJsonToList(json_measurement):
    list_of_measurements = []
    for i in range(len(array_columns)):  
        list_of_measurements.append(json_measurement[array_columns[i]])
    return list_of_measurements

def sortListOfMeasurementPerHour(measurement_list):
    sort_list_by_hour = []
    for i in range(LAST_HOURS):
        hour_n = []
        for j in range(len(QHAWAX_ARRAY)):
            list_measurement_by_qhawax_by_hour = convertJsonToList(measurement_list[j][i])
            list_measurement_by_qhawax_by_hour = np.array(list_measurement_by_qhawax_by_hour)
            hour_n.append(list_measurement_by_qhawax_by_hour)
        hour_n = np.array(hour_n)
        sort_list_by_hour.append(hour_n)
    return sort_list_by_hour

def obtener_lista_diccionario_columnas_indice(lista_conjuntos_de_datos_interpolacion_espacial):
    lista_diccionario_columnas_indice = []
    for conjunto_de_datos_interpolacion_espacial in lista_conjuntos_de_datos_interpolacion_espacial:
        columnas_conjunto_de_datos_interpolacion_espacial = array_columns# list(conjunto_de_datos_interpolacion_espacial.columns) # ['']
        diccionario_columnas_indice = {}
        
        for i in range(len(columnas_conjunto_de_datos_interpolacion_espacial)):
            diccionario_columnas_indice[columnas_conjunto_de_datos_interpolacion_espacial[i]] = i
        
        lista_diccionario_columnas_indice.append(diccionario_columnas_indice)

    return lista_diccionario_columnas_indice

def matriz_de_distancias(x0, y0, x1, y1):
    observado = np.vstack((x0, y0)).T
    interpolado = np.vstack((x1, y1)).T

    # Realizar una matriz de distancias entre observaciones por pares
    d0 = np.subtract.outer(observado[:,0], interpolado[:,0])
    d1 = np.subtract.outer(observado[:,1], interpolado[:,1])

    return np.hypot(d0, d1)

def obtener_interpolacion_idw(x, y, z, xi, yi):
    distancias = matriz_de_distancias(x,y, xi,yi)
    # En IDW, los pesos son la inversa de las distancias
    pesos = 1.0 / distancias

    # Sumar todos los pesos a 1
    pesos /= pesos.sum(axis=0)

    # Multiplicar todos los pesos de cada punto interpolado por todos los valores de la variable a interpolar observados
    zi = np.dot(pesos.T, z)
    return zi

def obtenerInterpolacionEnUnPunto(conjunto_de_datos_interpolacion_espacial, indice_columna_coordenadas_x, indice_columna_coordenadas_y, coordenada_x_prediccion, coordenada_y_prediccion):
    coordenadas_x_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_coordenadas_x]
    coordenadas_y_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_coordenadas_y]
    valores_predichos = []

    for indice_columna_interpolacion in DICCIONARIO_INDICES_VARIABLES_PREDICCION.values():

        valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial = conjunto_de_datos_interpolacion_espacial[:, indice_columna_interpolacion]
        valor_variable_interpolacion_interpolado = obtener_interpolacion_idw(coordenadas_x_conjunto_de_datos_interpolacion_espacial, coordenadas_y_conjunto_de_datos_interpolacion_espacial, valores_variable_interpolacion_conjunto_de_datos_interpolacion_espacial, coordenada_x_prediccion, coordenada_y_prediccion)
        valores_predichos.append(valor_variable_interpolacion_interpolado)

    valores_predichos.insert(INDICE_PM1, 0.0)

    return np.array(valores_predichos)

def obtenerListaInterpolacionesPasadasEnUnPunto(lista_conjunto_de_datos_interpolacion_espacial, indice_columna_coordenadas_x, indice_columna_coordenadas_y, coordenada_x_prediccion, coordenada_y_prediccion):
    #cada elemento es el dataframe de los 8 modulos en la hora N.
    conjunto_valores_predichos = [obtenerInterpolacionEnUnPunto(conjunto_de_datos_interpolacion_espacial, indice_columna_coordenadas_x, indice_columna_coordenadas_y, coordenada_x_prediccion, coordenada_y_prediccion) for conjunto_de_datos_interpolacion_espacial in lista_conjunto_de_datos_interpolacion_espacial]

    return np.array(conjunto_valores_predichos)

measurement_list = getListOfMeasurementOfAllModules() #arreglo de jsons ordenados por modulos
sort_list_without_json = sortListOfMeasurementPerHour(measurement_list) #arreglo de jsons ordenados por hora de menor a mayor
lista_diccionario_columnas_indice = obtener_lista_diccionario_columnas_indice(sort_list_without_json) #indices donde se guarda cada columna de los contaminantes
indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]
conjunto_valores_predichos = obtenerListaInterpolacionesPasadasEnUnPunto(sort_list_without_json, indice_columna_coordenadas_x, indice_columna_coordenadas_y, coordenada_x_prediccion, coordenada_y_prediccion)
    #cual seria el orden de los dataframes de las horas de los modulos?

conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
    #cual seria el orden de los 11 elementos?
for i in range(len(conjunto_valores_predichos)):
    print("Elemento: "+str(i)+" -----------------------")
    print(conjunto_valores_predichos[i])