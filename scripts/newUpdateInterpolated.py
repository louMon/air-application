import itertools
import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
import random
import time
import multiprocessing
import numpy as np
import pandas as pd
import math

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'

GET_ALL_POLLUTANTS = BASE_URL_IA+ 'api/get_all_pollutants/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'
STORE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_spatial_prediction/'
DELETE_ALL_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_all_spatial_prediction/'
GET_LAST_HOUR_MEASUREMENT =BASE_URL_IA + 'api/air_quality_measurements_period_all_modules/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
LAST_HOURS =24
QHAWAX_ARRAY = [37,38,39,40,41,43,45,47,48,49,50,51,52,54] #IOAR QHAWAXS
QHAWAX_LOCATION = [[-12.045286,-77.030902],[-12.050278,-77.026111],[-12.041025,-77.043454],
                   [-12.044226,-77.050832],[-12.0466667,-77.080277778],[-12.0450749,-77.0278449],
                   [-12.047538,-77.035366],[-12.054722,-77.029722],[-12.044236,-77.012467],
                   [-12.051526,-77.077941],[-12.042525,-77.033486],[-12.046736,-77.047594],
                   [-12.045394,-77.036852],[-12.057582,-77.071778]]
array_columns = ["CO","H2S","NO2","O3","PM10","PM25","SO2","lat","lon"]
DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0, 'H2S': 1, 'NO2': 2, 'O3': 3, 'PM10': 4, 'PM25': 5, 'SO2': 6}
ALL_DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0, 'H2S': 1, 'NO2': 2, 'O3': 3, 'PM10': 4, 'PM25': 5, 'SO2': 6,'timestamp_zone':7,'lat':8,'lon':9,'alt':10}
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
INDICE_PM1 = 4

sort_list_without_json = None
indice_columna_coordenadas_x = None
indice_columna_coordenadas_y = None
json_data_pollutant = None
pool_size = 24

def verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values):
    for key,value in ALL_DICCIONARIO_INDICES_VARIABLES_PREDICCION.items(): 
        if(sensor_name==key):
            pollutant_array_json[key]=sensor_values
            continue
    return pollutant_array_json

def completeHourlyValuesByQhawax(valid_processed_measurements,qhawax_location_specific):
    average_valid_processed_measurement = []
    pollutant_array_json = {'CO': [], 'H2S': [], 'NO2': [], 'O3': [], 'PM10': [], 'PM25': [], 'SO2': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}
    for sensor_name in valid_processed_measurements[0]: #Recorro por contaminante para verificar None
        sensor_values = [measurement[sensor_name] for measurement in valid_processed_measurements]
        if(None in sensor_values):
            df_sensor =pd.DataFrame(sensor_values)
            df_sensor = df_sensor.interpolate(method="linear",limit=4,limit_direction='both')
            sensor_values = df_sensor[0].tolist() 
        pollutant_array_json = verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values)
    for elem_hour in range(len(valid_processed_measurements)): #Recorro por la cantidad de horas de cada qhawax (algunos tienen 24, 23, 22..)
        json = {}
        for key,value in pollutant_array_json.items(): # Recorro por la cantidad de elementos para rearmar el json
            json[key] = pollutant_array_json[key][elem_hour]
            #Estas lineas son para actualizar las posiciones de los qhawaxs con su real ubicacion
            if(key=='lat'):
                json[key] =  qhawax_location_specific[0]
            if(key=='lon'):
                json[key] =  qhawax_location_specific[1]
        average_valid_processed_measurement.append(json)

    return average_valid_processed_measurement

def getListOfMeasurementOfAllModules():
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=LAST_HOURS-1)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(QHAWAX_ARRAY)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(QHAWAX_ARRAY[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        if len(hourly_processed_measurements) < LAST_HOURS/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
            continue
        hourly_processed_measurements = completeHourlyValuesByQhawax(hourly_processed_measurements,QHAWAX_LOCATION[i])
        list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def convertJsonToList(json_measurement):
    list_of_measurements = []
    for i in range(len(array_columns)):
        if(array_columns[i] in json_measurement):
            list_of_measurements.append(json_measurement[array_columns[i]])
    return list_of_measurements

def sortListOfMeasurementPerHour(measurement_list):
    sort_list_by_hour = []
    for i in range(LAST_HOURS):
        hour_n = []
        for j in range(len(measurement_list)): #Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos
            if(LAST_HOURS == len(measurement_list[j])): #Para evitar que se caiga cuando un qhawax le faltan horas en el periodo buscado
                list_measurement_by_qhawax_by_hour = convertJsonToList(measurement_list[j][i])
                list_measurement_by_qhawax_by_hour = np.array(list_measurement_by_qhawax_by_hour)
                hour_n.append(list_measurement_by_qhawax_by_hour)
        new_hour_n_array = []
        for hour_elem in range(len(hour_n)):
            flag=False
            for measurement_elem in range(len(hour_n[hour_elem])):
                if(math.isnan(hour_n[hour_elem][measurement_elem])):
                    flag=True
                    continue
            if(flag==False):
                new_hour_n = hour_n[hour_elem]
                new_hour_n_array.append(new_hour_n)
        new_hour_n_array = np.array(new_hour_n_array)
        sort_list_by_hour.append(new_hour_n_array)
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
    #print(conjunto_de_datos_interpolacion_espacial)
    #print("====================================================================================================================")
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


def getPollutantID(json_data_pollutant,pollutant_name):
    for i in range(len(json_data_pollutant)):
        if(pollutant_name == json_data_pollutant[i]['pollutant_name']):
            return json_data_pollutant[i]['id']
    return None

def iterateByGrids(grid_elem):
    conjunto_valores_predichos = obtenerListaInterpolacionesPasadasEnUnPunto(sort_list_without_json, \
                                                                             indice_columna_coordenadas_x, \
                                                                             indice_columna_coordenadas_y, \
                                                                             grid_elem['lat'], \
                                                                             grid_elem['lon'])
    conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
    for i in range(len(conjunto_valores_predichos)):
        for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
            pollutant_id = getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                spatial_json={"pollutant_id":int(pollutant_id),"grid_id":int(grid_elem["id"]),"ppb_value":None,
                              "ug_m3_value":round(float(conjunto_valores_predichos[i][value]),3),"hour_position":int(i+1)}
                print(spatial_json)
                response = requests.post(STORE_SPATIAL_PREDICTION, json=spatial_json)


if __name__ == '__main__':
    start_time = time.time()
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_POLLUTANTS).text) 
    response_delete = requests.post(DELETE_ALL_SPATIAL_PREDICTION)

    #Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
    measurement_list = getListOfMeasurementOfAllModules()
    print("Obtener data de la base de datos de qHAWAXs y completar los None si cumplen con las condiciones")
    
    #Arreglo de jsons ordenados por hora del mas antiguo al mas actual
    sort_list_without_json = sortListOfMeasurementPerHour(measurement_list)
    print("Arreglo de jsons ordenados por hora del mas antiguo al mas actual")
    #Interpolando
    lista_diccionario_columnas_indice = obtener_lista_diccionario_columnas_indice(sort_list_without_json)
    indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
    indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]
    print("Interpolando")

    pool = multiprocessing.Pool(pool_size)
    pool_results = pool.map(iterateByGrids, json_data_grid)

    pool.close()
    pool.join()

    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})
    print("--- %s seconds ---" % (time.time() - start_time))

    print(datetime.datetime.now(dateutil.tz.tzutc()))
    print("===================================================================================")

