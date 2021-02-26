#import concurrent.futures
#from concurrent.futures import ThreadPoolExecutor
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

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
#BASE_URL = 'http://0.0.0.0:5000/'

GET_ALL_POLLUTANTS = BASE_URL_IA+ 'api/get_all_pollutants/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'
STORE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_spatial_prediction/'
DELETE_ALL_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_all_spatial_prediction/'
GET_LAST_HOUR_MEASUREMENT =BASE_URL_IA + 'api/air_quality_measurements_period_all_modules/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
LAST_HOURS =10
QHAWAX_ARRAY = [37,38,39,40,41,43,45,47,48,49,50,51,52,54] #IOAR QHAWAXS
QHAWAX_LOCATION = [[-12.045286,-77.030902],[-12.050278,-77.026111],[-12.041025,-77.043454],
                   [-12.0466667,-77.08027778],[-12.044182,-77.050756],[-12.0450749,-77.0278449],
                   [-12.047538,-77.035366],[-12.054722,-77.029722],[-12.044236,-77.012467],
                   [-12.051526,-77.077941],[-12.042525,-77.033486],[-12.046736,-77.047594],
                   [-12.045394,-77.036852],[-12.057582,-77.071778]]
array_columns = ["CO","H2S","NO2","O3","PM10","PM25","SO2","lat","lon"]
DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0, 'H2S': 1, 'NO2': 2, 'O3': 3, 'PM10': 4, 'PM25': 5, 'SO2': 6}
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'
coordenada_x_prediccion = -12.013600
coordenada_y_prediccion = -77.03367
INDICE_PM1 = 4

hours_number = 10
hours=[]
pool_size = 10

def setArrayCountNonePollutants():
    array_none_pollutants = []
    for i in range(len(QHAWAX_ARRAY)):
        json_pollutants = {}
        for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
            json_pollutants[key]=0
        array_none_pollutants.append(json_pollutants)
    return array_none_pollutants

def getListOfMeasurementOfAllModules(array_json_count_pollutants):
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=LAST_HOURS-1)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(QHAWAX_ARRAY)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(QHAWAX_ARRAY[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        array_json_measurement_by_module = json.loads(response.text)
        if(LAST_HOURS != len(array_json_measurement_by_module)):
            setOneMoreNoneAllPollutants(array_json_count_pollutants[i], LAST_HOURS - len(array_json_measurement_by_module))
        for j in range(len(array_json_measurement_by_module)): #iterando cada medicion por hora del modulo N [{},{},{}]
            new_json = {}
            for key,value in array_json_measurement_by_module[j].items():  # Here you will get key and value.
                if value is not None:
                    new_json[key] = value
                else:
                    array_json_count_pollutants[i][key] +=1
            new_json['lat'] =  QHAWAX_LOCATION[i][0]
            new_json['lon'] =  QHAWAX_LOCATION[i][1]
            array_json_measurement_by_module[j]=new_json
        list_of_hours.append(array_json_measurement_by_module)
    return list_of_hours,array_json_count_pollutants

def validateNumberOfNonePollutants(measurement_list, array_json_count_pollutants):
    pollutant = {'CO': 0, 'H2S': 0, 'NO2': 0, 'O3': 0, 'PM10': 0, 'PM25': 0, 'SO2': 0}
    total_of_measurement = LAST_HOURS * len(QHAWAX_ARRAY)
    #Count none pollutants
    for i in range(len(array_json_count_pollutants)):
        for key,value in array_json_count_pollutants[i].items():
            if(value>0): 
                pollutant[key]+=value
    #Quita el pollutant que no cumpla con la condicion (lo quito de array_columns, DICCIONARIO_INDICES_VARIABLES_PREDICCION)
    for key,value in pollutant.items():
        if(value>(total_of_measurement/2 +1)): 
            print("Retirar del json de qhawax: " +str(value))
            for k in range(len(measurement_list)):
                for j in range(len(measurement_list[k])):
                    measurement_list[k][j].pop(key, None)
            print("Retirar del array_columns: " +str(value))
            array_columns.remove(key)
            print("Retirar del DICCIONARIO_INDICES_VARIABLES_PREDICCION: " +str(value))
            DICCIONARIO_INDICES_VARIABLES_PREDICCION.pop(key, None)

    return measurement_list


def iterateGridsByPollutantsByHours(param):
    grid, pollutant,hour = param
    print("==========================================================================================")

    conjunto_valores_predichos = obtenerListaInterpolacionesPasadasEnUnPunto(sort_list_without_json, \
                                                                             indice_columna_coordenadas_x, \
                                                                             indice_columna_coordenadas_y, \
                                                                             grid['lat'], \
                                                                             grid['lon'])

    spatial_json={"pollutant_id":pollutant["id"],"grid_id":grid["id"],"ppb_value":None,\
                  "ug_m3_value":random.randrange(850),"hour_position":hour}
    print(spatial_json)
    response = requests.post(STORE_SPATIAL_PREDICTION, json=spatial_json)

if __name__ == '__main__':
    start_time = time.time()
    hours = range(1,hours_number+1)  #[1,2,3,4,5,6,7,8,9]
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) #[1,2,3,4]
    json_data_pollutant = json.loads(requests.get(GET_ALL_POLLUTANTS).text) #[1,2,3]
    response_delete = requests.post(DELETE_ALL_SPATIAL_PREDICTION)
    paramlist = list(itertools.product(json_data_grid,json_data_pollutant,hours))

    #Inicializar contadores None en cero de cada contaminante por qHAWAX
    array_json_count_pollutants = setArrayCountNonePollutants()

    #Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
    measurement_list, array_json_count_pollutants = getListOfMeasurementOfAllModules(array_json_count_pollutants)

    #Validar si es que en un contaminante hay mas de la mitad de Nones para descartarlo.
    measurement_list = validateNumberOfNonePollutants(measurement_list, array_json_count_pollutants)

    if(len(DICCIONARIO_INDICES_VARIABLES_PREDICCION)>1):
        #Arreglo de jsons ordenados por hora del mas antiguo al mas actual
        sort_list_without_json = sortListOfMeasurementPerHour(measurement_list) 
        #Interpolando
        lista_diccionario_columnas_indice = obtener_lista_diccionario_columnas_indice(sort_list_without_json)
        indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
        indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]

        pool = multiprocessing.Pool(pool_size)
        pool_results = pool.map(iterateGridsByPollutantsByHours, paramlist)

        pool.close()
        pool.join()

        response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})
        print("--- %s seconds ---" % (time.time() - start_time))
