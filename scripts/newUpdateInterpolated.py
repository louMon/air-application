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
INDICE_PM1 = 4

sort_list_without_json = None
indice_columna_coordenadas_x = None
indice_columna_coordenadas_y = None
json_data_pollutant = None
pool_size = 15

def setArrayCountNonePollutants():
    array_none_pollutants = []
    for i in range(len(QHAWAX_ARRAY)):
        json_pollutants = {}
        for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
            json_pollutants[key]=0
        array_none_pollutants.append(json_pollutants)
    return array_none_pollutants

def setOneMoreNoneAllPollutants(measurement, number_none): #Ir acumulando los Nones en el json
    for key,value in measurement.items():
        measurement[key]+= number_none
    return measurement

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
                    new_json[key] = 0
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
        for j in range(len(QHAWAX_ARRAY)):
            #print("Longitud de los elementos por arreglo de qhawax: "+str(len(measurement_list[j])))
            if(LAST_HOURS == len(measurement_list[j])): #Para evitar que se caiga cuando un qhawax le faltan horas en el periodo buscado
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
        pool_results = pool.map(iterateByGrids, json_data_grid)

        pool.close()
        pool.join()

        response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})
        print("--- %s seconds ---" % (time.time() - start_time))
