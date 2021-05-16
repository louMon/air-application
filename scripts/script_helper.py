import math
import numpy as np
import pandas as pd

ALL_DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0,'NO2': 1,'PM25': 2,'timestamp_zone':3,'lat':4,'lon':5,'alt':6}
DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0,'NO2': 1, 'PM25': 2}
array_columns = ["CO","NO2","PM25","lat","lon"]
INDICE_PM1 = 4

def getDetailOfEnvStation(json_all_env_station):
    STATION_ID_ARRAY = [env_station['id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ] 
    QHAWAX_ID_ARRAY = [ env_station['module_id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ] 
    QHAWAX_LOCATION = [ [env_station['lat'],env_station['lon']] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ]         
    return STATION_ID_ARRAY,QHAWAX_ID_ARRAY,QHAWAX_LOCATION

def completeHourlyValuesByQhawax(valid_processed_measurements,qhawax_specific_location,pollutant_array_json):
    average_valid_processed_measurement = []
    for sensor_name in valid_processed_measurements[0]: #Recorro por contaminante para verificar None
        if(sensor_name in pollutant_array_json):
            sensor_values = [measurement[sensor_name] if(sensor_name in measurement) else None for measurement in valid_processed_measurements]
            if(None in sensor_values) and not all([value is None for value in sensor_values]):
                df_sensor =pd.DataFrame(sensor_values)
                df_sensor = df_sensor.interpolate(method="linear",limit=4,limit_direction='both')
                sensor_values = df_sensor[0].tolist() 
            pollutant_array_json = verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values)
    for elem_hour in range(len(valid_processed_measurements)): #Recorro por la cantidad de horas de cada qhawax (algunos tienen 24, 23, 22..)
        json = {}
        for key,value in pollutant_array_json.items(): # Recorro por la cantidad de elementos para rearmar el json
            if(value!=[]):
                json[key] = pollutant_array_json[key][elem_hour]
                #Estas lineas son para actualizar las posiciones de los qhawaxs con su real ubicacion
                if(key=='lat'):
                    json[key] =  qhawax_specific_location[0]
                if(key=='lon'):
                    json[key] =  qhawax_specific_location[1]
        average_valid_processed_measurement.append(json)

    return average_valid_processed_measurement

def convertJsonToList(json_measurement):
    list_of_measurements = []
    for i in range(len(array_columns)):
        if(array_columns[i] in json_measurement):
            list_of_measurements.append(json_measurement[array_columns[i]])
    return list_of_measurements

def sortListOfMeasurementPerHour(measurement_list,LAST_HOURS):
    sort_list_by_hour = []
    for i in range(LAST_HOURS):
        hour_n = []
        for j in range(len(measurement_list)): #Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos
            if(LAST_HOURS <= len(measurement_list[j])): #Para evitar que se caiga cuando un qhawax le faltan horas en el periodo buscado
                list_measurement_by_qhawax_by_hour = convertJsonToList(measurement_list[j][i])
                list_measurement_by_qhawax_by_hour = np.array(list_measurement_by_qhawax_by_hour)
                hour_n.append(list_measurement_by_qhawax_by_hour)
        new_hour_n_array = []
        for hour_elem in range(len(hour_n)):
            flag=False
            for measurement_elem in range(len(hour_n[hour_elem])):
                if(hour_n[hour_elem][measurement_elem] == None or math.isnan(hour_n[hour_elem][measurement_elem])):
                    flag=True
                    continue
            if(flag==False):
                new_hour_n = hour_n[hour_elem]
                new_hour_n_array.append(new_hour_n)
        new_hour_n_array = np.array(new_hour_n_array)
        sort_list_by_hour.append(new_hour_n_array)
    return sort_list_by_hour

def filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k):
    new_sort_list_without_json = []
    for hour in sort_list_without_json:
        hour_n = []
        for next_qhawax in hour:
            new_next_qhawax = list(next_qhawax)
            position_hour = new_next_qhawax[len(next_qhawax)-2:]
            indicePosicion = near_qhawaxs.index(position_hour)
            dictQhawaxMeasurement = {"position":indicePosicion,"next_qhawax":next_qhawax}
            hour_n.append(dictQhawaxMeasurement)
        new_sort_list_without_json.append(hour_n)

    result_measurement_by_hour = []
    for hour in new_sort_list_without_json:
        hour.sort(key=sortByPosition)
        result_hour=[]
        for next_qhawax in hour:
            result_hour.append(next_qhawax["next_qhawax"])
        result_hour = result_hour[:k]
        result_measurement_by_hour.append(result_hour)
    return result_measurement_by_hour

def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
    R = 6371 # Radius of the earth in km
    dLat = deg2rad(lat2-lat1)  #  deg2rad below
    dLon = deg2rad(lon2-lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c #  Distance in km
    return d

def getNearestStations(stations, lat , lon):
    return sorted(stations, key= lambda station:(getDistanceFromLatLonInKm(lat,lon, station[0], station[1])), reverse=False)

def sortByPosition(e):
    return e['position']

def deg2rad(deg):
    return deg * (math.pi/180)

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
    if(type(conjunto_de_datos_interpolacion_espacial) is list):
        conjunto_de_datos_interpolacion_espacial = np.array(conjunto_de_datos_interpolacion_espacial)
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
    conjunto_valores_predichos = [obtenerInterpolacionEnUnPunto(conjunto_de_datos_interpolacion_espacial, indice_columna_coordenadas_x, indice_columna_coordenadas_y, coordenada_x_prediccion, coordenada_y_prediccion) for conjunto_de_datos_interpolacion_espacial in lista_conjunto_de_datos_interpolacion_espacial]
    return np.array(conjunto_valores_predichos)

def verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values):
    for key,value in ALL_DICCIONARIO_INDICES_VARIABLES_PREDICCION.items(): 
        if(sensor_name==key):
            pollutant_array_json[key]=sensor_values
            continue
    return pollutant_array_json

def getPollutantID(json_data_pollutant,pollutant_name):
    for i in range(len(json_data_pollutant)):
        if(pollutant_name == json_data_pollutant[i]['pollutant_name']):
            return json_data_pollutant[i]['id']
    return None

def getDiccionaryListWithEachIndexColumn(sort_list_without_json):
    lista_diccionario_columnas_indice = []
    for conjunto_de_datos_interpolacion_espacial in sort_list_without_json:
        columnas_conjunto_de_datos_interpolacion_espacial = array_columns# list(conjunto_de_datos_interpolacion_espacial.columns) # ['']
        diccionario_columnas_indice = {}
        for i in range(len(columnas_conjunto_de_datos_interpolacion_espacial)):
            diccionario_columnas_indice[columnas_conjunto_de_datos_interpolacion_espacial[i]] = i
        
        lista_diccionario_columnas_indice.append(diccionario_columnas_indice)

    return lista_diccionario_columnas_indice