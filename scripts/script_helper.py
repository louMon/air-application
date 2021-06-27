import math
import numpy as np
import pandas as pd
import datetime
import dateutil.tz

dictionary_of_all_var_index= {'CO': 0,'NO2': 1,'PM25': 2,'timestamp_zone':3,'lat':4,'lon':5,'alt':6}
dictionary_of_var_index_prediction= {'CO': 0,'NO2': 1, 'PM25': 2}
array_columns = ["CO","NO2","PM25","lat","lon"]
pm1_index = 4

def getDetailOfEnvStation(json_all_env_station):
    array_station_id = [env_station['id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ] 
    array_module_id = [ env_station['module_id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ] 
    array_qhawax_location = [ [env_station['lat'],env_station['lon']] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=54) ]         
    return array_station_id,array_module_id,array_qhawax_location

def getQhawaxFirstVersion(all_qhawax):
    qWid_compid = []
    for qhawax in all_qhawax:
        if(qhawax["qhawax_id"]>=37):
            element = (qhawax["qhawax_id"],3)
            qWid_compid.append(element)
    return qWid_compid

def completeHourlyValuesByQhawax(valid_processed_measurements,qhawax_specific_location,pollutant_array_json):
    average_valid_processed_measurement = []
    '''Recorro por contaminante para verificar None'''
    for sensor_name in valid_processed_measurements[0]: 
        if(sensor_name in pollutant_array_json):
            '''Consolido todas las mediciones de esta estacion de las 24 horas'''
            sensor_values = [measurement[sensor_name] if(sensor_name in measurement) else None for measurement in valid_processed_measurements]
            if(None in sensor_values) and not all([value is None for value in sensor_values]):
                df_sensor =pd.DataFrame(sensor_values)
                df_sensor = df_sensor.interpolate(method="linear",limit=4,limit_direction='both')
                sensor_values = df_sensor[0].tolist() 
            pollutant_array_json = verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values)
    '''Recorro por la cantidad de horas de cada qhawax (algunos tienen 24, 23, 22..) '''
    for elem_hour in range(len(valid_processed_measurements)): 
        json = {}
        '''Recorro por la cantidad de elementos para rearmar el json''' 
        for key,value in pollutant_array_json.items(): 
            if(value!=[]):
                json[key] = pollutant_array_json[key][elem_hour]
                '''Actualizar posiciones de los qhawaxs con su real ubicacion'''
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

def sortListOfMeasurementPerHourHistorical(measurement_list,last_hours):
    final_timestamp = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) #+ datetime.timedelta(hours=5) #hora del servidor
    initial_timestamp = final_timestamp - datetime.timedelta(hours=last_hours-1)#cantidad de horas que se vaya a utilizar como comparativo
    sort_list_by_hour = []
    total_number_stations = len(measurement_list)
    pivot_initial = initial_timestamp
    '''Se itera con las horas totales'''
    for index_hour in range(last_hours): 
        '''Se inicializa el arreglo de mediciones por hora'''
        hour_list = [] 
        ''' Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos '''
        for index_station in range(total_number_stations):
            if(index_hour<len(measurement_list[index_station])):
                if("timestamp_zone" in measurement_list[index_station][index_hour]):
                    datetime_each_station = datetime.datetime.strptime(measurement_list[index_station][index_hour]["timestamp_zone"], '%a, %d %b %Y %H:%M:%S GMT')
                    if(datetime_each_station == pivot_initial):
                        list_measurement_by_qhawax_by_hour = convertJsonToList(measurement_list[index_station][index_hour])
                        list_measurement_by_qhawax_by_hour = np.array(list_measurement_by_qhawax_by_hour)
                        hour_list.append(list_measurement_by_qhawax_by_hour)
                    else:
                        measurement_list[index_station].insert(0, {})
        pivot_initial = pivot_initial + datetime.timedelta(hours=1)
        new_hour_list = []
        for index_new_hour in range(len(hour_list)):
            flag=False
            for index_new_station in range(len(hour_list[index_new_hour])):
                if(hour_list[index_new_hour][index_new_station] == None or math.isnan(hour_list[index_new_hour][index_new_station])):
                    flag=True
                    continue
            if(flag==False and len(hour_list[index_new_hour])>0):
                new_hour_list.append(hour_list[index_new_hour])
        new_hour_list = np.array(new_hour_list)
        sort_list_by_hour.append(new_hour_list)
    return sort_list_by_hour

def sortListOfMeasurementPerHourScript(measurement_list,last_hours,weeks):
    final_timestamp = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) - datetime.timedelta(weeks=weeks) + datetime.timedelta(hours=5) #hora del servidor
    initial_timestamp = final_timestamp - datetime.timedelta(hours=last_hours-1)#cantidad de horas que se vaya a utilizar como comparativo
    sort_list_by_hour = []
    total_number_stations = len(measurement_list)
    pivot_initial = initial_timestamp
    '''Se itera con las horas totales'''
    for index_hour in range(last_hours):
        '''Se inicializa el arreglo de mediciones por hora'''
        hour_list = []
        ''' Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos '''
        for index_station in range(total_number_stations):
            if(index_hour<len(measurement_list[index_station])):
                if("timestamp_zone" in measurement_list[index_station][index_hour]):
                    datetime_each_station = datetime.datetime.strptime(measurement_list[index_station][index_hour]["timestamp_zone"], '%a, %d %b %Y %H:%M:%S GMT')
                    if(datetime_each_station == pivot_initial):
                        list_measurement_by_qhawax_by_hour = convertJsonToList(measurement_list[index_station][index_hour])
                        list_measurement_by_qhawax_by_hour = np.array(list_measurement_by_qhawax_by_hour)
                        hour_list.append(list_measurement_by_qhawax_by_hour)
                    else:
                        measurement_list[index_station].insert(0, {})
        pivot_initial = pivot_initial + datetime.timedelta(hours=1)
        new_hour_list = []
        for index_new_hour in range(len(hour_list)):
            flag=False
            for index_new_station in range(len(hour_list[index_new_hour])):
                if(hour_list[index_new_hour][index_new_station] == None or math.isnan(hour_list[index_new_hour][index_new_station])):
                    flag=True
                    continue
            if(flag==False and len(hour_list[index_new_hour])>0):
                new_hour_list.append(hour_list[index_new_hour])
        new_hour_list = np.array(new_hour_list)
        sort_list_by_hour.append(new_hour_list)
    return sort_list_by_hour

def sortListOfMeasurementPerHourFuture(measurement_list,last_hours):
    sort_list_by_hour = []
    for i in range(last_hours):
        hour_n = []
        ''' Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos '''
        for j in range(len(measurement_list)): 
            '''Para evitar que se caiga cuando un qhawax le faltan horas en el periodo buscado'''
            if(last_hours <= len(measurement_list[j])): 
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

def sortByPosition(e):
    return e['position']

def deg2rad(deg):
    return deg * (math.pi/180)

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

def distanceMatrix(x0, y0, x1, y1):
    observado = np.vstack((x0, y0)).T
    interpolado = np.vstack((x1, y1)).T
    d0 = np.subtract.outer(observado[:,0], interpolado[:,0])
    d1 = np.subtract.outer(observado[:,1], interpolado[:,1])
    return np.hypot(d0, d1)

def getIDWInterpolation(x, y, z, xi, yi):
    distances = distanceMatrix(x,y, xi,yi)
    # IDW, weigths are the inverse of the distance inverse
    weigths = 1.0 / distances
    # Add all the weights to 1
    weigths /= weigths.sum(axis=0)
    # Multiply all the weights of each interpolated point by all the observed values ​​of the variable to be interpolated
    zi = np.dot(weigths.T, z)
    return zi

def getInterpolationMethonInOnePoint(spatial_interpolation_dataset, index_column_x, index_column_y, predict_column_x, predict_column_y):
    if(type(spatial_interpolation_dataset) is list):
        spatial_interpolation_dataset = np.array(spatial_interpolation_dataset)
    predicted_values = []
    if(len(spatial_interpolation_dataset)>0):
        spatial_interpolation_dataset_x_column = spatial_interpolation_dataset[:, index_column_x]
        spatial_interpolation_dataset_y_column = spatial_interpolation_dataset[:, index_column_y]
        for indice_columna_interpolacion in dictionary_of_var_index_prediction.values():
            spatial_interpolation_dataset_values = spatial_interpolation_dataset[:, indice_columna_interpolacion]
            value_interpolated = getIDWInterpolation(spatial_interpolation_dataset_x_column, spatial_interpolation_dataset_y_column, spatial_interpolation_dataset_values, predict_column_x, predict_column_y)
            predicted_values.append(value_interpolated)
        predicted_values.insert(pm1_index, 0.0)
    return np.array(predicted_values)


def getListofPastInterpolationsAtOnePoint(spatial_interpolation_dataset_list, index_column_x, index_column_y, predict_column_x, predict_column_y):
    predicted_values_dataset = [getInterpolationMethonInOnePoint(spatial_interpolation_dataset, index_column_x, index_column_y, predict_column_x, predict_column_y) for spatial_interpolation_dataset in spatial_interpolation_dataset_list]
    return np.array(predicted_values_dataset)

def verifyPollutantSensor(sensor_name,pollutant_array_json,sensor_values):
    for key,value in dictionary_of_all_var_index.items(): 
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