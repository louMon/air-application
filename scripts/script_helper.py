import math
import numpy as np
import pandas as pd
import datetime
import dateutil.tz
from pykrige.ok import OrdinaryKriging
from pykrige.uk import UniversalKriging
from scipy.interpolate import NearestNDInterpolator

dictionary_of_all_var_index= {'CO': 0,'NO2': 1,'PM25': 2,'timestamp_zone':3,'lat':4,'lon':5,'alt':6}
array_columns = ["CO","NO2","PM25","lat","lon"]
pm1_index = 4
pollutants = 3

def getDetailOfEnvStation(json_all_env_station):
    array_station_id = [env_station['id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=55) ] 
    array_module_id = [ env_station['module_id'] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=55) ] 
    array_qhawax_location = [ [env_station['lat'],env_station['lon']] for env_station in json_all_env_station if env_station['module_id']!=None and (env_station['module_id']>=37 and env_station['module_id']<=55) ]         
    return array_station_id,array_module_id,array_qhawax_location

def getQhawaxFirstVersion(all_qhawax):
    qWid_compid = []
    for qhawax in all_qhawax:
        if(qhawax["qhawax_id"]>=37):
            element = (qhawax["qhawax_id"],3)
            qWid_compid.append(element)
    return qWid_compid

def validLimitsByPollutant(pollutant, value_of_array):
    '''Reemplazo los valores extremos de cada contaminante por None para que no afecten la interpolacion'''
    new_value_of_array = []
    for each_value in value_of_array:
        if(type(each_value) is float):
            if pollutant =='CO':
                if(each_value<0 or each_value>30100):
                    each_value = None
            elif pollutant == 'NO2':
                if(each_value<0 or each_value>600): 
                    each_value = None
            elif pollutant == 'PM25':
                if(each_value<0 or each_value>500): 
                    each_value = None
        new_value_of_array.append(each_value)
    return new_value_of_array

#No completa nada de mediciones, solo coloca las posiciones
def completeHourlyValuesByQhawax(valid_processed_measurements,qhawax_specific_location,pollutant_array_json):
    average_valid_processed_measurement = []
    for sensor_name in valid_processed_measurements[0]:
        if(sensor_name in pollutant_array_json):
            '''Consolido todas las mediciones de esta estacion de las N horas'''
            sensor_values = [measurement[sensor_name] if(sensor_name in measurement) else None for measurement in valid_processed_measurements]
            sensor_values = validLimitsByPollutant(sensor_name, sensor_values)
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
                elif(key=='lon'):
                    json[key] =  qhawax_specific_location[1] 
        average_valid_processed_measurement.append(json)
    return average_valid_processed_measurement

def convertJsonToList(json_measurement):
    list_of_measurements = []
    for i in range(len(array_columns)):
        if(array_columns[i] in json_measurement):
            list_of_measurements.append(json_measurement[array_columns[i]])
    return list_of_measurements

def completeIncompleteHours(one_qhawax_measurements,initial_timestamp, final_timestamp,last_hours):
    pivot_time = initial_timestamp
    idx_measurement = 0
    new_one_qhawax_measurement = []
    lat = one_qhawax_measurements[idx_measurement]["lat"] if(len(one_qhawax_measurements)>0) else -1
    lon = one_qhawax_measurements[idx_measurement]["lon"] if(len(one_qhawax_measurements)>0) else -1
    if(lat!=-1 and lon!=-1):
        while len(new_one_qhawax_measurement) != last_hours:
            if(len(one_qhawax_measurements)>=idx_measurement+1):
                datetime_each_station = datetime.datetime.strptime(one_qhawax_measurements[idx_measurement]['timestamp_zone'], '%a, %d %b %Y %H:%M:%S GMT')
                element_to_complete = one_qhawax_measurements[idx_measurement]
                if(datetime_each_station!=pivot_time):
                    element_to_complete = {'CO': -1, 'NO2': -1, 'PM25': -1, 
                                            'timestamp_zone': str(pivot_time), 
                                            'lat': lat, 'lon': lon,'alt':0}
            else:
                element_to_complete = {'CO': -1, 'NO2': -1, 'PM25': -1, 
                                            'timestamp_zone': str(pivot_time), 
                                            'lat': lat, 'lon': lon, 'alt':0}
            new_one_qhawax_measurement.append(element_to_complete)
            idx_measurement+=1
            pivot_time = pivot_time + datetime.timedelta(hours=1)
    return new_one_qhawax_measurement   

def newSortListOfMeasurementPerHourScript(measurement_list,last_hours, weeks):
    final_timestamp = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=5) #hora del servidor
    if(weeks!=-1):
        final_timestamp = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) - datetime.timedelta(weeks=weeks) + datetime.timedelta(hours=5) #hora del servidor
    initial_timestamp = final_timestamp - datetime.timedelta(hours=last_hours-1)#cantidad de horas que se vaya a utilizar como comparativo
    same_lenght_measurement_list = []
    for qhawax_measurement in measurement_list:
        if(last_hours!=len(qhawax_measurement)): #La cantidad total de horas es diferente a la cantidad de mediciones por qhawax
            same_lenght_measurement_list.append(completeIncompleteHours(qhawax_measurement,initial_timestamp, final_timestamp,last_hours))
        else:
            same_lenght_measurement_list.append(qhawax_measurement)
    #Se toma cualquier elemento del arreglo ya que tiene la misma longitud
    sort_list_by_hour = []
    '''Se itera con las horas totales'''
    for index_hour in range(last_hours): #horas
        #Se itera con la cantidad de qhawaxs que existen
        new_hour_list = []
        for index_qhawax in range(len(measurement_list)): #qhawaxs
            new_hour_list.append(same_lenght_measurement_list[index_qhawax][index_hour])
        sort_list_by_hour.append(new_hour_list)
    return sort_list_by_hour

def helperSortBasedNearQhawaxs(near_qhawaxs, hour_measurement):
    for measurement in hour_measurement:
        for idx, qhawax in enumerate(near_qhawaxs):
            if(measurement["lat"] == qhawax[0] and measurement["lon"] == qhawax[1]):
                measurement["position"] =  idx
                continue
    return hour_measurement

def sortBasedNearQhawaxs(near_qhawaxs,sort_list_without_json):
    new_sort_list_without_json = []
    for qhawax_by_hour in sort_list_without_json:
        new_qhawax_by_hour = helperSortBasedNearQhawaxs(near_qhawaxs,qhawax_by_hour)
        new_qhawax_by_hour.sort(key = lambda x:x['position'])
        new_sort_list_without_json.append(new_qhawax_by_hour)
    return new_sort_list_without_json

def createJsonEachPollutant(pollutant,monitoring_stations,hour_n):
    dict_N= {pollutant:monitoring_stations[pollutant],'lat':monitoring_stations['lat'],
               'lon':monitoring_stations['lon'],'timestamp_zone':monitoring_stations['timestamp_zone'],
               'alt':monitoring_stations['alt']}
    hour_n.append(dict_N)
    return hour_n

def separatePollutants(removed_values_out_control):
    new_sort_list_without_json = []
    for qhawax_hour in removed_values_out_control:
        new_qhawax_hour = []
        for monitoring_stations in qhawax_hour:
            hour_n = []
            hour_n = createJsonEachPollutant('CO',monitoring_stations,hour_n)
            hour_n = createJsonEachPollutant('NO2',monitoring_stations, hour_n)
            hour_n = createJsonEachPollutant('PM25',monitoring_stations,hour_n)
            new_qhawax_hour.append(hour_n)
        new_sort_list_without_json.append(new_qhawax_hour)
    return new_sort_list_without_json

def getPollutantValuesWithException(qhawax_hour,k,pollutant_position,pollutant_name):
    count_to_k = 0
    hour_idx = 0
    result_array = []
    while count_to_k<k and hour_idx<len(qhawax_hour):
    #Iteramos por hora para obtener los valores diferentes de None por contaminante
    #Todos pertenecen a la misma hora y estan ordenados por la estacion mas cercana
        json_pollutant = qhawax_hour[hour_idx][pollutant_position]
        if(json_pollutant[pollutant_name]!=None):
            count_to_k+=1
            result_array.append(json_pollutant)
        hour_idx+=1
    #Devuelvo un arreglo de jsons por cada k del contaminante
    #[{},{},{}]
    return result_array

def newFilterMeasurementBasedOnNearestStations(sort_list_without_json,k):
    new_sort_list_without_json = []
    for qhawax_hour in sort_list_without_json:
        CO_array = getPollutantValuesWithException(qhawax_hour,k,0,'CO')
        NO2_array = getPollutantValuesWithException(qhawax_hour,k,1,'NO2')
        PM25_array = getPollutantValuesWithException(qhawax_hour,k,2,'PM25')
        array_sorted_k = []
        for index_station in range(len(CO_array)):
            final_array = []
            final_array.append(CO_array[index_station])
            final_array.append(NO2_array[index_station])
            final_array.append(PM25_array[index_station])
            array_sorted_k.append(final_array)
        new_sort_list_without_json.append(array_sorted_k)
    return new_sort_list_without_json
    
def removeOutControlValues(filtered_sort_list_without_json):
    '''Remuevo lo que se agrego dummy para tener las horas completas'''
    for qhawax_hour in filtered_sort_list_without_json:
        for measurement in qhawax_hour:
            if('position' in measurement.keys()):
                removed_value = measurement.pop('position')
    new_filtered_sort_list_without_json = []
    for qhawax_hour in filtered_sort_list_without_json:
        new_helper = []
        for measurement in qhawax_hour:
            if(measurement["CO"]!=-1):
                new_helper.append(measurement)
        new_filtered_sort_list_without_json.append(new_helper)
    return new_filtered_sort_list_without_json

def convertToNumpyMatrix(filtered_sort_list_without_json):
    numpy_complete =  []
    for qhawax_hour in filtered_sort_list_without_json:
        numpy_hour_measurement =  []
        for measurement in qhawax_hour:
            numpy_all_pollutant_measurement =  []
            #Aqui se tendria que agregar el arreglo que contenga los tres contaminantes de esa hora
            for pollutant_elem in measurement:
                numpy_measurement =  []
                for key, value in pollutant_elem.items():
                    if(key not in ['timestamp_zone']):
                        numpy_measurement.append(value)
                arr = np.array(numpy_measurement)
                numpy_all_pollutant_measurement.append(arr)
            numpy_hour_measurement.append(numpy_all_pollutant_measurement)
        arr_measurement = np.array(numpy_hour_measurement)
        numpy_complete.append(arr_measurement)
    return numpy_complete

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

#####Interpolacion con IDW
def getIDWInterpolation(x, y, z, xi, yi):
    distances = distanceMatrix(x,y, xi,yi)
    # IDW, weigths are the inverse of the distance inverse
    weigths = 1.0 / distances
    # Add all the weights to 1
    weigths /= weigths.sum(axis=0)
    # Multiply all the weights of each interpolated point by all the observed values ​​of the variable to be interpolated
    zi = np.dot(weigths.T, z)
    return zi

def areSame(arr):
    first = arr[0]  
    for i in range(1, len(arr)):
        if (arr[i] != first):
            return False  
    return True

def getPositionsOfInvalidValues(arr):
    invalid_positions_arr = []
    for idx_elem in range(len(arr)):
        if (arr[idx_elem] == -1):
            invalid_positions_arr.append(idx_elem)
    return invalid_positions_arr

#[2,4,9]
#[1,2,3,4,5,6,7,8,90,10]
def removeInvalidElements(arrPositions, arrElements):
    idx_positions = 0
    resulted_array = []
    if(len(arrPositions)!=0):
        for idx_elem in range(len(arrElements)):
            if(idx_positions<len(arrPositions)):
                if(idx_elem<arrPositions[idx_positions]):
                    resulted_array.append(arrElements[idx_elem])
                else: idx_positions+=1
            else: break
        return resulted_array
    return arrElements

#####Interpolacion con Krigging
def getKrigingInterpolation(x, y, z, xi, yi):
    invalid_positions = getPositionsOfInvalidValues(z)
    x = removeInvalidElements(invalid_positions, x)
    y = removeInvalidElements(invalid_positions, y)
    z = removeInvalidElements(invalid_positions, z)
    if(areSame(z)==False):
        #modelo_kriging = OrdinaryKriging(x, y, z, variogram_model="linear")
        modelo_kriging = OrdinaryKriging(x, y, z, variogram_model="gaussian")
        #modelo_kriging = OrdinaryKriging(x, y, z, variogram_model="exponential")
        #modelo_kriging = OrdinaryKriging(x, y, z, variogram_model="spherical")
        zi = modelo_kriging.execute("points", xi, yi)
        return zi[0]
    return None

####Interpolacion con NNIDW
def getNNIDWInterpolation(x, y, z, xi, yi):
    puntos_modelo = np.vstack((x, y)).T
    modelo_nnidw = NearestNDInterpolator(puntos_modelo, z)
    puntos_evaluacion = np.array([xi, yi])
    zi = modelo_nnidw(puntos_evaluacion)
    return zi[0]

def getPollutantName(pollutant_position):
    if(pollutant_position==0): return 'CO'
    elif(pollutant_position==1): return 'NO2'
    elif(pollutant_position==2): return 'PM25'
    else: None

def getPollutantAndPositionsArray(spatial_interpolation_dataset, pollutant_position,k):
    pollutant_array,lat_array,lon_array= [],[],[]
    for k_idx in range(len(spatial_interpolation_dataset)):
        pollutantName = getPollutantName(pollutant_position)
        pollutant_value = spatial_interpolation_dataset[k_idx][pollutantName]
        lat_value = spatial_interpolation_dataset[k_idx]['lat']
        lon_value = spatial_interpolation_dataset[k_idx]['lon']
        pollutant_array.append(pollutant_value)
        lat_array.append(lat_value)
        lon_array.append(lon_value)

        #pollutant_value = spatial_interpolation_dataset[k_idx][pollutantName][0]
        #lat_value = spatial_interpolation_dataset[k_idx][pollutantName][1]
        #lon_value = spatial_interpolation_dataset[k_idx][pollutantName][2]
        #pollutant_array.append(pollutant_value)
        #lat_array.append(lat_value)
        #lon_array.append(lon_value)
    return pollutant_array,lat_array,lon_array

def getInterpolationMethodInOnePoint(spatial_interpolation_dataset, predict_column_x, predict_column_y, k):
    if(type(spatial_interpolation_dataset) is list):
        spatial_interpolation_dataset = np.array(spatial_interpolation_dataset)
    predicted_values = []
    if(len(spatial_interpolation_dataset)>0):
        for polluntant_idx in range(pollutants):
            pollutant_array,lat_array,lon_array = getPollutantAndPositionsArray(spatial_interpolation_dataset, polluntant_idx,k)
            if(polluntant_idx==0): #Cuando es el caso de Monoxido de Carbono se aplica IDW (0=CO)
                value_interpolated = getIDWInterpolation(lon_array, lat_array, pollutant_array, predict_column_x, predict_column_y)
            else: #Cuando es el caso de NO2 o PM2.5 se aplica Kriging Gausiano
                value_interpolated = getKrigingInterpolation(lon_array, lat_array, pollutant_array, predict_column_x, predict_column_y)
            predicted_values.append(value_interpolated)
        predicted_values.insert(pm1_index, 0.0)
    return np.array(predicted_values)


def getListofPastInterpolationsAtOnePoint(spatial_interpolation_dataset_list, predict_column_x, predict_column_y,k):
    predicted_values_dataset = [getInterpolationMethodInOnePoint(spatial_interpolation_dataset, predict_column_x, predict_column_y,k) for spatial_interpolation_dataset in spatial_interpolation_dataset_list]
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
        columnas_conjunto_de_datos_interpolacion_espacial = array_columns
        diccionario_columnas_indice = {}
        for i in range(len(columnas_conjunto_de_datos_interpolacion_espacial)):
            diccionario_columnas_indice[columnas_conjunto_de_datos_interpolacion_espacial[i]] = i
        lista_diccionario_columnas_indice.append(diccionario_columnas_indice)
    return lista_diccionario_columnas_indice