import requests
import datetime
import dateutil.tz
import json
import time
import multiprocessing
import numpy as np
import script_helper as helper

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
STORE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_spatial_prediction/'
DELETE_ALL_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_all_spatial_prediction/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_env_station/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'

dictionary_of_var_index_prediction= {'CO': 0,'NO2': 1, 'PM25': 2}
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}
name_column_x = 'lon'
name_column_y = 'lat'

index_column_x = None
index_column_y = None
sort_list_without_json = None
json_data_pollutant = None
array_qhawax_location = None
pool_size = 8
last_hours =24

def getListOfMeasurementOfAllModules(array_module_id,array_qhawax_location):
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=last_hours-1)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(array_module_id)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(array_module_id[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        if len(hourly_processed_measurements) < last_hours/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
            continue
        hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,array_qhawax_location[i],pollutant_array_json)
        list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def iterateByGrids(grid_elem):
    near_qhawaxs = helper.getNearestStations(array_qhawax_location, grid_elem['lat'] , grid_elem['lon'])
    new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k=4)
    conjunto_valores_predichos = helper.getListofPastInterpolationsAtOnePoint(new_sort_list_without_json, \
                                                                             index_column_x, \
                                                                             index_column_y, \
                                                                             grid_elem['lat'], \
                                                                             grid_elem['lon'])
    conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
    for i in range(len(conjunto_valores_predichos)):
        for key,value in dictionary_of_var_index_prediction.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                spatial_json={"pollutant_id":int(pollutant_id),"grid_id":int(grid_elem["id"]),"ppb_value":None,
                              "ug_m3_value":round(float(conjunto_valores_predichos[i][value]),3),"hour_position":int(i+1)}
                response = requests.post(STORE_SPATIAL_PREDICTION, json=spatial_json)

if __name__ == '__main__':
    start_time = time.time()
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    response_delete = requests.post(DELETE_ALL_SPATIAL_PREDICTION)
    #Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
    measurement_list = getListOfMeasurementOfAllModules(array_module_id,array_qhawax_location)
    #Arreglo de jsons ordenados por hora del mas antiguo al mas actual
    sort_list_without_json = helper.sortListOfMeasurementPerHour(measurement_list,last_hours)
    #Interpolando
    dictionary_list_of_index_columns = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
    index_column_x = dictionary_list_of_index_columns[0][name_column_x]
    index_column_y = dictionary_list_of_index_columns[0][name_column_y]
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    pool = multiprocessing.Pool(pool_size)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()

    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.datetime.now())
    print("===================================================================================")

