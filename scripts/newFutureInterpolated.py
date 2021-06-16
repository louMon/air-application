import requests
import datetime
import dateutil.tz
import json
import time
import multiprocessing
import numpy as np
import script_helper as helper
from global_constants import pool_size_future_interpolate, last_hours_future_interpolate,\
                             name_column_x, name_column_y, dictionary_of_var_index_prediction,k_value

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
STORE_FUTURE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_all_spatial_prediction/'
DELETE_ALL_FUTURE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_all_spatial_prediction/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_FUTURE_RECORDS = BASE_URL_IA + 'api/get_future_records_of_every_station/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_env_station/'
GET_UPDATED_QHAWAX = BASE_URL_QAIRA + '/api/QhawaxFondecyt/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'

#Global variables
index_column_x = None
index_column_y = None
json_data_pollutant = None
array_qhawax_location = None
sort_list_without_json = None
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'hour_position':[],'lat':[],'lon':[]}

def getListOfMeasurementOfAllModules(array_station_id,array_qhawax_location):
    list_of_hours = []
    for i in range(len(array_station_id)): #arreglo de los qhawaxs
        json_params = {'environmental_station_id': str(array_station_id[i])}
        response = requests.get(GET_HOURLY_FUTURE_RECORDS, params=json_params)
        if(response.text ==200):
            hourly_processed_measurements = response.json()
            if len(hourly_processed_measurements) < last_hours_future_interpolate/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
                continue
            hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,array_qhawax_location[i],pollutant_array_json)
            list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def iterateByGrids(grid_elem):
    near_qhawaxs = helper.getNearestStations(array_qhawax_location, grid_elem['lat'] , grid_elem['lon'])
    new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k_value)
    dataset_interpolated = helper.getListofPastInterpolationsAtOnePoint(new_sort_list_without_json, \
                                                                             index_column_x, \
                                                                             index_column_y, \
                                                                             grid_elem['lat'], \
                                                                             grid_elem['lon'])
    timestamp = datetime.datetime.now().replace(minute=0,second=0, microsecond=0) + datetime.timedelta(hours=1)
    for i in range(len(dataset_interpolated)):
        for key,value in dictionary_of_var_index_prediction.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                future_spatial_json={"pollutant_id":int(pollutant_id),"grid_id":int(grid_elem["id"]),
                                    "ug_m3_value":round(float(dataset_interpolated[i][value]),3) if(len(dataset_interpolated[i])>0) else None,
                                    "hour_position":int(i+25),"timestamp":str(timestamp)}
                response = requests.post(STORE_FUTURE_SPATIAL_PREDICTION, json=future_spatial_json)
        timestamp = timestamp + datetime.timedelta(hours=1)

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)

    all_qhawax_station = json.loads(requests.get(GET_UPDATED_QHAWAX).text)
    qWid_compid = helper.getQhawaxFirstVersion(all_qhawax_station)

    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    measurement_list = getListOfMeasurementOfAllModules(array_station_id,array_qhawax_location)
    sort_list_without_json = helper.sortListOfMeasurementPerHourFuture(measurement_list,last_hours_future_interpolate)
    dictionary_list_of_index_columns = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
    index_column_x = dictionary_list_of_index_columns[0][name_column_x]
    index_column_y = dictionary_list_of_index_columns[0][name_column_y]
    response_delete = requests.post(DELETE_ALL_FUTURE_SPATIAL_PREDICTION)
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":3,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    pool = multiprocessing.Pool(pool_size_future_interpolate)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()
    print("Empezo a las {a} y termino a las {b}".format(a=start_time,b=datetime.datetime.now()))
    print("===================================================================================")

