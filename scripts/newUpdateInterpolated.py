import requests
import datetime
import dateutil.tz
import json
import csv
import time
import os
import multiprocessing
import numpy as np
import pandas as pd
import script_helper as helper
from global_constants import pool_size_historical_interpolate, last_hours_historical_interpolate,\
                             name_column_x, name_column_y, dictionary_of_var_index_prediction,k_value

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
STORE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_all_spatial_prediction/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_env_station/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'
DELETE_ALL_FUTURE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_total_spatial_prediction/'

FILE_ADDRESS = '/var/www/html/air-application/historical_measurements.csv'
#FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/historical_measurements.csv'

#Global variables
index_column_x = None
index_column_y = None
json_data_pollutant = None
array_qhawax_location = None
sort_list_without_json = None
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}

def getListOfMeasurementOfAllModules(array_module_id,array_qhawax_location):
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=last_hours_historical_interpolate-1)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(array_module_id)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(array_module_id[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        if len(hourly_processed_measurements) < last_hours_historical_interpolate/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
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
    timestamp = datetime.datetime.now().replace(minute=0,second=0, microsecond=0) - datetime.timedelta(hours=24)
    for i in range(len(dataset_interpolated)):
        for key,value in dictionary_of_var_index_prediction.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                with open(FILE_ADDRESS, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([int(pollutant_id), int(grid_elem["id"]), round(float(dataset_interpolated[i][value]),3) if(len(dataset_interpolated[i])>0) else None,int(i+1),str(timestamp)])
        timestamp = timestamp + datetime.timedelta(hours=1)

def saveMeasurement(row):
    spatial_json={"pollutant_id":row[0],"grid_id":row[1],"ug_m3_value":None if(row[2]=='') else row[2],"hour_position":row[3],"timestamp":row[4]}
    response = requests.post(STORE_SPATIAL_PREDICTION, json=spatial_json)

if __name__ == '__main__':
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    measurement_list = getListOfMeasurementOfAllModules(array_module_id,array_qhawax_location)
    sort_list_without_json = helper.sortListOfMeasurementPerHourHistorical(measurement_list,last_hours_historical_interpolate)
    dictionary_list_of_index_columns = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
    index_column_x = dictionary_list_of_index_columns[0][name_column_x]
    index_column_y = dictionary_list_of_index_columns[0][name_column_y]
    
    pool = multiprocessing.Pool(pool_size_historical_interpolate)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()
    
    start_time = datetime.datetime.now()
    #Borrado de datos de la tabla temporal
    response_delete = requests.post(DELETE_ALL_FUTURE_SPATIAL_PREDICTION)
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    with open(FILE_ADDRESS) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        pool = multiprocessing.Pool(pool_size_historical_interpolate)
        pool_results = pool.map(saveMeasurement, csv_reader)
        pool.close()
        pool.join()
            
    os.remove(FILE_ADDRESS)
    print("File Removed!")
    print("Luego de terminar los calculos {a} y luego de leer cada json {b}".format(a=start_time,b=datetime.datetime.now()))
    print("===================================================================================")
    #Registro de datos en la tabla temporal

