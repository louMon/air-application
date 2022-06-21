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
from global_constants import pool_size_interpolate, last_hours_future_interpolate, last_hours_historical_interpolate, \
                             name_column_x, name_column_y, dictionary_of_var_index_prediction,k_value

#BASE_URL_IA = 'http://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_IA = 'https://air-quality.pucp-air-quality.cloudns.ph/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_fondecyt_env_station/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'
GET_HOURLY_FUTURE_RECORDS = BASE_URL_IA + 'api/get_future_records_of_every_station/'
#GET_UPDATED_QHAWAX = BASE_URL_QAIRA + '/api/QhawaxFondecyt/'

TEMPORAL_FILE_ADDRESS = '/var/www/html/air-application/temporal_file.csv'
ORIGINAL_FILE_ADDRESS = '/var/www/html/air-application/original_file.csv'
#TEMPORAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/temporal_file.csv'
#ORIGINAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/original_file.csv'

#Global variables
json_data_pollutant = None
array_qhawax_location = None
sort_list_without_json = None
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}

def getListOfMeasurementOfAllModulesHistoricalSpatial(array_module_id,array_qhawax_location):
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) #hora del servidor
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=last_hours_historical_interpolate-1)).strftime("%d-%m-%Y %H:%M:%S") #cantidad de horas que se vaya a utilizar como comparativo
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(array_module_id)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(array_module_id[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        if(len(hourly_processed_measurements)>0):
            hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,array_qhawax_location[i],pollutant_array_json)
            list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def getListOfMeasurementOfAllModulesFutureSpatial(array_station_id,array_qhawax_location):
    list_of_hours = []
    for i in range(len(array_station_id)): #arreglo de los qhawaxs
        json_params = {'environmental_station_id': str(array_station_id[i])}
        response = requests.get(GET_HOURLY_FUTURE_RECORDS, params=json_params)
        if response.status_code == 200:
            hourly_processed_measurements = response.json()
            if(len(hourly_processed_measurements)>0):
                hourly_processed_measurements = helper.completeHourlyValuesByQhawaxFutureInterpolation(hourly_processed_measurements,array_qhawax_location[i],pollutant_array_json)
                list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def iterateByGridsHistorical(grid_elem):
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    measurement_list = getListOfMeasurementOfAllModulesHistoricalSpatial(array_module_id,array_qhawax_location)
    sort_list_without_json = helper.newSortListOfMeasurementPerHourScript(measurement_list,last_hours_historical_interpolate,-1)
    near_qhawaxs = helper.getNearestStations(array_qhawax_location,grid_elem['lat'] , grid_elem['lon'])
    new_sort_list_without_json = helper.sortBasedNearQhawaxs(near_qhawaxs,sort_list_without_json)
    removed_values_out_control = helper.removeOutControlValues(new_sort_list_without_json)
    #Separar los contaminantes por hora en un json
    separated_sort_list_without_json = helper.separatePollutants(removed_values_out_control)
    filtered_sort_list_without_json = helper.newFilterMeasurementBasedOnNearestStations(separated_sort_list_without_json,k_value)
    convertToNumpyMatrix = helper.convertToNumpyMatrix(filtered_sort_list_without_json)
    dataset_interpolated = helper.getListofPastInterpolationsAtOnePoint(new_sort_list_without_json, \
                                                                             grid_elem['lat'], \
                                                                             grid_elem['lon'],k_value)
    timestamp = datetime.datetime.now().replace(minute=0,second=0, microsecond=0) - datetime.timedelta(hours=24)
    for i in range(len(dataset_interpolated)):
        for key,value in dictionary_of_var_index_prediction.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                with open(TEMPORAL_FILE_ADDRESS, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([int(pollutant_id), grid_elem['lat'], grid_elem['lon'], round(float(dataset_interpolated[i][value]),3) if(len(dataset_interpolated[i])>0) else None,int(i+1),timestamp])
        timestamp = timestamp + datetime.timedelta(hours=1)

def iterateByGridsFuture(grid_elem):
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    #all_qhawax_station = json.loads(requests.get(GET_UPDATED_QHAWAX).text)
    measurement_list = getListOfMeasurementOfAllModulesFutureSpatial(array_station_id,array_qhawax_location)
    sort_list_without_json = helper.newSortListOfMeasurementPerHourScript(measurement_list,last_hours_future_interpolate,-1)

    near_qhawaxs = helper.getNearestStations(array_qhawax_location, grid_elem['lat'] , grid_elem['lon'])
    new_sort_list_without_json = helper.sortBasedNearQhawaxs(near_qhawaxs,sort_list_without_json)
    removed_values_out_control = helper.removeOutControlValues(new_sort_list_without_json)
    #Separar los contaminantes por hora en un json
    separated_sort_list_without_json = helper.separatePollutantsFuture(removed_values_out_control)
    filtered_sort_list_without_json = helper.newFilterMeasurementBasedOnNearestStations(separated_sort_list_without_json,k_value)
    convertToNumpyMatrix = helper.convertToNumpyMatrix(filtered_sort_list_without_json)
    dataset_interpolated = helper.getListofPastInterpolationsAtOnePointFuture(convertToNumpyMatrix, \
                                                                        grid_elem['lat'],grid_elem['lon'],k_value)
    timestamp = datetime.datetime.now().replace(minute=0,second=0, microsecond=0) + datetime.timedelta(hours=1)
    for i in range(len(dataset_interpolated)):
        for key,value in dictionary_of_var_index_prediction.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                with open(TEMPORAL_FILE_ADDRESS, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([int(pollutant_id), grid_elem['lat'], grid_elem['lon'], round(float(dataset_interpolated[i][value]),3) if(len(dataset_interpolated[i])>0) else None,int(i+25),timestamp])
        timestamp = timestamp + datetime.timedelta(hours=1)

def saveMeasurement(row):
    with open(ORIGINAL_FILE_ADDRESS, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([int(row[0]), row[1],row[2], None if(row[3]=='') else row[3],row[4],row[5]])

if __name__ == '__main__':
    
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":3,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    #General variables
    #json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    #array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    #json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 

    print("Iniciando interpolacion espacial pasada")
    print(datetime.datetime.now())

    #Spatial Interpolation
    print("************************************Spatial Interpolation**********************************************")
    pool = multiprocessing.Pool(pool_size_interpolate)
    pool_results = pool.map(iterateByGridsHistorical, json_data_grid)
    pool.close()
    pool.join()
    print("Termine interpolacion espacial pasada")
    print(datetime.datetime.now())

    time.sleep(100)

    print("Iniciando interpolacion espacial futura")
    print(datetime.datetime.now())

    #Future Interpolation
    print("************************************Future Interpolation**********************************************")
    pool = multiprocessing.Pool(pool_size_interpolate)
    pool_results = pool.map(iterateByGridsFuture, json_data_grid)
    pool.close()
    pool.join()
    
    print("Termine interpolacion espacial futura")
    print(datetime.datetime.now())

    time.sleep(100)

    print("Iniciando escritura")
    print(datetime.datetime.now())

    #Borrado de datos de la tabla original
    if(os.path.isfile(ORIGINAL_FILE_ADDRESS)):
        print("Remuevo archivo original")
        os.remove(ORIGINAL_FILE_ADDRESS)

    print("Escribiendo en el origen!!")
    print(datetime.datetime.now())

    os.system("cp temporal_file.csv original_file.csv")

    #with open(TEMPORAL_FILE_ADDRESS) as csv_file:
    #    csv_reader = csv.reader(csv_file, delimiter=',')
    #    pool = multiprocessing.Pool(pool_size_interpolate)
    #    pool_results = pool.map(saveMeasurement, csv_reader)
    #    pool.close()
    #    pool.join()
    #Borrado de datos de la tabla temporal       
    os.remove(TEMPORAL_FILE_ADDRESS)
    print("Remuevo archivo temporal")
    print("Luego de terminar los calculos {a} y luego de leer cada json {b}".format(a=start_time,b=datetime.datetime.now()))
    print("===================================================================================")
