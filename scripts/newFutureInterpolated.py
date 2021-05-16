import requests
import datetime
import dateutil.tz
import json
import time
import multiprocessing
import numpy as np
import script_helper as helper

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
STORE_FUTURE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/store_future_spatial_prediction/'
DELETE_ALL_FUTURE_SPATIAL_PREDICTION = BASE_URL_IA + 'api/delete_all_future_spatial_prediction/'
UPDATE_RUNNING_TIMESTAMP =BASE_URL_IA + 'api/update_timestamp_running/'
GET_HOURLY_FUTURE_RECORDS = BASE_URL_IA + 'api/get_future_records_of_every_station/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_env_station/'
GET_ALL_GRID = BASE_URL_IA + 'api/get_all_grid/'

DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0,'NO2': 1, 'PM25': 2}
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'hour_position':[],'lat':[],'lon':[]}
NOMBRE_COLUMNA_COORDENADAS_X = 'lon'
NOMBRE_COLUMNA_COORDENADAS_Y = 'lat'

indice_columna_coordenadas_x = None
indice_columna_coordenadas_y = None
sort_list_without_json = None
json_data_pollutant = None
QHAWAX_LOCATION = None
pool_size = 6
LAST_HOURS =6

def getListOfMeasurementOfAllModules(STATION_ID,QHAWAX_LOCATION):
    list_of_hours = []
    for i in range(len(STATION_ID)): #arreglo de los qhawaxs
        json_params = {'environmental_station_id': str(STATION_ID[i])}
        response = requests.get(GET_HOURLY_FUTURE_RECORDS, params=json_params)
        hourly_processed_measurements = response.json()
        if len(hourly_processed_measurements) < LAST_HOURS/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
            continue
        hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,QHAWAX_LOCATION[i],pollutant_array_json)
        list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

def iterateByGrids(grid_elem):
    near_qhawaxs = helper.getNearestStations(QHAWAX_LOCATION, grid_elem['lat'] , grid_elem['lon'])
    new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k=4)

    conjunto_valores_predichos = helper.obtenerListaInterpolacionesPasadasEnUnPunto(new_sort_list_without_json, \
                                                                             indice_columna_coordenadas_x, \
                                                                             indice_columna_coordenadas_y, \
                                                                             grid_elem['lat'], \
                                                                             grid_elem['lon'])
    conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
    for i in range(len(conjunto_valores_predichos)):
        for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
            pollutant_id = helper.getPollutantID(json_data_pollutant,key)
            if(pollutant_id!=None):
                future_spatial_json={"pollutant_id":int(pollutant_id),"grid_id":int(grid_elem["id"]),"ppb_value":None,
                              "ug_m3_value":round(float(conjunto_valores_predichos[i][value]),3),"hour_position":int(i+1)}
                response = requests.post(STORE_FUTURE_SPATIAL_PREDICTION, json=future_spatial_json)

if __name__ == '__main__':
    start_time = time.time()
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    STATION_ID, QHAWAX_ARRAY,QHAWAX_LOCATION = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    response_delete = requests.post(DELETE_ALL_FUTURE_SPATIAL_PREDICTION)

    #Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
    measurement_list = getListOfMeasurementOfAllModules(STATION_ID,QHAWAX_LOCATION)
    #Arreglo de jsons ordenados por hora del mas antiguo al mas actual
    sort_list_without_json = helper.sortListOfMeasurementPerHour(measurement_list,LAST_HOURS)
    #Interpolando
    lista_diccionario_columnas_indice = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
    indice_columna_coordenadas_x = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_X]
    indice_columna_coordenadas_y = lista_diccionario_columnas_indice[0][NOMBRE_COLUMNA_COORDENADAS_Y]
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":3,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    
    pool = multiprocessing.Pool(pool_size)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()

    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.datetime.now())
    print("===================================================================================")

