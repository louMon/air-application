import concurrent.futures
import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
import random
import time

BASE_URL = 'https://pucp-calidad-aire-api.qairadrones.com/'
#BASE_URL = 'http://0.0.0.0:5000/'

GET_ALL_POLLUTANTS = 'api/get_all_pollutants/'
GET_ALL_GRID = 'api/get_all_grid/'
STORE_SPATIAL_PREDICTION ='api/store_spatial_prediction/'
DELETE_ALL_SPATIAL_PREDICTION ='api/delete_all_spatial_prediction/'
GET_LAST_HOUR_MEASUREMENT ='api/air_quality_measurements_period_all_modules/'
UPDATE_RUNNING_TIMESTAMP ='api/update_timestamp_running/'
NOMBRE_MODELO = 'IDW'

response_grid = requests.get(BASE_URL + GET_ALL_GRID)
json_data_grid = json.loads(response_grid.text)

response_pollutants = requests.get(BASE_URL + GET_ALL_POLLUTANTS)
json_data_pollutant = json.loads(response_pollutants.text)

start_time = time.time()
hours = []
hours_number = 24
response_delete = requests.post(BASE_URL + DELETE_ALL_SPATIAL_PREDICTION)
#timestamp_utc = datetime.datetime.now() #debe tener este formato 07-02-2021 00:00:00, pero tiene 2021-02-07 23:31:42.317236
#response_measurements = requests.get(BASE_URL + GET_LAST_HOUR_MEASUREMENT,params={'initial_timestamp':timestamp_utc,'final_timestamp':timestamp_utc})
#print(response_measurements)
#measurements_real_points = json.loads(response_measurements.text)

def iterateGrids(grid):
    print('Processing grid from ' +(str(grid['lat']))+' , '+(str(grid['lon'])))
    for pollutant in json_data_pollutant: ## Itero por cada contaminante
        for i in range(hours_number): ##Itero las 24 horas hacia atras de dicho contaminante en dicha grilla
            spatial_json={"pollutant_id":pollutant["id"],"grid_id":grid["id"],"ppb_value":None,\
                          "ug_m3_value":random.randrange(850),"hour_position":i}
            response = requests.post(BASE_URL + STORE_SPATIAL_PREDICTION, json=spatial_json)

if __name__ == '__main__':
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(iterateGrids,json_data_grid)

response = requests.post(BASE_URL + UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})

print("--- %s seconds ---" % (time.time() - start_time))