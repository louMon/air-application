import concurrent.futures
import itertools
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

hours_number = 5
hours=[]

def iterateGridsByPollutantsByHours(param):
    spatial_json={"pollutant_id":param[1]["id"],"grid_id":param[0]["id"],"ppb_value":None,\
                  "ug_m3_value":random.randrange(850),"hour_position":param[2]}
    response = requests.post(BASE_URL + STORE_SPATIAL_PREDICTION, json=spatial_json)

if __name__ == '__main__':
    start_time = time.time()
    hours = range(1,hours_number+1)  #[1,2,3,4,5,6,7,8,9]
    json_data_grid = json.loads(requests.get(BASE_URL + GET_ALL_GRID).text) #[1,2,3,4]
    json_data_pollutant = json.loads(requests.get(BASE_URL + GET_ALL_POLLUTANTS).text) #[1,2,3]
    response_delete = requests.post(BASE_URL + DELETE_ALL_SPATIAL_PREDICTION)

    paramlist = list(itertools.product(json_data_grid,json_data_pollutant,hours))

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(iterateGridsByPollutantsByHours,paramlist)

    response = requests.post(BASE_URL + UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})
    print("--- %s seconds ---" % (time.time() - start_time))