from multiprocessing import Pool
import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
import random
#BASE_URL = 'https://qairamapnapi.qairadrones.com/'
BASE_URL = 'http://0.0.0.0:5000/'
GET_ALL_POLLUTANTS = 'api/get_all_pollutants/'
GET_ALL_GRID = 'api/get_all_grid/'
STORE_SPATIAL_PREDICTION ='api/store_spatial_prediction/'

response_grid = requests.get(BASE_URL + GET_ALL_GRID)
json_data_grid = json.loads(response_grid.text)

response_pollutants = requests.get(BASE_URL + GET_ALL_POLLUTANTS)
json_data_pollutant = json.loads(response_pollutants.text)

hours = 24

def getSpatialPrediction(pollutant,grid):
    for i in range(hours): ##Itero las 24 horas hacia atras de dicho contaminante en dicha grilla
        #Obtener informacion de los qhawax de las ultimas horas
        #IDW 
        #Mediciones horarias de los qhawax
        spatial_json={"pollutant_id":pollutant["id"],"grid_id":grid["id"],\
                  "ppb_value":random.randrange(1000),"ug_m3_value":random.randrange(850),"hour_position":i}

        print(spatial_json)
        response = requests.post(BASE_URL + STORE_SPATIAL_PREDICTION, json=spatial_json)
        print(response.text)


def interpolatedFunction():
    for grid in json_data_grid: ##Itero por cada grilla
        print('Processing grid from  (%s)...' % (str(grid['lat'])))

        for pollutant in json_data_pollutant: ## Itero por cada contaminante
            print('Processing pollutant from  %s...' % (pollutant['pollutant_name']))
            getSpatialPrediction(pollutant,grid)

if __name__ == '__main__':
    pool = Pool()                         # Create a multiprocessing Pool
    pool.map(process_image, data_inputs)  # process data_inputs iterable with pool
