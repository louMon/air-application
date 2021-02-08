import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
import random
import time

#BASE_URL = 'http://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL = 'http://0.0.0.0:5000/'

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
hours = 24
response_delete = requests.post(BASE_URL + DELETE_ALL_SPATIAL_PREDICTION)
timestamp_utc = datetime.datetime.now(dateutil.tz.tzutc())

#response_measurements = requests.get(BASE_URL + GET_LAST_HOUR_MEASUREMENT,params={'initial_timestamp':timestamp_utc,'final_timestamp':timestamp_utc})
#print(response_measurements)
#measurements_real_points = json.loads(response_measurements.text)
for grid in json_data_grid: ##Itero por cada grilla
    print('Processing grid from  (%s)...' % (str(grid['lat'])))

    for pollutant in json_data_pollutant: ## Itero por cada contaminante
        for i in range(hours): ##Itero las 24 horas hacia atras de dicho contaminante en dicha grilla
        	#puntuaciones = repetir_evaluacion_interpolacion_conjuntos_de_datos(measurements_real_points, 
            #                                pollutant["pollutant_name"], grid["lat"], grid["lon"],NOMBRE_MODELO)
			#listas_valores_variable_interpolacion_reales = [puntuacion[1] for puntuacion in puntuaciones]
			#listas_valores_variable_interpolacion_interpolados = [puntuacion[2] for puntuacion in puntuaciones]
            spatial_json={"pollutant_id":pollutant["id"],"grid_id":grid["id"],\
                      "ppb_value":None,"ug_m3_value":random.randrange(850),"hour_position":i}
            response = requests.post(BASE_URL + STORE_SPATIAL_PREDICTION, json=spatial_json)

response = requests.post(BASE_URL + UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0,second=0, microsecond=0))})

print("--- %s seconds ---" % (time.time() - start_time))