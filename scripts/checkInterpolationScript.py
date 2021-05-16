import script_helper as helper
import datetime
import dateutil.tz
import numpy as np
import requests
import json

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_ENV_STATION= BASE_URL_IA + 'api/get_all_env_station/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
DICCIONARIO_INDICES_VARIABLES_PREDICCION = {'CO': 0,'NO2': 1, 'PM25': 2}
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}
column_coordinates_x = 'lon'
column_coordinates_y = 'lat'
last_hours =12
weeks = 2

def getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks):
    list_of_hours = []
    #Hora del servidor menos la cantidad de semanas que solicita el usuario
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) - datetime.timedelta(weeks=weeks) 
    #Cantidad de horas que se vaya a utilizar como comparativo
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=last_hours-1)).strftime("%d-%m-%Y %H:%M:%S") 
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    for i in range(len(qhawax_array)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(qhawax_array[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        if len(hourly_processed_measurements) < last_hours/3: #La cantidad de horas debe ser mayor a la tercera parte de la cantidad total de horas permitidas.
            continue
        hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,qhawax_location[i],pollutant_array_json)
        list_of_hours.append(hourly_processed_measurements)
    return list_of_hours


if __name__ == '__main__':
	json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)      
	json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
	copy_all_env_station = json_all_env_station
	copy_all_qhawax_array = []
	
	for qhawax_index in range(len(json_all_env_station)):
		print("Entrando =================================")
		print(qhawax_index)
		element_to_interpolate = json_all_env_station[qhawax_index]
		json_all_env_station.remove(element_to_interpolate)  # removing third element
		#El qhawax que se tiene se retira del listado que se utilizara para la interpolacion
		print(element_to_interpolate)
		#print(json_all_env_station)
		qhawax_station_id, qhawax_array,qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
		#Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
		measurement_list = getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks)
		#Arreglo de jsons ordenados por hora del mas antiguo al mas actual
		sort_list_without_json = helper.sortListOfMeasurementPerHour(measurement_list,last_hours)
		list_of_indexs_column = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
		indice_columna_coordenadas_x = list_of_indexs_column[0][column_coordinates_x]
		indice_columna_coordenadas_y = list_of_indexs_column[0][column_coordinates_y]
		print("Entrando a k")
		for k in range(4,8):
			print(k)
			near_qhawaxs = helper.getNearestStations(qhawax_location, element_to_interpolate['lat'] , element_to_interpolate['lon'])
			print(near_qhawaxs)
			new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k)
			conjunto_valores_predichos = helper.obtenerListaInterpolacionesPasadasEnUnPunto(new_sort_list_without_json, indice_columna_coordenadas_x, indice_columna_coordenadas_y, element_to_interpolate['lat'], element_to_interpolate['lon'])
			conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
			for i in range(len(conjunto_valores_predichos)):
				for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
					pollutant_id = helper.getPollutantID(json_data_pollutant,key)
					if(pollutant_id!=None):
						spatial_json={"pollutant_id":int(pollutant_id),"ug_m3_value":round(float(conjunto_valores_predichos[i][value]),3),"hour_position":int(i+1)}
						print(spatial_json)
		#Crear nuevo arreglo con N-1 estaciones
		#Volver a igual con la lista original
		json_all_env_station = copy_all_env_station


	    