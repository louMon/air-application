import script_helper as helper
import datetime
import dateutil.tz
import numpy as np
import requests
import json
import pandas as pd

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_FONDECYT_ENV_STATION= 'http://0.0.0.0:5000/' + 'api/get_all_fondecyt_env_station/'
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
    print("La fecha inicial es: {}".format(initial_timestamp))
    print("La fecha final es: {}".format(final_timestamp))
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
	json_all_env_station = json.loads(requests.get(GET_ALL_FONDECYT_ENV_STATION).text)      
	json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
	copy_all_env_station = np.asarray(json_all_env_station)
	number_of_iteration = len(json_all_env_station)
	for qhawax_index in range(number_of_iteration):
		print("Entrando =================================")
		element_to_interpolate = json_all_env_station[qhawax_index]
		json_all_env_station.remove(element_to_interpolate)  # removing third element
		#El qhawax que se tiene se retira del listado que se utilizara para la interpolacion
		qhawax_station_id, qhawax_array,qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
		#Obtener data de la base de datos de qHAWAXs y contar los None de cada contaminante de cada qHAWAX
		measurement_list = getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks)
		#Arreglo de jsons ordenados por hora del mas antiguo al mas actual
		sort_list_without_json = helper.sortListOfMeasurementPerHour(measurement_list,last_hours)
		list_of_indexs_column = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
		indice_columna_coordenadas_x = list_of_indexs_column[0][column_coordinates_x]
		indice_columna_coordenadas_y = list_of_indexs_column[0][column_coordinates_y]
		
		k_values = []
		for k in range(4,8):
			near_qhawaxs = helper.getNearestStations(qhawax_location, element_to_interpolate['lat'] , element_to_interpolate['lon'])
			new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k)
			conjunto_valores_predichos = helper.obtenerListaInterpolacionesPasadasEnUnPunto(new_sort_list_without_json, indice_columna_coordenadas_x, indice_columna_coordenadas_y, element_to_interpolate['lat'], element_to_interpolate['lon'])
			conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
			k_values.append(conjunto_valores_predichos)
			#for element in conjunto_valores_predichos:
			#	print(element)
			#	for key,value in DICCIONARIO_INDICES_VARIABLES_PREDICCION.items():
			#		pollutant_id = helper.getPollutantID(json_data_pollutant,key)
			#		if(pollutant_id!=None):
			#			spatial_json={"pollutant_id":int(pollutant_id),"ug_m3_value":round(float(element[value]),3)}
			#			print(spatial_json)

				#		data_n_steps = {"hour":hour,"lat": CO, "lon": NO2, "CO_K4": PM25, "CO_K5": qWid, "CO_K6": compid,"CO_K7": compid,"CO_REAL": compid,"NO2_K4": PM25, "NO2_K5": qWid, "NO2_K6": compid,"NO2_K7": compid,"NO2_REAL": compid,"PM25_K4": PM25, "PM25_K5": qWid, "PM25_K6": compid,"PM25_K7": compid,"PM25_REAL": compid}
				#	    df = pd.DataFrame(data=data_n_steps)
				#	    print(df)

		#print("Imprimiendo interpolaciones de un punto ===========================================")
		#print(element_to_interpolate)
		one_qhawax_station_id, one_qhawax_array,one_qhawax_location = helper.getDetailOfEnvStation([element_to_interpolate])
		real_measurement = getListOfMeasurementOfAllModules(one_qhawax_array,one_qhawax_location,weeks)
		#print(real_measurement)
		#print(k_values)
		#Crear nuevo arreglo con N-1 estaciones
		#Volver a igual con la lista original
		json_all_env_station = np.asarray(json_all_env_station)
		json_all_env_station = (copy_all_env_station.copy()).tolist()



	    