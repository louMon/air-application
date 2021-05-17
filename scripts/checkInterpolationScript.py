import script_helper as helper
import datetime
import dateutil.tz
import numpy as np
import requests
import json
import pandas as pd
import math

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_FONDECYT_ENV_STATION= BASE_URL_IA + 'api/get_all_fondecyt_env_station/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}
column_coordinates_x = 'lon'
column_coordinates_y = 'lat'
last_hours =6
weeks = 2
k_number_min = 2
k_number_max = 8

def sortListOfMeasurementPerK(measurement_list,k_times):
    sort_list_by_k = []
    for i in range(k_times):
        k_n = []
        for j in range(len(measurement_list)): #Un qHAWAX puede que no tenga ningun elemento, entonces lo descartamos
            list_measurement_by_qhawax_by_hour = np.array(measurement_list[j][i])
            k_n.append(list_measurement_by_qhawax_by_hour)
        new_k_n_array = []
        for k_elem in range(len(k_n)):
            new_k_n_array.append(k_n[k_elem])
        new_k_n_array = np.array(new_k_n_array)
        sort_list_by_k.append(new_k_n_array)
    return sort_list_by_k

def getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks):
    list_of_hours = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) - datetime.timedelta(weeks=weeks) 
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
	df_consolidate = pd.DataFrame()
	json_all_env_station = json.loads(requests.get(GET_ALL_FONDECYT_ENV_STATION).text)      
	json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
	copy_all_env_station = np.asarray(json_all_env_station)
	k_diff = k_number_max -k_number_min
	for qhawax_index in range(len(json_all_env_station)):
		element_to_interpolate = json_all_env_station[qhawax_index]
		json_all_env_station.remove(element_to_interpolate)
		qhawax_station_id, qhawax_array,qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
		measurement_list = getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks)
		sort_list_without_json = helper.sortListOfMeasurementPerHour(measurement_list,last_hours)
		list_of_indexs_column = helper.getDiccionaryListWithEachIndexColumn(sort_list_without_json)
		indice_columna_coordenadas_x = list_of_indexs_column[0][column_coordinates_x]
		indice_columna_coordenadas_y = list_of_indexs_column[0][column_coordinates_y]		
		k_values = []
		for k in range(k_number_min,k_number_max):
			near_qhawaxs = helper.getNearestStations(qhawax_location, element_to_interpolate['lat'] , element_to_interpolate['lon'])
			new_sort_list_without_json = helper.filterMeasurementBasedOnNearestStations(near_qhawaxs,sort_list_without_json,k)
			conjunto_valores_predichos = helper.getListofPastInterpolationsAtOnePoint(new_sort_list_without_json, indice_columna_coordenadas_x, indice_columna_coordenadas_y, element_to_interpolate['lat'], element_to_interpolate['lon'])
			conjunto_valores_predichos=np.asarray(conjunto_valores_predichos).astype(np.float32)
			k_values.append(conjunto_valores_predichos)		
		one_qhawax_station_id, one_qhawax_array,one_qhawax_location = helper.getDetailOfEnvStation([element_to_interpolate])
		real_measurement = getListOfMeasurementOfAllModules(one_qhawax_array,one_qhawax_location,weeks)
		sort_list_by_k = sortListOfMeasurementPerK(k_values,k_diff)
		for print_index in range(len(sort_list_by_k)):
			co_interpolate = [ sort_list_by_k[print_index][index_co][0] for index_co in range(len(sort_list_by_k[print_index]))]
			no2_interpolate = [ sort_list_by_k[print_index][index_no2][1] for index_no2 in range(len(sort_list_by_k[print_index]))]
			pm25_interpolate = [ sort_list_by_k[print_index][index_pm25][2] for index_pm25 in range(len(sort_list_by_k[print_index]))]
			var_real_measurement = real_measurement[0][print_index]
			data = {"hour":[print_index+1]*k_diff,"lat": [element_to_interpolate['lat']]*k_diff, "lon": [element_to_interpolate['lon']]*k_diff,"k":range(k_number_min,k_number_max), "CO_interpolate": co_interpolate,"CO_REAL": [var_real_measurement["CO"]]*k_diff,"NO2_interpolate":no2_interpolate,"NO2_REAL": [var_real_measurement["NO2"]]*k_diff,"PM25_interpolate": pm25_interpolate,"PM25_REAL": [var_real_measurement["PM25"]]*k_diff}
			df = pd.DataFrame(data=data)
			print(df)
			df_consolidate = pd.concat([df_consolidate,df])
		json_all_env_station = np.asarray(json_all_env_station)
		json_all_env_station = (copy_all_env_station.copy()).tolist()

	#En el CSV colocar las constantes elegidas

	#Exportar CSV con todo el consolidado de dataframes
	df_consolidate.to_csv(r'/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/dfconsolidate.csv')



	    