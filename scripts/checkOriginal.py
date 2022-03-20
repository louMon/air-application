import script_helper as helper
import datetime
import dateutil.tz
import numpy as np
import requests
import json
import pandas as pd
import math

BASE_URL_IA = 'http://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
GET_ALL_FONDECYT_ENV_STATION= BASE_URL_IA + 'api/get_all_fondecyt_env_station/'
GET_HOURLY_DATA_PER_QHAWAX = BASE_URL_QAIRA + 'api/air_quality_measurements_period/'
GET_ALL_ACTIVE_POLLUTANTS = BASE_URL_IA+ 'api/get_all_active_pollutants/'
pollutant_array_json = {'CO': [], 'NO2': [], 'PM25': [],'timestamp_zone':[],'lat':[],'lon':[],'alt':[]}
last_hours =480
weeks = 20
k_number_min = 2
k_number_max = 8

def sortListOfMeasurementPerK(measurement_list,last_hours):#k_times):
    sort_list_by_k = []
    for i in range(last_hours):
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
    print("Hora UTC +5 horas")
    print("Fecha Inicial: {a}".format(a=initial_timestamp))
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    print("Fecha Final: {a}".format(a=final_timestamp))
    for i in range(len(qhawax_array)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(qhawax_array[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        if(len(response.text)>3): ###Para controlar los responses vacios
	        hourly_processed_measurements = helper.completeHourlyValuesByQhawax(response.json(),qhawax_location[i],pollutant_array_json)
	        list_of_hours.append(hourly_processed_measurements)
    return list_of_hours

if __name__ == '__main__':
	df_consolidate = pd.DataFrame()
	#Qhawax fisicos
	json_all_env_station = json.loads(requests.get(GET_ALL_FONDECYT_ENV_STATION).text)
	copy_all_env_station = np.asarray(json_all_env_station)
	k_diff = k_number_max -k_number_min
	for qhawax_index in range(len(json_all_env_station)):
		print("================================================================")
		print("En estacion {a}\n".format(a=qhawax_index))
		element_to_interpolate = json_all_env_station[qhawax_index]
		print("El elemento a interpolar es: {a}\n".format(a=element_to_interpolate))
		#Vemos la informacion del resto de estaciones que se utilizaran para interpolar la otra estacion
		json_all_env_station.remove(element_to_interpolate)
		qhawax_station_id, qhawax_array,qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
		measurement_list = getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks)
		sort_list_without_json = helper.newSortListOfMeasurementPerHourScript(measurement_list,last_hours,weeks)
		near_qhawaxs = helper.getNearestStations(qhawax_location, element_to_interpolate["lat"] , element_to_interpolate["lon"])
		print("Las estaciones mas cercanas")
		print(near_qhawaxs)
		new_sort_list_without_json = helper.sortBasedNearQhawaxs(near_qhawaxs,sort_list_without_json)
		removed_values_out_control = helper.removeOutControlValues(new_sort_list_without_json)
		print("Ahora a interpolar +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		k_values = []
		for k in range(k_number_min,k_number_max):
			print("\nNumero k: {a}".format(a=k))
			#Separar los contaminantes por hora en un json
			separated_sort_list_without_json = helper.separatePollutants(removed_values_out_control)
			#Aqui le estoy mandando un arreglo de N horas para que de cada hora se tenga solamente los near_qhawaxs
			filtered_sort_list_without_json = helper.newFilterMeasurementBasedOnNearestStations(separated_sort_list_without_json,k)	
			convertToNumpyMatrix = helper.convertToNumpyMatrix(filtered_sort_list_without_json)
			conjunto_valores_predichos = helper.getListofPastInterpolationsAtOnePoint(convertToNumpyMatrix, 
										 element_to_interpolate['lat'], element_to_interpolate['lon'],k)
			k_values.append(conjunto_valores_predichos)
			print("Longitud del arreglo de k => {a}: {b}".format(a=k, b=len(conjunto_valores_predichos)))
			print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		#Valores reales de la medicion de la estacion que fue interpolada
		one_qhawax_station_id, one_qhawax_array,one_qhawax_location = helper.getDetailOfEnvStation([element_to_interpolate])
		real_measurement = getListOfMeasurementOfAllModules(one_qhawax_array,one_qhawax_location,weeks)
		sort_list_by_k = sortListOfMeasurementPerK(k_values,last_hours)
		#print("EMPEZANDO CON EL DATAFRAME! *********************************************************************")
		for print_index in range(len(sort_list_by_k)):
			if(len(sort_list_by_k[print_index])>0):
				co_interpolate = [ sort_list_by_k[print_index][index_co][0] if len(sort_list_by_k[print_index][index_co])>0 else np.nan for index_co in range(len(sort_list_by_k[print_index])) ]
				no2_interpolate = [ sort_list_by_k[print_index][index_no2][1] if len(sort_list_by_k[print_index][index_no2])>0 else np.nan for index_no2 in range(len(sort_list_by_k[print_index])) ]
				pm25_interpolate = [ sort_list_by_k[print_index][index_pm25][2] if len(sort_list_by_k[print_index][index_pm25])>0 else np.nan for index_pm25 in range(len(sort_list_by_k[print_index])) ]
				var_real_measurement = (real_measurement[0][print_index] if(len(real_measurement[0])>print_index) else np.nan) if len(real_measurement)>0 else np.nan
				var_real_measurement_CO = [var_real_measurement["CO"]]*k_diff if isinstance(var_real_measurement, dict) is True else [var_real_measurement]*k_diff 
				var_real_measurement_NO2 = [var_real_measurement["NO2"]]*k_diff if isinstance(var_real_measurement, dict) is True else [var_real_measurement]*k_diff
				var_real_measurement_PM25 = [var_real_measurement["PM25"]]*k_diff if isinstance(var_real_measurement, dict) is True else [var_real_measurement]*k_diff
				
				data = {"hour":[print_index+1]*k_diff,
						"lat": [element_to_interpolate['lat']]*k_diff,
						"lon": [element_to_interpolate['lon']]*k_diff,
						"k":range(k_number_min,k_number_max), 
						"CO_interpolate": co_interpolate,
						"CO_REAL": var_real_measurement_CO,
						"NO2_interpolate":no2_interpolate,
						"NO2_REAL": var_real_measurement_NO2,
						"PM25_interpolate": pm25_interpolate,
						"PM25_REAL": var_real_measurement_PM25}
				array_helper = []
				if(math.isnan(data["CO_interpolate"][0])!=True):
					if isinstance(data["CO_interpolate"][0],(list,np.ndarray)):
						for element in data["CO_interpolate"]:
							array_helper.append(element[0])
						data["CO_interpolate"] = array_helper
				array_helper = []
				if(math.isnan(data["NO2_interpolate"][0])!=True):
					if isinstance(data["NO2_interpolate"][0],(list,np.ndarray)):
						for element in data["NO2_interpolate"]:
							array_helper.append(element[0])
						data["NO2_interpolate"] = array_helper
				array_helper = []
				if(math.isnan(data["PM25_interpolate"][0])!=True):
					if isinstance(data["PM25_interpolate"][0],(list,np.ndarray)):
						for element in data["PM25_interpolate"]:
							array_helper.append(element[0])
						data["PM25_interpolate"] = array_helper  
				df = pd.DataFrame(data=data)
				df_consolidate = pd.concat([df_consolidate,df])
		json_all_env_station = np.asarray(json_all_env_station)
		json_all_env_station = (copy_all_env_station.copy()).tolist()

	#En el CSV colocar las constantes elegidas
	#Exportar CSV con todo el consolidado de dataframes
	df_consolidate.to_csv(r'/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/dfGaussiano.csv')



	    