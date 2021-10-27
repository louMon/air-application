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
last_hours =10
weeks = 14
k_number_min = 4
k_number_max = 7

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
    measurements_per_qhawax = []
    final_timestamp = datetime.datetime.now(dateutil.tz.tzutc()).replace(minute=0, second=0, microsecond=0) - datetime.timedelta(weeks=weeks)
    initial_timestamp = (final_timestamp - datetime.timedelta(hours=last_hours-1)).strftime("%d-%m-%Y %H:%M:%S")
    print("Hora UTC +5 horas")
    print("Fecha Inicial: {a}".format(a=initial_timestamp))
    final_timestamp = final_timestamp.strftime("%d-%m-%Y %H:%M:%S")
    print("Fecha Final: {a}".format(a=final_timestamp))
    for i in range(len(qhawax_array)): #arreglo de los qhawaxs
        json_params = {'name': 'qH0'+str(qhawax_array[i]),'initial_timestamp':initial_timestamp,'final_timestamp':final_timestamp}
        response = requests.get(GET_HOURLY_DATA_PER_QHAWAX, params=json_params)
        hourly_processed_measurements = response.json()
        hourly_processed_measurements = helper.completeHourlyValuesByQhawax(hourly_processed_measurements,qhawax_location[i],pollutant_array_json)
        measurements_per_qhawax.append(hourly_processed_measurements)
    return measurements_per_qhawax

if __name__ == '__main__':
	df_consolidate = pd.DataFrame()
	#Qhawax fisicos
	json_all_env_station = json.loads(requests.get(GET_ALL_FONDECYT_ENV_STATION).text)
	#Obtener el detalle de las estaciones fisicas
	qhawax_station_id, qhawax_array,qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
	#Obtener las mediciones de cada estacion
	measurement_list= getListOfMeasurementOfAllModules(qhawax_array,qhawax_location,weeks)
	sort_list_without_json = helper.newSortListOfMeasurementPerHourScript(measurement_list,last_hours,weeks)
	new_json_all_env_stations = [{"id":1,"lat":-12.04450424,"lon":-77.07684871,"module_id":37},{"id":2,"lat":-12.05252969,"lon":-77.07267266,"module_id":38},
	{"id":3,"lat":-12.04850083,"lon":-77.06487536,"module_id":39},{"id":4,"lat":-12.0601624,"lon":-77.04162812,"module_id":40},{"id":5,"lat":-12.0434041,"lon":-77.04336517,"module_id":41},
	{"id":6,"lat":-12.04754028,"lon":-77.0540217,"module_id":42},{"id":7,"lat":-12.05068076,"lon":-77.03864895,"module_id":43},{"id":8,"lat":-12.04892278,"lon":-77.02926448,"module_id":44},
	{"id":9,"lat":-12.05972153,"lon":-77.03578787,"module_id":45},{"id":10,"lat":-12.05347333,"lon":-77.03151512,"module_id":46},{"id":11,"lat":-12.03563722,"lon":-77.03044477,"module_id":47},
	{"id":12,"lat":-12.05046177,"lon":-77.02258352,"module_id":48}]

	k_diff = k_number_max -k_number_min
	for element_to_interpolate in new_json_all_env_stations:
		print("El elemento a interpolar es: {a}\n".format(a=element_to_interpolate))
		k_values = []
		#Esta funcion ordena las estaciones con respecto a la estacion a interpolar
		near_qhawaxs = helper.getNearestStations(qhawax_location, element_to_interpolate["lat"] , element_to_interpolate["lon"])
		print("Las estaciones mas cercanas")
		print(near_qhawaxs)
		new_sort_list_without_json = helper.sortBasedNearQhawaxs(near_qhawaxs,sort_list_without_json)
		list_of_indexs_column = helper.getDiccionaryListWithEachIndexColumn(new_sort_list_without_json)
		indice_columna_coordenadas_x = list_of_indexs_column[0][column_coordinates_x]
		indice_columna_coordenadas_y = list_of_indexs_column[0][column_coordinates_y]
		removed_values_out_control = helper.removeOutControlValues(new_sort_list_without_json)
		print("Ahora a interpolar +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		for k in range(k_number_min,k_number_max):
			print("\nNumero k: {a}".format(a=k))
			#Aqui le estoy mandando un arreglo de N horas para que de cada hora se tenga solamente los near_qhawaxs
			filtered_sort_list_without_json = helper.newFilterMeasurementBasedOnNearestStations(removed_values_out_control,k)
			print("Luego de filtered_sort_list_without_json")
			print(filtered_sort_list_without_json)
			convertToNumpyMatrix = helper.convertToNumpyMatrix(filtered_sort_list_without_json)
			print("Luego de convertToNumpyMatrix")
			print(convertToNumpyMatrix)
			conjunto_valores_predichos = helper.getListofPastInterpolationsAtOnePoint(convertToNumpyMatrix, 
										 indice_columna_coordenadas_x, indice_columna_coordenadas_y, 
										 element_to_interpolate['lat'], element_to_interpolate['lon'])
			k_values.append(conjunto_valores_predichos)
			print("Longitud del arreglo de k => {a}: {b}".format(a=k, b=len(conjunto_valores_predichos)))
			print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

		sort_list_by_k = sortListOfMeasurementPerK(k_values,last_hours)
		for print_index in range(len(sort_list_by_k)):
			if(len(sort_list_by_k[print_index])>0):
				co_interpolate = [ sort_list_by_k[print_index][index_co][0] if len(sort_list_by_k[print_index][index_co])>0 else np.nan for index_co in range(len(sort_list_by_k[print_index])) ]
				no2_interpolate = [ sort_list_by_k[print_index][index_no2][1] if len(sort_list_by_k[print_index][index_no2])>0 else np.nan for index_no2 in range(len(sort_list_by_k[print_index])) ]
				pm25_interpolate = [ sort_list_by_k[print_index][index_pm25][2] if len(sort_list_by_k[print_index][index_pm25])>0 else np.nan for index_pm25 in range(len(sort_list_by_k[print_index])) ]
				data = {"hour":[print_index+1]*k_diff,
						"lat": [element_to_interpolate['lat']]*k_diff,
						"lon": [element_to_interpolate['lon']]*k_diff,
						"k":range(k_number_min,k_number_max), 
						"CO_interpolate": co_interpolate,
						"NO2_interpolate":no2_interpolate,
						"PM25_interpolate": pm25_interpolate}
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

	#En el CSV colocar las constantes elegidas
	#Exportar CSV con todo el consolidado de dataframes
	df_consolidate.to_csv(r'/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/dfconsolidate.csv')
	    