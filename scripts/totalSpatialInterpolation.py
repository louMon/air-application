






if __name__ == '__main__':
    start_time = datetime.datetime.now()
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text) 
    historical_measurement_list = getListOfMeasurementOfAllModules(array_module_id,array_qhawax_location)

    historical_response_delete = requests.post(DELETE_ALL_SPATIAL_PREDICTION)

    sort_historical_list_without_json = helper.sortListOfMeasurementPerHourHistorical(historical_measurement_list,last_hours_historical_interpolate)
    dictionary_historical_list_of_index_columns = helper.getDiccionaryListWithEachIndexColumn(sort_historical_list_without_json)
    index_column_x = dictionary_historical_list_of_index_columns[0][name_column_x]
    index_column_y = dictionary_historical_list_of_index_columns[0][name_column_y]
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":1,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    pool = multiprocessing.Pool(pool_size_historical_interpolate)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()
    print("Empezo a las {a} y termino a las {b}".format(a=start_time,b=datetime.datetime.now()))
    print("===================================================================================")


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    json_all_env_station = json.loads(requests.get(GET_ALL_ENV_STATION).text)
    array_station_id, array_module_id,array_qhawax_location = helper.getDetailOfEnvStation(json_all_env_station)
    json_data_grid = json.loads(requests.get(GET_ALL_GRID).text) 
    json_data_pollutant = json.loads(requests.get(GET_ALL_ACTIVE_POLLUTANTS).text)

    future_response_delete = requests.post(DELETE_ALL_FUTURE_SPATIAL_PREDICTION)

    future_measurement_list = getListOfMeasurementOfAllModules(array_station_id,array_qhawax_location)

    future_sort_list_without_json = helper.sortListOfMeasurementPerHourFuture(future_measurement_list,last_hours_future_interpolate)
    future_dictionary_list_of_index_columns = helper.getDiccionaryListWithEachIndexColumn(future_sort_list_without_json)
    index_column_x = dictionary_list_of_index_columns[0][name_column_x]
    index_column_y = dictionary_list_of_index_columns[0][name_column_y]
    response = requests.post(UPDATE_RUNNING_TIMESTAMP, json={"model_id":3,"last_running_timestamp":str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))})
    pool = multiprocessing.Pool(pool_size_future_interpolate)
    pool_results = pool.map(iterateByGrids, json_data_grid)
    pool.close()
    pool.join()
    print("Empezo a las {a} y termino a las {b}".format(a=start_time,b=datetime.datetime.now()))
    print("===================================================================================")
