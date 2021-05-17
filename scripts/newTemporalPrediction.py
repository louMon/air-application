import math
from math import sqrt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from keras.models import load_model
import requests
import datetime
import time
from global_constants import time_steps_in

BASE_URL_IA = 'https://pucp-calidad-aire-api.qairadrones.com/'
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'

DELETE_ALL_TEMPORAL_PREDICTION = BASE_URL_IA + 'api/delete_all_temporal_prediction/'
STORE_TEMPORAL_PREDICTION = BASE_URL_IA + 'api/store_temporal_prediction/'
RECORD_LAST_TEMPORAL_TIMESTAMP = BASE_URL_IA + 'api/update_timestamp_running/'
AVERAGE_VALID_PROCESSED = BASE_URL_QAIRA +'api/average_valid_processed_period/'
H5_PROD = '/var/www/html/air-application/'

# Define Station Parameters:
qWid_compid = [(37,3),(38,3),(39,3),(40,3),(41,3),(43,3),(45,3),(47,3),(48,3),(49,3),(50,3),(51,3),(52,3),(54,3)]
#Calculate time now and n_time_steps before.
CO, NO2, PM25, hum, compid, timestamp, qWid, temp = [], [], [], [], [], [], [], []

def minmax_scaler(X_input_norm, vector_max, vector_min):
    X_norm = X_input_norm.copy()
    for x in range(len(X_norm)):
        for y in range(len(X_norm[0])):
            X_norm[x][y] = (X_norm[x][y]-vector_min[y])/(vector_max[y]-vector_min[y])
    return X_norm

def response_json_predict(send_json, module_id):
    # API-Endpoint
    PARAMS = {"module_id":module_id, "measurement":send_json}
    resp = requests.post(STORE_TEMPORAL_PREDICTION, json=PARAMS)
    return resp.text

def response_nowtime(last_timestamp):
    # API-Endpoint
    PARAMS = {"model_id":2,"last_running_timestamp":last_timestamp}
    resp = requests.post(RECORD_LAST_TEMPORAL_TIMESTAMP, json=PARAMS)
    return resp.text

# Time difference for time features vectors:
def time_difference_features(X_input_norm, n_target_values):
    X_features_differences = []
    j=0
    while ((j+1) <= (len(X_input_norm)-1)):
        # Calculate time difference from the feature vector (Value from Target Vector(t) - Value from Target Vector(t-1))
        difference = X_input_norm[j+1] - X_input_norm[j]
        # Store difference result in array of arrays.
        X_features_differences.append(difference)
        j=j+1

    return np.array(X_features_differences)

def predict_air_quality(data, value_type, model, n_features, n_target_values, n_output, time_steps_in, vector_max, vector_min):

    # Generate array of pollutant data (always 18 elements initialized with NaN values so the missing will be interpolated).
    X_input = np.ones((time_steps_in,n_features))*np.nan
    for i in range(len(data)):
        if(type(data[i]) is dict):
            if value_type == "CO":
                X_input[i] = [data[i]["CO_ug_m3"], data[i]["humidity"], data[i]["temperature"]]
            elif value_type == "NO2":
                X_input[i] = [data[i]["NO2_ug_m3"], data[i]["humidity"], data[i]["temperature"]]
            elif value_type == "PM25":
                X_input[i] = [data[i]["PM25"], data[i]["humidity"], data[i]["temperature"]]
  
    # Replace possible outliers with nan values for future interpolation. 
    # Outlier for CO/NO2/PM25 = [MinValue * 50% , MaxValue * 150%] | Outlier for Hum =  [MinValue * 80% , MaxValue * 120%] | Outlier for Temp =  [MinValue * 70% , MaxValue * 130%] 
    prop_outlier_max=[1.5,1.2,1.3]
    prop_outlier_min=[0.5,0.8,0.7]
    for i in range(n_features):
        for j in range(time_steps_in):
            if ~(np.isnan(X_input[j][i])):
                if X_input[j][i] > (vector_max[i]*prop_outlier_max[i]):
                    X_input[j][i] = np.nan
                if X_input[j][i] < (vector_min[i]*prop_outlier_min[i]):
                    X_input[j][i] = np.nan

    # Value Interpolation.
    df_X_input =pd.DataFrame(X_input)
    df_X_input = df_X_input.interpolate(method="linear",limit=3,limit_direction='both')
  
    # Check if any NaNs after interpolation.
    if (df_X_input.isnull().values.any()):
        y_predicted = "Insufficient Data"
    else:
        # Feature and Target Normalization.
        X_input = np.array(df_X_input)
        X_input_norm = minmax_scaler(X_input, vector_max, vector_min)

        # Time Difference.
        X_input_norm_difference = time_difference_features(X_input_norm, n_target_values)
        X_input_norm_difference = X_input_norm_difference.reshape(1, n_steps_in_difference, n_features)

        y_difference_predicted = model.predict(X_input_norm_difference, verbose=0)[0]

        # Calculate real values from difference values.
        y_predicted = np.zeros(n_output)
        base_vector = X_input_norm[-1][0:n_target_values]
        count = 0
        previous_value = 0
        for i in range(n_target_values):
            vector = base_vector[i]
            for j in range(n_output):
                if (j==0):
                    y_predicted[count] = vector + y_difference_predicted[count]
                else:
                    y_predicted[count] = previous_value + y_difference_predicted[count]
                previous_value = y_predicted[count]
                count = count + 1

        # DeNormalize value.
        total = 0
        for i in range(n_output):
            for j in range(n_target_values):
                y_predicted[total] = (y_predicted[total] * (vector_max[j] - vector_min[j])) + vector_min[j]
                total = total + 1

    return y_predicted

if __name__ == '__main__':
    start_time = time.time()
    response_delete = requests.post(DELETE_ALL_TEMPORAL_PREDICTION)

    time_now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
    time_nsteps_before = (datetime.datetime.now() - datetime.timedelta(hours=time_steps_in)).strftime("%d-%m-%Y %H:%M:%S")

    vector_max_CO, vector_min_CO = [3504.922, 99.9, 28.075], [0.0, 0.0, 0.0]
    vector_max_NO2, vector_min_NO2 = [977.72, 99.9, 28.075], [0.0, 0.0, 0.0]
    vector_max_PM25, vector_min_PM25 = [98.516, 99.9, 28.075], [0.092, 0.0, 0.0]

    n_steps_in_difference = time_steps_in-1
    n_target_values = 1
    n_features = 3
    n_output = 6

    model_CO = load_model(H5_PROD+'h5Files/Model_LSTM_CO_Final.h5')
    model_NO2 = load_model(H5_PROD+'h5Files/Model_LSTM_NO_Final.h5')
    model_PM25 = load_model(H5_PROD+'h5Files/Model_LSTM_PM25_Final.h5')

    response_API_datapredict, response_API_nowtime = [], []

    # API-Endpoint
    for station_info in qWid_compid:
        # API-Parameters:
        qhawax_id = station_info[0]
        company_id = station_info[1]
        initial_timestamp = time_nsteps_before
        final_timestamp = time_now
            
        # Defining parameters:
        PARAMS = {"qhawax_id":qhawax_id, "company_id":company_id, "initial_timestamp":time_nsteps_before, "final_timestamp":time_now} 

        # Sending GET request to URL with Parameters concatenated:
        result = requests.get(url = AVERAGE_VALID_PROCESSED, params = PARAMS)
        # Parsing data in JSON format.
        data = result.json()
        #Saving values in dataframe:     
        CO.append(predict_air_quality(data, "CO", model_CO, n_features, n_target_values, n_output, time_steps_in, vector_max_CO, vector_min_CO))
        NO2.append(predict_air_quality(data, "NO2", model_NO2, n_features, n_target_values, n_output, time_steps_in, vector_max_NO2, vector_min_NO2))
        PM25.append(predict_air_quality(data, "PM25", model_PM25, n_features, n_target_values, n_output, time_steps_in, vector_max_PM25, vector_min_PM25))
        qWid.append(qhawax_id)
        compid.append(company_id)

    resp_2 = response_nowtime(str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0)))
    response_API_nowtime.append(resp_2)

    for i in range(len(qWid)):
        send_json = []
        for j in range(n_output):
            if CO[i] != "Insufficient Data" and NO2[i] != "Insufficient Data" and PM25[i] != "Insufficient Data":
                hour_station_json = {"CO_ug_m3":CO[i][j],"H2S_ug_m3":None,"NO2_ug_m3":NO2[i][j],"O3_ug_m3":None,"SO2_ug_m3":None,"PM10":None,"PM25":PM25[i][j], "hour_position":(j+1)}
            else:
                hour_station_json = {"CO_ug_m3":None,"H2S_ug_m3":None,"NO2_ug_m3":None,"O3_ug_m3":None,"SO2_ug_m3":None,"PM10":None,"PM25":None,"hour_position":(j+1)}
            send_json.append(hour_station_json)
        
        resp_1 = response_json_predict(send_json,qWid[i])
        last_timestamp = str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))
        response_API_datapredict.append(resp_1)

    print("Logs Envío a API - Predicciones:", response_API_datapredict)
    print("Logs Envío a API - TimeNow:", response_API_nowtime)
    
    data_n_steps = {"CO": CO, "NO2": NO2, "PM25": PM25, "QWid": qWid, "Compid": compid}
    df = pd.DataFrame(data=data_n_steps)
    print(df)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(datetime.datetime.now())
    print("===================================================================================")

