import requests
import datetime

def response_json_predict(send_json, module_id):
  # API-Endpoint
  POST1_url = "http://0.0.0.0:5000/api/store_temporal_prediction/"
  #PARMS = {"module_id":module_id, "measurement":send_json}
  PARMS = {'module_id': 37, 'measurement': [{'CO_ug_m3': 1917.6035579471884, 'H2S_ug_m3': None, 'NO2_ug_m3': 86.2179766863212, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 28.21349445231259, 'hour_position': 1}, {'CO_ug_m3': 1731.5356230972036, 'H2S_ug_m3': None, 'NO2_ug_m3': 89.80445123109966, 'O3_ug_m3':None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 29.53170750268549, 'hour_position': 2}, {'CO_ug_m3': 1506.8092874935194, 'H2S_ug_m3': None, 'NO2_ug_m3': 91.14654434490949, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 30.500631902188065, 'hour_position': 3}, {'CO_ug_m3': 1356.187428345911, 'H2S_ug_m3': 'nan', 'NO2_ug_m3': 87.17398879323154, 'O3_ug_m3': None, 'SO2_ug_m3':None, 'PM10': None, 'PM25': 31.230545031715188, 'hour_position': 4}, {'CO_ug_m3': 1263.4068421893453, 'H2S_ug_m3': None, 'NO2_ug_m3': 85.97106214862316, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 31.975118476111444, 'hour_position': 5}, {'CO_ug_m3': 1244.2221436248392, 'H2S_ug_m3': None, 'NO2_ug_m3': 82.83158501431345, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 32.41036298459768, 'hour_position': 6}]}
  print(PARMS)
  resp = requests.post(POST1_url, json=PARMS)
  print(resp.text)
  return resp

def response_nowtime(last_timestamp):
  # API-Endpoint
  POST2_url = "http://0.0.0.0:5000/api/update_timestamp_running/"
  PARAMS = {"model_id":2,"last_running_timestamp":last_timestamp}
  resp = requests.post(POST2_url, json=PARAMS)
  return resp

module_id = 37
send_json = [{'CO_ug_m3': 1882.082697430089, 'H2S_ug_m3': None, 'NO2_ug_m3': 86.46367639489473, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 25.564556832499804, 'hour_position': 1},{'CO_ug_m3': 1835.4723963968959, 'H2S_ug_m3': None, 'NO2_ug_m3': 87.91207158774138, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 26.760190622851255, 'hour_position': 2},{'CO_ug_m3': 1643.9513717958907, 'H2S_ug_m3': None, 'NO2_ug_m3': 86.91062008157374, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 27.840379309013485, 'hour_position': 3},{'CO_ug_m3': 1460.602826666562, 'H2S_ug_m3': None, 'NO2_ug_m3': 81.51200932785869, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 28.746213879525662, 'hour_position': 4},{'CO_ug_m3': 1348.13281464543, 'H2S_ug_m3': None, 'NO2_ug_m3': 79.81095024123789, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 29.46164571493864, 'hour_position': 5},{'CO_ug_m3': 1285.3779871385489, 'H2S_ug_m3': None, 'NO2_ug_m3': 77.12482813604177, 'O3_ug_m3': None, 'SO2_ug_m3': None, 'PM10': None, 'PM25': 29.950469173774124, 'hour_position': 6}]
#send_json = [{'CO_ug_m3': 1917.6035579471884, 'H2S_ug_m3': 'nan', 'NO2_ug_m3': 86.2179766863212, 'O3_ug_m3': 'nan', 'SO2_ug_m3': 'nan', 'PM10': 'nan', 'PM25': 28.21349445231259, 'hour_position': 1}, {'CO_ug_m3': 1731.5356230972036, 'H2S_ug_m3': nan, 'NO2_ug_m3': 89.80445123109966, 'O3_ug_m3': nan, 'SO2_ug_m3': nan, 'PM10': nan, 'PM25': 29.53170750268549, 'hour_position': 2}, {'CO_ug_m3': 1506.8092874935194, 'H2S_ug_m3': nan, 'NO2_ug_m3': 91.14654434490949, 'O3_ug_m3': nan, 'SO2_ug_m3': nan, 'PM10': nan, 'PM25': 30.500631902188065, 'hour_position': 3}, {'CO_ug_m3': 1356.187428345911, 'H2S_ug_m3': nan, 'NO2_ug_m3': 87.17398879323154, 'O3_ug_m3': nan, 'SO2_ug_m3': nan, 'PM10': nan, 'PM25': 31.230545031715188, 'hour_position': 4}, {'CO_ug_m3': 1263.4068421893453, 'H2S_ug_m3': nan, 'NO2_ug_m3': 85.97106214862316, 'O3_ug_m3': nan, 'SO2_ug_m3': nan, 'PM10': nan, 'PM25': 31.975118476111444, 'hour_position': 5}, {'CO_ug_m3': 1244.2221436248392, 'H2S_ug_m3': nan, 'NO2_ug_m3': 82.83158501431345, 'O3_ug_m3': nan, 'SO2_ug_m3': nan, 'PM10': nan, 'PM25': 32.41036298459768, 'hour_position': 6}]
resp = response_json_predict(send_json, module_id)
last_timestamp = str(datetime.datetime.now().replace(minute=0,second=0, microsecond=0))
print(last_timestamp)
print(type(last_timestamp))
resp = response_nowtime(last_timestamp)
print(resp.text)
