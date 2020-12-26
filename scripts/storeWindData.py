import requests
import datetime
import dateutil.parser
import dateutil.tz
import json
#BASE_URL = 'https://qairamapnapi.qairadrones.com/'
BASE_URL = 'http://0.0.0.0:5000/'
GET_ALL_STATION = 'api/get_all_env_station/'
STORE_WIND_MEASUREMENT ='api/store_wind_measurement/'

# Request all qhawax
wind_api ='https://api.openweathermap.org/data/2.5/weather?'
wind_token = '338a745cb306f321522cc8b041096b4c'
response = requests.get(BASE_URL + GET_ALL_STATION)
json_data = json.loads(response.text)

for station in json_data:
    print('Processing wind data from  %s ...' % (station['comercial_name']))
    wind_json={}
    wind_parameters = "lat="+str(station["lat"])+"&"+"lon="+str(station["lon"])+"&APPID="+wind_token
    wind_url = wind_api + wind_parameters

    r = requests.get(url = wind_url)
    data = r.json()

    wind_speed = "" 
    wind_deg = ""
    temp = ""
    pressure = ""
    humidity = ""
    if(data!=""):
      wind_json['weather_main'] = str(data['weather'][0]['main'])
      wind_json['weather_desc'] = str(data['weather'][0]['description'])
      if(data['main']['temp'] or data['main']['temp']==0):
        wind_json['temperature'] = str(data['main']['temp'])
      if(data['main']['pressure'] or data['main']['pressure']==0):
        wind_json['pressure'] = str(data['main']['pressure'])
      if(data['main']['humidity'] or data['main']['humidity']==0):
        wind_json['humidity'] = str(data['main']['humidity'])
      if(data['wind']['speed'] or data['wind']['speed']==0):
        wind_json['speed']  = str(data['wind']['speed'])
      if(data['wind']['deg'] or data['wind']['deg']==0):
        wind_json['degree'] = str(data['wind']['deg'])

      wind_json['timestamp'] = str(datetime.datetime.now())
      wind_json['station_id'] = station['id']

      response = requests.post(BASE_URL + STORE_WIND_MEASUREMENT, json=wind_json)
      print(response.text)


