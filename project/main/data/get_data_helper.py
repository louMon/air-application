from project.database.models import Traffic, Wind, Senamhi, InterpolatedPollutants, GridToPredict,Pollutant,\
                                    FutureInterpolatedPollutants,TemporalPollutants,EnvironmentalStation, \
                                    TotalSpatialInterpolation
import project.main.business.get_business_helper as get_business_helper
from collections import defaultdict
from functools import partial
from project import app, db
import numpy as np
import datetime
import requests
import json
import csv

session = db.session
time_steps_in=18
BASE_URL_QAIRA = 'https://qairamapnapi.qairadrones.com/'
AVERAGE_VALID_PROCESSED = BASE_URL_QAIRA +'api/average_valid_processed_period/'
TEMPORAL_FILE_ADDRESS = '/var/www/html/air-application/temporal_file.csv'
ORIGINAL_FILE_ADDRESS = '/var/www/html/air-application/original_file.csv'
#TEMPORAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/temporal_file.csv'
#ORIGINAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/original_file.csv'

def queryLastPredictedTemporalMeasurement(pollutant_name,last_hours,pollutant_unit):
    """ The next 6 air quality records of every environmental station based on temporal series models"""
    columns = None
    if (pollutant_unit=='ppb'):
        columns = (EnvironmentalStation.lat, EnvironmentalStation.lon, TemporalPollutants.hour_position,
                   TemporalPollutants.ppb_value)
    elif(pollutant_unit=='ugm3'):
        columns = (EnvironmentalStation.lat, EnvironmentalStation.lon, TemporalPollutants.hour_position,
                   TemporalPollutants.ug_m3_value)

    if(columns!=None):
        return session.query(*columns).join(EnvironmentalStation, TemporalPollutants.environmental_station_id == EnvironmentalStation.id). \
                                       join(Pollutant, TemporalPollutants.pollutant_id == Pollutant.id). \
                                       group_by(EnvironmentalStation.id, TemporalPollutants.id, Pollutant.id). \
                                       filter(Pollutant.pollutant_name == pollutant_name). \
                                       filter(TemporalPollutants.hour_position <= last_hours). \
                                       order_by(TemporalPollutants.hour_position.desc()).all()
    return None

def mergeSameHourPosition(records):
    id2record = defaultdict(partial(defaultdict, list))
    for record in records:
      merged_record = id2record[record['hour_position']]
      for key, value in record.items():
        merged_record[key].append(value)
    result = list()
    for record in id2record.items():
      result.append(dict(record[1]))
    return result

def queryFutureMeasurement(station_id):
    """ Get future measurements of environmental station """
    columns = (TemporalPollutants.hour_position,TemporalPollutants.ug_m3_value, 
               TemporalPollutants.pollutant_id, Pollutant.pollutant_name, 
               TemporalPollutants.environmental_station_id, TemporalPollutants.timestamp,
               EnvironmentalStation.lat, EnvironmentalStation.lon )
    future_measurements = session.query(*columns).\
                                  join(EnvironmentalStation, TemporalPollutants.environmental_station_id == EnvironmentalStation.id). \
                                  join(Pollutant, TemporalPollutants.pollutant_id == Pollutant.id). \
                                  group_by(EnvironmentalStation.id, TemporalPollutants.id, Pollutant.id). \
                                  filter(TemporalPollutants.environmental_station_id == station_id). \
                                  order_by(TemporalPollutants.hour_position.asc()).all()
    return [measurement._asdict() for measurement in future_measurements]

def queryFutureMeasurementByPollutant(station_id,pollutant):
    """ Get future measurements of environmental station filtering by pollutant """
    columns = (TemporalPollutants.hour_position,TemporalPollutants.ug_m3_value, 
               TemporalPollutants.pollutant_id, Pollutant.pollutant_name, 
               TemporalPollutants.environmental_station_id, TemporalPollutants.timestamp,
               EnvironmentalStation.lat, EnvironmentalStation.lon)
    future_measurements = session.query(*columns).\
                                  join(EnvironmentalStation, TemporalPollutants.environmental_station_id == EnvironmentalStation.id). \
                                  join(Pollutant, TemporalPollutants.pollutant_id == Pollutant.id). \
                                  group_by(EnvironmentalStation.id, TemporalPollutants.id, Pollutant.id). \
                                  filter(TemporalPollutants.environmental_station_id == station_id). \
                                  filter(Pollutant.pollutant_name == pollutant). \
                                  order_by(TemporalPollutants.hour_position.asc()).all()
    arr_measurement = [measurement._asdict() for measurement in future_measurements]
    for measurement in arr_measurement:
      measurement['hour_position'] +=18
      measurement['qhawax'] = 1
    return arr_measurement

def utilMeasurementTag(sameHourDictionary, measurement,flag):
    sameHourDictionary[measurement["pollutant_name"]]=measurement["ug_m3_value"]
    if(flag == "web"):
      sameHourDictionary["ug_m3_value"]=measurement["ug_m3_value"]
    return sameHourDictionary


def mergeSameHoursDictionary(predicted_measurements,total_hours,flag_web):
    #total_hours = 6
    all_hours = []
    for i in range(total_hours):
      sameHourDictionary ={}
      sameHourDictionary["qhawax"] = predicted_measurements[i]["qhawax"] if ('qhawax' in predicted_measurements[i]) else None
      sameHourDictionary["hour_position"] = i+1
      sameHourDictionary["lat"] = predicted_measurements[i]["lat"]
      sameHourDictionary["lon"] = predicted_measurements[i]["lon"]
      sameHourDictionary["timestamp"] = predicted_measurements[i]["timestamp"]
      for measurement in predicted_measurements:
        if(measurement["hour_position"]==i+1):
          #sameHourDictionary[measurement["pollutant_name"]]=measurement["ug_m3_value"]
          sameHourDictionary = utilMeasurementTag(sameHourDictionary, measurement,flag_web)
      all_hours.append(sameHourDictionary)
    return all_hours

def queryTotalSpatialMeasurementByPollutant(pollutant_name):
    """ The total records of air quality spatial prediction based on IDW models"""
    spatial_measurement = []
    pollutant_id = get_business_helper.queryGetPollutantID(pollutant_name)
    with open(ORIGINAL_FILE_ADDRESS) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
          if(int(row[0])==pollutant_id):
            spatial_json={"lat":float(row[1]),"lon":float(row[2]),"ug_m3_value":None if(row[3]=='') else float(row[3]),"hour_position":int(row[4]),"timestamp":row[5]}
            spatial_measurement.append(spatial_json)
    spatial_measurement.sort(key = lambda x:x["hour_position"])
    return spatial_measurement 

def setAverageValuesByHour(predicted_measurements):
    new_predicted_measurements = []
    for hour_element in predicted_measurements:
      hour_element["ug_m3_value"] = [measurement for measurement in hour_element["ug_m3_value"] if(measurement!=None) ]
      hour_element["timestamp"] = hour_element["timestamp"][0]
      hour_element["hour_position"] = hour_element["hour_position"][0]
      new_predicted_measurements.append(hour_element)
    return new_predicted_measurements

def getMaxMinOfMeasurements(predicted_measurements):
    ug_m3_measurement = []
    for hour_element in predicted_measurements:
        hour_element["ug_m3_value"] = [measurement for measurement in hour_element["ug_m3_value"] if(measurement!=None) ]
        arr_ug_m3_value = np.array(hour_element["ug_m3_value"])
        ug_m3_measurement = np.append(ug_m3_measurement, arr_ug_m3_value)
    max_measurement_by_pollutant = np.max(ug_m3_measurement)
    min_measurement_by_pollutant = np.min(ug_m3_measurement)
    median_measurement_by_pollutant = np.median(ug_m3_measurement)
    return max_measurement_by_pollutant, min_measurement_by_pollutant,median_measurement_by_pollutant

def queryHistoricalMeasurement(station_id, pollutant):
    historical_array=[]
    qhawax_id = get_business_helper.getQhawaxID(station_id)
    lat,lon = get_business_helper.getQhawaxLocation(station_id)
    pollutant_id = get_business_helper.queryGetPollutantID(pollutant)
    time_now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    time_nsteps_before = (datetime.datetime.now() - datetime.timedelta(hours=time_steps_in)).strftime("%d-%m-%Y %H:%M:%S")
    PARAMS = {"qhawax_id":qhawax_id, "company_id":3, "initial_timestamp":time_nsteps_before, "final_timestamp":time_now} 
    # Sending GET request to URL with Parameters concatenated:
    result = requests.get(url = AVERAGE_VALID_PROCESSED, params = PARAMS)
    # Parsing data in JSON format.
    measurements_by_station = result.json()
    hour_count = 1
    for measurement in measurements_by_station:
      for key,value in measurement.items():
        if(key == pollutant +'_ug_m3' or key == pollutant):
          '''Entre porque pollutant es igual NO2_ugm3, CO_ug_m3, PM25 '''
          historical_json = {'hour_position': hour_count, 'ug_m3_value': value, \
                             'pollutant_id': pollutant_id, 'pollutant_name': pollutant, \
                             'environmental_station_id': station_id, 'timestamp': measurement['timestamp_zone'], \
                             'lat': lat, 'lon': lon,'qhawax':0}
          historical_array.append(historical_json)
          hour_count+=1
    return historical_array
