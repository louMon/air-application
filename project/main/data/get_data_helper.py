from project.database.models import Traffic, Wind, Senamhi, InterpolatedPollutants, GridToPredict,Pollutant,\
                                    FutureInterpolatedPollutants,TemporalPollutants,EnvironmentalStation, \
                                    TotalSpatialInterpolation
import project.main.business.get_business_helper as get_business_helper
from collections import defaultdict
from functools import partial
from project import app, db
import numpy as np
import csv

session = db.session

#TEMPORAL_FILE_ADDRESS = '/var/www/html/air-application/temporal_file.csv'
#ORIGINAL_FILE_ADDRESS = '/var/www/html/air-application/original_file.csv'
TEMPORAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/temporal_file.csv'
ORIGINAL_FILE_ADDRESS = '/Users/lourdesmontalvo/Documents/Projects/Fondecyt/air-application/original_file.csv'

def queryLastPredictedSpatialMeasurement(pollutant_name,last_hours,pollutant_unit):
    """ The last historical records of air quality spatial prediction (last 6h, 12h, 24h) based on IDW models"""
    columns = None
    if (pollutant_unit=='ppb'):
    	columns = (GridToPredict.lat, GridToPredict.lon, InterpolatedPollutants.hour_position,
                   InterpolatedPollutants.ppb_value)
    elif(pollutant_unit=='ugm3'):
    	columns = (GridToPredict.lat, GridToPredict.lon, InterpolatedPollutants.hour_position,
    			   InterpolatedPollutants.ug_m3_value)

    if(columns!=None):
    	return session.query(*columns).join(GridToPredict, InterpolatedPollutants.grid_id == GridToPredict.id). \
	    							                 join(Pollutant, InterpolatedPollutants.pollutant_id == Pollutant.id). \
	                                   group_by(GridToPredict.id, InterpolatedPollutants.id, Pollutant.id). \
	                                   filter(Pollutant.pollutant_name == pollutant_name). \
	                                   filter(InterpolatedPollutants.hour_position <= last_hours). \
	                                   order_by(InterpolatedPollutants.hour_position.desc()).all()
    return None

#def queryLastFutureRecordsOfSpatialMeasurement(pollutant_name,last_hours,pollutant_unit):
#    """ The last future records of air quality spatial prediction (next 6h) based on IDW models"""
#    columns = None
#    if (pollutant_unit=='ppb'):
#    	columns = (GridToPredict.lat, GridToPredict.lon, FutureInterpolatedPollutants.hour_position,
#                   FutureInterpolatedPollutants.ppb_value)
#    elif(pollutant_unit=='ugm3'):
#    	columns = (GridToPredict.lat, GridToPredict.lon, FutureInterpolatedPollutants.hour_position,
#    			      FutureInterpolatedPollutants.ug_m3_value)

#    if(columns!=None):
#    	return session.query(*columns).join(GridToPredict, FutureInterpolatedPollutants.grid_id == GridToPredict.id). \
#	    							                 join(Pollutant, FutureInterpolatedPollutants.pollutant_id == Pollutant.id). \
#	                                   group_by(GridToPredict.id, FutureInterpolatedPollutants.id, Pollutant.id). \
#	                                   filter(Pollutant.pollutant_name == pollutant_name). \
#	                                   filter(FutureInterpolatedPollutants.hour_position < last_hours). \
#	                                   order_by(FutureInterpolatedPollutants.hour_position.asc()).all()
#   return None

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
    print("llegue a mergeSameHourPosition")
    id2record = defaultdict(partial(defaultdict, list))
    print(id2record)
    for record in records:
      merged_record = id2record[record['hour_position']]
      #print(merged_record)
      for key, value in record.items():
        merged_record[key].append(value)

    result = list()
    for record in id2record.items():
      result.append(dict(record[1]))
    print("Termine con mergeSameHourPosition")
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
    return [measurement._asdict() for measurement in future_measurements]

def mergeSameHoursDictionary(predicted_measurements):
    total_hours = 6
    all_hours = []
    for i in range(total_hours):
      sameHourDictionary ={}
      sameHourDictionary["hour_position"] = i+1
      sameHourDictionary["lat"] = predicted_measurements[0]["lat"]
      sameHourDictionary["lon"] = predicted_measurements[0]["lon"]
      sameHourDictionary["timestamp"] = predicted_measurements[0]["timestamp"]
      for measurement in predicted_measurements:
        if(measurement["hour_position"]==i+1):
          sameHourDictionary[measurement["pollutant_name"]]=measurement["ug_m3_value"]
      all_hours.append(sameHourDictionary)
    return all_hours

def queryTotalSpatialMeasurementByPollutant(pollutant_name):
    """ The total records of air quality spatial prediction based on IDW models"""
    spatial_measurement = []
    with open(ORIGINAL_FILE_ADDRESS) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
          lat,lon = get_business_helper.getGridPosition(int(row[1]))
          print(lat, lon)
          spatial_json={"pollutant_id":int(row[0]),"lat":lat,"lon":lon,"ug_m3_value":None if(row[2]=='') else float(row[2]),"hour_position":int(row[3]),"timestamp":row[4]}
          spatial_measurement.append(spatial_json)
    return spatial_measurement 

def setAverageValuesByHour(predicted_measurements):
    new_predicted_measurements = []
    ug_m3_measurement = []
    for hour_element in predicted_measurements:
      hour_element["ug_m3_value"] = [measurement for measurement in hour_element["ug_m3_value"] if(measurement!=None) ]
      arr_ug_m3_value = np.array(hour_element["ug_m3_value"])
      ug_m3_measurement = np.append(ug_m3_measurement, arr_ug_m3_value)
    max_measurement_by_pollutant = np.max(ug_m3_measurement)
    min_measurement_by_pollutant = np.min(ug_m3_measurement)
    for hour_element in predicted_measurements:
      hour_element["ug_m3_value"] = [measurement for measurement in hour_element["ug_m3_value"] if(measurement!=None) ]
      hour_element["max"] = max_measurement_by_pollutant
      hour_element["min"] = min_measurement_by_pollutant
      hour_element["timestamp"] = hour_element["timestamp"][0]
      hour_element["hour_position"] = hour_element["hour_position"][0]
      new_predicted_measurements.append(hour_element)
    return new_predicted_measurements



