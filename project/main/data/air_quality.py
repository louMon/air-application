from flask import jsonify, make_response, request
from datetime import timedelta
from project import app
import dateutil.parser
import numpy as np
import datetime
import dateutil
import requests
import json

QAIRAMAP_URL = 'https://qairamapnapi.qairadrones.com/'
GET_HOURLY_VALID_BY_QHAWAX = 'api/air_quality_measurements_period/'
GET_ALL_MODULES = 'api/AllQhawaxByCompany/?company_array=37,38,39,40,41,43,&number_companies=8'

@app.route('/api/air_quality_measurements/', methods=['POST'])
def storeAirQualityData():
    """ To record processed measurement and valid processed measurement every five seconds - Json input of Air Quality Measurement """
    try:
        data_json = request.get_json()
        post_data_helper.storeAirQualityDataInDB(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/air_quality_measurements_period_all_modules/', methods=['GET'])
def getAirQualityMeasurementsTimePeriodAllModules():
    """ To list all measurement in ppb of air quality measurement table in a define period of time - This is an hourly average measurement """
    all_measurements = []
    try:
        initial_timestamp_utc = request.args.get('initial_timestamp')#'%d-%m-%Y %H:%M:%S') UTC 00
        final_timestamp_utc = request.args.get('final_timestamp')# '%d-%m-%Y %H:%M:%S') UTC 00
        all_qhawaxs_response = requests.get(QAIRAMAP_URL + GET_ALL_MODULES, params={"company_array": "21,22,23,24,25,26,27,28,","number_companies":"8"})
        all_qhawaxs = json.loads(all_qhawaxs_response.text)
        for qhawax in all_qhawaxs:
            response_measurements = requests.get(QAIRAMAP_URL + GET_HOURLY_VALID_BY_QHAWAX, params={'name': qhawax['name'], 'initial_timestamp':initial_timestamp_utc,'final_timestamp':final_timestamp_utc})
            measurements = json.loads(response_measurements.text)
            all_measurements = all_measurements + measurements
        return make_response(jsonify(all_measurements), 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/gas_average_measurement/', methods=['GET'])
def getGasAverageMeasurementsEvery24():
    """ To list all values by a define gas or dust in ug/m3 of air quality measurement table of the last 24 hours"""
    try:
        qhawax_name = request.args.get('qhawax')
        gas_name = request.args.get('gas')
        gas_average_measurement = get_data_helper.queryDBGasAverageMeasurement(qhawax_name, gas_name)
        gas_average_measurement_list = util_helper.getFormatData(gas_average_measurement)
        if(gas_average_measurement_list is not None):
            return make_response(jsonify(gas_average_measurement_list), 200)
        return make_response(jsonify('Measurements not found'), 200)
    except (ValueError,TypeError) as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/average_valid_processed_period/', methods=['GET'])
def getAverageValidProcessedMeasurementsTimePeriodByCompany():
    """ To list all average measurement of valid processed measurement table in a define period of time and company """
    try:
        qhawax_id = int(request.args.get('qhawax_id'))
        company_id = int(request.args.get('company_id'))
        initial_timestamp = datetime.datetime.strptime(request.args.get('initial_timestamp'), '%d-%m-%Y %H:%M:%S')
        final_timestamp = datetime.datetime.strptime(request.args.get('final_timestamp'), '%d-%m-%Y %H:%M:%S')

        if (get_business_helper.qhawaxBelongsCompany(qhawax_id,company_id) is not True):
            return make_response(jsonify('Valid Measurements not found'), 200)

        average_valid_processed_measurements = get_data_helper.queryDBValidAirQuality(qhawax_id, initial_timestamp, final_timestamp)
        average_valid_processed_measurements_list = [measurement._asdict() for measurement in average_valid_processed_measurements]
        return make_response(jsonify(average_valid_processed_measurements_list), 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

