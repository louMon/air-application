import project.main.data.post_data_helper as post_data_helper
import project.main.data.get_data_helper as get_data_helper
import project.main.business.get_business_helper as get_business_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/store_temporal_prediction/', methods=['POST'])
def storeTemporalPrediction():
    """ Recent Temporal Prediction function to record prediction """
    try:
        data_json = request.get_json()
        array_pollutant = {'CO_ug_m3':'CO','H2S_ug_m3':'H2S','NO2_ug_m3':'NO2','O3_ug_m3':'O3','PM10':'PM10','PM25':'PM25','SO2_ug_m3':'SO2'}
        station_id = get_business_helper.getStationID(data_json['module_id'])
        for measurement in data_json["measurement"]:
            for pollutant_name,value in array_pollutant.items():
                pollutant_id = get_business_helper.getPollutantID(value)
                if(pollutant_id!=None):
                    new_json = {"pollutant_id":pollutant_id,"environmental_station_id":station_id,"ug_m3_value":measurement[pollutant_name],"hour_position":int(measurement['hour_position'])}
                    post_data_helper.storeTemporalPredictionDB(new_json)
        return make_response('OK', 200)
    except (TypeError,KeyError) as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_historical_of_temporal/', methods=['GET'])
def getHistoricalOfTemporalPrediction():
    """ Get station - Input Id of station """
    pollutant_name = str(request.args.get('pollutant'))
    last_hours = int(request.args.get('last_hours'))
    pollutant_unit = str(request.args.get('pollutant_unit'))
    try:
        #Aqui entraria la validacion de a partir de cierta hora ya apuntara a la otra tabla
        predicted_measurements = get_data_helper.queryLastPredictedTemporalMeasurement(pollutant_name,last_hours,pollutant_unit)
        if(predicted_measurements!=None):
            predicted_measurements = [measurement._asdict() for measurement in predicted_measurements]
            predicted_measurements = get_data_helper.mergeSameHourPosition(predicted_measurements)
            return make_response(jsonify(predicted_measurements), 200)
        return make_response('Pollutant Unit is not in the right way', 404)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_future_records_of_every_station/', methods=['GET'])
def getFutureRecordsOfEveryStation():
    """ Get future records to interpolate spatially in future (6h)"""
    station_id = int(request.args.get('environmental_station_id'))
    try:
        #Aqui entraria la validacion de a partir de cierta hora ya apuntara a la otra tabla
        predicted_measurements = get_data_helper.queryFutureMeasurement(station_id)
        if(predicted_measurements!=[]):
            return make_response(jsonify(predicted_measurements), 200)
        return make_response('There is no future records yet', 404)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/delete_all_temporal_prediction/', methods=['POST'])
def deleteAllTemporalPrediction():
    """ delete all temporal prediction """
    try:
        post_data_helper.deleteAllTemporalPredictionInDB()
        return make_response('Recent Temporal Prediction value has been deleted', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)
