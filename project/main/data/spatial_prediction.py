import project.main.data.post_data_helper as post_data_helper
import project.main.data.get_data_helper as get_data_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/store_spatial_prediction/', methods=['POST'])
def storeSpatialPrediction():
    """ Recent Spatial Prediction function to record prediction """
    try:
        data_json = request.get_json()
        post_data_helper.storeSpatialPredictionDB(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_historical_of_spatial/', methods=['GET'])
def getHistoricalOfSpatialPrediction():
    """ Get station - Input Id of station """
    pollutant_name = str(request.args.get('pollutant'))
    last_hours = int(request.args.get('last_hours'))
    pollutant_unit = str(request.args.get('pollutant_unit'))
    try:
        #Aqui entraria la validacion de a partir de cierta hora ya apuntara a la otra tabla
        predicted_measurements = get_data_helper.queryLastPredictedSpatialMeasurement(pollutant_name,last_hours,pollutant_unit)
        if(predicted_measurements!=None):
            predicted_measurements = [measurement._asdict() for measurement in predicted_measurements]
            predicted_measurements = get_data_helper.mergeSameHourPosition(predicted_measurements)
            predicted_measurements = get_data_helper.setAverageValuesByHour(predicted_measurements)
            return make_response(jsonify(predicted_measurements), 200)
        return make_response('Pollutant Unit is not in the right way', 404)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/delete_all_spatial_prediction/', methods=['POST'])
def deleteAllSpatialPrediction():
    """ Endpoint to delete all interpolated historical records """
    try:
        post_data_helper.deleteAllSpatialPredictionInDB()
        return make_response('Spatial Prediction value has been deleted', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

