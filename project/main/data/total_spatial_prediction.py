import project.main.data.post_data_helper as post_data_helper
import project.main.data.get_data_helper as get_data_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/get_total_spatial_interpolation/', methods=['GET'])
def getTotalSpatialPrediction():
    """ Get station - Input Id of station """
    pollutant_name = str(request.args.get('pollutant'))
    try:
        #Aqui entraria la validacion de a partir de cierta hora ya apuntara a la otra tabla
        predicted_measurements = get_data_helper.queryTotalSpatialMeasurementByPollutant(pollutant_name)
        if(predicted_measurements!=None):
            predicted_measurements = get_data_helper.mergeSameHourPosition(predicted_measurements)
            predicted_measurements = get_data_helper.setAverageValuesByHour(predicted_measurements)
            return make_response(jsonify(predicted_measurements), 200)
        return make_response('Pollutant Unit is not in the right way', 404)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_max_min_by_pollutant/', methods=['GET'])
def getMaxMinByPollutant():
    """ Get Max and Min of 24 hour pollutant measurement """
    pollutant_name = str(request.args.get('pollutant'))
    try:
        #Aqui entraria la validacion de a partir de cierta hora ya apuntara a la otra tabla
        predicted_measurements = get_data_helper.queryTotalSpatialMeasurementByPollutant(pollutant_name)
        if(predicted_measurements!=None):
            predicted_measurements = get_data_helper.mergeSameHourPosition(predicted_measurements)
            max_val, min_val, median_val = get_data_helper.getMaxMinOfMeasurements(predicted_measurements)
            return make_response(jsonify({"max":max_val,"min":min_val,"median":median_val}), 200)
        return make_response('Pollutant Unit is not in the right way', 404)
    except Exception as e:
        json_message = jsonify({'error': 'Dont forget to write NO2, PM25 or CO =>\'%s\'' % (e)})
        return make_response(json_message, 400)