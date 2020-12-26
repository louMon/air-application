from flask import jsonify, make_response, request
from project import app
import datetime

@app.route('/api/air_five_minutes_measurements/', methods=['POST'])
def storeAirFiveMinutesData():
    """ Air Daily Measurement function to record daily average measurement """
    try:
        data_json = request.get_json()
        post_data_helper.storeAirFiveMinutesDataInDB(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/air_five_minutes_measurements_period/', methods=['GET'])
def getFiveMinutesMeasurementsPeriod():
    """ To list all measurement in ppb of air quality measurement table in a define period of time - This is an hourly average measurement """
    try:
        qhawax_id = int(request.args.get('qhawax_id'))
        company_id = int(request.args.get('company_id'))
        initial_timestamp_utc = datetime.datetime.strptime(request.args.get('initial_timestamp'), '%d-%m-%Y %H:%M:%S')
        final_timestamp_utc = datetime.datetime.strptime(request.args.get('final_timestamp'), '%d-%m-%Y %H:%M:%S')
        
        if (get_business_helper.qhawaxBelongsCompany(qhawax_id,company_id) is not True):
            return make_response(jsonify('Five Measurements not found'), 200)
        
        air_quality_measurements = get_data_helper.queryDBFiveMinutes(qhawax_id, initial_timestamp_utc, final_timestamp_utc)
        if(air_quality_measurements is not None):
            air_quality_measurements_list = [measurement._asdict() for measurement in air_quality_measurements]
            return make_response(jsonify(air_quality_measurements_list), 200)
        return make_response(jsonify('Five Measurements not found'), 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)
