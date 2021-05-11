import project.main.business.get_business_helper as get_business_helper
import project.main.business.post_business_helper as post_business_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/store_env_station/', methods=['POST'])
def storeEnvStation():
    """ Env Station function to record daily average measurement """
    data_json = request.get_json()
    try:
        post_business_helper.storeEnvStation(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_all_env_station/', methods=['GET'])
def getAllEnvStation():
    """ To list all station in a combo box - No parameters required """
    try:
        allStation = get_business_helper.queryGetAllEnvStation()
        allStation = [station._asdict() for station in allStation]
        return make_response(jsonify(allStation), 200)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/get_env_station/', methods=['GET'])
def getEnvStation():
    """ Get station - Input Id of station """
    station_id = int(request.args.get('station_id'))
    try:
        station_detail = get_business_helper.queryGetEnvStation(station_id)
        return make_response(jsonify(station_detail), 200)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)