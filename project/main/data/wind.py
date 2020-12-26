import project.main.data.post_data_helper as post_data_helper
import project.main.data.get_data_helper as get_data_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/store_wind_measurement/', methods=['POST'])
def storeWindData():
    """ Air Daily Measurement function to record daily average measurement """
    try:
        data_json = request.get_json()
        post_data_helper.storeWindDataInDB(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)