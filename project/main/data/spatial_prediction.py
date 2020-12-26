import project.main.data.post_data_helper as post_data_helper
import project.main.data.get_data_helper as get_data_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/store_spatial_prediction/', methods=['POST'])
def storeSpatialPrediction():
    """ Spatial Prediction function to record prediction """
    try:
        data_json = request.get_json()
        print(data_json)
        post_data_helper.storeSpatialPredictionDB(data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)