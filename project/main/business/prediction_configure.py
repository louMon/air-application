import project.main.business.get_business_helper as get_business_helper
import project.main.business.post_business_helper as post_business_helper
from flask import jsonify, make_response, request
from project import app

@app.route('/api/get_prediction_configure_by_model_type/', methods=['GET'])
def getPredictionConfigureByModelType():
    """ To list all grid in a combo box - No parameters required """
    model_type = str(request.args.get('model_type'))
    try:
        spatialConfigure = get_business_helper.queryGetSpatialConfigure(model_type)
        return make_response(jsonify(spatialConfigure), 200)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/update_timestamp_running/', methods=['POST'])
def updateTimestampRunning():
    """ Grid function to record grid to predict """
    try:
        data_json = request.get_json()
        post_business_helper.updateTimestampRunning(data_json)
        return make_response('Running timestamp updated', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)


