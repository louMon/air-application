from flask import jsonify, make_response, request
import project.main.business.get_business_helper as get_business_helper
from project import app

@app.route('/api/get_all_grid/', methods=['GET'])
def getAllGrid():
    """ To list all grid in a combo box - No parameters required """
    try:
        allGrids = get_business_helper.queryGetGridToPredict()
        if allGrids is not None:
            allGrids = [grid._asdict() for grid in allGrids]
            return make_response(jsonify(allGrids), 200)
        return make_response(jsonify('Grid not found'), 200)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/store_grid_to_predict/', methods=['POST'])
def storeGridToPredict():
    """ Grid function to record grid to predict """
    try:
        data_json = request.get_json()
        if (utils.TileDoesNotExist(data_json)):
            print("Grid does not exist")
            post_data_helper.storeGridToPredictInDB(data_json)
            return make_response('Grid recorded', 200)
        else:
            print("Grid exists")
            return make_response('Grid already exists', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

