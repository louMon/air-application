import project.main.business.get_business_helper as get_business_helper
import project.main.business.post_business_helper as post_business_helper
from flask import jsonify, make_response, request
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
        if (get_business_helper.TileDoesNotExist(data_json)):
            post_business_helper.storeGridToPredictInDB(data_json)
            return make_response('Grid recorded', 200)
        return make_response('Grid already exists', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)


@app.route('/api/delete_all_grids/', methods=['POST'])
def deleteAllGrids():
    """ Grid function to delete grid to predict """
    try:
        post_business_helper.deleteGridToPredictInDB()
        return make_response('Grids has been deleted', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

