from flask import jsonify, make_response, request
import project.main.business.get_business_helper as get_business_helper
from project import app

@app.route('/api/get_all_pollutants/', methods=['GET'])
def getAllPollutants():
    """ To list all areas in a combo box - No parameters required """
    try:
        allPollutants = get_business_helper.queryGetPollutants()
        if allPollutants is not None:
            allPollutants = [pollutant._asdict() for pollutant in allPollutants]
            return make_response(jsonify(allPollutants), 200)
        return make_response(jsonify('Pollutants not found'), 200)
    except Exception as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)
