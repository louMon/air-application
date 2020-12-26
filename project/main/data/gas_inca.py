from flask import jsonify, make_response, request
from project import app, socketio
import datetime
import dateutil.parser
import dateutil.tz

@app.route('/api/saveGasInca/', methods=['POST'])
def handleGasInca():
    """ To record gas and dust measurement in gas inca table - Json input of Air Quality Measurement """
    try:
        data_json = request.get_json()
        post_data_helper.storeGasIncaInDB(data_json)
        socketio.emit('gas_inca_summary', data_json)
        return make_response('OK', 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)

@app.route('/api/last_gas_inca_data/', methods=['GET'])
def getLastGasIncaData():
    """ To list all measurement of the last hour from the gas inca table - No parameters required """
    try:
        final_timestamp_gases = datetime.datetime.now(dateutil.tz.tzutc())
        initial_timestamp_gases = final_timestamp_gases - datetime.timedelta(hours=1)
        gas_inca_last_data = get_data_helper.queryDBGasInca(initial_timestamp_gases, final_timestamp_gases)
        gas_inca_last_data_list = []
        if gas_inca_last_data is not None:  
            for measurement in gas_inca_last_data:
                measurement = measurement._asdict()
                measurement['qhawax_name'] = same_helper.getQhawaxName(measurement['qhawax_id'])
                gas_inca_last_data_list.append(measurement)
            return make_response(jsonify(gas_inca_last_data_list), 200)
        return make_response(jsonify('Gas Inca not found'), 200)
    except TypeError as e:
        json_message = jsonify({'error': '\'%s\'' % (e)})
        return make_response(json_message, 400)
