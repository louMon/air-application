from project.database.models import AirQualityMeasurement, GasInca, FiveMinutesMeasurement, Traffic, \
                                    Wind, Senamhi, InterpolatedPollutants, GridToPredict,Pollutant
from collections import defaultdict
from functools import partial
from project import app, db

session = db.session

def queryLastPredictedMeasurement(pollutant_name,last_hours,pollutant_unit):
    """ Get All qHAWAXs in field - No parameters required """
    columns = None
    if (pollutant_unit=='ppb'):
    	columns = (GridToPredict.lat, GridToPredict.lon, GridToPredict.has_qhawax,InterpolatedPollutants.hour_position,
                   InterpolatedPollutants.ppb_value, InterpolatedPollutants.id)
    elif(pollutant_unit=='ugm3'):
    	columns = (GridToPredict.lat, GridToPredict.lon, GridToPredict.has_qhawax,InterpolatedPollutants.hour_position,
    			   InterpolatedPollutants.ug_m3_value, InterpolatedPollutants.id)

    if(columns!=None):
    	return session.query(*columns).join(GridToPredict, InterpolatedPollutants.grid_id == GridToPredict.id). \
	    							   join(Pollutant, InterpolatedPollutants.pollutant_id == Pollutant.id). \
	                                   group_by(GridToPredict.id, InterpolatedPollutants.id, Pollutant.id). \
	                                   filter(Pollutant.pollutant_name == pollutant_name). \
	                                   filter(InterpolatedPollutants.hour_position < last_hours). \
	                                   order_by(InterpolatedPollutants.hour_position.asc()).all()
    return None

def mergeSameHourPosition(records):
	id2record = defaultdict(partial(defaultdict, list))
	for record in records:
		merged_record = id2record[record['hour_position']]
		for key, value in record.items():
			merged_record[key].append(value)
	
	result = list()
	for record in id2record.items():
		result.append(dict(record[1]))
	return result
    