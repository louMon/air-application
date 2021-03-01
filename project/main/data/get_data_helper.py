from project.database.models import AirQualityMeasurement, GasInca, FiveMinutesMeasurement, Traffic, \
                                    Wind, Senamhi, InterpolatedPollutants, GridToPredict,Pollutant,\
                                    PastInterpolatedPollutants
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

def queryNotRecentPredictedMeasurement(pollutant_name,last_hours,pollutant_unit):
    """ Get All qHAWAXs in field - No parameters required """
    columns = None
    if (pollutant_unit=='ppb'):
    	columns = (GridToPredict.lat, GridToPredict.lon, GridToPredict.has_qhawax,PastInterpolatedPollutants.hour_position,
                   PastInterpolatedPollutants.ppb_value, PastInterpolatedPollutants.id)
    elif(pollutant_unit=='ugm3'):
    	columns = (GridToPredict.lat, GridToPredict.lon, GridToPredict.has_qhawax,PastInterpolatedPollutants.hour_position,
    			   PastInterpolatedPollutants.ug_m3_value, PastInterpolatedPollutants.id)

    if(columns!=None):
    	return session.query(*columns).join(GridToPredict, PastInterpolatedPollutants.grid_id == GridToPredict.id). \
	    							   join(Pollutant, PastInterpolatedPollutants.pollutant_id == Pollutant.id). \
	                                   group_by(GridToPredict.id, PastInterpolatedPollutants.id, Pollutant.id). \
	                                   filter(Pollutant.pollutant_name == pollutant_name). \
	                                   filter(PastInterpolatedPollutants.hour_position < last_hours). \
	                                   order_by(PastInterpolatedPollutants.hour_position.asc()).all()
    return None

def queryAllRecentInterpolatedPollutantst():
    """ Get All qHAWAXs in field - No parameters required """
    columns = (InterpolatedPollutants.hour_position,InterpolatedPollutants.ug_m3_value, 
    		   InterpolatedPollutants.ppb_value, InterpolatedPollutants.pollutant_id. InterpolatedPollutants.grid_id)

	recent_predicted_measurements = session.query(*columns).order_by(InterpolatedPollutants.hour_position.asc()).all()
	return [measurement._asdict() for measurement in recent_predicted_measurements]

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
    