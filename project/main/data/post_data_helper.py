from project.database.models import Traffic, Wind, Senamhi, InterpolatedPollutants, \
                                    TemporalPollutants,FutureInterpolatedPollutants, \
                                    TotalSpatialInterpolation
from project import app, db, socketio
import dateutil.parser
import dateutil.tz
import datetime
import time

session = db.session

def storeWindDataInDB(data):
    """  Helper Wind function to store wind data """
    wind_data = Wind(**data)
    session.add(wind_data)
    session.commit()

def storeTrafficDataInDB(data):
    """  Helper Traffic function to store traffic data """
    traffic_data = Traffic(**data)
    session.add(traffic_data)
    session.commit()

def storeSenamhiDataInDB(data):
    """  Helper Senamhi function to store senamhi data """
    senamhi_data = Senamhi(**data)
    session.add(senamhi_data)
    session.commit()

def storeTemporalPredictionDB(data_json):
    temporal_predict = TemporalPollutants(**data_json)
    session.add(temporal_predict)
    session.commit()

def deleteAllTemporalPredictionInDB():
    session.query(TemporalPollutants).filter(TemporalPollutants.environmental_station_id >= 1).delete(synchronize_session=False)
    session.commit()