from project.database.models import AirQualityMeasurement, Traffic, \
                                    Wind, Senamhi, InterpolatedPollutants, PastInterpolatedPollutants,TemporalPollutants
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

def storeHourlyDataInDB(data):
    """  Helper Hourly Measurement function to store hourly data """
    air_quality_data = AirQualityMeasurement(**data)
    session.add(air_quality_data)
    session.commit()

def storeSpatialPredictionDB(data_json):
    spatial_predict = InterpolatedPollutants(**data_json)
    session.add(spatial_predict)
    session.commit()

def storeLastSpatialPredictionDB(data_json):
    last_spatial_predict = PastInterpolatedPollutants(**data_json)
    session.add(last_spatial_predict)
    session.commit()

def deleteAllSpatialPredictionInDB():
    session.query(InterpolatedPollutants).filter(InterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
    session.commit()

def deleteAllLastSpatialPredictionInDB():
    session.query(PastInterpolatedPollutants).filter(PastInterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
    session.commit()

def storeTemporalPredictionDB(data_json):
    temporal_predict = TemporalPollutants(**data_json)
    session.add(temporal_predict)
    session.commit()

def deleteAllTemporalPredictionInDB():
    session.query(TemporalPollutants).delete(synchronize_session=False)
    session.commit()