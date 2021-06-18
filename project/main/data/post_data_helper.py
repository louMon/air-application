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

def storeSpatialPredictionDB(data_json):
    spatial_predict = InterpolatedPollutants(**data_json)
    session.add(spatial_predict)
    session.commit()

def deleteAllSpatialPredictionInDB():
    session.query(InterpolatedPollutants).filter(InterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
    session.commit()

def storeTemporalPredictionDB(data_json):
    temporal_predict = TemporalPollutants(**data_json)
    session.add(temporal_predict)
    session.commit()

def deleteAllTemporalPredictionInDB():
    session.query(TemporalPollutants).filter(TemporalPollutants.environmental_station_id >= 1).delete(synchronize_session=False)
    session.commit()

#def storeFutureSpatialPredictionDB(data_json):
#    future_spatial_predict = FutureInterpolatedPollutants(**data_json)
#    session.add(future_spatial_predict)
#    session.commit()

#def deleteAllFutureSpatialPredictionInDB():
#    session.query(FutureInterpolatedPollutants).filter(FutureInterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
#    session.commit()

def storeAllSpatialPredictionDB(data_json):
    spatial_predict = TotalSpatialInterpolation(**data_json)
    session.add(spatial_predict)
    session.commit()

def deleteTotalSpatialPredictionInDB():
    session.query(TotalSpatialInterpolation).filter(TotalSpatialInterpolation.grid_id >= 1).delete(synchronize_session=False)
    session.commit()