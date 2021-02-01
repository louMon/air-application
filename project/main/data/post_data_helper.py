from project.database.models import AirQualityMeasurement, GasInca, FiveMinutesMeasurement, Traffic, \
                                    Wind, Senamhi, InterpolatedPollutants
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

def storeFiveMinuteDataInDB(data):
    """  Helper Five Minute Measurement function to store five minute average data """
    five_minute_data = FiveMinutesMeasurement(**data)
    session.add(five_minute_data)
    session.commit()

def storeHourlyDataInDB(data):
    """  Helper Hourly Measurement function to store hourly data """
    air_quality_data = AirQualityMeasurement(**data)
    session.add(air_quality_data)
    session.commit()

def storeGasIncaDataInDB(data):
    """  Helper Gas Inca function to store gas inca data """
    gas_inca_data = GasInca(**data)
    session.add(gas_inca_data)
    session.commit()

def storeSpatialPredictionDB(data_json):
    spatial_predict = InterpolatedPollutants(**data_json)
    session.add(spatial_predict)
    session.commit()

def deleteAllSpatialPredictionInDB():
    session.query(InterpolatedPollutants).filter(InterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
    session.commit()