from project import db

class User(db.Model):
    __tablename__ = 'user_ai'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(300), nullable=False, unique=True)
    password_hash = db.Column(db.String(300), nullable=False)

class GasInca(db.Model):
    __tablename__ = 'gas_inca'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False)
    CO = db.Column(db.Float)
    CO2 = db.Column(db.Float)
    H2S = db.Column(db.Float)
    NO = db.Column(db.Float)
    NO2 = db.Column(db.Float)
    O3 = db.Column(db.Float)
    PM1 = db.Column(db.Float)
    PM25 = db.Column(db.Float)
    PM10 = db.Column(db.Float)
    SO2 = db.Column(db.Float)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))
    main_inca = db.Column(db.Float)

class AirQualityMeasurement(db.Model):
    __tablename__ = 'air_quality_measurement'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False)
    timestamp_zone = db.Column(db.DateTime, nullable=False)
    CO = db.Column(db.Float)
    CO_ug_m3 = db.Column(db.Float)
    H2S = db.Column(db.Float)
    H2S_ug_m3 = db.Column(db.Float)
    NO2 = db.Column(db.Float)
    NO2_ug_m3 = db.Column(db.Float)
    O3 = db.Column(db.Float)
    O3_ug_m3 = db.Column(db.Float)
    PM25 = db.Column(db.Float)
    PM10 = db.Column(db.Float)
    SO2 = db.Column(db.Float)
    SO2_ug_m3 = db.Column(db.Float)
    uv = db.Column(db.Float)
    uva = db.Column(db.Float)
    uvb = db.Column(db.Float)
    spl = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    temperature = db.Column(db.Float)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    alt = db.Column(db.Float)
    I_temperature = db.Column(db.Float)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))

class FiveMinutesMeasurement(db.Model):
    __tablename__ = 'five_minute_measurement'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False)
    timestamp_zone = db.Column(db.DateTime, nullable=False)
    CO = db.Column(db.Float)
    CO_ug_m3 = db.Column(db.Float)
    H2S = db.Column(db.Float)
    H2S_ug_m3 = db.Column(db.Float)
    NO2 = db.Column(db.Float)
    NO2_ug_m3 = db.Column(db.Float)
    O3 = db.Column(db.Float)
    O3_ug_m3 = db.Column(db.Float)
    PM25 = db.Column(db.Float)
    PM10 = db.Column(db.Float)
    SO2 = db.Column(db.Float)
    SO2_ug_m3 = db.Column(db.Float)
    uv = db.Column(db.Float)
    spl = db.Column(db.Float)
    humidity = db.Column(db.Float)
    pressure = db.Column(db.Float)
    temperature = db.Column(db.Float)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))

class EnvironmentalStation(db.Model):
    __tablename__ = 'environmental_station'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    address = db.Column(db.String(300), nullable=False)
    comercial_name = db.Column(db.String(300), nullable=False)
    district = db.Column(db.String(300), nullable=False)
    module_id = db.Column(db.Integer)

class Pollutant(db.Model):
    __tablename__ = 'pollutant'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_name = db.Column(db.String(300), nullable=False)
    type = db.Column(db.String(300), nullable=False)

class LastPredict(db.Model):
    __tablename__ = 'last_predict'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    last_time_spatial_prediction = db.Column(db.DateTime, nullable=False)
    last_time_temporal_prediction = db.Column(db.DateTime, nullable=False)

class GridToPredict(db.Model):
    __tablename__ = 'grid_to_predict'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    has_qhawax = db.Column(db.Boolean, nullable=False)

class InterpolatedPollutants(db.Model):
    __tablename__ = 'interpolated_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    grid_id = db.Column(db.Integer,db.ForeignKey('grid_to_predict.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

class PastInterpolatedPollutants(db.Model):
    __tablename__ = 'past_interpolated_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    grid_id = db.Column(db.Integer,db.ForeignKey('grid_to_predict.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

class TemporalPollutants(db.Model):
    __tablename__ = 'temporal_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    environmental_station_id = db.Column(db.Integer,db.ForeignKey('environmental_station.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

class PredictionConfigure(db.Model):
    __tablename__ = 'prediction_configure'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    model_name = db.Column(db.String(300), nullable=False)
    last_running_timestamp = db.Column(db.DateTime, nullable=False)
    table_name = db.Column(db.String(300), nullable=False)

class Wind(db.Model):
    __tablename__ = 'wind'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    weather_main = db.Column(db.String(300), nullable=False)
    weather_desc = db.Column(db.String(300), nullable=False)
    temperature = db.Column(db.Float)
    pressure = db.Column(db.Float)
    humidity = db.Column(db.Float)
    speed = db.Column(db.Float)
    degree = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, nullable=False)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))

class Traffic(db.Model):
    __tablename__ = 'traffic'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    Traffic_Green_1 = db.Column(db.Float)
    Traffic_Orange_1 = db.Column(db.Float)
    Traffic_Red_1 = db.Column(db.Float)
    Traffic_Dark_1 = db.Column(db.Float)
    Traffic_Green_2 = db.Column(db.Float)
    Traffic_Orange_2 = db.Column(db.Float)
    Traffic_Red_2 = db.Column(db.Float)
    Traffic_Dark_2 = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, nullable=False)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))

class Senamhi(db.Model):
    __tablename__ = 'senamhi'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    dominentpol = db.Column(db.String(300), nullable=False)
    url = db.Column(db.String(300), nullable=False)
    co_iaqi = db.Column(db.Float)
    o3_iaqi = db.Column(db.Float)
    dew_iaqi = db.Column(db.Float)
    h_iaqi = db.Column(db.Float)
    no2_iaqi = db.Column(db.Float)
    p_iaqi = db.Column(db.Float)
    pm10_iaqi = db.Column(db.Float)
    pm25_iaqi = db.Column(db.Float)
    t_iaqi = db.Column(db.DateTime, nullable=False)
    w_iaqi = db.Column(db.DateTime, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    station_id = db.Column(db.Integer, db.ForeignKey('environmental_station.id'))
