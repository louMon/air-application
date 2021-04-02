from project import db

#Registro de las estaciones para las predicciones temporales de cada uno.
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

#Registro de los contaminantes gas o polvo con su estado correspondiente
class Pollutant(db.Model):
    __tablename__ = 'pollutant'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_name = db.Column(db.String(300), nullable=False)
    type = db.Column(db.String(300), nullable=False)
    status = db.Column(db.Boolean, nullable=False)

#Registro de las grillas que no son un modulo o estacion
class GridToPredict(db.Model):
    __tablename__ = 'grid_to_predict'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    has_qhawax = db.Column(db.Boolean, nullable=False)

#Tabla de registro de la ultima vez de ejecucion de cada script
class PredictionConfigure(db.Model):
    __tablename__ = 'prediction_configure'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    model_name = db.Column(db.String(300), nullable=False)
    last_running_timestamp = db.Column(db.DateTime, nullable=False)
    table_name = db.Column(db.String(300), nullable=False)

#Interpolacion espacial historica
class InterpolatedPollutants(db.Model):
    __tablename__ = 'interpolated_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    grid_id = db.Column(db.Integer,db.ForeignKey('grid_to_predict.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

#Calidad del aire a futuro
class TemporalPollutants(db.Model):
    __tablename__ = 'temporal_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    environmental_station_id = db.Column(db.Integer,db.ForeignKey('environmental_station.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

#Interpolacion espacial a futuro
class FutureInterpolatedPollutants(db.Model):
    __tablename__ = 'future_interpolated_pollutants'

    # Column's definition
    id = db.Column(db.Integer, primary_key=True)
    pollutant_id = db.Column(db.Integer,db.ForeignKey('pollutant.id'))
    grid_id = db.Column(db.Integer,db.ForeignKey('grid_to_predict.id'))
    ppb_value = db.Column(db.Float)
    ug_m3_value = db.Column(db.Float)
    hour_position= db.Column(db.Integer)

#Registro de variables de viento del API Open Weather API
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

#Registro de variables de trafico del API de Google Maps
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

#Registro de variables de la estacion del Senahmi
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
