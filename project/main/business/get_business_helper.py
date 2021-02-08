from project.database.models import EnvironmentalStation, User, Pollutant, GridToPredict, PredictionConfigure
from project import app, db

session = db.session

def queryGetAllEnvStation():
	fields = (EnvironmentalStation.id, EnvironmentalStation.lat, EnvironmentalStation.lon, \
			  EnvironmentalStation.address, EnvironmentalStation.comercial_name, EnvironmentalStation.district)
	stations = session.query(*fields).all()
	return None if (stations is []) else session.query(*fields).order_by(EnvironmentalStation.id.desc()).all()

def queryGetPollutants():
    """ Helper Eca Noise function to list all zones - No parameters required """
    columns = (Pollutant.id, Pollutant.pollutant_name, Pollutant.type)
    return session.query(*columns).order_by(Pollutant.id.desc()).all()

def queryGetGridToPredict():
    """ Helper Eca Noise function to list all zones - No parameters required """
    columns = (GridToPredict.id, GridToPredict.lat, GridToPredict.lon, GridToPredict.has_qhawax)
    return session.query(*columns).order_by(GridToPredict.id.desc()).all()

def queryCountOfGridPredict():
    """ Helper Eca Noise function to list all zones - No parameters required """
    columns = (GridToPredict.id)
    return session.query(*columns).count()

def TileDoesNotExist(json):
    grid_id = session.query(GridToPredict.id).filter_by(lat=str(json["lat"]), lon=str(json["lon"])).all()
    if(grid_id):
        return False
    return True

def queryGetSpatialConfigure(model_type):
    """ Helper Eca Noise function to list all zones - No parameters required """
    id_value = 1 if(model_type=='Spatial') else 2
    last_running = session.query(PredictionConfigure.last_running_timestamp).filter_by(id=id_value).all()[0][0]
    return beautyFormatDate(last_running)

def beautyFormatDate(date):
    return addZero(date.day)+"-"+addZero(date.month)+"-"+addZero(date.year)+" "+addZero(date.hour)+":"+addZero(date.minute)+":"+addZero(date.second)

def addZero(number):
    return "0"+str(number) if (number<10) else str(number)