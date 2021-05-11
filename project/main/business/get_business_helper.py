from project.database.models import EnvironmentalStation, Pollutant, GridToPredict, PredictionConfigure
from project import app, db

session = db.session

dictModelConfigure = {"Historical_Spatial":1,"Forecasting":2,"Future_Spatial":3}

def queryGetAllEnvStation():
	fields = (EnvironmentalStation.id, EnvironmentalStation.lat, EnvironmentalStation.lon, EnvironmentalStation.module_id,\
			  EnvironmentalStation.address, EnvironmentalStation.comercial_name, EnvironmentalStation.district)
	stations = session.query(*fields).all()
	return None if (stations is []) else session.query(*fields).order_by(EnvironmentalStation.id.asc()).all()

def queryGetActivePollutants():
    """ Helper Eca Noise function to list all zones - No parameters required """
    columns = (Pollutant.id, Pollutant.pollutant_name, Pollutant.type)
    return session.query(*columns).filter_by(status=True).order_by(Pollutant.id.desc()).all()

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

def queryGetModelConfigure(model_type):
    """ Helper Eca Noise function to list all zones - No parameters required """
    if(model_type in dictModelConfigure):
        id_value = dictModelConfigure[model_type]
        last_running = session.query(PredictionConfigure.last_running_timestamp).filter_by(id=id_value).all()[0][0]
        return beautyFormatDate(last_running)
    return None

def beautyFormatDate(date):
    return addZero(date.month)+"-"+addZero(date.day)+"-"+addZero(date.year)+" "+addZero(date.hour)+":"+addZero(date.minute)+":"+addZero(date.second)

def addZero(number):
    return "0"+str(number) if (number<10) else str(number)

def getPollutantID(pollutant_name):
    """ Helper Pollutant function to get Pollutant ID """
    return session.query(Pollutant.id).filter_by(pollutant_name=pollutant_name).order_by(Pollutant.id.desc()).first()[0]

def getStationID(module_id):
    """ Helper Environamental function to get environmental ID """
    return session.query(EnvironmentalStation.id).filter_by(module_id=module_id).order_by(EnvironmentalStation.id.desc()).first()[0]
