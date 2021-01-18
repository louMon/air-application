from project.database.models import EnvironmentalStation, User, Pollutant, GridToPredict
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
    print(json)
    grid_id = session.query(GridToPredict.id).filter_by(lat=str(json["latitude"]), lon=str(json["longitude"])).all()
    print("Grid id: ",grid_id)
    if(tile_id):
        return False
    return True
