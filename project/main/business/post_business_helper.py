from project.database.models import EnvironmentalStation, User, GridToPredict
from project import app, db

session = db.session

def storeEnvStation(data_json):
	env_station = EnvironmentalStation(**data_json)
	session.add(env_station)
	session.commit()

def storeGridToPredictInDB(data_json):
	grid_to_predict = GridToPredict(**data_json)
	session.add(grid_to_predict)
	session.commit()