from project.database.models import EnvironmentalStation, User, GridToPredict,\
									InterpolatedPollutants, PredictionConfigure
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

def deleteGridToPredictInDB():
	session.query(InterpolatedPollutants).filter(InterpolatedPollutants.grid_id >= 1).delete(synchronize_session=False)
	session.commit()
	session.query(GridToPredict).filter(GridToPredict.id >= 1).delete(synchronize_session=False)
	session.commit()

def updateTimestampRunning(json):
    session.query(PredictionConfigure). \
            filter_by(id=json["model_id"]).update(values={"last_running_timestamp":json["last_running_timestamp"]})
    session.commit()