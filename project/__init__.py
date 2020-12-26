from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO
from flask_cors import CORS, cross_origin

# Config
app = Flask(__name__)
app.config.from_object('config')
socketio = SocketIO(app,cors_allowed_origins="*",async_handlers=True) 
CORS(app)

# Extensions
db = SQLAlchemy(app)

from project.main.business import environmental_station,user, grid_to_predict, pollutant
from project.main.data import air_quality, gas_inca, air_five_minutes_quality,traffic, wind, senahmi, spatial_prediction
import project.database.models as models
from project.database.models import User, AirQualityMeasurement, GasInca,FiveMinutesMeasurement,\
									EnvironmentalStation, Wind, Traffic, Senamhi, InterpolatedPollutants, \
									GridToPredict, LastPredict, Pollutant

db.create_all()
