# Example of calculating Euclidean distance
from math import sqrt
import math

# Test distance function
stations = [[-12.045286,-77.030902],[-12.050278,-77.026111],[-12.041025,-77.043454],
           [-12.044226,-77.050832],[-12.0466667,-77.080277778],[-12.0450749,-77.0278449],
           [-12.047538,-77.035366],[-12.054722,-77.029722],[-12.044236,-77.012467],
           [-12.051526,-77.077941],[-12.042525,-77.033486],[-12.046736,-77.047594],
           [-12.045394,-77.036852],[-12.057582,-77.071778]]
look_for_this_position = [-12.057479, -77.085395]

def deg2rad(deg):
	return deg * (math.pi/180)

def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
	R = 6371 # Radius of the earth in km
	dLat = deg2rad(lat2-lat1)  #  deg2rad below
	dLon = deg2rad(lon2-lon1)
	a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
	d = R * c #  Distance in km
	return d

def getNearestStations(stations, lat , lon, k=4):
	sorted_stations=sorted(stations, key= lambda station:(getDistanceFromLatLonInKm(lat,lon, station[0], station[1])), reverse=False)
	return sorted_stations[:k]

neighbors = getNearestStations(stations, look_for_this_position[0],look_for_this_position[1], 4)
for neighbor in neighbors:
	print(neighbor)
#sat -12.0466667,-77.080277778
# -12.051526,-77.077941
# -12.057582,-77.071778