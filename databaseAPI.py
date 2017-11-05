#!/usr/bin/python

"""
	Author: Peter Meglis
	Date: 5 November 2017
"""

import MySQLdb

host = "aa4wkh4lrsd01w.cui8zwjstn9r.us-west-2.rds.amazonaws.com"
username = "admin"
password = "masterpass"
database = "mainDB"
db = MySQLdb.connect(host, username, password, database)
cursor = db.cursor()


"""
	Inserts a new entry into the mapInstances table in the mainDB with startLoc and endLoc
"""
def insertMapInstance(startLoc, endLoc):
	try:
	   cursor.execute("INSERT INTO mapInstances(startLoc, endLoc) VALUES ('%s', '%s')" % (startLoc, endLoc))
	   db.commit()
	   cursor.execute("SELECT MAX(instanceKey) FROM mapInstances")
	   instanceKey = cursor.fetchone()[0]
	except:
	   db.rollback()
	return instanceKey

"""
	Inserts a new entry into the routeLocations table in the mainDB with these parameters
"""
def insertRouteLocation(instanceKey, latitude, longitude, cityName, temp, timeOfWeather, precipProbability, weatherType, precipitation, rainAlert):
	try:
	   cursor.execute("INSERT INTO routeLocations VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (instanceKey, latitude, longitude, cityName, temp, timeOfWeather, precipProbability, weatherType, precipitation, rainAlert))
	   db.commit()
	except:
	   db.rollback()
	

"""
	Inserts the route information into the database and returns the instance key for the route
"""
def insert(startLoc, endLoc, table):
	instanceKey = insertMapInstance(startLoc, endLoc)
	for item in table:
		insertRouteLocation(instanceKey, *item)
	db.close()
	return instanceKey



if __name__ == "__main__":
	# print(insertMapInstance("Boston", "New York"))
	# insertRouteLocation(10, 40.86, -77.86, "State College", 47, "2017-11-05T00:00:00-04:00", 99, 5, True, True)
	loc1 = [40.86, -77.86, "Boston", 47, "2017-11-05T00:00:00-04:00", 99, 5, 1, 0]
	loc2 = [50.86, 27.86, "Michigan City", 47, "2017-11-05T00:00:00-08:00", 0, 5, 1, 1]

	insert("State College", "California", [loc1, loc2])
	# insertRouteLocation(123, 40.86, -77.86, "State College", 47, "2017-11-05T00:00:00-04:00", 99, 5, 1, 0)