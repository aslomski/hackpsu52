#!/usr/bin/python

"""
    author: peter meglis
    date: 5 november 2017
"""

import MySQLdb as mysqldb

host = "aa4wkh4lrsd01w.cui8zwjstn9r.us-west-2.rds.amazonaws.com"
username = "admin"
password = "masterpass"
database = "mainDB"
db = mysqldb.connect(host, username, password, database)
cursor = db.cursor()


"""
    inserts a new entry into the mapinstances table in the maindb with startloc and endloc
"""
def insertmapinstance(startloc, startLat, startLon, endloc, endLat, endLon, imageURL):
    instancekey = ""
    try:
        cursor.execute("insert into mapInstances (startLoc, startLatitude, startLongitude, endLoc, endLatitude, endLongitude) values ('%s', '%s', '%s', '%s', '%s', '%s')" % (startloc, startLat, startLon, endloc, endLat, endLon))
        db.commit()
        cursor.execute("select max(instancekey) from mapInstances")
        instancekey = cursor.fetchone()[0]
        cursor.execute("insert into radarInstance (instanceKey, radarImage) values ('%s', '%s')" % (instancekey, imageURL))
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()
    return instancekey

"""
    inserts a new entry into the routelocations table in the maindb with these parameters
"""
def insertroutelocation(instancekey, latitude, longitude, cityname, temp, timeofweather, precipprobability, weathertype, precipitation, rainalert):
    try:
        cursor.execute("insert into routeLocations values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (instancekey, latitude, longitude, cityname, temp, timeofweather, precipprobability, weathertype, precipitation, rainalert))
        db.commit()
    except Exception as e:
        print(e)
        db.rollback()


"""
    inserts the route information into the database and returns the instance key for the route
"""
def insert(startloc, startLat, startLon, endloc, endLat, endLon, imageURL, table):
    instancekey = insertmapinstance(startloc, startLat, startLon, endloc, endLat, endLon, imageURL)
    print("InstanceKey", instancekey)
    for item in table:
        insertroutelocation(instancekey, *item)
    db.close()
    return instancekey



if __name__ == "__main__":
    # print(insertmapinstance("boston", "new york"))
    # insertroutelocation(10, 40.86, -77.86, "state college", 47, "2017-11-05t00:00:00-04:00", 99, 5, true, true)
    loc1 = [40.86, -77.86, "boston", 47, "2017-11-05t00:00:00-04:00", 99, 5, 1, 0]
    loc2 = [50.86, 27.86, "michigan city", 47, "2017-11-05t00:00:00-08:00", 0, 5, 1, 1]

    insert("state college", "california", [loc1, loc2])
    # insertroutelocation(123, 40.86, -77.86, "state college", 47, "2017-11-05t00:00:00-04:00", 99, 5, 1, 0)