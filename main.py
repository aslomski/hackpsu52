import json, requests, polyline
import pandas as pd
import numpy as np
from multiprocessing import Pool


def fetch_route(start="State+College,PA", end="New+York,NY"):
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = dict(
        origin=start.replace(" ", "+"),
        destination=end.replace(" ", "+"),
        waypoints='',
        sensor='false',
        key = 'AIzaSyC-1ZUi1jWmmoVd98MNuFsBBkM_tTykrDs'
    )

    resp = requests.get(url=url, params=params)
    data = json.loads(resp.text)
    return data


def fetch_locationinfo(point):
    lat, lon = point
    url = "http://dataservice.accuweather.com/locations/v1/cities/geoposition/search?" \
          "apikey=HackPSU2017&q=%s%%2C%s&language=en-us" % (lat, lon)
    resp = requests.get(url=url)
    data = json.loads(resp.text)
    return data


def get_locationkey(locationinfo):
    return locationinfo['Key']

def get_locationname(locationinfo):
    return locationinfo['EnglishName']


def fetch_weather(key):
    url = "http://dataservice.accuweather.com/forecasts/v1/hourly/1hour/%s?apikey=HackPSU2017&detail=true" % key
    resp = requests.get(url=url)
    data = json.loads(resp.text)
    return data


def decode(string):
    return polyline.decode(string)


def parse(json):
    sum = 0
    legs = json['routes'][0]['legs']
    for leg in legs:
        sum += leg['duration']['value']
    return sum

def get_polyline(json):
    s = json['routes'][0]['overview_polyline']
    return s

def remove_duplicate(list):
    seen = set()
    seen_add = seen.add
    return [x for x in list if not (x in seen or seen_add(x))]


def fetch_coords(start, end):
    poly = get_polyline(fetch_route(start, end))['points']
    fullpoints = decode(poly)

    pool = Pool(8)
    locations = pool.map(fetch_locationinfo, fullpoints)
    df_name = pd.DataFrame(locations)[['Key', 'EnglishName']]
    df = pd.DataFrame(fullpoints)
    df.columns = ('lat', 'lon')
    df_full = pd.concat([df, df_name], axis = 1)
    return df_full

def fetch_city_weather(df_full):
    df_nondup = df_full.drop_duplicates(subset= 'Key')
    df_nondup.reset_index(inplace=True)
    keys = list(df_nondup['Key'])

    pool = Pool(8)
    weathers = pool.map(fetch_weather, keys)
    weathers = [x[0] for x in weathers]
    df_weather = pd.DataFrame(weathers)[['DateTime', 'EpochDateTime', 'IconPhrase', 'PrecipitationProbability', 'Temperature', 'WeatherIcon']]
    # print(df_weather)
    df_weather['Temperature'] = [x['Value'] for x in df_weather['Temperature']]
    precip = [12,13,14,15,16,17,18,19,20,21,22,23,25,26,29,39,40,41,42,43,44]
    df_weather['Precipitation'] = (df_weather['WeatherIcon'].isin(precip))

    df_nondup = pd.concat([df_nondup, df_weather], axis=1)
    # print (df_nondup)
    return df_nondup

#
def first_rain(df):
    res = []
    for index, row in df.iterrows():
        lower_bound = max(0, index-10)
        if (df.loc[lower_bound:index]['Precipitation']).any() and (True in res[lower_bound:index]):
            res.append(False)
        elif row['Precipitation']:
            res.append(True)
        else:
            res.append(False)
    return pd.DataFrame(res)


def main(start, end):
    df_full = fetch_coords(start, end)
    df_nondup = fetch_city_weather(df_full)
    df_nondup['RainAlert'] = first_rain(df_nondup)
    return df_full, df_nondup


def dump(df):
    # dump into database
    return instance_key


if __name__ == "__main__":
    start = "State College,PA"
    end = "New York, NY"
    df_full, df_nondup = fetch_city_weather(start, end)
    df_nondup['RainAlert'] = first_rain(df_nondup)