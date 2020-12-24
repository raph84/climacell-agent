'''
pip install -U pytest

> pytest
'''

from utils import utcnow, ceil_dt, tz, utc_to_toronto, iterate_keys_for_datetime
from datetime import datetime
from pytz import timezone
import pytz


def test_ceil_dt_15():
    dt = utcnow().replace(minute=12)
    test = ceil_dt(dt, 15)
    assert test.minute == 15
    assert test.tzinfo == tz


def test_ceil_dt_30():
    dt = utcnow().replace(minute=22)
    test = ceil_dt(dt, 15)
    assert test.minute == 30
    assert test.tzinfo == tz


def test_ceil_dt_45():
    dt = utcnow().replace(minute=33)
    test = ceil_dt(dt, 15)
    assert test.minute == 45
    assert test.tzinfo == tz


def test_ceil_dt_00():
    dt = utcnow().replace(minute=46)
    test = ceil_dt(dt, 15)
    assert test.minute == 0
    assert test.tzinfo == tz

def test_naive_to_toronto():
    dt = datetime.strptime("20201223 101010","%Y%m%d %H%M%S")
    assert dt.tzinfo is None

    dt = utc_to_toronto(dt)

    assert dt.tzname() == 'EST'

    assert datetime.utcoffset(dt).seconds == 68400


def test_tz_to_toronto():
    dt = pytz.timezone('America/Chicago').localize(datetime.strptime("20201223 101010", "%Y%m%d %H%M%S"))

    assert dt.tzinfo is not None

    dt = utc_to_toronto(dt)

    assert dt.tzname() == 'EST'

    assert datetime.utcoffset(dt).seconds == 68400


def test_toronto_to_toronto():
    dt = pytz.timezone('America/Toronto').localize(
        datetime.strptime("20201223 101010", "%Y%m%d %H%M%S"))

    assert dt.tzinfo is not None

    dt = utc_to_toronto(dt)

    assert dt.tzname() == 'EST'

    assert datetime.utcoffset(dt).seconds == 68400



def test_iterate_keys_for_datetime():

    conv = iterate_keys_for_datetime(lst)

    assert conv[0]["observation_time"][
        "value"] == '2020-12-23T14:15:01.835000-05:00'


lst = [{
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -8.37,
        "units": "C"
    },
    "feels_like": {
        "value": -15.19,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.62,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.81,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 6.19,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1024.5625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 55.63,
        "units": "%"
    },
    "wind_direction": {
        "value": 273.5,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 177.4375,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T19:15:01.835Z"
    },
    "name": "realtime-20201223-191503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.06,
        "units": "C"
    },
    "feels_like": {
        "value": -15.44,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.62,
        "units": "C"
    },
    "wind_speed": {
        "value": 4,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 5.06,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1024.375,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 58.69,
        "units": "%"
    },
    "wind_direction": {
        "value": 287.13,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 268.875,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T18:15:01.649Z"
    },
    "name": "realtime-20201223-181503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.5,
        "units": "C"
    },
    "feels_like": {
        "value": -17.06,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.5,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.31,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.63,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1024.4375,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 61.44,
        "units": "%"
    },
    "wind_direction": {
        "value": 308.31,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 312.9375,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T17:15:00.825Z"
    },
    "name": "realtime-20201223-171503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.5,
        "units": "C"
    },
    "feels_like": {
        "value": -17.75,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.19,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.38,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.88,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1023.3125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 63.06,
        "units": "%"
    },
    "wind_direction": {
        "value": 300.81,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 260.9375,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T16:15:00.563Z"
    },
    "name": "realtime-20201223-161502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.44,
        "units": "C"
    },
    "feels_like": {
        "value": -18.25,
        "units": "C"
    },
    "dewpoint": {
        "value": -13.69,
        "units": "C"
    },
    "wind_speed": {
        "value": 7.44,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 8.88,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1022.5,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 71,
        "units": "%"
    },
    "wind_direction": {
        "value": 307.75,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 165.6875,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T15:15:00.295Z"
    },
    "name": "realtime-20201223-151502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.44,
        "units": "C"
    },
    "feels_like": {
        "value": -18.62,
        "units": "C"
    },
    "dewpoint": {
        "value": -14.62,
        "units": "C"
    },
    "wind_speed": {
        "value": 8,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.44,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1022.0625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 65.81,
        "units": "%"
    },
    "wind_direction": {
        "value": 312.69,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 164.875,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T14:15:01.020Z"
    },
    "name": "realtime-20201223-141503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.69,
        "units": "C"
    },
    "feels_like": {
        "value": -19.69,
        "units": "C"
    },
    "dewpoint": {
        "value": -14.19,
        "units": "C"
    },
    "wind_speed": {
        "value": 9.56,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 11.56,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1020.125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 69.63,
        "units": "%"
    },
    "wind_direction": {
        "value": 298.25,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 0,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "clear"
    },
    "observation_time": {
        "value": "2020-12-23T13:15:00.325Z"
    },
    "name": "realtime-20201223-131502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -10.12,
        "units": "C"
    },
    "feels_like": {
        "value": -20.19,
        "units": "C"
    },
    "dewpoint": {
        "value": -16.12,
        "units": "C"
    },
    "wind_speed": {
        "value": 9.44,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 11.88,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1020.1875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 61.44,
        "units": "%"
    },
    "wind_direction": {
        "value": 303.38,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-23T12:15:01.074Z"
    },
    "name": "realtime-20201223-121503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.5,
        "units": "C"
    },
    "feels_like": {
        "value": -19.75,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.56,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.31,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 12.5,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1017.1875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 61.19,
        "units": "%"
    },
    "wind_direction": {
        "value": 294.88,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 750,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-23T11:15:00.733Z"
    },
    "name": "realtime-20201223-111503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -9.12,
        "units": "C"
    },
    "feels_like": {
        "value": -19.37,
        "units": "C"
    },
    "dewpoint": {
        "value": -15.12,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.63,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.06,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1015.625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 61.69,
        "units": "%"
    },
    "wind_direction": {
        "value": 294.06,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 688,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-23T10:15:00.324Z"
    },
    "name": "realtime-20201223-101502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -8.44,
        "units": "C"
    },
    "feels_like": {
        "value": -18.81,
        "units": "C"
    },
    "dewpoint": {
        "value": -14.12,
        "units": "C"
    },
    "wind_speed": {
        "value": 11.5,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 14.38,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1014.6875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 63.44,
        "units": "%"
    },
    "wind_direction": {
        "value": 293.06,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 688,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-23T09:15:00.964Z"
    },
    "name": "realtime-20201223-091502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -7.87,
        "units": "C"
    },
    "feels_like": {
        "value": -17.81,
        "units": "C"
    },
    "dewpoint": {
        "value": -13.56,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.75,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.69,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1012.8125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 63.5,
        "units": "%"
    },
    "wind_direction": {
        "value": 292,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 688,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-23T08:15:00.632Z"
    },
    "name": "realtime-20201223-081502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -6.12,
        "units": "C"
    },
    "feels_like": {
        "value": -15.44,
        "units": "C"
    },
    "dewpoint": {
        "value": -11.69,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.81,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.31,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1010.1875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 64.56,
        "units": "%"
    },
    "wind_direction": {
        "value": 293,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 688,
        "units": "m"
    },
    "cloud_base": {
        "value": 688,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T07:15:00.308Z"
    },
    "name": "realtime-20201223-071502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -4.31,
        "units": "C"
    },
    "feels_like": {
        "value": -13.06,
        "units": "C"
    },
    "dewpoint": {
        "value": -10.37,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.81,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.44,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1009.25,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 62.63,
        "units": "%"
    },
    "wind_direction": {
        "value": 297.69,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 750,
        "units": "m"
    },
    "cloud_base": {
        "value": 750,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T06:15:00.999Z"
    },
    "name": "realtime-20201223-061503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -2.44,
        "units": "C"
    },
    "feels_like": {
        "value": -10.56,
        "units": "C"
    },
    "dewpoint": {
        "value": -7.56,
        "units": "C"
    },
    "wind_speed": {
        "value": 10.63,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.25,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1006.875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 68,
        "units": "%"
    },
    "wind_direction": {
        "value": 298.75,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 625,
        "units": "m"
    },
    "cloud_base": {
        "value": 625,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T05:15:00.792Z"
    },
    "name": "realtime-20201223-051502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.44,
        "units": "C"
    },
    "feels_like": {
        "value": -8.62,
        "units": "C"
    },
    "dewpoint": {
        "value": -6.19,
        "units": "C"
    },
    "wind_speed": {
        "value": 9,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 13.5,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1006.0625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 70.06,
        "units": "%"
    },
    "wind_direction": {
        "value": 309.19,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 563,
        "units": "m"
    },
    "cloud_base": {
        "value": 563,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T04:15:00.555Z"
    },
    "name": "realtime-20201223-041502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.81,
        "units": "C"
    },
    "feels_like": {
        "value": -5.56,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.31,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.38,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.56,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1003.0625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 89.69,
        "units": "%"
    },
    "wind_direction": {
        "value": 294.13,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 188,
        "units": "m"
    },
    "cloud_base": {
        "value": 188,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T03:15:00.344Z"
    },
    "name": "realtime-20201223-031502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.94,
        "units": "C"
    },
    "feels_like": {
        "value": -5.62,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.06,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.19,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.94,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1004.125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 85.81,
        "units": "%"
    },
    "wind_direction": {
        "value": 293.81,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 250,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T02:15:01.093Z"
    },
    "name": "realtime-20201223-021503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.81,
        "units": "C"
    },
    "feels_like": {
        "value": -6.25,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.94,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.56,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 8.31,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1002.5,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 85.38,
        "units": "%"
    },
    "wind_direction": {
        "value": 272.13,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 250,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T01:15:00.829Z"
    },
    "name": "realtime-20201223-011503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.94,
        "units": "C"
    },
    "feels_like": {
        "value": -6.87,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.56,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.38,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1001.6875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 89,
        "units": "%"
    },
    "wind_direction": {
        "value": 263.19,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 188,
        "units": "m"
    },
    "cloud_base": {
        "value": 188,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-23T12:24:53.968Z"
    },
    "sunset": {
        "value": "2020-12-23T20:43:45.815Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-23T00:15:00.613Z"
    },
    "name": "realtime-20201223-001502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1,
        "units": "C"
    },
    "feels_like": {
        "value": -6.62,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.94,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.81,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.75,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 999.9375,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 80.5,
        "units": "%"
    },
    "wind_direction": {
        "value": 276.25,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 33.31,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T23:15:00.263Z"
    },
    "name": "realtime-20201222-231502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.87,
        "units": "C"
    },
    "feels_like": {
        "value": -6.56,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.62,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.94,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 8.31,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 999.75,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 87.94,
        "units": "%"
    },
    "wind_direction": {
        "value": 262.25,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 16.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": None,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T22:15:00.686Z"
    },
    "name": "realtime-20201222-221503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.87,
        "units": "C"
    },
    "feels_like": {
        "value": -6.56,
        "units": "C"
    },
    "dewpoint": {
        "value": -3,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.94,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 8.94,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.5,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 85.56,
        "units": "%"
    },
    "wind_direction": {
        "value": 259.44,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 250,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 29.0625,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T21:15:00.265Z"
    },
    "name": "realtime-20201222-211502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.87,
        "units": "C"
    },
    "feels_like": {
        "value": -6.69,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.06,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.13,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.44,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.4375,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 85.38,
        "units": "%"
    },
    "wind_direction": {
        "value": 260.44,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 250,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 28.75,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T20:15:01.023Z"
    },
    "name": "realtime-20201222-201502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -0.87,
        "units": "C"
    },
    "feels_like": {
        "value": -6.69,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.31,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.19,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.75,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 997.8125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 83.75,
        "units": "%"
    },
    "wind_direction": {
        "value": 272,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 313,
        "units": "m"
    },
    "cloud_base": {
        "value": 313,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 87.125,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T19:15:00.790Z"
    },
    "name": "realtime-20201222-191502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1,
        "units": "C"
    },
    "feels_like": {
        "value": -6.75,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.06,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.06,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.75,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 85.88,
        "units": "%"
    },
    "wind_direction": {
        "value": 277.75,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 250,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 149.9375,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T18:15:00.317Z"
    },
    "name": "realtime-20201222-181502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.19,
        "units": "C"
    },
    "feels_like": {
        "value": -6.94,
        "units": "C"
    },
    "dewpoint": {
        "value": -3.12,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.94,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 9.44,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.25,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 86.88,
        "units": "%"
    },
    "wind_direction": {
        "value": 274.56,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 33.31,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 250,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 195.875,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T17:15:01.971Z"
    },
    "name": "realtime-20201222-171503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.31,
        "units": "C"
    },
    "feels_like": {
        "value": -6.37,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.81,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.69,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 8.56,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.5,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 89.5,
        "units": "%"
    },
    "wind_direction": {
        "value": 281.63,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 33.31,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 188,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 195.0625,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T16:15:01.802Z"
    },
    "name": "realtime-20201222-161503"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.19,
        "units": "C"
    },
    "feels_like": {
        "value": -7.19,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.62,
        "units": "C"
    },
    "wind_speed": {
        "value": 6.5,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 10.56,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 998.6875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 89.75,
        "units": "%"
    },
    "wind_direction": {
        "value": 319.06,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 33.31,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 188,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 108.8125,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T15:15:00.822Z"
    },
    "name": "realtime-20201222-151502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.5,
        "units": "C"
    },
    "feels_like": {
        "value": -6.94,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.94,
        "units": "C"
    },
    "wind_speed": {
        "value": 5.25,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.63,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 999.625,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 89.69,
        "units": "%"
    },
    "wind_direction": {
        "value": 320.25,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 33.31,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": None,
        "units": "m"
    },
    "cloud_base": {
        "value": 188,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 26.25,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_clear"
    },
    "observation_time": {
        "value": "2020-12-22T14:15:00.612Z"
    },
    "name": "realtime-20201222-141502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.31,
        "units": "C"
    },
    "feels_like": {
        "value": -6.19,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.19,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.31,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.5,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 999.1875,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 93.69,
        "units": "%"
    },
    "wind_direction": {
        "value": 339.31,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 50,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 125,
        "units": "m"
    },
    "cloud_base": {
        "value": 125,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "partly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T13:15:00.397Z"
    },
    "name": "realtime-20201222-131502"
}, {
    "lat": 48.400643,
    "lon": -68.646753,
    "temp": {
        "value": -1.56,
        "units": "C"
    },
    "feels_like": {
        "value": -6.44,
        "units": "C"
    },
    "dewpoint": {
        "value": -2.81,
        "units": "C"
    },
    "wind_speed": {
        "value": 4.31,
        "units": "m/s"
    },
    "wind_gust": {
        "value": 7.25,
        "units": "m/s"
    },
    "baro_pressure": {
        "value": 1000.125,
        "units": "hPa"
    },
    "visibility": {
        "value": 10,
        "units": "km"
    },
    "humidity": {
        "value": 91.38,
        "units": "%"
    },
    "wind_direction": {
        "value": 340.19,
        "units": "degrees"
    },
    "precipitation": {
        "value": 0,
        "units": "mm/hr"
    },
    "precipitation_type": {
        "value": "none"
    },
    "cloud_cover": {
        "value": 66.69,
        "units": "%"
    },
    "cloud_ceiling": {
        "value": 125,
        "units": "m"
    },
    "cloud_base": {
        "value": 125,
        "units": "m"
    },
    "surface_shortwave_radiation": {
        "value": 0,
        "units": "w/sqm"
    },
    "sunrise": {
        "value": "2020-12-22T12:24:29.234Z"
    },
    "sunset": {
        "value": "2020-12-22T20:43:11.079Z"
    },
    "moon_phase": {
        "value": "first_quarter"
    },
    "weather_code": {
        "value": "mostly_cloudy"
    },
    "observation_time": {
        "value": "2020-12-22T12:15:01.197Z"
    },
    "name": "realtime-20201222-121503"
}]
