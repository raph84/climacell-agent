from datetime import datetime, timedelta
import math
from pytz import timezone
import pytz
import pandas as pd
from dateutil.parser import parse

tz = timezone('America/Toronto')
tz_utc = timezone('UTC')


def ceil_dt(dt, delta):
    ceil = datetime.min + math.ceil(
        (dt.replace(tzinfo=None) - datetime.min) /
        timedelta(minutes=delta)) * timedelta(minutes=delta)

    return ceil.replace(tzinfo=tz)


def utcnow():
    return datetime.now(tz=tz)

def get_tz():
    return tz


def get_utc_tz():
    return tz_utc

def utc_to_toronto(dt):
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = tz_utc.localize(dt).astimezone(tz)
    else:
        dt = dt.astimezone(tz)
    return dt


def scan_and_apply_tz(dictio):

    if isinstance(dictio, dict):
        for i in dictio.keys():
            dictio[i] = scan_and_apply_tz(dictio[i])
    if isinstance(dictio, list):
        for idx,i in enumerate(dictio):
            i2 = scan_and_apply_tz(i)
            dictio[idx] = i2

    if isinstance(dictio, datetime):
        dictio2 = utc_to_toronto(dictio)

        return dictio2
    else:
        try:
            parsed = parse(dictio)
            dictio2 = utc_to_toronto(parsed).isoformat()
        except (ValueError, TypeError):
            return dictio
            
        return dictio2

    return dictio
