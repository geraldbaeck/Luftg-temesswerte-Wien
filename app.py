#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT_NAME
DESCRIPTION

Usage: SCRIPT_NAME.py [-hv]
       SCRIPT_NAME.py [--logLevel=<LOGLEVEL>][--environment=<ENV>]

Options:
  -h --help             Show this screen.
  -v --version          Show version.
  --logLevel=LOGLEVEL   The level of the logging output  [default: INFO]
  --environment=ENV     The execution environment
                        (development, staging or production)  [default: development]
"""

__appname__ = "APPLICATION_NAME"
__author__  = "Gerald Bäck (https://github.com/geraldbaeck/)"
__version__ = "0.0.1"
__license__ = "UNLICENSE"

DEFAULT_LOGLEVEL    = "DEBUG"
DEFAULT_ENVIRONMENT = "development"

# System libraries
import csv
from datetime import datetime
from datetime import timedelta
import json
import logging

# 3rd party libraries
import boto3  # https://boto3.readthedocs.io/en/latest/index.html
from chalice import Chalice
import requests

# Own libraries


# configure chalice (AWS Lambda framework)
app = Chalice(app_name='luftguetemesswerte')
app.log.setLevel(logging.DEBUG)
app.debug = True

# default encoder for JSON.dumps to serialize datetime objects
json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime) else None)

#Global configurations
DATA_URL = "https://www.wien.gv.at/ma22-lgb/umweltgut/lumesakt-v2.csv"

def downloadCSV(etag='"20144-13d2-55adbd9be0540"'):
    content = False
    headers = {'If-None-Match': etag}
    r = requests.get(DATA_URL, headers=headers)

    if r.status_code == 200:
        content = r.content.decode('ISO-8859-1')
    elif r.status_code == 304:
        # do nothing because file has alredy been loaded
        app.log.debug("CSV has already been loaded (ETag match).")
    else:
        app.log.warning("Http download error {}".format(r.status_code))

    return content


def save_datapoint(datapoint):
    datapoint['_id'] = "{time:%Y%m%d%H%M}_{station}_{name}_{type}".format(**datapoint)
    # app.log.debug(datapoint)
    return True


def store_to_S3(content, file_name, time, content_type):
    file_tmplt = "{time:%Y}/{time:%m}/{time:%d}/{time:%Y%m%d%H%M}_{file_name}"
    file_key = file_tmplt.format(time=time, file_name=file_name)
    client = boto3.client('s3')
    client.put_object(
        ACL="public-read",
        Body=content,
        Bucket="luftguetemesswerte",
        CacheControl="public, max-age=31536000, immutable",
        ContentType=content_type,
        Expires=datetime(2099, 9, 9),
        Key=file_key,
        ServerSideEncryption="AES256",
        StorageClass="REDUCED_REDUNDANCY",
    )

# converts string to datetime
# fixes error with 24:00 time instead of 0:00
def get_date(date_string):
    convert_date = lambda date_old: datetime.strptime(date_old, '%d.%m.%Y, %H:%M')
    if ", 24:" in date_string:
        date_string = date_string.replace(", 24:", ", 00:")
        date_new = convert_date(date_string) + timedelta(days=1)
    else:
        date_new = convert_date(date_string)
    return date_new


@app.route('/')
def index():
    """ Main entry point of the app """
    # download the csv file
    csv_content = downloadCSV()

    if csv_content:
        content = csv_content.splitlines()

        # get file date from first line of csv
        # first line eg. Lumes;v2.10;29.09.17-10:30:00
        date_raw = content[0].split(';')[-1]
        data_time = datetime.strptime(date_raw, '%d.%m.%y-%H:%M:%S')

        # Messobjekte (eg. Zeit, O2, NO,...)
        header = content[1].replace("\n", "").split(";")
        header[0] = "NAME"

        # Messwertspezifikation (eg HMW,..)
        types = content[2].replace("\n", "").split(";")

        # Einheiten (eg. °C,...)
        units = content[3].replace("\n", "").split(";")

        # read the actual data
        datapoint = dict()  # ein Messpunkt
        readCSV = csv.reader(content[4:], delimiter=';')
        datapoints = []
        for row in readCSV:
            station = row[0]  # Messstation (eg STEF, TAB,....)

            # iterate header/Messgrößen
            # und erstelle Datenpunkte für jede Messstation und Messgröße
            # iteration startet in col[1] weil erste col = station
            for i, h in enumerate(header[1:]):
                if h.startswith("Zeit"):
                    if datapoint:
                        save_datapoint(datapoint)  # alten Messpunkt speichern
                        datapoints.append(datapoint)
                    datapoint = {
                        'station': station,
                        'name': h.replace("Zeit-", ""),
                        'time': get_date(row[i+1])  # eg. 29.09.2017, 10:30
                    }
                elif row[i+1] == 'NE':
                    datapoint = {}
                else:
                    datapoint[h] = float(row[i+1].replace(",", '.'))
                    if types[i+1]:
                        datapoint['type'] = types[i+1]
                    if units[i+1] and units[i+1] is not "MESZ":
                        datapoint['unit'] = units[i+1]

        # save the csv file to  S3
        store_to_S3(csv_content, "original.csv", data_time, "text/csv")
        store_to_S3(json.dumps(datapoints), ".json", data_time, "application/json")
