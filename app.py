#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT_NAME
DESCRIPTION
"""

__appname__ = "APPLICATION_NAME"
__author__ = "Gerald Bäck (https://github.com/geraldbaeck/)"
__version__ = "0.0.1"
__license__ = "UNLICENSE"

# System libraries
import csv
import json
import logging
from datetime import datetime, timedelta

import requests

# 3rd party libraries
import boto3  # https://boto3.readthedocs.io/en/latest/index.html
from chalice import Chalice  # https://chalice.readthedocs.io/en/latest/
from chalice import Rate, Response

# Own libraries

# configure chalice (AWS Lambda framework)
app = Chalice(app_name='luftguetemesswerte')
app.log.setLevel(logging.DEBUG)
app.debug = True

# default encoder for JSON.dumps to serialize datetime objects
json.JSONEncoder.default = lambda self, o: (o.isoformat() if isinstance(o, datetime) else None)

# Global configurations
DATA_URL = "https://www.wien.gv.at/ma22-lgb/umweltgut/lumesakt-v2.csv"
S3_BUCKET = "luftguetemesswerte"
GITHUB_URL = "https://github.com/geraldbaeck/Luftg-temesswerte-Wien"


def dict_to_item(raw):
    if type(raw) is dict:
        resp = {}
        for k, v in raw.items():
            if type(v) is str:
                resp[k] = {
                    'S': v
                }
            elif type(v) is int:
                resp[k] = {
                    'I': str(v)
                }
            elif type(v) is dict:
                resp[k] = {
                    'M': dict_to_item(v)
                }
            elif type(v) is list:
                resp[k] = []
                for i in v:
                    resp[k].append(dict_to_item(i))
            elif type(v) is datetime:
                resp[k] = {
                    'S':  v.isoformat()
                }
        return resp
    elif type(raw) is str:
        return {
            'S': raw
        }
    elif type(raw) is int:
        return {
            'I': str(raw)
        }


def store_last_etag(etag):
    client = boto3.client('s3')
    client.put_object(
        ACL="private",
        Body=etag,
        Bucket=S3_BUCKET,
        CacheControl="no-cache, no-store, must-revalidate",
        ContentType="text/plain",
        Expires=datetime.now() + timedelta(hours=1),
        Key="etag",
        ServerSideEncryption="AES256",
        StorageClass="REDUCED_REDUNDANCY", )


def get_last_etag():
    client = boto3.client('s3')
    try:
        obj = client.get_object(Bucket=S3_BUCKET, Key="etag")
        etag = obj['Body'].read()
    except client.exceptions.NoSuchKey:
        etag = "NoSuchKey"
    return etag


def downloadCSV(etag=None):
    if not etag:
        etag = get_last_etag()
    content = False
    headers = {'If-None-Match': etag}
    r = requests.get(DATA_URL, headers=headers)

    if r.status_code == 200:
        content = r.content.decode('ISO-8859-1')
        store_last_etag(r.headers["ETag"])
    elif r.status_code == 304:
        # do nothing because file has alredy been loaded
        app.log.debug("CSV has already been loaded (ETag match).")
    else:
        app.log.warning("Http download error {}".format(r.status_code))

    return content


def save_datapoint(datapoint):
    id_template = "{time:%Y%m%d%H%M}_{station}_{name}_{type}"
    datapoint['_id'] = id_template.format(**datapoint)
    client = boto3.client('dynamodb')
    client.put_item(
        Item=dict_to_item(datapoint),
        TableName=S3_BUCKET,
    )


def store_to_S3(content, file_name, time, content_type):
    file_tmplt = "{time:%Y}/{time:%m}/{time:%d}/{time:%Y%m%d%H%M}{file_name}"
    file_key = file_tmplt.format(time=time, file_name=file_name)
    client = boto3.client('s3')
    client.put_object(
        ACL="public-read",
        Body=content,
        Bucket=S3_BUCKET,
        CacheControl="public, max-age=31536000, immutable",
        ContentType=content_type,
        Expires=datetime(2099, 9, 9),
        Key=file_key,
        ServerSideEncryption="AES256",
        StorageClass="REDUCED_REDUNDANCY", )
    app.log.debug("{} saved to S3:{}".format(file_key, S3_BUCKET))


# converts string to datetime
# fixes error with 24:00 time instead of 0:00
def get_date(date_string):
    def convert_date(old_date):
        return datetime.strptime(old_date, '%d.%m.%Y, %H:%M')

    if ", 24:" in date_string:
        date_string = date_string.replace(", 24:", ", 00:")
        date_new = convert_date(date_string) + timedelta(days=1)
    else:
        date_new = convert_date(date_string)
    return date_new


def process_csv_data(content, header, types, units):
    datapoint = dict()  # ein Messpunkt
    datapoints = []
    readCSV = csv.reader(content, delimiter=';')
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
                    'time': get_date(row[i + 1])  # eg. 29.09.2017, 10:30
                }
            elif row[i + 1] == 'NE':
                datapoint = {}
            else:
                datapoint[h] = float(row[i + 1].replace(",", '.'))
                if types[i + 1]:
                    datapoint['type'] = types[i + 1]
                if units[i + 1] and units[i + 1] is not "MESZ":
                    datapoint['unit'] = units[i + 1]
    return datapoints


@app.route('/')
def index():
    return Response(
        body='<html><body><a href="{0}">{0}</a></body></html>'.format(
            GITHUB_URL),
        status_code=302,
        headers={'Content-Type': 'text/html',
                 'Location': GITHUB_URL})


@app.schedule(Rate(14, unit=Rate.MINUTES))
def download(event):
    """ Main entry point of the app """
    csv_content = downloadCSV()

    if csv_content:
        content = csv_content.splitlines()

        # get file date from first line of csv
        # first line eg. Lumes;v2.10;29.09.17-10:30:00
        date_raw = content[0].split(';')[-1]
        timestamp = datetime.strptime(date_raw, '%d.%m.%y-%H:%M:%S')

        # Messobjekte (eg. Zeit, O2, NO,...)
        header = content[1].replace("\n", "").split(";")
        header[0] = "NAME"

        # Messwertspezifikation (eg HMW,..)
        types = content[2].replace("\n", "").split(";")

        # Einheiten (eg. °C,...)
        units = content[3].replace("\n", "").split(";")

        # read the actual data
        datapoints = process_csv_data(content[4:], header, types, units)

        # save the csv file to  S3
        store_to_S3(csv_content, "_original.csv", timestamp, "text/csv")
        store_to_S3(
            json.dumps(datapoints), ".json", timestamp, "application/json")
