import os
import sys
import urllib.request
import json
import pandas as pd
import matplotlib.pyplot as plt
import time
import random
import requests


import hashlib
import hmac
import base64

class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)

        hash.hexdigest()
        return base64.b64encode(hash.digest())

class api_info :

    madup_account = "wooddd12@madit.kr"
    customer_id = 1147019

def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(round(time.time() * 1000))
    signature = Signature.generate(timestamp, method, uri, secret_key)

    return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp,
            'X-API-KEY': api_key, 'X-Customer': str(customer_id), 'X-Signature': signature}

def getresults(uri, method, params):
    BASE_URL = 'https://api.searchad.naver.com'
    API_KEY = api_info.api_key
    SECRET_KEY = api_info.secret_key
    CUSTOMER_ID = api_info.customer_id
    r = requests.get(BASE_URL + uri, params=params,
                     headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

    return r

uri = '/ncc/adgroups'
method = 'GET'
params = {}
adgroup_res = getresults(uri, method, params)

adgroup_json = json.loads(adgroup_res.text)
adgroup_list = []

for j in adgroup_json :
    if j['adgroupType'] == 'SHOPPING' :
        adgroup_list.append(j['nccAdgroupId'])

for id in adgroup_list:
    uri = f'/ncc/adgroups/{id}/restricted-keywords'
    method = 'GET'
    params = {}

    temp_res = getresults(uri, method, params)
    temp_json = json.loads(temp_res.text)

