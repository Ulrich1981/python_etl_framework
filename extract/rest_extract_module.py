#!/usr/bin/env python3.10

import pandas as pd
import yaml
from connection import connection
import requests
import json
import multiprocessing
from datetime import datetime


def get_max_value(config):
    print(datetime.now())
    schema = config["schema"]
    table = config["table"]
    max_value = config["max_value"]
    total_value = config["total_value"]
    with open("connection/db_config.yaml", "r") as stream:
        conn_config = yaml.safe_load(stream)[config["dwh_connection"]]
    with connection.get_connection(conn_config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX({max_value}) FROM {schema}.{table}")
            max_value = cur.fetchall()[0][0] or config["default_max_value"]
    return max_value

def get_response(config, start_at, max_results, max_value):
    url = config["url"].format(start_at = start_at,
                     max_results = max_results,
                     max_value = max_value)
    response = requests.get(url, headers = config["headers"])
    return json.loads(response.text)

def process_part(start_at, config, return_dict, max_value, sema):
    response_json = get_response(config, start_at, config["max_results"], max_value)
    return_dict[start_at] = pd.json_normalize(response_json[config["entries"]])
    print(start_at)
    print(datetime.now())
    sema.release()

def rest_extract(config):
    max_value = get_max_value(config)
    response_json = get_response(config = config, start_at = 0, max_results = 1, max_value = max_value)
    start_points = range(0,response_json[config["total_value"]],config["max_results"])

    manager = multiprocessing.Manager()
    concurrency = 10
    sema = multiprocessing.Semaphore(concurrency)
    return_dict = manager.dict()
    jobs = []
    for i in start_points:
        sema.acquire()
        p = multiprocessing.Process(target=process_part, args=(i, config, return_dict, max_value, sema))
        jobs.append(p)
        p.start()
    for proc in jobs:
        proc.join()

    df_out = pd.DataFrame()
    for key in return_dict.keys():
        df_out = df_out.append(return_dict[key], ignore_index=True)
    return df_out


def extract(config):
    if "max_value" in config:
        max_value = get_max_value(config)
    if config["output"] == "pandas_df":
        df_out = rest_extract(config)
        return df_out