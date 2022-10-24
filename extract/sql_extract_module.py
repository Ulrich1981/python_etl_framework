#!/usr/bin/env python3.10

import pandas as pd
import yaml
from connection import connection

def get_sql_response(config):
    with open("connection/db_config.yaml", "r") as stream:
        conn_config = yaml.load(stream, Loader=yaml.Loader)[config["dwh_connection"]]
    with connection.open_ssh_tunnel(conn_config) as s:
        conn = connection.get_connection(conn_config)
        return pd.read_sql_query(config["sql_statement"], conn)

def extract(config):
    if "sql_statement" in config:
        out = get_sql_response(config)
        if "output" in config and config["output"] == "single_value":
            out = out.iloc[0,0]
        return out