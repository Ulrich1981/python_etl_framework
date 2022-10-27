#!/usr/bin/env python3.10

from sshtunnel import SSHTunnelForwarder

def get_connection(conn_config):
    type = conn_config["type"]
    if type == "mysql":
        import mysql.connector
        return mysql.connector.connect(**conn_config["db_credentials"])
    if type == "postgres":
        import psycopg2
        return psycopg2.connect(**conn_config["db_credentials"])
    if type == "postgres_alchemy":
        import psycopg2
        from sqlalchemy import create_engine
        from sqlalchemy.engine import url as sa_url
        db_connect_url = sa_url.URL(drivername='postgresql+psycopg2',**conn_config["db_credentials"])
        return create_engine(db_connect_url)
    if type == "redshift":
        import redshift_connector 
        return redshift_connector.connect(**conn_config["db_credentials"])
        

        
def open_ssh_tunnel(conn_config):
    return SSHTunnelForwarder(**conn_config["ssh_credentials"])
