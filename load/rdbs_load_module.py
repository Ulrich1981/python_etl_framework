#!/usr/bin/env python3.10

import yaml
from connection import connection
import pandas as pd
import io
from datetime import datetime

def data2csv(df):
    s_buf = io.StringIO()
    df = df.replace({r'\n|\r|\t|\xa0': ' '}, regex=True)
    df = df.replace({r'\\': '\\\\\\\\'}, regex=True)
    df = df.replace({u'\xa0': ''})
    df_string = df.to_csv(index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
    df_string = df_string.replace(u'\\xa0', u'')
    s_buf.write(df_string)
    s_buf.seek(0)
    return s_buf

def upsert(config, csv_data):
    with open("connection/db_config.yaml", "r") as stream:
        conn_config = yaml.safe_load(stream)[config["dwh_connection"]]
    with connection.get_connection(conn_config) as conn:
        schema = config["schema"]
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}")
            fields = config["field_list"]
            table_name = config["table"]
            schema_name = config["schema"]
            set_columns = ",\n".join(["%s = excluded.%s" % (x,x) for x,y in fields.items() if not "is_key" in y])
            key_columns = ",".join(["%s" % (x) for x,y in fields.items() if "is_key" in y])
            source_table = f"source_{table_name}"
            sql_template = f"""INSERT INTO {schema_name}.{table_name}
                                SELECT *
                                FROM {schema_name}.{source_table}
                                ON CONFLICT ({key_columns}) DO UPDATE 
                                  SET {set_columns}"""
            cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{source_table}")
            cur.execute(f"CREATE TABLE {schema_name}.{source_table} (LIKE {table_name} INCLUDING ALL);");
            cur.copy_from(csv_data, source_table, columns=[x for x in fields])
            print("Data uploaded")
            print(datetime.now())
            cur.execute(sql_template)
            print("Data upserted")
            cur.execute(f"DROP TABLE {schema_name}.{source_table}")
            csv_data.close()
            conn.commit()

def insert(config, csv_data):
    with open("db_config.yaml", "r") as stream:
        conn_config = yaml.safe_load(stream)[config["dwh_connection"]]
    with connection.get_connection(conn_config) as conn:
        schema = config["schema"]
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}")
            fields = config["field_list"]
            table_name = config["table"]
            schema_name = config["schema"]
            source_table = f"source_{table_name}"
            sql_template = f"""INSERT INTO {schema_name}.{table_name}
                                SELECT *
                                FROM {schema_name}.{source_table}""";
            cur.execute(f"CREATE TABLE {schema_name}.{source_table} (LIKE {table_name} INCLUDING ALL);");
            cur.copy_from(csv_data, source_table, columns=[x for x in fields])
            print("Data uploaded")
            print(datetime.now())
            cur.execute(sql_template)
            print("Data inserted")
            cur.execute(f"DROP TABLE {schema_name}.{source_table}")
            csv_data.close()
            conn.commit()

def load(config, df = None, file_path = None):
    if isinstance(df, pd.DataFrame):
        csv_data = data2csv(df)
    if config["update_strategy"] == "upsert":
        upsert(config, csv_data)
    if config["update_strategy"] == "insert":
        insert(config, csv_data)