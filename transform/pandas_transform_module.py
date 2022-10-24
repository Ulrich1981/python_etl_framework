#!/usr/bin/env python3.10

import pandas as pd

def transform(config, df):
    df_out = pd.DataFrame()
    for key, value in config["field_list"].items():
        if value["map"] in df:
            df_out[key] = df[value["map"]]
        elif value["map"].startswith("pd.") or value["map"].startswith("df."):
            df_out[key] = eval(value["map"])
        else:
            df_out[key] = df.eval(value["map"])
    return df_out