"""This script compares two databases in terms of the table structure
and the containing data. The following is compared:
- list of table
- list of columns per table
- complete table hashes
- complete table hashes of a selected timeframe
  (last month, up to last month)"""

import pandas as pd
from extract import sql_extract_module


def get_columns(dwh_name, table_where):
    """fetch a list of all columns in all tables"""
    sql_statement = f"""
        SELECT
            t.table_schema AS schema_name,
            t.table_name,
            c.column_name,
            'COALESCE(SUBSTRING("' ||
                c.column_name ||
                '"' ||
                CASE WHEN c.data_type = 'boolean'
                     THEN '::int' ELSE '' END ||
                '::text,0,20),'''')' AS column_cast,
            c.data_type
        FROM
            information_schema.tables t
        LEFT join information_schema.columns c 
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE
        t.table_schema NOT IN ('information_schema', 'pg_catalog')
        {table_where}"""
    return sql_extract_module.extract(
        {"sql_statement": sql_statement, "dwh_connection": dwh_name}
    )


def get_special_columns(df):
    """From a df of columns return the concatenation of all columns
       as well as the first date column"""
    out_dict = {}
    out_dict["column_cast"] = df[["column_cast"]].apply(" || ".join)["column_cast"]
    date_columns = df[
        df["data_type"].isin(
            ["date", "timestamp with time zone", "timestamp without time zone"]
        )
    ]["column_name"].reset_index()
    out_dict["date_column"] = (
        date_columns.loc[0]["column_name"] if len(date_columns) > 0 else None
    )
    return pd.Series(out_dict, index=["column_cast", "date_column"])


def get_tables(columns, add_special_columns=False):
    """Get a df with all tables from the df with all columns"""
    print("get_tables")
    if add_special_columns:
        return (
            columns.groupby(["table_name", "schema_name"])
            .apply(get_special_columns)
            .reset_index()
        )
    return columns[["table_name", "schema_name"]].drop_duplicates()


def get_tbl_checksum(row, dwh_name, where=""):
    """Calculate a checksum from all columns in a table"""
    name = row["table_name"]
    schema = row["schema_name"]
    col_concat = row["column_cast"]
    sql_statement = f"""
        SELECT
            SUM(checksum)
        FROM
            (SELECT
                ASCII(MD5({col_concat})) AS checksum
            FROM
                {schema}.{name}
            {where}) s"""
    return sql_extract_module.extract(
        {
            "sql_statement": sql_statement,
            "dwh_connection": dwh_name,
            "output": "single_value",
        }
    )


def compare_datasets(data, item):
    """Two df are compared with each other.

       return: all rows, that are in both df."""
    df0 = data[dwh_list[0]]
    df1 = data[dwh_list[1]]
    print(df0.keys())
    if df1.equals(df0):
        print(f"{item} is equal on both sides")
        return df0
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.precision",
        3,
    ):
        dwh_name0 = dwh_list[0]
        dwh_name1 = dwh_list[1]
        df_left = prune_df(df0, df1, keep="left_only")
        df_right = prune_df(df0, df1, keep="right_only")
        print(
            f"""
{item} is not equal on both sides.
Here are the entries in {dwh_name0}, that do not exist in {dwh_name1}:
        """
        )
        print(df_left)
        print(
            f"""
And here are the entries in {dwh_name1}, that do not exist in {dwh_name0}:
        """
        )
        print(df_right)
        print(
            """


        """
        )
    return prune_df(df0, df1)


def prune_df(df1, df2, keep="both"):
    """return: sub df of the two input df"""
    return (
        df1.merge(df2, how="outer", indicator=True)
        .loc[lambda x: x["_merge"] == keep]
        .drop(columns="_merge")
    )


dwh_list = ["DWH_MULTI", "DWH_SINGLE"]

table_where = """AND t.table_type = 'VIEW'
                 AND t.table_schema IN ('bi_access', 'gold')"""

entities_to_compare = [
    "tables",
    "columns",
    "tbl_checksum_last_m",
    "tbl_checksum_to_last_m",
    "tbl_checksum",
]

data = {item: {} for item in entities_to_compare}


# In order to compare only the necessary parts per entity, the following is done:
# (Note: every COMPARE step also prunes the compared entity)
#
#  1. fetch all columns of all tables
#  2. take all the tables from the columns df
#  3. COMPARE the TABLES df
#  4. prune the columns df, to keep only the tables being in both df
#  5. COMPARE the COLUMNS df
#  6. concat the columns per table being in both tables
#  7. find the first date or timestamp column per table
#  8. COMPARE the HASHES LAST MONTH per table
#  9. COMPARE the HASHES UNTIL LAST MONTH per table
# 10. COMPARE the HASHES COMPLETE per table
for dwh_name in dwh_list:
    #  1.
    print(1)
    data["columns"][dwh_name] = get_columns(dwh_name, table_where)
    #  2.
    print(2)
    data["tables"][dwh_name] = get_tables(data["columns"][dwh_name], dwh_name)

#  3.
print(3)
tables_merged = compare_datasets(data["tables"], "tables")
#  4.
for dwh_name in dwh_list:
    print(4)
    data["columns"][dwh_name] = prune_df(
        data["columns"][dwh_name], tables_merged[["table_name", "schema_name"]]
    )

#  5.
print(5)
columns_merged = compare_datasets(data["columns"], "columns")
#  6. + 7.
for dwh_name in dwh_list:
    print(6)
    data["tables"][dwh_name] = get_tables(columns_merged, dwh_name, add_special_columns=True)
    #  8.
    for index, row in data["tables"][dwh_name].iterrows():
        print(8)
        WHERE = ""
        date_column = row["date_column"]
        if date_column:
            WHERE = f"""WHERE {date_column} < DATE_TRUNC('month', CURRENT_DATE)
            AND {date_column} >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH)
            """
        data["tables"][dwh_name].at[index, "checksum"] = get_tbl_checksum(row, dwh_name, WHERE)

data["tables"] = compare_datasets(data["tables"], "tables")
#  9.
for dwh_name in dwh_list:
    for index, row in data["tables"][dwh_name].iterrows():
        print(9)
        WHERE = ""
        date_column = row["date_column"]
        if date_column:
            WHERE = f"WHERE {date_column} < DATE_TRUNC('month', CURRENT_DATE)"
        data["tables"][dwh_name].at[index, "checksum"] = get_tbl_checksum(row, dwh_name, WHERE)

data["tables"] = compare_datasets(data["tables"], "tables")
# 10.
for dwh_name in dwh_list:
    for index, row in data["tables"][dwh_name].iterrows():
        print(10)
        date_column = row["date_column"]
        data["tables"][dwh_name].at[index, "checksum"] = get_tbl_checksum(row, dwh_name)
data["tables"] = compare_datasets(data["tables"], "tables")
