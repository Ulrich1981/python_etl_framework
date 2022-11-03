"""This script compares two databases in terms of the table structure
and the containing data. The following is compared:
- list of table
- list of columns per table
- complete table hashes
- complete table hashes of a selected timeframe
  (last month, up to last month)"""

import pandas as pd
from extract import sql_extract_module
import yaml
import string
import click
import re
from datetime import datetime

NOW = datetime.now().replace(microsecond=0).isoformat().replace(":","_")

@click.group()
def cli():
    pass


def get_schemas(dwh_name):
    """fetch a list of all schemas"""
    sql_statement = f"""
        SELECT DISTINCT
            t.table_schema AS schema_name
        FROM
            information_schema.tables t
        WHERE
        t.table_schema NOT IN ('information_schema', 'pg_catalog')
    """
    return sql_extract_module.extract(
        {"sql_statement": sql_statement, "dwh_connection": dwh_name}
    )


def get_columns(dwh_name, filter_options, table_regex = "(.*)"):
    """fetch a list of all columns in all tables"""

    table_where = get_table_where(filter_options)
    sql_statement = f"""
        SELECT
            t.table_schema AS schema_name,
            t.table_name,
            c.column_name,
            c.data_type
        FROM
            information_schema.tables t
        LEFT join information_schema.columns c 
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE
        t.table_schema NOT IN ('information_schema', 'pg_catalog')
        {table_where}"""
    df_out = sql_extract_module.extract(
        {"sql_statement": sql_statement, "dwh_connection": dwh_name}
    )
    df_out["table_join"] = ("" if filter_options["ignore_schema"]
                               else df_out["schema_name"] + ".") + \
                               df_out["table_name"].str.extract(table_regex)[0]

    with open("connection/db_config.yaml", "r") as db_config_stream:
        db_type = yaml.load(db_config_stream, Loader=yaml.Loader)[dwh_name]["type"]
        with open("column_hashes.yaml", "r") as column_hashes_stream:
            column_hashes = yaml.load(column_hashes_stream, Loader=yaml.Loader)[db_type]

    df_out["column_cast"] = (df_out.apply(lambda row: column_hashes[row["data_type"]]
                                  .format(column=row["column_name"]), axis=1))
    return df_out


def get_special_columns(df):
    """From a df of columns return the concatenation of all columns
       as well as the first date column"""
    out_dict = {}
    out_dict["column_cast"] = df[["column_cast"]].apply(", ".join)["column_cast"]
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
    if add_special_columns:
        return (
            columns.groupby(["table_name", "table_join", "schema_name"])
            .apply(get_special_columns)
            .reset_index()
        )
    return columns[["table_name", "table_join", "schema_name"]].drop_duplicates()


def get_tbl_checksum(row, dwh_name, where=""):
    """Calculate a checksum from all columns in a table"""
    sql_statement = f"""
        SELECT
            count(*) as count_rows, {row["column_cast"].iloc[0]}
        FROM
            {row["schema_name"].iloc[0]}.{row["table_name"].iloc[0]}
        {where}"""
    return sql_extract_module.extract(
        {
            "sql_statement": sql_statement,
            "dwh_connection": dwh_name,
        }
    )


def get_tbl_compare(row0, row1, where_key):
    m = {"all": "all",
         "this_month": "m-0",
         "last_month": "m-1",
         "before_last_month": "< m-1"}[where_key]
    row0 = row0 - row1
    count_diff = row0.loc[0]["count_rows"]
    row0 = row0.drop("count_rows", axis=1).transpose()
    col_list = ", ".join(row0[row0[0] != 0].transpose().keys())
    return {"column list with differences "+m:col_list,
            "difference count(left table-right table) "+m:count_diff}



def get_all_checksums(tables, filter_options):
    where_keys = ["all", "this_month", "last_month", "before_last_month"]
    dwh_list = filter_options["dwh_list"]
    rows_out = {}
    where = {}
    df_out = pd.DataFrame()
    for dwh_name in dwh_list:
        with open("connection/db_config.yaml", "r") as db_config_stream:
            db_type = yaml.load(db_config_stream, Loader=yaml.Loader)[dwh_name]["type"]
            with open("compare_where_statements.yaml", "r") as compare_where_statements:
                compare_where_statements = yaml.load(compare_where_statements, Loader=yaml.Loader)[db_type]
        where[dwh_name] = compare_where_statements

    for table_join in tables[dwh_list[0]]["table_join"]:
        compare_all = {}
        compare_all["table_join"] = table_join
        for where_key in where_keys:
            for dwh_name in dwh_list:
                row = tables[dwh_name][tables[dwh_name]["table_join"] == table_join]
                date = row["date_column"].iloc[0]
                filter_where = string.Template(where[dwh_name][where_key]).substitute(date=date) if date else ""
                rows_out[dwh_name] = get_tbl_checksum(row, dwh_name, filter_where)
            if date or where_key == "all":
                compare = get_tbl_compare(rows_out[dwh_list[0]], 
                                          rows_out[dwh_list[1]],
                                          where_key)
                compare_all = {**compare_all, **compare}
        df_out = df_out.append(compare_all, ignore_index=True)
    return df_out


def compare_datasets(data, item, filter_options):
    """Two df are compared with each other.

       return: all rows, that are in both df."""
    [(dwh_name0, df0), (dwh_name1, df1)] = data.items()

    df_print = prune_df(df0, df1, filter_options, keep="all").loc[lambda x: x["_merge"] != 'both']
    if "schema_name" in df_print:
        df_print["schema_name_x"] = df_print["schema_name"]
        df_print["schema_name_y"] = df_print["schema_name"]
    df_print["schema.table left"] = df_print["schema_name_x"] + "." + df_print["table_name_x"]
    df_print["schema.table right"] = df_print["schema_name_y"] + "." + df_print["table_name_y"]
    df_print = (df_print.groupby(["table_join"])
                   .agg(lambda x: ",".join(x.dropna().unique().astype(str))).reset_index())
    df_print["columns only left table"] = df_print[df_print["_merge"] == "left_only"]["column_name"] if "column_name" in df_print else None
    df_print["columns only right table"] = df_print[df_print["_merge"] == "right_only"]["column_name"] if "column_name" in df_print else None
    df_print = df_print[["table_join",
                         "schema.table left",
                         "schema.table right",
                         "columns only left table",
                         "columns only right table"]]

    df_pruned = prune_df(df0, df1, filter_options)
    data_out = {dwh_name0: df_pruned[0], dwh_name1: df_pruned[1]}
    return data_out, df_print


def prune_df(df1, df2, filter_options = {}, keep="both"):
    join_list = df2.keys().drop("table_name")
    if filter_options["ignore_schema"] and "schema_name" in join_list:
        join_list = join_list.drop("schema_name")
    if "column_cast" in join_list:
        join_list = join_list.drop("column_cast")
    join_list = list(join_list)
    df_out = df1.merge(df2, how="outer", indicator=True, on = join_list)
    if keep == "all":
        return df_out

    df_out = df_out.loc[lambda x: x["_merge"] == keep].drop(columns=["_merge"])

    """return: sub df of the two input df"""
    df1_pruned = df_out.rename(columns=lambda x: re.sub('_x','',x))[df1.keys()].drop_duplicates()
    df2_pruned = df_out.rename(columns=lambda x: re.sub('_y','',x))[df2.keys()].drop_duplicates()

    return [df1_pruned, df2_pruned]


def compare_columns_and_tables(dwh_list, filter_options):
    """
    In order to compare only the necessary parts per entity, the following is done:
    (Note: every COMPARE step also prunes the compared entity)
    
     1. fetch all columns of all tables
     2. take all the tables from the columns df
     3. COMPARE the TABLES df
     4. prune the columns df, to keep only the tables being in both df
     5. COMPARE the COLUMNS df
    """

    columns = {}
    tables = {}

    regex_list = {dwh_list[0]: filter_options["left_table_regex"] or "(.*)",
                  dwh_list[1]: filter_options["right_table_regex"] or "(.*)",}

    for dwh_name in dwh_list:
        #  1.
        columns[dwh_name] = get_columns(dwh_name, filter_options, regex_list[dwh_name])
        columns[dwh_name] = (columns[dwh_name].loc[columns[dwh_name]
                                .table_name.str.contains(regex_list[dwh_name]), :])
        #  2.
        tables[dwh_name] = get_tables(columns[dwh_name])

    #  3.
    tables, df_print = compare_datasets(tables, "tables", filter_options)
    #  4.
    for dwh_name in dwh_list:
        columns[dwh_name], tables[dwh_name] = prune_df(
            columns[dwh_name], tables[dwh_name], filter_options
        )

    #  5.
    columns, df_col_print = compare_datasets(columns, "columns", filter_options)
    df_print = df_print.append(df_col_print, ignore_index=True)

    return tables, columns, df_print


@cli.command()
@click.option("-d", "--dwh_list",
              default="DWH_SINGLE, DWH_MULTI",
              help="The list of the two DWH aliases of the form '..., ...'")
@click.option("-s", "--schemas",
              help="The list of the common schemas to be checked of the form '..., ...'. Leave blank, if all schemas should be used.",
              required=False)
@click.option("-t", "--tables",
              help="The list of the common tables to be checked of the form '..., ...'. Leave blank, if all tables should be used.",
              required=False)
@click.option("-y", "--table_type",
              help="The table types ('VIEW', 'BASE TABLE') to be checked. Leave blank, if both types should be used.",
              required=False)
@click.option("-lx", "--left_table_regex",
              help="Use only the capture group from a regex for the table names of the left database.",
              required=False)
@click.option("-rx", "--right_table_regex",
              help="Use only the capture group from a regex for the table names of the right database.",
              required=False)
@click.option("-i", "--ignore_schema",
              is_flag=True,
              help="Compare the tables only by name and not by schema.",
              required=False)
def compare_all_columns(dwh_list, **filter_options):

    dwh_list = dwh_list.replace(" ","").split(',') if type(dwh_list) is str else dwh_list

    tables, columns, df_print = compare_columns_and_tables(dwh_list, filter_options)
    df_print.drop("table_join", axis=1).to_csv(f"comparison_results_{NOW}.csv", index=False)


@cli.command()
@click.option("-d", "--dwh_list",
              default="DWH_SINGLE, DWH_MULTI",
              help="The list of the two DWH aliases of the form '..., ...'")
@click.option("-s", "--schemas",
              help="The list of the common schemas to be checked of the form '..., ...'. Leave blank, if all schemas should be used.",
              required=False)
@click.option("-t", "--tables",
              help="The list of the common tables to be checked of the form '..., ...'. Leave blank, if all tables should be used.",
              required=False)
@click.option("-y", "--table_type",
              help="The table types ('VIEW', 'BASE TABLE') to be checked. Leave blank, if both types should be used.",
              required=False)
@click.option("-lx", "--left_table_regex",
              help="Use only the capture group from a regex for the table names of the left database.",
              required=False)
@click.option("-rx", "--right_table_regex",
              help="Use only the capture group from a regex for the table names of the right database.",
              required=False)
@click.option("-i", "--ignore_schema",
              is_flag=True,
              help="Compare the tables only by name and not by schema.",
              required=False)
def complete_compare(**filter_options):
    """
    In order to compare only the necessary parts per entity, the following is done:
    (Note: every COMPARE step also prunes the compared entity)
    
     1. fetch all columns of all tables
     2. take all the tables from the columns df
     3. COMPARE the TABLES df
     4. prune the columns df, to keep only the tables being in both df
     5. COMPARE the COLUMNS df
     6. concat the columns per table being in both tables
     7. find the first date or timestamp column per table
     8. COMPARE the HASHES LAST MONTH per table
     9. COMPARE the HASHES UNTIL LAST MONTH per table
    10. COMPARE the HASHES COMPLETE per table
    """

    filter_options["dwh_list"] = (filter_options["dwh_list"].replace(" ","").split(',')
                    if type(filter_options["dwh_list"]) is str
                    else filter_options["dwh_list"])
    table_where = get_table_where(filter_options)

    #  1. - 5.
    tables, columns, df_print = compare_columns_and_tables(filter_options["dwh_list"],
                                                 filter_options = filter_options)
    #  6. + 7.
    
    for dwh_name in filter_options["dwh_list"]:
        tables[dwh_name] = get_tables(columns[dwh_name], add_special_columns=True)

    #  8. - 10.
    df_print = df_print.merge(
                   get_all_checksums(tables, filter_options = filter_options),
                   how="outer")
    df_print.drop("table_join", axis=1).to_csv(f"comparison_results_{NOW}.csv", index=False)

@cli.command()
def main():
    """
    This is the main function of the script.
    Users can decide, which databases to compare as well as
    the comparison level.
    """

    # list of databases
    with open("connection/db_config.yaml", "r") as stream:
        db_names = list(yaml.load(stream, Loader=yaml.Loader).keys())

    # Choose the two databases to compare
    db_out = {}
    for i in [1,2]:
        db_join = ", ".join(db_names)
        db_out[i] = None
        while db_out[i] not in db_names:
            if db_out[i]:
                print("You didn't choose one of the available options.")
            print(f"""
Which is the database number {i} you want to use
for your comparison? ({db_join}):""")
            db_out[i] = input()
        db_names.remove(db_out[i])
    db_out = list(db_out.values())

    # comparison method
    comparison_method = None
    while not (comparison_method and int(comparison_method) in range(1,6)):
        if comparison_method:
            print("Please choose a number in the list.")
        print("""
1. Return a list of all tables not being present in both databases
2. Return a list of all columns not being in both tables,
   where the tables are present in both databases""")
# 3. Return a list of all tables, where the columns being present
   # on both sides don't hold the same data
# 4. Return a list of all tables, where the columns being present
   # on both sides don't hold the same data for the last month
   # (first date/timestamp column used)
        print("""
3. Compare a specific table
4. Do a complete compare (tables, columns, hashes - this will take a while!)
        """)
        comparison_method = input()

    schema_list = {}
    for dwh_name in db_out:
        schema_list[dwh_name] = get_schemas(dwh_name)
    schema_list = prune_df(*schema_list.values())["schema_name"].tolist()

    schema_out = None
    while not (schema_out and (schema_out == [""] or set(schema_out.replace(" ","").split(',')).issubset(schema_list))):
        if schema_out:
            print("You didn't write a proper list of schemas.")
        schema_string = ", ".join(schema_list)
        print(f"""
You can write a list (comma separated) of schemas you would like to compare.
Leave empty, if you would like to include all schemas ({schema_string}):""")
        schema_out = input()

    type_list = ["VIEW", "BASE TABLE"]
    type_out = None
    while not (type_out and (type_out == [""] or set([type_out]).issubset(type_list))):
        if schema_out:
            print("You didn't write a proper type.")
        type_string = ", ".join(type_list)
        print(f"""
You can write a list (comma separated) of types you would like to compare.
Leave empty, if you would like to include all types ({type_string}):""")
        type_out = input()

    if comparison_method == "1":
        compare_all_tables(db_out, schemas = schema_out, table_type = type_out)
    if comparison_method == "2":
        compare_all_columns(db_out, schemas = schema_out, table_type = type_out)
    if comparison_method == "3":
        tables = compare_all_tables(db_out, print_results = False, schemas = schema_out, table_type = type_out)
        table_list = (tables.schema_name + "." + tables.table_name).tolist()
        table_out = None
        while not (table_out and table_out in table_list):
            if table_out:
                print("You didn't choose a table from the list")
            print(f"""
Please choose a table to compare ({table_list}):""")
            table_out = input()
        [schema, table] = table_out.split(".")
        complete_compare(db_out, schemas = schema, tables = table)
    if comparison_method == "4":
        complete_compare(db_out, schemas = schema_out, table_type = type_out)


def get_table_where(filter_options):
    schemas = "','".join(filter_options["schemas"].replace(" ","").split(',')) if filter_options["schemas"] else None
    schema_where = f"AND t.table_schema in ('{schemas}')" if schemas else ""
    tables = "','".join(filter_options["tables"].replace(" ","").split(',')) if filter_options["tables"] else None
    table_where = f"AND t.table_name in ('{tables}')" if tables else ""
    types = filter_options["table_type"] if "table_type" in filter_options else None
    type_where = f"AND t.table_type = '{types}'" if types else ""
    return schema_where + type_where + table_where


if __name__ == "__main__":
    cli()