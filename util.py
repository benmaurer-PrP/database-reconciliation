import pandas as pd
import colorama
import traceback
import pyodbc
import pymysql
import numpy as np
from sqlalchemy import create_engine
from colorama import Fore,Style
from queries import *


def fetch_data_from_sql(server: str, database: str, query: str):
    print(f' \u2502 Querying {server}/{database}...')
    try:
        # connection_string = (
        #     f"mssql+pyodbc://{server}/{database}"
        #     "?driver=ODBC+Driver+17+for+SQL+Server"
        # )
        connection_string = connections[server]
        engine = create_engine(connection_string)
        df = pd.read_sql_query(query, con=engine)
        engine.dispose()
        if df.shape[0] == 0:
            print(f'   {Fore.RED}{server}/{database} query returned 0 rows{Style.RESET_ALL}')
        else:
            print(f'   {Fore.GREEN}{df.shape[0]} lines retrieved successfully{Style.RESET_ALL}')
        return df
    except:
        print(f'   {Fore.RED}Unable to resolve {server}/{database} query{Style.RESET_ALL}')
        traceback.print_exc()
        return None


def reconcile_store_items():
    print('Reconciling gp item store code, repo item catalogs, and nodus catalogs')
    # Fetching data
    df_nodus = fetch_data_from_sql(server='pp-nodus', database='esss', query=queries["pp-nodus"]["catalogs"])
    df_gp = fetch_data_from_sql(server='pp-gp', database='ppi', query=queries["pp-gp"]["store_code"])
    df_repo = fetch_data_from_sql(server='cloud-prod.ctwh7mw4cphw.us-east-1.rds.amazonaws.com', database='og',
                                  query=queries["cloud-prod.ctwh7mw4cphw.us-east-1.rds.amazonaws.com"]["catalogs"])

    # Merging on item, catalog
    df_item_repo = df_gp[["itemnmbr"]].merge(df_repo, how='left', on='itemnmbr')
    df_item_repo_nodus = df_item_repo.merge(df_nodus, how='outer', left_on=['itemnmbr', 'repo_catalog'], right_on=['itemnmbr', 'nodus_catalog'])
    df_store_check = df_item_repo_nodus.merge(df_gp, how='left', on='itemnmbr')
    print(f' \u2502 Dataframes merged successfully')

    # Checking criteria
    contains_B = df_store_check["uscatvls_1"].str.contains("B", na=False)  # True if 'B' is found in 'active'
    not_contains_B = ~contains_B

    # Define "not blank" for code_prod and code_test
    #   - Means not NaN, not empty string
    not_blank_nodus = df_store_check["nodus_catalog"].notna() & (df_store_check["nodus_catalog"] != "")
    not_blank_repo = df_store_check["repo_catalog"].notna() & (df_store_check["repo_catalog"] != "")

    # Condition 1: If uscatvls_1 does not contain 'B', nodus catalogs should be blank
    cond1 = not_contains_B & not_blank_nodus

    # Condition 2: If uscatvls_1 contains 'B', repo and nodus catalogs should match
    cond2 = contains_B & (df_store_check["nodus_catalog"] != df_store_check["repo_catalog"])

    # Condition 3: If uscatvls_1 contains 'B', neither repo or nodus should have blank catalogs
    cond3 = contains_B & (
            (~not_blank_nodus)  # code_prod is blank
            | (~not_blank_repo)  # code_test is blank
    )
    cond4 = not_blank_nodus & ~not_blank_repo
    # Combine conditions with OR (any condition matched)
    combined_condition = cond1 | cond2 | cond3 | cond4
    df_store_check["condition_triggered"] = np.select(
        [cond1, cond2, cond3, cond4],
        [
            "On store without 'B' GP tag"
            , "Nodus catalogs do not match repo catalogs"
            , "'B' tag on item without catalogs added"
            , "On store, not repo"
        ],
        default="No condition"
    )
    df_filtered = df_store_check[df_store_check["condition_triggered"] != "No condition"].copy()
    df_filtered = df_filtered[["itemnmbr", "uscatvls_1", "repo_catalog", "nodus_catalog", "condition_triggered"]]
    if df_filtered.shape[0] == 0:
        print(f'{Fore.GREEN}No mismatched items identified.{Style.RESET_ALL}')
    else:
        print(f'{Fore.RED}Identified {df_filtered.shape[0]} possibly mismatched items.{Style.RESET_ALL}')
        df_filtered.to_csv('output_filtered.csv', index=False)

if __name__ == '__main__':
    pass