from sqlalchemy import create_engine, text
import json
import traceback
import pyodbc
import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from urllib.parse import quote
import os


# Note it was a pain in the butt to get the connection string right in SQL alchemy!!!
def save_df_to_db(df, schema_name, table_name):
    """save a pandasdf as table in my SQLDEV instance. Need to update connection string if using any other db instance. Returns True if successful and False otherwise."""

    # # -------------------------------
    # # EXAMPLE USAGE - Sample data for HectreDW.Dim_Date
    # data = {
    #     'DateID': [1, 2],
    #     'Date': ['2024-11-06', '2024-11-07'],
    #     'Year': [2024, 2024],
    #     'Month': [11, 12],
    #     'Day': [1, 1],
    # }
    # sample_df = pd.DataFrame(data)

    # # Ensure the 'Date' column is of datetime type
    # sample_df['Date'] = pd.to_datetime(sample_df['Date'])
    # print(sample_df)
    # success = save_df_to_db(sample_df, schema_name, table_name)
    # # -------------------------------

    connection_string_sqlalchemy = (
        "mssql+pyodbc://DESKTOP-JGNU8D2\\SQLDEV/HectreDW?"
        "driver=ODBC+Driver+17+for+SQL+Server&"
        "trusted_connection=yes&"
        "encrypt=yes&"
        "trustServerCertificate=yes"
    )
    # connection_string_sqlalchemy = connection_string_sqlalchemy.replace("'", "")
    logging.info(f"connection string: {connection_string_sqlalchemy}")
    engine = create_engine(connection_string_sqlalchemy)

    # TEST CONNECTION IF DESIRED
    query = "SELECT TOP 10 * FROM dbo.Dim_Date"
    test_df = pd.read_sql(query, engine)
    logging.info(test_df)

    try:
        df.to_sql(
            table_name, con=engine, schema=schema_name, if_exists="replace", index=False
        )
        operation_successful = True
    except Exception as e:
        operation_successful = False
        logging.error("An error occurred while writing to the database: %s", e)
        logging.error("Stack Trace:\n%s", traceback.format_exc())
    # Record the success status
    if operation_successful:
        logging.info("db operation was successful.")
    else:
        logging.info("db operation failed.")
    return operation_successful


def create_tbls_from_json(json_file_path):
    """read json data and create dims and facts as a dictionary (accessible as e.g. 'result[['dim_picker']]"""

    # # Usage example:
    # result = create_tbls_from_json("C:\dev\hectre-test\data.json")
    # # Access the DataFrames
    # print(result["dim_picker"])
    # print(result["dim_bin"])

    # Load JSON data from the file
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    # 1. Create Dim_Picker
    pickers_data = [
        {"PickerID": picker["id"], "Name": picker["name"]}
        for picker in json_data["pickers"]
    ]
    df_dim_picker = pd.DataFrame(pickers_data)

    # 2. Create Dim_Bin
    bins_data = []
    for bin_data in json_data["bins"]:
        for picker_id in bin_data["pickers"]:
            bins_data.append(
                {
                    "BinID": bin_data["binId"],
                    "Block": bin_data["block"],
                    "Variety": bin_data["variety"],
                }
            )
    df_dim_bin = pd.DataFrame(bins_data)

    # 3. Create Dim_Defect
    defects = set()
    for sample in json_data["samples"]:
        for defect in sample["defects"]:
            defects.add(defect["name"])
    df_dim_defect = pd.DataFrame({"Name": list(defects)})

    # 4. Create Dim_Date
    dates = []
    for bin_data in json_data["bins"]:
        created_date = datetime.strptime(
            bin_data["createdDate"], "%d/%m/%Y %I:%M:%S %p"
        )
        dates.append(
            {
                "DateID": created_date.year * 10000
                + created_date.month * 100
                + created_date.day,
                "Date": created_date.date(),
                "Year": created_date.year,
                "Month": created_date.month,
                "Day": created_date.day,
            }
        )
    df_dim_date = pd.DataFrame(dates)

    # 5. Create Fact_Picking
    fact_picking_data = []
    for bin_data in json_data["bins"]:
        for picker_id in bin_data["pickers"]:
            created_date = datetime.strptime(
                bin_data["createdDate"], "%d/%m/%Y %I:%M:%S %p"
            )
            date_id = (
                created_date.year * 10000 + created_date.month * 100 + created_date.day
            )
            fact_picking_data.append(
                {
                    "BinID": bin_data["binId"],
                    "PickerID": picker_id,
                    "Block": bin_data["block"],
                    "DateID": date_id,
                }
            )
    df_fact_picking = pd.DataFrame(fact_picking_data)

    # 6. Create Fact_Sampling
    fact_sampling_data = []
    for sample in json_data["samples"]:
        created_date = datetime.strptime(sample["createdDate"], "%d/%m/%Y %I:%M:%S %p")
        date_id = (
            created_date.year * 10000 + created_date.month * 100 + created_date.day
        )
        for defect in sample["defects"]:
            fact_sampling_data.append(
                {
                    "SampleID": sample["id"],
                    "BinID": sample["binId"],
                    "DefectID": df_dim_defect[
                        df_dim_defect["Name"] == defect["name"]
                    ].index[0]
                    + 1,  # Using index to get DefectID
                    "Percent": defect["percent"],
                    "DateID": date_id,
                }
            )
    df_fact_sampling = pd.DataFrame(fact_sampling_data)

    # Return as a dictionary (can be accessed like result['dim_picker'])
    return {
        "Dim_Picker": df_dim_picker,
        "Dim_Bin": df_dim_bin,
        "Dim_Defect": df_dim_defect,
        "Dim_Date": df_dim_date,
        "Fact_Picking": df_fact_picking,
        "Sact_Sampling": df_fact_sampling,
    }