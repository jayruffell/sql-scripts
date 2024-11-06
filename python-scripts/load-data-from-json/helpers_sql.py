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
    # logging.info(f"connection string: {connection_string_sqlalchemy}")
    engine = create_engine(connection_string_sqlalchemy)

    # # TEST CONNECTION IF DESIRED
    # query = "SELECT TOP 10 * FROM dbo.Dim_Date"
    # test_df = pd.read_sql(query, engine)
    # logging.info(test_df)
    logging.info(print(df))
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

def create_tables_from_json(json_file_path):
    """Creates DataFrames for dimension, bridge, and fact tables from the raw JSON data."""

        # Load JSON data from the file
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    # 1. Create DimPicker
    dim_picker_data = [
        {"PickerID": picker["id"], "PickerName": picker["name"]}
        for picker in json_data["pickers"]
    ]
    df_dim_picker = pd.DataFrame(dim_picker_data)

    # 2. Create DimDefect
    defects = set()
    for sample in json_data["samples"]:
        for defect in sample["defects"]:
            defects.add(defect["name"])

    df_dim_defect = pd.DataFrame({"DefectID": range(1, len(defects) + 1), "DefectType": list(defects)})

    # 3. Create DimBin
    dim_bin_data = []
    for bin_data in json_data["bins"]:
        for picker_id in bin_data["pickers"]:
            dim_bin_data.append({
                "BinID": bin_data["binId"],
                "BinLocation": bin_data["block"],
                "Variety": bin_data["variety"]
            })

    df_dim_bin = pd.DataFrame(dim_bin_data).drop_duplicates() 

    # 4. Create BridgeSamplePicker (SampleID - PickerID)
    bridge_sample_picker_data = []
    for sample in json_data["samples"]:
        for picker_id in sample["pickers"]:
            bridge_sample_picker_data.append({
                "SampleID": sample["id"],
                "PickerID": picker_id
            })

    df_bridge_sample_picker = pd.DataFrame(bridge_sample_picker_data)

    # 5. Create BridgeSampleDefect (SampleID - DefectID)
    bridge_sample_defect_data = []
    for sample in json_data["samples"]:
        for defect in sample["defects"]:
            defect_id = df_dim_defect[df_dim_defect["DefectType"] == defect["name"]].iloc[0]["DefectID"]
            bridge_sample_defect_data.append({
                "SampleID": sample["id"],
                "DefectID": defect_id,
                "PerformanceScore": defect["percent"]
            })

    df_bridge_sample_defect = pd.DataFrame(bridge_sample_defect_data)

    # 6. Create BridgeBinPicker (BinID - PickerID)
    bridge_bin_picker_data = []
    for bin_data in json_data["bins"]:
        for picker_id in bin_data["pickers"]:
            bridge_bin_picker_data.append({
                "BinID": bin_data["binId"],
                "PickerID": picker_id
            })

    df_bridge_bin_picker = pd.DataFrame(bridge_bin_picker_data)

    # **Fact Tables**:
    # 7. Create FactSample (SampleID - BinID - DefectID - PerformanceScore - TotalPickers)
    fact_sample_data = []
    for sample in json_data["samples"]:
        for defect in sample["defects"]:
            defect_id = df_dim_defect[df_dim_defect["DefectType"] == defect["name"]].iloc[0]["DefectID"]
            total_pickers = len(sample["pickers"])
            fact_sample_data.append({
                "SampleID": sample["id"],
                "BinID": sample["binId"],
                "DefectID": defect_id,
                "PerformanceScore": defect["percent"],
                "TotalPickers": total_pickers
            })
    
    df_fact_sample = pd.DataFrame(fact_sample_data)

    # 8. Create FactBin (BinID - TotalPickers)
    fact_bin_data = []
    for bin_data in json_data["bins"]:
        total_pickers = len(bin_data["pickers"])
        fact_bin_data.append({
            "BinID": bin_data["binId"],
            "TotalPickers": total_pickers
        })

    df_fact_bin = pd.DataFrame(fact_bin_data)

    return {
        "DimPicker": df_dim_picker,
        "DimDefect": df_dim_defect,
        "DimBin": df_dim_bin,
        "BridgeSamplePicker": df_bridge_sample_picker,
        "BridgeSampleDefect": df_bridge_sample_defect,
        "BridgeBinPicker": df_bridge_bin_picker,
        "FactSample": df_fact_sample,
        "FactBin": df_fact_bin
    }

