from sqlalchemy import create_engine, text
import traceback
import pyodbc
import requests
import pandas as pd
import numpy as np
import logging
from urllib.parse import quote
import os

username = os.getenv('TEST_DB_USERNAME')
password = quote(os.getenv('TEST_DB_PASSWORD'))
# username = quote('userForTestingSQLServerAuthentication')
# password = quote('t3stingAuthentication!')

# Note it was a pain in the butt to get the connection string right in SQL alchemy!!!
def save_to_db(df, db_name, schema_name, table_name):
    """save a pandasdf as table in my SQLDEV instance. Need to update connection string if using any other db instance. Returns True if successful and False otherwise."""
    
    connection_string_sqlalchemy = (
        "mssql+pyodbc://DESKTOP-JGNU8D2\\SQLDEV/AdventureWorksDW?"
        "driver=ODBC+Driver+17+for+SQL+Server&"
        "trusted_connection=yes&"
        "encrypt=yes&"
        "trustServerCertificate=yes"
    )
    # connection_string_sqlalchemy = connection_string_sqlalchemy.replace("'", "")
    logging.info(f"connection string: {connection_string_sqlalchemy}")

    # connection_string_sqlalchemy = (
    #     f"mssql+pyodbc://DESKTOP-JGNU8D2\\SQLDEV/{db_name}?"
    #     "driver=ODBC+Driver+17+for+SQL+Server&"
    #     "trusted_connection=yes&"
    #     "encrypt=yes&"
    #     "trustServerCertificate=yes"
    # )

    engine = create_engine(connection_string_sqlalchemy)

    # TEST CONNECTION IF DESIRED
    query = "SELECT TOP 10 * FROM Sales.Dim_Date"
    test_df = pd.read_sql(query, engine)
    logging.info(test_df)

    # engine = create_engine(connection_string_sqlalchemy)

    try:
        df.to_sql(table_name, con=engine, schema=schema_name, if_exists="replace", index=False)
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
