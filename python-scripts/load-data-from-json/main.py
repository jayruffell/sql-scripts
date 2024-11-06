# run from run.py or command line - standard convention

import requests
import pandas as pd
import numpy as np
import helpers_sql as hpsql # cant run interactively for some reason.
import argparse
import logging
import os

# COnfigure logging - note can also just redirect output when running from command line, e.g. python main.py ... > output.log 2>&1.
log_file_path = os.path.join(os.path.dirname(__file__), "script.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format for log messages
    handlers=[
        logging.FileHandler(log_file_path),  # Log to a file
        logging.StreamHandler()  # Also log to console
    ]
)

def main(schema_name):
    """Placeholder docstring"""

    logging.info("converting json file to dims and facts")

    dims_and_facts_dfs = hpsql.create_tbls_from_json("C:\dev\hectre-test\data.json")
    
    logging.info("iterating thru each df and save to corresponding table name")
    for table_name, df in dims_and_facts_dfs.items():
        success = hpsql.save_df_to_db(
            df=df,
            schema_name=schema_name,
            table_name=table_name
        )
        # Optional: Print or log success/failure
        print(f"Table {table_name} save success: {success}")

    # success = hpsql.save_df_to_db(
    #     df = list(dims_and_facts_dfs.values())[0],
    #     schema_name = schema_name,
    #     table_name = list(dims_and_facts_dfs.keys())[0])
    # return success

# Notes on below, from chatgpt
# -- "The if __name__ == "__main__": block ensures that the main() function runs only when the script is executed directly, not when imported as a module. This is a common pattern in Python."
# -- the parsing stuff allows passing args in from run.py OR cmdline.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process start and end dates.")
    parser.add_argument("schema_name", type=str)
    args = parser.parse_args()

    main(args.schema_name)