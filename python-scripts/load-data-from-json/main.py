# run from run.py or command line - standard convention

import requests
import pandas as pd
import numpy as np
import helpers as hp # cant run interactively for some reason.
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

# # TEMP troubleshooting environmental variable differences between  manual powershell runs and task scheduler
# for key, value in os.environ.items():
#     logging.info(f"{key}={value}")

def main(schema_name, table_name):
    """Placeholder docstring"""

    logging.info(f"Schema Name: {schema_name}")
    logging.info(f"Table Name: {table_name}")
    # -------------------------------
    # Sample data
    data = {
        'DateID': [1, 2],
        'Date': ['2024-11-06', '2024-11-07'],
        'Year': [2024, 2024],
        'Month': [11, 12],
        'Day': [1, 1],
    }

    # Create DataFrame
    sample_df = pd.DataFrame(data)

    # Ensure the 'Date' column is of datetime type
    sample_df['Date'] = pd.to_datetime(sample_df['Date'])
    print(sample_df)
    # -------------------------------

    success = hpsql.save_df_to_db(sample_df, schema_name, table_name)
    return success

# Notes on below, from chatgpt
# -- "The if __name__ == "__main__": block ensures that the main() function runs only when the script is executed directly, not when imported as a module. This is a common pattern in Python."
# -- the parsing stuff allows passing args in from run.py OR cmdline.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process start and end dates.")
    parser.add_argument("schema_name", type=str)
    parser.add_argument("table_name", type=str)
    args = parser.parse_args()

    main(args.schema_name, args.table_name)