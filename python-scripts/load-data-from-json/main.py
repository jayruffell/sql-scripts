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

def main(start_year_for_db, end_year_for_db_excl, schema_name, table_name):
    """pull one year of holidays by date from an api (only a few years' data available), convert to a many-year dataframe and a flag for hol or not, and save to SQL Server (behavour set to REPLACE any existing table). Returns True if successful and False otherwise."""

    logging.info(f"Start Date: {start_year_for_db}")
    logging.info(f"End Date: {end_year_for_db_excl}")
    logging.info(f"Schema Name: {schema_name}")
    logging.info(f"Table Name: {table_name}")

    # -----------
    # get api data
    # -----------

    api_start_date = "2023-01-01" 
    api_end_date = "2023-12-31"

    url = f"https://openholidaysapi.org/PublicHolidays?countryIsoCode=DE&languageIsoCode=DE&validFrom={api_start_date}&validTo={api_end_date}"
    response = requests.get(url)
    holiday_data = response.json()

    # -----------
    # wrangle
    # -----------

    # Convert to DataFrame
    holiday_df = pd.DataFrame(holiday_data)
    # holiday_df.info()
    hols_2023 = holiday_df['startDate']
    hols_2023 = pd.to_datetime(hols_2023)

    # create df of public hols from start to end year, using same days and months each yr.
    all_hols_df = hp.create_hols_df(hols_1_yr=hols_2023, start_year_for_db=start_year_for_db, end_year_for_db_excl=end_year_for_db_excl)

    # join public hols df with a full df of all dates, and a flag for public hol or not
    final_hols_df = hp.create_full_df(start_year_for_db, end_year_for_db_excl, all_hols_df)

    success = hpsql.save_to_db(final_hols_df, schema_name, table_name)
    return success

# Notes on below, from chatgpt
# -- "The if __name__ == "__main__": block ensures that the main() function runs only when the script is executed directly, not when imported as a module. This is a common pattern in Python."
# -- the parsing stuff allows passing args in from run.py OR cmdline.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process start and end dates.")
    parser.add_argument("start_year_for_db", type=int)
    parser.add_argument("end_year_for_db_excl", type=int)
    parser.add_argument("schema_name", type=str)
    parser.add_argument("table_name", type=str)
    args = parser.parse_args()

    main(args.start_year_for_db, args.end_year_for_db_excl, args.db_name, args.schema_name, args.table_name)