from datetime import date
import pandas as pd
import numpy as np

# function to take a single-year date series and change the year (only had 1yr of data from API, wanted to repeat those dates for multiple years)
def change_year_of_hols(date_series, updated_year):
    
    assert date_series.dtype == 'datetime64[ns]'

    df = pd.DataFrame({
        'day': date_series.dt.day,
        'month': date_series.dt.month,
        'year': updated_year
        })
    updated_series = pd.to_datetime(df[['year', 'month', 'day']])
    return updated_series

# create df of public hols from start to end year, using same days and months each yr. cols are date and is_hol (all == 1)
def create_hols_df(hols_1_yr, start_year_for_db, end_year_for_db_excl):
    year_series = list(range(start_year_for_db, end_year_for_db_excl))
    all_years_series = []
    
    for year in year_series:
        year_i_result = change_year_of_hols(hols_1_yr, year)
        all_years_series.append(year_i_result)
    
    all_hols_df = pd.concat(all_years_series).to_frame("date") \
    .assign(is_hol=1)
    return all_hols_df

# join public hols df with a full df of all dates, and a flag for public hol or not
def create_full_df(start_year_for_db, end_year_for_db_excl, hols_df):
    first_date = date(start_year_for_db, 1, 1)
    end_date = date(end_year_for_db_excl-1, 12, 31)
    all_dates_series = pd.date_range(first_date, end_date)
    all_dates_df = pd.DataFrame(all_dates_series, columns=['date'])
    final_hols_df = pd.merge(
        all_dates_df, hols_df, on = 'date', how = 'left') \
        .fillna(0)
    return final_hols_df