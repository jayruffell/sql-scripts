# 
# from chatgpt - run.py is standard syntax for running the main functions logic.  but I've also set up main so that it can take commandline arguments, in which case it runs from eg. powershell.
import main

main.main(start_year_for_db=1960, end_year_for_db_excl=2024, db_name = 'staging', schema_name = 'python', table_name = "holidays")