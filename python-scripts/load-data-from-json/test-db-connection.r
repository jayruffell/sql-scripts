# testing with R, cos having loads of trouble with python!
# Install packages if you haven't already
# install.packages(c("DBI", "dbplyr", "odbc", "dplyr"))

# Load libraries
library(DBI)
library(dbplyr)
library(dplyr)
library(odbc)  # Required for SQL Server connections


con <- dbConnect(
  odbc::odbc(),
  Driver = "ODBC Driver 17 for SQL Server",  # Ensure the driver matches what you have
  Server = "DESKTOP-JGNU8D2\\SQLDEV",
  Trusted_Connection = "Yes",
  Encrypt = "Yes",                           # Matches "encrypt": "Mandatory"
  TrustServerCertificate = "Yes"             # Matches "trustServerCertificate": true
)

print(dbListTables(con))

# server_name = 'DESKTOP-JGNU8D2\\SQLDEV'  # Change this to your server name
# db_name = 'AdventureWorksDW'       # Change this to your database name


# # Connect to SQL Server
# con <- dbConnect(
#   odbc::odbc(),
#   Driver = "ODBC Driver 17 for SQL Server",  # Ensure you have the right ODBC driver installed
#   Server = server_name,
#   Database = db_name # ,
# #   UID = "your_username",      # Use Windows authentication by omitting UID and PWD if not needed
# #   PWD = "your_password",
#   # Port = 1433                 # Default SQL Server port
# )
# odbc::odbcListDrivers() %>%
# View()

