import logging
import pyodbc
import azure.functions as func
from sqlalchemy import create_engine
import pandas as pd
import msal

# Entra ID (Azure AD) Authentication Details
CLIENT_ID = "<your-client-id>"
CLIENT_SECRET = "<your-client-secret>"
TENANT_ID = "<your-tenant-id>"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://database.windows.net//.default"]

# SQL Server connection details
SQL_SERVER_ON_PREM = "<on-prem-sql-server-ip>"
SQL_DATABASE_ON_PREM = "<db-name>"

SQL_SERVER_AZURE = "<azure-sql-server-name>.database.windows.net"
SQL_DATABASE_AZURE = "<azure-db-name>"

def get_access_token():
    """Use MSAL to get an access token for Azure SQL and On-Prem SQL Server."""
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    
    # Acquire token for both Azure and On-Prem SQL scopes
    result = app.acquire_token_for_client(scopes=SCOPE)
    if 'access_token' in result:
        return result['access_token']
    else:
        raise Exception(f"Error acquiring access token: {result.get('error_description')}")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure Function ETL Process Started.')

    try:
        # Step 1: Get the access token
        access_token = get_access_token()
        logging.info("Acquired access token.")

        # Step 2: Extract data from Azure SQL Database
        azure_connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER_AZURE};"
            f"DATABASE={SQL_DATABASE_AZURE};"
            f"Authentication=ActiveDirectoryAccessToken;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        
        azure_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={azure_connection_string}', connect_args={"attrs_before": {"UID": "", "PWD": access_token}})
        azure_connection = azure_engine.connect()
        query = "SELECT * FROM <table_name>"  # Adjust query as needed
        df = pd.read_sql(query, azure_connection)
        logging.info(f"Extracted {len(df)} records from Azure SQL.")
        
        # Step 3: Transform the data if needed
        # For example, filtering or modifying columns
        # df = df[df['some_column'] > 100]

        # Step 4: Load data into On-Premise SQL Server
        on_prem_connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER_ON_PREM};"
            f"DATABASE={SQL_DATABASE_ON_PREM};"
            f"Authentication=ActiveDirectoryAccessToken;"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        
        on_prem_engine = create_engine(f'mssql+pyodbc:///?odbc_connect={on_prem_connection_string}', connect_args={"attrs_before": {"UID": "", "PWD": access_token}})
        on_prem_connection = on_prem_engine.connect()

        # You can either truncate the destination table or insert data directly
        df.to_sql('<destination_table>', on_prem_connection, if_exists='replace', index=False)
        logging.info(f"Loaded {len(df)} records into On-Premise SQL.")

        azure_connection.close()
        on_prem_connection.close()

        return func.HttpResponse(f"ETL process completed. {len(df)} records transferred.", status_code=200)

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        return func.HttpResponse(f"Error in ETL process: {str(e)}", status_code=500)
