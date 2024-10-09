
import requests
import json
from requests_oauthlib import OAuth2Session

import pandas as pd
import datetime as dt
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas



#
# config
# -> should be put into a config or database
#


# snowflake connection
v_snowflake_account = 'aretoconsulting.eu-central-1'
v_snowflake_user = 'MAT_DV_GEN'
v_snowflake_password = 'a2YAM%&Sv2sL'
v_snowflake_database = 'MAT_DV_GEN_DPC_DEV'
v_snowflake_warehouse = 'ARETO_TRIAL_XS'
v_snowflake_meta_data_schema = 'meta'
v_snowflake_role = 'SYSADMIN'


# Matillion connection
matillion_api_client_id = '902a5219-3ee2-46a2-97dd-8914c55ea58b'
matillion_api_client_secret = 'GCUNr1REMhzG4KIvOBva2PCsaHvgwY7w'


# Dataspot connection
v_dataspot_username = 'integration.jetvault@areto.de'
v_dataspot_password = ')UU9)Stk%HZa$8fY7$'
v_dataspot_client_id = '1bd81c44-fb84-46ee-a498-d6faa2da31de'
v_dataspot_token_url = f'https://login.microsoftonline.com/organizations/oauth2/v2.0/token'




#
# create access token via openid connect
#


def get_access_token():

    # Get ID token

    token_data={
            "username": v_dataspot_username,
            "password": v_dataspot_password,
            "client_id": v_dataspot_client_id,
            "grant_type": "password",
            "scope": "openid profile"
        }


    # Token-Anfrage senden
    token_response = requests.post(v_dataspot_token_url, data=token_data)

    # Antwort aufbereiten
    token_response_json = token_response.json()    

    if 'id_token' in token_response_json:
        access_token = token_response_json['id_token']        
    else:
        raise Exception("Error while getting the Access Token: " + str(token_response_json))       
        

    return access_token



#
# functions for database interaction
#

# create connection object
def get_db_connection():

    
    try:
        conn = snowflake.connector.connect(
            user=v_snowflake_user,
            password=v_snowflake_password,
            account=v_snowflake_account,
            warehouse=v_snowflake_warehouse,
            database=v_snowflake_database,
            schema=v_snowflake_meta_data_schema,
            role=v_snowflake_role
            )    

        return conn
    except Exception as e:
        raise Exception(f"Error executing SQL query: {str(e)}")
        return None



#save defined hub loads to the database
def save_hub_load_config(json_data):

    #
    # add missing columns to data frame with NULL values
    #

    # required columns for hub load config table
    required_columns = ['STAGE_SCHEMA', 'STAGE_TABLE', 'HUB_SCHEMA', 'HUB_NAME', 'HUB_ALIAS', 
                    'H_COLUMN_NAME', 'BK_COLUMN_NAME', 'BK_SOURCE_COLUMN_LIST', 'SLICE_SRC_COLUMN_LIST']


    # Convert keys in JSON to uppercase and fill in missing columns
    for row in json_data:
        if isinstance(row, dict):
            # Convert the keys to uppercase
            row_upper = {key.upper(): value for key, value in row.items()}
            
            # Fill missing columns with None
            for col in required_columns:
                if col not in row_upper:
                    row_upper[col] = None
            
            # Replace the original row with the updated row with uppercase keys
            row.clear()  # Clear original row data
            row.update(row_upper)  # Update with new uppercase data

    # transform to data frame, to write it into the database
    df_data = pd.DataFrame(json_data)


    #
    # write data frame to database
    #

    conn = get_db_connection()

    # write the DataFrame to Snowflake and replace existing data
    write_pandas(conn, df = df_data, table_name = 'HUB_LOAD', overwrite =  True)



#save sat load config to the 
def save_satellite_load_config(json_data):

    #
    # add missing columns to data frame with NULL values
    #

    # required columns for hub load config table
    required_columns = ['STAGE_SCHEMA', 'STAGE_TABLE', 'SAT_SCHEMA', 'SAT_NAME', 'REFERENCED_OBJECT_NAME', 
                    'REFERENCED_HASH_COLUMN', 'DELTA_HASH_SRC_COLUMN_LIST']


    # Convert keys in JSON to uppercase and fill in missing columns
    for row in json_data:
        if isinstance(row, dict):
            # Convert the keys to uppercase
            row_upper = {key.upper(): value for key, value in row.items()}
            
            # Fill missing columns with None
            for col in required_columns:
                if col not in row_upper:
                    row_upper[col] = None
            
            # Replace the original row with the updated row with uppercase keys
            row.clear()  # Clear original row data
            row.update(row_upper)  # Update with new uppercase data

    # transform to data frame, to write it into the database
    df_data = pd.DataFrame(json_data)


    #
    # write data frame to database
    #

    conn = get_db_connection()

    # write the DataFrame to Snowflake and replace existing data
    write_pandas(conn, df = df_data, table_name = 'SATELLITE_LOAD', overwrite =  True)



#save linke load config to the 
def save_link_load_config(json_data):

    #
    # add missing columns to data frame with NULL values
    #

    # required columns for hub load config table
    required_columns = ['STAGE_SCHEMA', 'STAGE_TABLE', 'LINK_SCHEMA', 'LINK_NAME', 'L_COLUMN_NAME', 
                    'REFERENCED_HUB_NAME_1', 'REFERENCED_HUB_NAME_2']


    # Convert keys in JSON to uppercase and fill in missing columns
    for row in json_data:
        if isinstance(row, dict):
            # Convert the keys to uppercase
            row_upper = {key.upper(): value for key, value in row.items()}
            
            # Fill missing columns with None
            for col in required_columns:
                if col not in row_upper:
                    row_upper[col] = None
            
            # Replace the original row with the updated row with uppercase keys
            row.clear()  # Clear original row data
            row.update(row_upper)  # Update with new uppercase data

    # transform to data frame, to write it into the database
    df_data = pd.DataFrame(json_data)


    #
    # write data frame to database
    #

    conn = get_db_connection()

    # write the DataFrame to Snowflake and replace existing data
    write_pandas(conn, df = df_data, table_name = 'LINK_LOAD', overwrite =  True)