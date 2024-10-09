
import streamlit as st

import requests
import json
from requests_oauthlib import OAuth2Session
import pandas as pd

#helper functions
from helper.dataspot_linage_helper import get_access_token
from helper.dataspot_linage_helper import get_db_connection






v_tenant_name = "Andre's Mandant"
v_tenant_id = "c73d73b3-5c52-4137-bccf-062f08fd8dd"
# -> tenant für showcase

v_source_system_uuid = 'b50fa775-b57d-478a-9e3d-f5c596beae55'



# provides the list of all stage-schema, which where
# configured in the jetvault app
def get_stage_schemas():
    
    conn = get_db_connection()        

    # Your SQL query
    query = """SELECT
                stage_schema                    stage_schema, 
                stage_schema 					business_key,
                stage_schema					label,
                'Loading type: ' || load_type   description
            FROM 
                META.LOAD_CONFIG"""

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df


# get all available tables in a stage schema
def get_stage_tables(stage_schema):

    conn = get_db_connection()        

    # Your SQL query
    query = f"""SELECT
                --base information
                table_schema    stage_schema,
                TABLE_NAME      stage_table,
                --attributes for dataspot
                table_name		business_key,
                TABLE_name		label,
                'Stage table'	description
            FROM 
                INFORMATION_SCHEMA."TABLES" t
            WHERE 
                1=1
                --filter automatically created views
                AND TABLE_NAME NOT LIKE 'VW%'
                --filter stage schema
                AND TABLE_SCHEMA = '{stage_schema}'
            """

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df


# get all attributes for a single source table
def get_stage_columns(stage_schema, stage_table):

    conn = get_db_connection()        

    # Your SQL query
    query = f"""SELECT 
                table_schema    stage_schema,
                TABLE_name      stage_table,
                column_name,
                --dataspot attribute
                column_name			business_key,
                COLUMN_NAME 		label,
                ORDINAL_POSITION 	"ORDER"
            FROM 
                INFORMATION_SCHEMA."COLUMNS"
            WHERE 
                1=1	
                --filter based on parameter
                AND table_schema = '{stage_schema}'
                AND TABLE_NAME = '{stage_table}'
            ORDER BY 
                ORDINAL_POSITION
            """

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df




#
# Button to start sync
#

if st.button("Synchronize source systems to Dataspot", type="primary"):

    st.write("Starting source system syncronisation")


    # get access token for dataspot
    access_token = get_access_token()

    # header is same for each call
    headers = {
        'Authorization': f'Bearer {access_token}',
        "dataspot-tenant" : v_tenant_name
    }


    st.write("...getting stage tables")

    # get all stage schemas
    df_stage_schemas = get_stage_schemas()

    # each stage schema represents a source system
    # and is created in the data model Quellsysteme

    # Schleife über die Zeilen mit iterrows()
    for index_s, stage_schema in df_stage_schemas.iterrows():

        st.write(f"...creating source system {stage_schema['BUSINESS_KEY']}")

        # business key is used to create the url for the new source system
        v_new_source_system_url = f"https://partner.dataspot.io/rest/areto/schemes/b50fa775-b57d-478a-9e3d-f5c596beae55/collections/{stage_schema['BUSINESS_KEY']}"

        #print(v_new_source_system_url)

        # payload get's create based on all needed information
        v_payload = {
            "_type": "Collection",  
            "description": stage_schema['DESCRIPTION'],
            "label": stage_schema['LABEL'],
            "status": "WORKING"
        }

        #print(v_payload)
        
        # PATCH Method is used, as it does now overwrites attribues, which already exists in
        # dataspot
        # a non exiting object is creates
        # no objects get deleted in dataspot
        response = requests.patch(v_new_source_system_url, headers=headers, json=v_payload)

        # Antwort auswerten
        if response.status_code == 200:
            st.write("Anfrage erfolgreich.")
            st.write("Antwort der API:")
            st.write(response.text)  # Antwort der API anzeigen

            # ID is extracted from reponse, as it is needed
            # as a parent for all table
            json_response= response.json()

            v_collection_id = json_response['id']

            st.write("......done")

        else:
            st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
            st.write(response.text)


        st.write(f"......getting all stage tables for {stage_schema['BUSINESS_KEY']}")


        # get all tables for a stage schema
        # each table is created as a classifier
        df_stage_table = get_stage_tables(stage_schema['STAGE_SCHEMA'])

        # Schleife über die Zeilen mit iterrows()
        for index_t, stage_table in df_stage_table.iterrows():

            # business key is used to create the url for the new classifier
            v_new_stage_table_url = f"https://partner.dataspot.io/rest/areto/schemes/b50fa775-b57d-478a-9e3d-f5c596beae55/collections/{v_collection_id}/classifiers/{stage_table['BUSINESS_KEY']}"

        
            # payload get's create based on all needed information
            v_payload = {
                "_type": "UmlClass",  
                "description": stage_table['DESCRIPTION'],
                "label": stage_table['LABEL'],
                "parentId" : v_collection_id,
                "inCollection": v_collection_id,
                "modelId" : v_source_system_uuid,
                "status": "WORKING"
            }


            # PATCH Method is used, as it does now overwrites attribues, which already exists in
            # dataspot
            # a non exiting object is creates
            # no objects get deleted in dataspot
            response = requests.patch(v_new_stage_table_url, headers=headers, json=v_payload)

            # Antwort auswerten
            if response.status_code == 200:
                st.write("Anfrage erfolgreich.")
                st.write("Antwort der API:")
                #print(response.text)  # Antwort der API anzeigen

                # ID is extracted from reponse, as it is needed
                # as a parent for all table
                json_response= response.json()

                v_classifier_id = json_response['id']

                st.write("......done")

            else:
                st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
                st.write(response.text)


            # get all columns for a stage table
            # each attribute is created as a attribute for the classifier
            df_stage_columns = get_stage_columns(stage_schema['STAGE_SCHEMA'], stage_table['STAGE_TABLE'])
        
            st.write(f"........creating columns for stage table {stage_table['BUSINESS_KEY']}")

            # Schleife über die Zeilen mit iterrows()
            for index_c, stage_column in df_stage_columns.iterrows():
                
            
                # attributes are passed as a complete list for each table
                v_new_stage_columns_url = f"https://partner.dataspot.io/rest/areto/schemes/b50fa775-b57d-478a-9e3d-f5c596beae55/collections/{v_collection_id}/classifiers/{v_classifier_id}/attributes/{stage_column['BUSINESS_KEY']}"

                v_payload = {
                            "_type": "attribute",
                            "label": stage_column['LABEL'],
                            "order": stage_column['ORDER'],                        
                            "parentId" : v_classifier_id,
                            "modelId" : v_source_system_uuid,
                            "status": "WORKING"                        
                            }

                v_payload =    {
                            "_type": "UmlAttribute",                        
                            "tenantId": v_tenant_id,
                            #"required": "OPTIONAL",
                            "description": "test Kommentar",
                            #"cardinality": "MANY",
                            "order": stage_column['ORDER'],
                            #"hasDomain": "f829232a-594c-4b39-b8c2-78e74f8cd157",
                            "label": stage_column['LABEL'],
                            "modelId": "b50fa775-b57d-478a-9e3d-f5c596beae55",
                            "publicState": "DRAFT",
                            "status": "WORKING",
                            #"href": "/web/areto/attributes/efb0b910-a37c-4a82-b117-d2c03ba5ce11",
                            "parentId": v_classifier_id
                            }            
                            

                # PATCH Method is used, as it does now overwrites attribues, which already exists in
                # dataspot
                # a non exiting object is creates
                # no objects get deleted in dataspot
                response = requests.patch(v_new_stage_columns_url, headers=headers, json=v_payload)

                # Antwort auswerten
                if response.status_code == 200:
                    st.write("Anfrage erfolgreich.")
                    st.write("Antwort der API:")
                    #print(response.text)  # Antwort der API anzeigen

                    # ID is extracted from reponse, as it is needed
                    # as a parent for all table                

                    st.write(".........done")

                else:
                    st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
                    st.write(response.text)




