
import streamlit as st

import requests
import json
from requests_oauthlib import OAuth2Session
import pandas as pd

#helper functions
from helper.dataspot_linage_helper import get_access_token
from helper.dataspot_linage_helper import get_db_connection




#v_tenant_id = "areto DWH"
# -> tenant für Areto DWH

v_tenant_name = "Andre's Mandant"
v_tenant_id = "c73d73b3-5c52-4137-bccf-062f08fd8dd"
# -> tenant für showcase

v_reporting_uuid = 'bb26ed28-c0db-4939-80c2-bb97363ab92d'



# provides the list of all stage-schema, which where
# configured in the jetvault app
def get_br_schemas():
    
    conn = get_db_connection()        

    # Your SQL query
    query = """
            SELECT distinct
                business_rule_schema	BR_SCHEMA,
                --attributes for dataspot
                business_rule_schema 					business_key,
                business_rule_schema					label,
                'Business Rules collection'     description
            FROM 
                meta.BUSINESS_RULES	
            """
    

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df


# get all available tables in a stage schema
def get_business_rules(br_schema):

    conn = get_db_connection()        

    # Your SQL query
    query = f"""
            SELECT 
                business_rule_schema	br_SCHEMA,
                business_rule_name		br_name,
                --attributes for dataspot
                business_rule_name		business_key,
                business_rule_name		label,
                'Business Rule'	        description
            FROM 
                meta.BUSINESS_RULES	
            WHERE
                BUSINESS_RULE_SCHEMA  = '{br_schema}'
                and ACCESS_LAYER_LOAD = 1
            """

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df


# get all attributes for a single source table
def get_br_columns(br_schema, br_name):

    conn = get_db_connection()        

    # Your SQL query
    query = f"""SELECT 
                table_schema,
                table_name,
                column_name,
                --dataspot attribute
                column_name			business_key,
                COLUMN_NAME 		label,
                ORDINAL_POSITION 	"ORDER"
            FROM 
                INFORMATION_SCHEMA."COLUMNS"
            WHERE 
                TABLE_SCHEMA  = '{br_schema}'
                AND table_name = '{br_name}'
            ORDER BY ORDINAL_POSITION
            """

    # Execute the query and fetch results into a DataFrame
    df = pd.read_sql_query(query, conn)

    return df







#
# Button to start sync
#

if st.button("Synchronize business rules to Dataspot", type="primary"):

    st.write("Starting business rule syncronisation")



    # get access token for dataspot
    access_token = get_access_token()

    # header is same for each call
    headers = {
        'Authorization': f'Bearer {access_token}',
        "dataspot-tenant" : v_tenant_name
    }


    st.write("...getting business rules")

    # get all stage schemas
    df_br_schemas = get_br_schemas()

    # each stage schema represents a source system
    # and is created in the data model Quellsysteme

    # Schleife über die Zeilen mit iterrows()
    for index_s, br_schema in df_br_schemas.iterrows():

        st.write(f"...creating source system {br_schema['BUSINESS_KEY']}")

        # business key is used to create the url for the new source system
        v_new_br_collection_url = f"https://partner.dataspot.io/rest/areto/schemes/{v_reporting_uuid}/collections/{br_schema['BUSINESS_KEY']}"

        #print(v_new_source_system_url)

        # payload get's create based on all needed information
        v_payload = {
            "_type": "Collection",  
            "description": br_schema['DESCRIPTION'],
            "label": br_schema['LABEL'],
            "status": "WORKING"
        }

        #print(v_payload)
        
        # PATCH Method is used, as it does now overwrites attribues, which already exists in
        # dataspot
        # a non exiting object is creates
        # no objects get deleted in dataspot
        response = requests.patch(v_new_br_collection_url, headers=headers, json=v_payload)

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


        st.write(f"......getting all busniss rules for {br_schema['BUSINESS_KEY']}")


        # get all tables for a stage schema
        # each table is created as a classifier
        df_business_rules = get_business_rules(br_schema['BR_SCHEMA'])

        # Schleife über die Zeilen mit iterrows()
        for index_t, business_rule in df_business_rules.iterrows():

            # business key is used to create the url for the new classifier
            v_new_business_rule_url = f"https://partner.dataspot.io/rest/areto/schemes/{v_reporting_uuid}/collections/{v_collection_id}/classifiers/{business_rule['BUSINESS_KEY']}"

        
            # payload get's create based on all needed information
            v_payload = {
                "_type": "UmlClass",  
                "description": business_rule['DESCRIPTION'],
                "label": business_rule['LABEL'],
                "parentId" : v_collection_id,
                "inCollection": v_collection_id,
                "modelId" : v_reporting_uuid,
                "status": "WORKING"
            }


            # PATCH Method is used, as it does now overwrites attribues, which already exists in
            # dataspot
            # a non exiting object is creates
            # no objects get deleted in dataspot
            response = requests.patch(v_new_business_rule_url, headers=headers, json=v_payload)

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
            df_br_columns = get_br_columns(br_schema['BR_SCHEMA'], business_rule['BR_NAME'])
        
            st.write(f"........creating columns for stage table {business_rule['BUSINESS_KEY']}")

            # Schleife über die Zeilen mit iterrows()
            for index_c, br_column in df_br_columns.iterrows():
                
            
                # attributes are passed as a complete list for each table
                v_new_br_columns_url = f"https://partner.dataspot.io/rest/areto/schemes/{v_reporting_uuid}/collections/{v_collection_id}/classifiers/{v_classifier_id}/attributes/{br_column['BUSINESS_KEY']}"

                v_payload = {
                            "_type": "attribute",
                            "label": br_column['LABEL'],
                            "order": br_colusmn['ORDER'],                        
                            "parentId" : v_classifier_id,
                            "modelId" : v_reporting_uuid,
                            "status": "WORKING"                        
                            }

                v_payload =    {
                            "_type": "UmlAttribute",                        
                            "tenantId": v_tenant_id,
                            #"required": "OPTIONAL",
                            "description": "test Kommentar",
                            #"cardinality": "MANY",
                            "order": br_column['ORDER'],
                            #"hasDomain": "f829232a-594c-4b39-b8c2-78e74f8cd157",
                            "label": br_column['LABEL'],
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
                response = requests.patch(v_new_br_columns_url, headers=headers, json=v_payload)

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

