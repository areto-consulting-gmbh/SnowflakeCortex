
import streamlit as st

import requests
import json
from requests_oauthlib import OAuth2Session
import pandas as pd

#helper functions
from helper.dataspot_linage_helper import get_access_token
from helper.dataspot_linage_helper import get_db_connection
from helper.dataspot_linage_helper import save_hub_load_config
from helper.dataspot_linage_helper import save_satellite_load_config
from helper.dataspot_linage_helper import save_link_load_config



#v_tenant_id = "areto DWH"
# -> tenant für Areto DWH

v_tenant_name = "Andre's Mandant"
v_tenant_id = "c73d73b3-5c52-4137-bccf-062f08fd8dd"
# -> tenant für showcase


v_source_system_uuid = 'b50fa775-b57d-478a-9e3d-f5c596beae55'
v_business_model_uuid = '44fb19c0-82fe-4c4a-8282-a4f154127a29'



#
# Button to start sync
#

if st.button("Synchronize data vault config to Jetvault", type="primary"):


    st.write("Loading Hub Load configs from Dataspot")

    # get access token for dataspot
    access_token = get_access_token()

    # header is same for each call
    headers = {
        'Authorization': f'Bearer {access_token}',
        "dataspot-tenant" : v_tenant_name
    }


    #
    # get hub load config based on transformations for business model
    #

    payload = {
    "sql": """
    --all hub loads based on business keys defined in business model
    select
        col_stg.label                           STAGE_SCHEMA,
        cl_stg.label                            STAGE_TABLE,
        'DATA_VAULT'                            HUB_SCHEMA,
        'H_' || upper(cl.label)                 HUB_NAME,
        'H_' || upper(cl.label)                 HUB_ALIAS,
        null                                    H_COLUMN_NAME,
        null                                    BK_COLUMN_NAME,
        string_agg(at_stg.label, ',')           BK_SOURCE_COLUMN_LIST,
        null                                    SLICE_SRC_COLUMN_LIST
    from
        --business model view
        scheme_view s
        --all collections for the business model
        join collection_view col
            on s.id = col.in_scheme
        --all business objects in the business model
        join classifier_view cl
            on cl.in_collection = col.id
        --all attributes for the business objects
        join attribute_view at
            on at.parent_id = cl.id
                --only business key columns
                and at.identifying = True
        -- get transformation for business keys and the corresponding stage source
        join transformsto_view tf_to
            on at.id = tf_to.transforms_to
        join transformsfrom_view tf_from
            on tf_to.resource_id = tf_from.resource_id
        -- stage source attribute
        join attribute_view at_stg
            on tf_from.transforms_from = at_stg.id
        -- stage source table
        join classifier_view cl_stg
            on at_stg.parent_id = cl_stg.id
        -- get stage source schema
        join collection_view col_stg
            on cl_stg.in_collection = col_stg.id
    where
        --collection is part of the business model scheme
        s.id = '44fb19c0-82fe-4c4a-8282-a4f154127a29'    
        --additional filter for source scheme? in case there are multiple source schemes?
    group by 
        1,2,3,4
    UNION
    --all hub loads based on relations defined in the business model
    select
        --Jetvault Information
        col_stg.label        STAGE_SCHEMA,
        cl_stg.label        STAGE_TABLE,
        'DATA_VAULT'        HUB_SCHEMA,
        'H_' || upper(cl_o2.label)  HUB_NAME,
        --check if there are multiple hub loads for the same source table & hub
        --if yes, an hub alias has to be used
        case
            when count(*) over (partition by col_stg.label, cl_stg.label, cl_o2.label) > 1 then
                'H_' || upper(cl_o2.label) || '_' || upper(cl_stg.label) || '_' || upper(at_stg.label)
            else
                'H_' || upper(cl_o2.label)
        end HUB_ALIAS,
        null    H_COLUMN_NAME,
        null    H_BK_COLUMN_NAME,
        string_agg(at_stg.label, ',') over (partition by col_stg.label, cl_stg.label, cl_o2.label)  BK_SOURCE_COLUMN_LIST,
        null    SLICE_SRC_COLUMN_LIST
    from
        --business model view
        scheme_view s
        --all collections for the business model	
        --get all relations for business model
        join association_view ass
            on ass.model_id = s.id
        --join classifiers of relation to get the business objects
        --only the object 2 is needed, as only the target of the relation
        --is important for the load
        join classifier_view cl_o2
            on ass.has_range = cl_o2.id
        join attribute_view at_o2
            on at_o2.parent_id = cl_o2.id
                and COALESCE (at_o2.identifying, False) = True
        --join transformation to get the source columns of the relation
        join transformsto_view tf_to
            on ass.id = tf_to.transforms_to
        join transformsfrom_view tf_from
            on tf_to.resource_id = tf_from.resource_id
        --get source tables from transformation
        -- attribute of the transformation
        join attribute_view at_stg
            on tf_from.transforms_from = at_stg.id
        -- stage table object for the source attribute
        join classifier_view cl_stg
            on at_stg.parent_id = cl_stg.id
        -- source schema of the stage table
        join collection_view col_stg
            on cl_stg.in_collection = col_stg.id
    where
        --collection is part of the business model scheme
        s.id = '44fb19c0-82fe-4c4a-8282-a4f154127a29'
        --only relations
        and ass._type = 'Relationship'
            """
    }


    # get all objects in the business model

    v_url = f"https://partner.dataspot.io/api/areto/queries/download?format=JSON"



    response = requests.put(v_url, headers=headers, json=payload)

    # Antwort auswerten
    if response.status_code == 200:
        st.write("Anfrage erfolgreich.")
        st.write("Antwort der API:")
        st.write(response.text)  # Antwort der API anzeigen

        # ID is extracted from reponse, as it is needed
        # as a parent for all table
        json_response= response.json()
        
    else:
        st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
        st.write(response.text)



    #
    # write hub loads to database
    #


    st.write("writing Hub Load configs to database")


    save_hub_load_config(json_response)



    st.write("Loading Satellite Load configs from Dataspot")


    # get access token for dataspot
    access_token = get_access_token()

    # header is same for each call
    headers = {
        'Authorization': f'Bearer {access_token}',
        "dataspot-tenant" : v_tenant_name
    }


    #
    # get hub load config based on transformations for business model
    #

    payload = {
    "sql": """
    select
        col_stg.label                           STAGE_SCHEMA,
        cl_stg.label                            STAGE_TABLE,
        'DATA_VAULT'                            SAT_SCHEMA,
        'H_' || upper(cl.label) || '_S_' || upper(cl.label)                SAT_NAME,
        'H_' || upper(cl.label)                 REFERENCED_OBJECT_NAME,
        null                                    REFERENCE_HASH_COLUMN,
        string_agg(at_stg.label, ',')           DELTA_HASH_SRC_COLUMN_LIST
    from
        --business model view
        scheme_view s
        --all collections for the business model
        join collection_view col
            on s.id = col.in_scheme
        --all business objects in the business model
        join classifier_view cl
            on cl.in_collection = col.id
        --all attributes for the business objects
        join attribute_view at
            on at.parent_id = cl.id
        --defined transformations for the attributes
        join transformsto_view tf_to
            on at.id = tf_to.transforms_to
        --source of the transformation
        join transformsfrom_view tf_from
            on tf_to.resource_id = tf_from.resource_id
        -- attribute of the transformation
        join attribute_view at_stg
            on tf_from.transforms_from = at_stg.id
        -- stage table object for the source attribute
        join classifier_view cl_stg
            on at_stg.parent_id = cl_stg.id
        -- source schema of the stage table
        join collection_view col_stg
            on cl_stg.in_collection = col_stg.id
    where
        --collection is part of the business model scheme
        s.id = '44fb19c0-82fe-4c4a-8282-a4f154127a29'
        --only business key columns
        and COALESCE (at.identifying, False) = False
    group by 1,2,3,4,5
            """
    }


    # get all objects in the business model

    v_url = f"https://partner.dataspot.io/api/areto/queries/download?format=JSON"



    response = requests.put(v_url, headers=headers, json=payload)

    # Antwort auswerten
    if response.status_code == 200:
        st.write("Anfrage erfolgreich.")
        st.write("Antwort der API:")
        st.write(response.text)  # Antwort der API anzeigen

        # ID is extracted from reponse, as it is needed
        # as a parent for all table
        json_response= response.json()
        

        st.write(json_response)
    else:
        st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
        st.write(response.text)



    save_satellite_load_config(json_response)



    st.write("Loading Link Load configs from Dataspot")


    # get access token for dataspot
    access_token = get_access_token()

    # header is same for each call
    headers = {
        'Authorization': f'Bearer {access_token}',
        "dataspot-tenant" : v_tenant_name
    }


    #
    # requirement for link loads: the source column already have to be mapped as source columns 
    # to both BKs in the business model
    #


    #
    # get link loads based on defined relation
    #

    payload = {
    "sql": """
    select distinct
        --Jetvault Information
        col_stg.label        STAGE_SCHEMA,
        cl_stg.label        STAGE_TABLE,
        'DATA_VAULT'                LINK_SCHEMA,
        'L_' || upper(cl_o1.label) || '_' || upper(cl_o2.label) || '_' || replace(upper(ass.name),' ', '_')  LINK_NAME,
        upper(cl_o1.label) || '_' || upper(cl_o2.label) || '_L'  L_COLUMN_NAME,    
        --check if there are multiple hub loads for the same source table & hub
        --if yes, an hub alias has to be used
        case
            --multiple associations
            when max(ass.name) over (partition by col_stg.label, cl_stg.label, cl_o2.label) <> 
                min(ass.name) over (partition by col_stg.label, cl_stg.label, cl_o2.label) then
                'H_' || upper(cl_o2.label) || '_' || upper(cl_stg.label) || '_' || upper(at_stg.label)        
            else
                'H_' || upper(cl_o2.label)
        end REFERENCED_HUB_NAME_1,    
        'H_' || upper(cl_o1.label)  REFERENCED_HUB_NAME_2
    from
        --business model view
        scheme_view s
        --all collections for the business model	
        --get all relations for business model
        join association_view ass
            on ass.model_id = s.id
        --join classifiers of relation to get the business objects
        join classifier_view cl_o1
            on ass.has_domain = cl_o1.id   
        join classifier_view cl_o2
            on ass.has_range = cl_o2.id    
        --join transformation to get the source columns of the relation
        join transformsto_view tf_to
            on ass.id = tf_to.transforms_to
        join transformsfrom_view tf_from
            on tf_to.resource_id = tf_from.resource_id
        --get source tables from transformation
        -- attribute of the transformation
        join attribute_view at_stg
            on tf_from.transforms_from = at_stg.id
        -- stage table object for the source attribute
        join classifier_view cl_stg
            on at_stg.parent_id = cl_stg.id
        -- source schema of the stage table
        join collection_view col_stg
            on cl_stg.in_collection = col_stg.id
    where
        --collection is part of the business model scheme
        s.id = '44fb19c0-82fe-4c4a-8282-a4f154127a29'
        --only relations
        and ass._type = 'Relationship'
            """
    }


    # get all objects in the business model

    v_url = f"https://partner.dataspot.io/api/areto/queries/download?format=JSON"



    response = requests.put(v_url, headers=headers, json=payload)

    # Antwort auswerten
    if response.status_code == 200:
        st.write("Anfrage erfolgreich.")
        st.write("Antwort der API:")
        st.write(response.text)  # Antwort der API anzeigen

        # ID is extracted from reponse, as it is needed
        # as a parent for all table
        json_response= response.json()
        
    else:
        st.write(f"Anfrage fehlgeschlagen mit Statuscode {response.status_code}")
        st.write(response.text)


    #
    # write hub loads to database
    #


    st.write("writing Link Load configs to database")


    save_link_load_config(json_response)
