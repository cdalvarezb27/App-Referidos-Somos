import pymongo
import pandas as pd
import numpy as np
from pytz import timezone
import psycopg2
import supabase
from pyairtable import Api, Base, Table
import re
from datetime import datetime


from dotenv import load_dotenv
import os

load_dotenv("credenciales.env")

# =============================================================================
# ~ FETCH DATA FROM AIRTABLE TOWER TABLE
# =============================================================================
api_key = os.getenv("API_KEY")
subsite_base_id = os.getenv("SUBSITE_BASE_ID")
subsite_table_id = os.getenv("SUBSITE_TABLE_ID")
subsite_api = Api(api_key)


subsite_from_at = ['Name', 'Estado', 'home_passed_tower', '#_floors', 'apts_per_floor', 'recordId','somos_code (from Site) 2' ,'created']

subsite_df = pd.DataFrame(subsite_api.all(subsite_base_id,subsite_table_id,fields=subsite_from_at)).fields.apply(pd.Series)

subsite_df_copy = subsite_df.copy()


# =============================================================================
# ~ # Rename columns subsites
# =============================================================================

subsite_df_copy.rename(columns={'Estado':'status',
                             'Name':'subsite_detail',
                             "home_passed_tower":"home_passed",
                             '#_floors':'floors',
                             'apts_per_floor':'apts_per_floor',
                             'somos_code (from Site) 2':'somos_code',
                             'recordId':'airtable_id',
                             'created':'created_at'},inplace=True)



# =============================================================================
# ~ Create columns subsites
# =============================================================================

subsite_df_copy['live_date'] = None

subsite_df_copy['authenticated'] = None

subsite_df_copy['updated_at'] = None

# =============================================================================
# ~  DATA WRANGLING subsites
# =============================================================================

# Change time format for created
def time_format(row):
    # date format
    date_format = '%Y-%m-%d %H:%M:%S'

    # get year/month/date portion of the string
    ymd = row['created_at'].split('T')[0]

    # get hour/minute/second portion of the string
    hms = row['created_at'].split('T')[1].split('.')[0]

    # concatenate both
    actual_time = ymd + ' ' + hms

    #actual_time_datetime = datetime.strptime(actual_time,date_format)

    return actual_time

subsite_df_copy['created_at'] = subsite_df_copy.apply(lambda row: time_format(row), axis=1)

subsite_df_copy['created_at'] = pd.to_datetime(subsite_df_copy['created_at'], format='%Y-%m-%d %H:%M:%S')


subsite_df_copy['subsite_detail'] = subsite_df_copy['subsite_detail'].str.replace('❌', '').str.replace('✅', '').str.replace('⚠️', '')
subsite_df_copy['subsite_detail'] = subsite_df_copy['subsite_detail'].str[5:].str.lower()

subsite_df_copy = subsite_df_copy.astype({"somos_code":str,
                                          "created_at":str})

subsite_df_copy["somos_code"] = subsite_df_copy["somos_code"].str.replace("'", "").str.replace("[", "").str.replace("]", "").str.replace(" ","")
subsite_df_copy['status'] = subsite_df_copy['status'].str.lower()

# =============================================================================
# Add Fake subsite
# =============================================================================
new_row = {'subsite_detail': 'a validar — torre (1)', 'status': 'activo', 'floors': '', 'apts_per_floor': '', 'airtable_id': 'NN', 'created_at': '2022-10-18 21:22:35', 'somos_code': 'XXXX', 'home_passed': '', 'live_date': '', 'authenticated': '', 'updated_at': '2022-10-18 21:22:35'}

subsite_df_copy = subsite_df_copy.append(new_row, ignore_index=True)

# =============================================================================
# clean floors column and replace updated_at with created_at
# =============================================================================
subsite_df_copy['updated_at'] = subsite_df_copy['updated_at'].fillna(subsite_df_copy['created_at'])

valores_reemplazar = [
    'Piso 1: 2 Apartamentos y 2 Locales    Pisos 2 y 3: 3 Apartamentos    Pisos 4,6,8,10,12,14: 2 Apartamentos    Pisos 5,7,9,11,13: 3 Apartamentos    Pisos 15 y 16: 1 Apartamento',
    'Piso 1: 3 Apartamentos    Piso 2 al 20: 4 Apartamentos ',
    '24+S',
    '25 del 1 hacia abajo son parqueaderos ',
    '25 del 1 hacia abajo son parqueaderos '
]
# Reemplazar los valores en 'floors' por None
subsite_df_copy['floors'] = subsite_df_copy['floors'].replace(valores_reemplazar, None)

# =============================================================================
# clean subsite_df_copy
# =============================================================================
subsite_df_copy = subsite_df_copy.replace(np.nan,None).replace('nan',None).replace('',None)

# =============================================================================
# subsites table replace
# =============================================================================

# Mapeo de valores de status
subsite_df_copy["status"] = subsite_df_copy["status"].map({
    'en proceso': 0,
    'activo': 1,
    'inactivo': 2
})

subsite_df_copy = subsite_df_copy.astype({"status":str})

columns_to_clean = ["status"]

subsite_df_copy[columns_to_clean] = subsite_df_copy[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

subsite_df_copy = subsite_df_copy.replace('nan',None).replace(np.nan, None)



