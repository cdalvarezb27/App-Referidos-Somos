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


api_key = os.getenv("API_KEY")
site_base_id = os.getenv("SITE_BASE_ID")
site_table_id = os.getenv("SITE_TABLE_ID")
site_api = Api(api_key)

# =============================================================================
# ~ FETCH DATA FROM AIRTABLE SITE TABLE
# =============================================================================

# SITES TABLE

site_from_at = ['somos_code','Name','state_stepper','site_type','type','topologia','phone_porteria_string','growth_phases','costo_total','recordId','Centro de costos','Estrato','archivado-motivo','razon_retiro','created']

df_sites_airtable = pd.DataFrame(site_api.all(site_base_id,site_table_id,fields=site_from_at)).fields.apply(pd.Series)

df_sites_airtable_copy = df_sites_airtable.copy()


# =============================================================================
# ~ FETCH DATA FROM MONGODB SITES TABLE
# =============================================================================
database = "somos-core"

uri = "mongodb+srv://camilo_david_alvarez_bravo:15pKOQAD37C860W2@db-mongodb-nyc3-898-910be9fa.mongo.ondigitalocean.com/admin?replicaSet=db-mongodb-nyc3-898&tls=true&authSource=admin&tlsCAFile=C:/Users/cdalv/mongodb/ca-certificate.crt"

client = pymongo.MongoClient(
    uri
)

# conection to mongodb "sites" collection
db = client[database]

collection_sites = db["sites"]

cursor_sites = collection_sites.find()

data_sites = [doc for doc in cursor_sites]

df_sites_mongodb = pd.DataFrame(data_sites)

df_sites_mongodb_copy = df_sites_mongodb[["_id","code","updatedAt"]]

# ============================================================================
# ~ # merge between df_sites_mongodb_copy and df_sites_airtable_copy
# =============================================================================

df_sites_merged = df_sites_mongodb_copy.merge(df_sites_airtable_copy, left_on="code", right_on="somos_code", how="right")

# =============================================================================
# ~ # Rename columns in df_sites_merged
# =============================================================================


df_sites_merged.rename(columns={'_id': 'mongodb_id',
                             'Name':'name',
                             'state_stepper':'status',
                             'site_type':'sales_type',
                             'type':'site_type',
                             'topologia':'topology',
                             'phone_porteria_string':'phone_lobby',
                             'growth_phases':'growth_phase',
                             'costo_total':'total_cost_cents',
                             'recordId':'airtable_id',
                             'Centro de costos':'cost_center',
                             'razon_retiro':'retired_reason',
                             'archivado-motivo':'archived_reason',
                             'Estrato':'stratum_id',
                             'updatedAt':'updated_at',
                             'created':'created_at'},inplace=True)

df_sites_merged = df_sites_merged.drop(['code'], axis=1)


# =============================================================================
# ~ # Data cleaning of phone_loby column
# =============================================================================
df_sites_merged = df_sites_merged.astype({"phone_lobby":str})

df_sites_merged['phone_lobby'] = df_sites_merged['phone_lobby'].str.replace('(','') \
                                                         .str.replace(')','') \
                                                         .str.replace(' ','') \
                                                         .str.replace('64298-292-3147853749','64298292-3147853749') \
                                                         .str.replace('36596322758188','3659632-2758188') \
                                                         .str.replace('30659603227058188','3065960322-7058188')\
                                                         .str.replace('320\xa09395193\u202c','3209395193') \
                                                         .str.replace('320\xa09395193\u202c','3004728345')\
                                                         .str.replace('320\xa09395193\u202c','3209395193')\
                                                         .str.replace('300\xa04728345\u202c','3004728345') \
                                                         .str.replace('sininfo','')

df_sites_merged['phone_lobby'] = df_sites_merged['phone_lobby'].apply(lambda x: 'nan' if (x == '0' or x == '3' or x == '0000' or x == '000000') else x)

df_sites_merged['phone_lobby'] = df_sites_merged['phone_lobby'].replace('nan','None')
# put column phone_lobby in json format
for i in range(len(df_sites_merged['phone_lobby'])):
    if len(df_sites_merged['phone_lobby'][i]) <= 11:
        df_sites_merged['phone_lobby'][i] = df_sites_merged['phone_lobby'][i].replace('-','')

def format_phone(phone):
    if phone == 'None':
        return {}
    phones = phone.split('-')
    if len(phones) == 2:
        return {"telefono 1": phones[0], "telefono 2": phones[1]}
    else:
        return {"telefono 1": phones[0]}

df_sites_merged['phone_lobby'] = df_sites_merged['phone_lobby'].apply(format_phone)
df_sites_merged = df_sites_merged.astype({"phone_lobby":str})
#df_sites_merged.to_json(orient='records')
# clean format phone_lobby
df_sites_merged['phone_lobby'] = df_sites_merged['phone_lobby'].str.replace("'", '"')

# =============================================================================
# ~ # Data cleaning of created_at and updated_at
# =============================================================================

df_sites_merged = df_sites_merged.astype({"updated_at":str})

df_sites_merged['updated_at'] = df_sites_merged['updated_at'].str.split('.').str[0]

# Convertir la columna "created_at" a datetime
df_sites_merged['created_at'] = pd.to_datetime(df_sites_merged['created_at'])

# Formatear la columna "created_at" al formato "%Y-%m-%d %H:%M:%S"
df_sites_merged['created_at'] = df_sites_merged['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

df_sites_merged = df_sites_merged.drop_duplicates(subset=["somos_code"])

# =============================================================================
# ~ # towers_count column
# =============================================================================

towers_count_base_id = "appGvpqKX2V4zqE9u"
towers_count_table_id = "tblQ59i5H6CfsmR9G"
towers_count_api = Api(api_key)


towers_count_from_at = ["somos_code (from Site) 3"]

towers_count_df = pd.DataFrame(towers_count_api.all(towers_count_base_id,towers_count_table_id,fields=towers_count_from_at)).fields.apply(pd.Series)

towers_count_df.rename(columns={"somos_code (from Site) 3":"somos_code"},inplace=True)

towers_count_df = towers_count_df.astype({"somos_code":str})

towers_count_df["somos_code"] = towers_count_df["somos_code"].str.replace("[","").str.replace("]","").str.replace("'","")

# Agrupar y contar por la columna "document_number"
towers_count_df = towers_count_df.groupby('somos_code').size().reset_index(name='towers_count')


# =============================================================================
# ~ # Merge between df_sites_merged and towers_count_df
# =============================================================================

df_sites_merged = df_sites_merged.merge(towers_count_df,on='somos_code',how='left')

# =============================================================================
# ~ # Data cleaning of mongodb_id, status, name, towers_count and stratum_id
# =============================================================================

df_sites_merged = df_sites_merged.astype({"mongodb_id":str,
                                          "status":str,
                                          "stratum_id":str,
                                          "towers_count":str,
                                          "cost_center":str})

df_sites_merged["stratum_id"] = df_sites_merged["stratum_id"].str.replace('.0','')

df_sites_merged["towers_count"] = df_sites_merged["towers_count"].str.replace('.0','')

df_sites_merged['status'] = df_sites_merged['status'].str.lower()

df_sites_merged['name'] = df_sites_merged['name'].str.lower()

# =============================================================================
# ~ # Add XXXX somos_code
# =============================================================================
new_row = {'mongodb_id': None, 'updated_at': '2022-11-11 15:42:02', 'name': 'xxxx a validar', 'somos_code': 'XXXX', 'status': 'activo', 'created_at': '2023-11-11 15:42:02', 'stratum_id': '4', 'airtable_id': 'NN', 'total_cost_cents': 0, 'growth_phase': 'Gamma alta', 'sales_type': 'Grey field', 'site_type': 'POP', 'cost_center': None, 'topology': None, 'phone_lobby': '{}', 'towers_count': '0'}

df_sites_merged = df_sites_merged.append(new_row, ignore_index=True)

# =============================================================================
# ~ # Add columns 'id' and 'admin_id' that doesn't exist
# =============================================================================

df_sites_merged['admin_id'] = None

# =============================================================================
# ~ # Cleaning empty format
# =============================================================================

df_sites_merged = df_sites_merged.replace(np.nan,None)
df_sites_merged = df_sites_merged.replace('None',None)
df_sites_merged = df_sites_merged.replace('nan',None)
df_sites_merged = df_sites_merged.replace('',None)
df_sites_merged = df_sites_merged.replace('NOT_FOUND',None)
df_sites_merged = df_sites_merged.replace('NaT',None)

df_sites_merged = df_sites_merged[df_sites_merged['somos_code'].notna()]

# =============================================================================
# Multiply values *100
# =============================================================================

df_sites_merged['total_cost_cents'] = df_sites_merged['total_cost_cents'].apply(lambda x: x * 100)

# =============================================================================
# ~ # replace null values in updated_at with created_at values
# =============================================================================
df_sites_merged['updated_at'] = df_sites_merged['updated_at'].fillna(df_sites_merged['created_at'])
# =============================================================================
# sites table replace
# =============================================================================

# Mapeo de valores de site_type
df_sites_merged["site_type"] = df_sites_merged["site_type"].map({
    'POP': 0,
    'CN': 1,
    'DN': 2
})

# Mapeo de valores de sales_type
df_sites_merged["sales_type"] = df_sites_merged["sales_type"].map({
    'Green field': 0,
    'Legacy': 1,
    'Grey field': 2
})

# Mapeo de valores de archived_reason
df_sites_merged["archived_reason"] = df_sites_merged["archived_reason"].map({
    'Archivado Pipefy':0,
    'No acepta propuesta':1,
    'No viable':2,
    'No target':3,
    'Comercial no viable':4,
    'No acepta vista':5,
    'Sin interes':6,
    'No contesta':7,
    'Datos incorrectos':8
})


# Mapeo de valores de retired_reason
df_sites_merged["retired_reason"] = df_sites_merged["retired_reason"].map({
    'Poca penetración':0,
    'Solicitud de la administración':1
})



# Mapeo de valores de growth_phase

df_sites_merged["growth_phase"] = df_sites_merged["growth_phase"].map({
    'Long Tail':0,
    'Gamma alta':1,
    'No admin':2,
    'Gamma baja':3,
    'Viabilidad para coordinar':4,
    'Para gestión comercial':5,
    'Desplegable':6,
    'Live 3 meses':7,
    'Live 1 mes':8,
    'Gamma media':9,
    'Por negociar':10,
    'En Viabilidad':11,
    'Mid Growth':12,
    'Backlog':13,
    'Archivado':14,
    'Retirados':15,
    'Live 2 meses':16,
    'Live 4 meses':17
})

# Mapeo de valores de status

df_sites_merged["status"] = df_sites_merged["status"].map({
    'inactivo': 0,
    'activo': 1,
    'retirado': 2,
    'cancelado': 3,
    'en proceso': 4
})



df_sites_merged = df_sites_merged.astype({"site_type":str,
                                          "sales_type":str,
                                          "archived_reason":str,
                                          "retired_reason":str,
                                          "growth_phase":str,
                                          "status":str})

columns_to_clean = ["status", "growth_phase", "site_type", "retired_reason", "archived_reason", "sales_type"]
df_sites_merged[columns_to_clean] = df_sites_merged[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

df_sites_merged = df_sites_merged.replace('nan',None).replace(np.nan, None).replace('NO HAY INFO',None).replace('',None)


# PRAL, MALO, MRAT, BATC

# Agrupar y contar por la columna "document_number"
counts_df = df_sites_merged.groupby(['somos_code']).size().reset_index(name='counts')

# Filtrar los counts mayores que 1 para determinar los duplicados
duplicates_sites = counts_df[counts_df['counts'] > 1]


