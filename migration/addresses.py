import numpy as np
from pytz import timezone
import psycopg2
from pyairtable import Api, Base, Table

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

site_from_at = ['address','latitude','longitude','Ciudad','country','Barrio','google_maps_link','name_zonita','Name_zona','recordId','somos_code','created']

df_sites_airtable = pd.DataFrame(site_api.all(site_base_id,site_table_id,fields=site_from_at)).fields.apply(pd.Series)

df_addresses_airtable = df_sites_airtable.copy()
# Rename airtable addresses column
df_addresses_airtable.rename(columns={'Ciudad':'city',
                                     'recordId':'airtable_id',
                                     'Barrio':'neighborhood',
                                     'name_zonita':'subzone',
                                     'Name_zona':'zone',
                                     'created':'created_at'},inplace=True)

# =============================================================================
# ~ FETCH DATA FROM MONGODB SITES TABLE
# =============================================================================

database = "somos-core"

uri = os.getenv("MONGODB_CONNECTION_STRING")

client = pymongo.MongoClient(
    uri
)

# conection to mongodb "sites" collection
db = client[database]

collection_sites = db["sites"]

cursor_sites = collection_sites.find()

data_sites = [doc for doc in cursor_sites]

df_sites_mongodb = pd.DataFrame(data_sites)


df_addresses_mongodb = df_sites_mongodb[['address','code','updatedAt']]

df_addresses_mongodb["google_place_id"] = df_sites_mongodb["address"].apply(lambda x: x.get('googlePlaceId'))

df_addresses_mongodb.drop("address", axis=1, inplace=True)

# Rename mongodb addresses column
df_addresses_mongodb.rename(columns={'code':'somos_code',
                                     'updatedAt':'updated_at'},inplace=True)

# Change column type data
df_addresses_airtable = df_addresses_airtable.astype({"airtable_id":str,
                                          "neighborhood":str,
                                          "subzone":str,
                                          "zone":str,
                                          "latitude":str,
                                          "longitude":str})


df_addresses_airtable[["zone","subzone"]] = df_addresses_airtable[["zone","subzone"]].apply(lambda x: x.str.replace("'","").str.replace("[","").str.replace("]",""))

# Merge between addresses (mongodb and airtable)
df_addresses_merge = df_addresses_mongodb.merge(df_addresses_airtable, on="somos_code", how="right")


df_addresses_merge["id"] = range(1, len(df_addresses_merge)+1)


# =============================================================================
# ~ Crea una nueva row para añadir un address inexistente
# =============================================================================
new_row = {'somos_code':'XXXX','updated_at': '2022-10-21 16:51:20.604000', 'google_place_id': 'NN', 'google_maps_link': 'NN', 'latitude': '0', 'longitude': '0', 'city': 'NN', 'neighborhood': 'NN', 'address': 'NN', 'country': 'COL', 'subzone': 'NN', 'zone': 'NN', 'created_at': '2022-09-26T21:15:09.000Z', 'id': len(df_addresses_merge)+1}

df_addresses_merge =  df_addresses_merge.append(new_row, ignore_index=True)

# Convertir la columna "created_at" a datetime
df_addresses_merge['created_at'] = pd.to_datetime(df_addresses_merge['created_at'])

# Formatear la columna "created_at" al formato "%Y-%m-%d %H:%M:%S"
df_addresses_merge['created_at'] = df_addresses_merge['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Convert "updated_at" column to datetime
df_addresses_merge['updated_at'] = pd.to_datetime(df_addresses_merge['updated_at'], errors='coerce')

# Format non-null values in "updated_at" column
df_addresses_merge['updated_at'] = df_addresses_merge['updated_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else x)

df_addresses_merge = df_addresses_merge.astype({"updated_at":str})

df_addresses_merge= df_addresses_merge.replace('nan',None).replace(np.nan,None).replace('NaT',None)

# replace null values in updated_at with created_at
df_addresses_merge['updated_at'] = df_addresses_merge['updated_at'].fillna(df_addresses_merge['created_at'])
