import pymongo
import pandas as pd
from pytz import timezone
from pyairtable import Api, Base, Table
from datetime import datetime
from dotenv import load_dotenv
import os
import numpy as np

load_dotenv("credenciales.env")

# =============================================================================
# ~ # Fetch data from subscriptions table
# ============================================================================

database = "somos-core"

uri = os.getenv("MONGODB_CONNECTION_STRING")

def fetch_mongo_table(table_name,database=database,uri=uri):
    # create the client
    client = pymongo.MongoClient(
    uri
    )
    # choose the client's database
    db = client[database]
    # choose the database's table
    collection_table = db[table_name]
    # fetch data
    cursor_table = collection_table.find()
    table_data = [doc for doc in cursor_table]
    df  = pd.DataFrame(table_data)
    return df

# Fetch data from subscriptions
subscriptions_df = fetch_mongo_table(table_name="subscriptions")

# subscriptions data wrangling
subscriptions_df["status"] = subscriptions_df["status"].apply(lambda x: x.lower())
subscriptions_df["appointment_id"] = None
subscriptions_df["mongo_customer_id"] = subscriptions_df["customerId"].apply(lambda x: str(x))
subscriptions_df["billing_period"] = subscriptions_df["billing"].apply(lambda x: x["period"])
subscriptions_df["service_id"] = subscriptions_df["services"].apply(lambda x: [str(item["serviceId"]) for item in x])
subscriptions_df["service_status"] = subscriptions_df["services"].apply(lambda x: [str(item["status"]) for item in x])
subscriptions_df["subsite_id"] = None
subscriptions_df["unit_details"] = None
subscriptions_df["unit_id"] = subscriptions_df["unitId"].apply(lambda x: str(x))
subscriptions_df["cutoff_day"] = subscriptions_df["cutoffDate"]
subscriptions_df["airtable_id"] = subscriptions_df["airtableId"].apply(lambda x: x if isinstance(x,str) else None)
subscriptions_df["start_date"] = subscriptions_df["installationDate"].apply(lambda x: str(x))
subscriptions_df["free_trial_until"] = subscriptions_df["startDate"].apply(lambda x: str(x))
subscriptions_df["end_date"] = subscriptions_df["services"].apply(lambda x: [str(item["endDate"]) if "endDate" in item else None for item in x])
subscriptions_df["created_at"] = subscriptions_df["createdAt"]
subscriptions_df["updated_at"] = subscriptions_df["updatedAt"]
subscriptions_df["offset_payment_days"] = -10
subscriptions_df["mongodb_id"] = subscriptions_df["_id"].apply(lambda x: str(x))

# Get the correct ERD columns and then explode columns to now turn each one into an independent subscription
final_subscriptions_columns = ["status","appointment_id","mongo_customer_id","billing_period","service_id","service_status",
                               "subsite_id","unit_details","cutoff_day","airtable_id",
                               "start_date","end_date","created_at","updated_at","offset_payment_days","unit_id","mongodb_id","free_trial_until"]

final_subscriptions_df = subscriptions_df[final_subscriptions_columns]#.explode()

final_subscriptions_df[["service_id", "service_status", "start_date", "end_date"]] = final_subscriptions_df[["service_id", "service_status", "start_date", "end_date"]].applymap(lambda x: str(x).replace("[", "").replace("]", "").replace("'", "").replace(" ", ""))

# coloca el service status y billing_period como lower case
final_subscriptions_df[['service_status', 'billing_period']] = final_subscriptions_df[['service_status', 'billing_period']].applymap(lambda x: x.lower() if isinstance(x, str) else x)

final_subscriptions_df["billing_period"].replace("")

# Separa los dataframes, uno para las suscripciones que no necesitan tratamiento especial y otro para las que sí
one_subscription = final_subscriptions_df[final_subscriptions_df['service_id'].str.len() == 24]

multiple_subscriptions = final_subscriptions_df[final_subscriptions_df['service_id'].str.len() > 24]

# Separar las columnas separadas por coma
columns_to_split = ["service_id", "service_status", "end_date"]
for column in columns_to_split:
    multiple_subscriptions[column] = multiple_subscriptions[column].str.split(",")

# Crear nuevas filas basadas en las columnas separadas
multiple_subscriptions = multiple_subscriptions.explode(columns_to_split)

# Realizar la asignación basada en la condición
multiple_subscriptions = multiple_subscriptions.reset_index(drop=True)
multiple_subscriptions.loc[multiple_subscriptions['status'] == 'active', 'status'] = multiple_subscriptions['service_status']

# =============================================================================
# ~ # Lógica para desagregar los servicios de dos y tres routers/tvs
# ============================================================================

'''
Televisión: 
    - Cambiar 64220ecf07ceab94fa226bc8 (tres dispositivos TV) por 3 de 
    64220ff107ceab94fa226d36 (1 dispositivo tv adicional)
    
    - Cambiar 64220fa407ceab94fa226cde (dos dispositivos TV) por 2 de
    64220ff107ceab94fa226d36 (1 dispositivo tv adicional)
    
Internet:
    - Cambiar 6422109607ceab94fa226db1 (tres routers adicionales) por 3 de
    637e3f14094225aa9ef2e595 (1 dispositivo VILO extra)
    
    - Cambiar 642210f407ceab94fa226dfb (dos routers adicionales) por 2 de
    637e3f14094225aa9ef2e595 (1 dispositivo VILO extra) 
'''

# Lista de service_id a duplicar y número de duplicaciones correspondientes
service_ids = ['64220ecf07ceab94fa226bc8', '64220fa407ceab94fa226cde', '6422109607ceab94fa226db1', '642210f407ceab94fa226dfb']
duplications = [2, 1, 2, 1]

# Iterar sobre la lista de service_id y duplicaciones
for service_id, duplication in zip(service_ids, duplications):
    # Filtrar las filas con el service_id actual
    subset = multiple_subscriptions[multiple_subscriptions['service_id'] == service_id]
    # Duplicar las filas seleccionadas según el número de duplicaciones
    duplicated_rows = pd.concat([subset] * duplication, ignore_index=True)
    # Concatenar el DataFrame original con las filas duplicadas
    multiple_subscriptions = pd.concat([multiple_subscriptions, duplicated_rows], ignore_index=True)


# Hacer replace de los servicios con múltiples devices
original_values = ['64220ecf07ceab94fa226bc8', '64220fa407ceab94fa226cde', '6422109607ceab94fa226db1', '642210f407ceab94fa226dfb']
replacement_values = ['64220ff107ceab94fa226d36', '64220ff107ceab94fa226d36', '637e3f14094225aa9ef2e595', '637e3f14094225aa9ef2e595']

# Reemplazar los valores en la columna "service_id"
multiple_subscriptions['service_id'] = multiple_subscriptions['service_id'].replace(original_values, replacement_values)

final_subscriptions_df = pd.concat([one_subscription, multiple_subscriptions])

#final_subscriptions_df["id"] = range(1, len(final_subscriptions_df)+1)
final_subscriptions_df.drop("service_status", axis=1, inplace=True)


# =============================================================================
# ~ # columns to datetime
# ============================================================================

final_subscriptions_df['start_date'] = pd.to_datetime(final_subscriptions_df['start_date'], format='%Y-%m-%d%H:%M:%S', errors='coerce')

final_subscriptions_df = final_subscriptions_df.astype({'start_date':str})

#final_subscriptions_df['start_date'] = pd.to_datetime(final_subscriptions_df['start_date'], format='%Y-%m-%d%H:%M:%S', errors='coerce')
final_subscriptions_df = final_subscriptions_df.astype({'start_date':str}).replace('NaT',None)

# Extraer la parte de la cadena que sigue el formato deseado
final_subscriptions_df['end_date'] = final_subscriptions_df['end_date'].str.extract(r'(\d{4}-\d{2}-\d{2}\d{2}:\d{2}:\d{2})')
final_subscriptions_df['end_date'] = pd.to_datetime(final_subscriptions_df['end_date'], format='%Y-%m-%d%H:%M:%S', errors='coerce')
final_subscriptions_df = final_subscriptions_df.astype({'end_date':str}).replace('NaT',None)

final_subscriptions_df['created_at'] = pd.to_datetime(final_subscriptions_df['created_at']).dt.floor('s')
final_subscriptions_df['updated_at'] = pd.to_datetime(final_subscriptions_df['updated_at']).dt.floor('s')

final_subscriptions_df = final_subscriptions_df.astype({'created_at':str,
                                                        'updated_at':str})

final_subscriptions_df.rename(columns={'end_date':'retired_at'},inplace=True)

# =============================================================================
#  fill start_date None with created_at values
# =============================================================================

final_subscriptions_df['start_date'] = final_subscriptions_df['start_date'].fillna(final_subscriptions_df['created_at'])

# =============================================================================
# ~ # free_trial_until replace with start_date + 30
# ============================================================================

# convert column "start_date" to datetime
final_subscriptions_df['start_date'] = pd.to_datetime(final_subscriptions_df['start_date'])

# Create new column "free_trial_until" adding 30 days to "start_date"
#final_subscriptions_df['free_trial_until'] = final_subscriptions_df['start_date'] + pd.DateOffset(days=30)

# restore start_date column format str
final_subscriptions_df = final_subscriptions_df.astype({'start_date':str,
                                                        'free_trial_until':str})
# =============================================================================
# ~ # cutoff_day cleaning
# ============================================================================

final_subscriptions_df["cutoff_day"] = final_subscriptions_df["cutoff_day"].apply(lambda x: str(x).replace('.0','')).astype(str)

final_subscriptions_df = final_subscriptions_df.replace('nan',None)

# =============================================================================
# ~ # Merge between mongodb subscriptions data and airtable subscriptions data
# ============================================================================

api_key = os.getenv("API_KEY")
subscriptions_airtable_base = os.getenv("CUSTOMERS_AIRTABLE_BASE")
subscriptions_airtable_table_id = os.getenv("SUBSCRIPTIONS_AIRTABLE_TABLE_ID") 
subscriptions_api = Api(api_key)


subscriptions_from_at = ['recordId (from Torre [Sync]) (from unidad)','record_id']

subs_airtable = pd.DataFrame(subscriptions_api.all(subscriptions_airtable_base,subscriptions_airtable_table_id,fields=subscriptions_from_at)).fields.apply(pd.Series)

subs_df = subs_airtable.copy()

subs_df.rename(columns={'recordId (from Torre [Sync]) (from unidad)':'subsite_id_subscriptions',
                        'record_id':'airtable_id'},inplace=True)

subs_df = subs_df.astype({'airtable_id':str,
                          'subsite_id_subscriptions':str})

subs_df["subsite_id_subscriptions"] = subs_df["subsite_id_subscriptions"].str.replace("[", "").str.replace("]", "").str.replace("'", "").str.replace(" ", "")


final_subscriptions_df = final_subscriptions_df.merge(subs_df,on='airtable_id',how='left')

final_subscriptions_df["status"].value_counts()

final_subscriptions_df["status"] = final_subscriptions_df["status"].str.replace("inactive","retired")
# =============================================================================
# ~ # Subscriptions table replace
# ============================================================================

# Aplicar el mapeo
final_subscriptions_df["status"] = final_subscriptions_df["status"].map({
    'active': 1,
    'retired': 2
})

# Mapeo de valores de billing_period
final_subscriptions_df["billing_period"] = final_subscriptions_df["billing_period"].map({
    'monthly': 0,
    'annual': 1
})

final_subscriptions_df = final_subscriptions_df.astype({"status":str,
                                      "billing_period":str})

columns_to_clean = ["status","billing_period"]

final_subscriptions_df[columns_to_clean] = final_subscriptions_df[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

final_subscriptions_df = final_subscriptions_df.replace('nan',None).replace(np.nan, None)

