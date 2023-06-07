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
# CUSTOMERS TABLE MONGODB
# =============================================================================


DATABASE = "somos-core"
URI = os.getenv("MONGODB_CONNECTION_STRING")
def fetch_mongo_table(table_name,database=DATABASE,uri=URI):
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

# Fecth Customers data
customers_df = fetch_mongo_table(table_name="customers")

# Data wrangling
customers_df["first_name"] = customers_df["name"]
customers_df["last_name"]= customers_df["surname"]
customers_df["phone_country_code"] = customers_df["phone"].apply(lambda x: x.get("code") if isinstance(x,dict) else None)
customers_df["phone"] = customers_df["phone"].apply(lambda x: x.get("number") if isinstance(x,dict) else None)
customers_df["customer_type"] = "B2C"
customers_df["document_type"] = customers_df["document"].apply(lambda x: x.get("documentType") if isinstance(x,dict) else None)
customers_df["document_number"] = customers_df["document"].apply(lambda x: x.get("documentNumber") if isinstance(x,dict) else None)
customers_df["mongodb_id"] = customers_df["_id"].apply(lambda x: str(x))
customers_df["created_at"] = customers_df["createdAt"]
customers_df["updated_at"] = customers_df["updatedAt"]

# Create columns from which we have no information
customers_df[["billing_cycle","referral_code","referred_by","payment_type"]] = None

# Get the correct ERD columns
final_customers_columns = ["first_name","last_name","phone_country_code","phone","customer_type",
                           "document_type","document_number","mongodb_id","created_at","updated_at",
                           "referral_code","referred_by","payment_type","email"]

final_customers_df = customers_df[final_customers_columns]


# Coherce dates to string types
final_customers_df = final_customers_df.astype({'created_at':str,'updated_at':str})

final_customers_df[['created_at','updated_at']] = final_customers_df[['created_at','updated_at']].applymap(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S'))

# Coherce dates to string types
final_customers_df = final_customers_df.astype({'created_at':str,'updated_at':str})

# Finishing touches to int columns
final_customers_df["phone"] = final_customers_df["phone"].apply(lambda x: str(x).replace('.0',''))

# =============================================================================
# CUSTOMERS AIRTABLE
# =============================================================================


api_key = os.getenv("API_KEY")
customers_airtable = os.getenv("CUSTOMERS_AIRTABLE")
customer_table_id = os.getenv("CUSTOMER_TABLE_ID")
customers_api = Api(api_key)


customers_from_at = ['name','surname','phone_code','phone_number','email','created','updated_at','document_number','document_type','airtable_customer_id','CHECKBOX']

customers_df_airtable = pd.DataFrame(customers_api.all(customers_airtable,customer_table_id,fields=customers_from_at)).fields.apply(pd.Series)

#Change column names to fit supabase table names
customers_df_airtable.rename(columns={'name':'first_name',
                              'surname':'last_name',
                              'phone_code':'phone_country_code',
                              'phone_number':'phone',
                              'airtable_customer_id':'airtable_id',
                              'created':'created_at'},inplace=True)


customers_df_airtable = customers_df_airtable.astype({"first_name":str, 
                            "last_name":str,
                            "phone_country_code":str,
                            "phone":str,
                            "email":str,
                            "document_number":str,
                            "document_type":str,
                            "airtable_id":str})


customers_df_airtable['phone'] = customers_df_airtable['phone'].str.replace('(','') \
                                                         .str.replace(')','').str.replace('-','').str.replace(' ','')

customers_df_airtable[['document_number','phone_country_code']] = customers_df_airtable[['document_number','phone_country_code']].applymap(lambda x: str(x).replace('.0', ''))


# Definir la función time_format
def time_format(row):
    # date format
    date_format = '%Y-%m-%d %H:%M:%S'

    # get year/month/date portion of the string
    ymd = row.split('T')[0]

    # get hour/minute/second portion of the string
    hms = row.split('T')[1].split('.')[0]

    # concatenate both
    actual_time = ymd + ' ' + hms

    #actual_time_datetime = datetime.strptime(actual_time,date_format)

    return actual_time


customers_df_airtable[['created_at', 'updated_at']] = customers_df_airtable[['created_at', 'updated_at']].apply(lambda x: x.apply(lambda y: time_format(y)))

# Convertir las columnas a datetime
customers_df_airtable[['created_at', 'updated_at']] = customers_df_airtable[['created_at', 'updated_at']].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))


customers_df_airtable = customers_df_airtable.astype({"created_at":str,
                                                      "updated_at":str})


customers_df_airtable = customers_df_airtable.replace('nan',None).replace(np.nan,None)


# =============================================================================
# TAKE ONLY CHECKBOX = True CUSTOMERS
# =============================================================================
customers_df_airtable_duplicados = customers_df_airtable.loc[customers_df_airtable["CHECKBOX"] == True]

customers_df_airtable = customers_df_airtable[~customers_df_airtable['document_number'].isin(customers_df_airtable_duplicados['document_number'])]

customers_df_airtable = pd.concat([customers_df_airtable, customers_df_airtable_duplicados])

# Agrupar y contar por la columna "document_number"
counts_customers = customers_df_airtable.groupby(['document_number']).size().reset_index(name='counts')

# Filtrar los counts mayores que 1 para determinar los duplicados
duplicates_customers = counts_customers[counts_customers['counts'] > 1]

# =============================================================================
# B2B CUSTOMERS
# =============================================================================

customers_df_airtable_B2B = customers_df_airtable.loc[customers_df_airtable['document_type'] == 'NIT']

customers_df_airtable_B2B['customer_type'] = 'B2B'

# =============================================================================
# MERGE TO OBTAIN B2B + B2C CUSTOMERS
# =============================================================================

final_customers_df = final_customers_df.merge(customers_df_airtable[["document_number", "airtable_id"]], on="document_number", how="left")

df_customers_concatenado = pd.concat([customers_df_airtable_B2B, final_customers_df], ignore_index=True).replace('nan',None).replace(np.nan,None)

df_customers_concatenado = df_customers_concatenado.drop('CHECKBOX', axis=1)


# =============================================================================
# Payment type REPLACE
# =============================================================================
df_customers_concatenado["payment_type"] = 'prepaid'

df_customers_concatenado.loc[df_customers_concatenado['customer_type'] == 'B2B', 'payment_type'] = 'postpaid'

# =============================================================================
# create column "manual_invoicing"
# =============================================================================
# create column "manual_invoicing" based on logic

df_customers_concatenado['manual_invoicing'] = np.where(df_customers_concatenado['customer_type'] == 'B2B', '1', '0')

# =============================================================================
# customers TABLE REPLACE
# =============================================================================

# Mapeo de valores de document_type
df_customers_concatenado["document_type"] = df_customers_concatenado["document_type"].map({
    'PAS': 0,
    'CC': 1,
    'CE': 2,
    'NIT': 3,
    'PEP': 4
})

# Mapeo de valores de customer_type
df_customers_concatenado["customer_type"] = df_customers_concatenado["customer_type"].map({
    'B2B': 0,
    'B2C': 1
})

# Mapeo de valores de payment_type
df_customers_concatenado["payment_type"] = df_customers_concatenado["payment_type"].map({
    'prepaid': 0,
    'postpaid': 1
})

df_customers_concatenado = df_customers_concatenado.astype({"document_type":str,
                                                            "customer_type":str,
                                                            "payment_type":str})

columns_to_clean = ["document_type","customer_type","payment_type"]

df_customers_concatenado[columns_to_clean] = df_customers_concatenado[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

df_customers_concatenado = df_customers_concatenado.replace('nan',None).replace(np.nan, None)

# Agrupar y contar por la columna "document_number"
counts = df_customers_concatenado.groupby('document_number').size().reset_index(name='counts')

# Filtrar los counts mayores que 1 para determinar los duplicados
duplicates = counts[counts['counts'] > 1]
