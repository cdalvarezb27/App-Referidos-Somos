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
# CUSTOMERS AIRTABLE
# =============================================================================


api_key = os.getenv("API_KEY")
customers_airtable = os.getenv("CUSTOMERS_AIRTABLE")
customer_table_id = os.getenv("CUSTOMER_TABLE_ID")
customers_api = Api(api_key)


customers_from_at = ['name','surname','phone_code','phone_number','email','created','updated_at','document_number','document_type','airtable_customer_id','CHECKBOX','suscripcion_status']

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
                            "airtable_id":str,
                            "suscripcion_status":str})


customers_df_airtable['phone'] = customers_df_airtable['phone'].str.replace('(','') \
                                                         .str.replace(')','').str.replace('-','').str.replace(' ','')

customers_df_airtable['suscripcion_status'] = customers_df_airtable['suscripcion_status'].str.replace("[","").str.replace("]","").str.replace("]","").str.replace("'","")

customers_df_airtable[['document_number','phone_country_code']] = customers_df_airtable[['document_number','phone_country_code']].applymap(lambda x: str(x).replace('.0', ''))

customers_df_airtable = customers_df_airtable[(customers_df_airtable["document_type"] == "PPT") & (customers_df_airtable["suscripcion_status"] == "Activo")]

customers_df_airtable = customers_df_airtable.drop(["suscripcion_status", "CHECKBOX"], axis=1)

customers_df_airtable['document_type'] = customers_df_airtable['document_type'].replace('PPT', '5')

customers_df_airtable['manual_invoicing'] =  '0'

customers_df_airtable['payment_type'] =  '0'

customers_df_airtable['customer_type'] =  '1'

customers_df_airtable.replace()

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


conn = psycopg2.connect(
    host="ec2-3-234-204-26.compute-1.amazonaws.com",
    database="d9gg37ud6998ra",
    user="pkgplptxcoasph",
    password="f1044c330298ce191e58dc67765ec63e582563e489d9fd75b2e8dad3ce85bb79"
)

cursor = conn.cursor()

cursor.execute("""
SELECT *
FROM quotes
""")

rows = cursor.fetchall()

df_quotes = pd.DataFrame(rows)

column_names = [desc[0] for desc in cursor.description]
df_quotes.columns = column_names

conn.close()

customers_df_airtable = customers_df_airtable.merge(df_quotes[["id","phone"]],on='phone',how='left')

customers_df_airtable['id'] = customers_df_airtable['id'].astype(str).apply(lambda x: x.replace('.0', ''))

customers_df_airtable['id'] = customers_df_airtable['id'].replace('nan', None)

customers_df_airtable.rename(columns={'id':'quote_id'},inplace=True)


from sqlalchemy import create_engine

engine = create_engine('Your Database connection')

customers_df_airtable.to_sql('customers', con=engine, if_exists='append', index=False)
