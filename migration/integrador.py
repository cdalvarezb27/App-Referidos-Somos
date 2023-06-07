import pymongo
import pandas as pd
import numpy as np
from pytz import timezone
import supabase
from pyairtable import Api, Base, Table
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os



from quotes import leads_df
from customers import df_customers_concatenado
from sites import df_sites_merged
from addresses import df_addresses_merge
from subsites import subsite_df_copy
from services import services_copy
from subscriptions import final_subscriptions_df
from technicians import technicians_df_copy
from appointments import df_instalaciones_filtrado
from payments import payments
from strata import df_strata

# Cargar dotenv
load_dotenv("credenciales.env")

# =============================================================================
# ~ # Merge to add quote_id to customers table
# =============================================================================

leads_df_copy_renamed = leads_df[['phone', 'id']].rename(columns={'id': 'quote_id'})

df_customers_concatenado = df_customers_concatenado.merge(leads_df_copy_renamed, on="phone", how="left")

df_customers_concatenado["quote_id"] = df_customers_concatenado["quote_id"].fillna(0).astype(int).astype(str).replace('0', None)

df_customers_concatenado["id"] = range(1, len(df_customers_concatenado) + 1)

df_customers_concatenado = df_customers_concatenado.replace('nan', None).replace(np.nan, None)


# =============================================================================
# ~ # Merge between sites and addresses
# =============================================================================
df_addresess_merge_sites = df_addresses_merge[["id","somos_code"]].rename(columns={'id': 'address_id'})

df_sites_merged = df_sites_merged.merge(df_addresess_merge_sites,on="somos_code",how="left")

df_sites_merged['id'] = range(1, len(df_sites_merged)+1)

df_sites_merged = df_sites_merged.replace('nan',None).replace(np.nan,None)

df_addresses_merge.drop(["airtable_id","somos_code"], axis=1, inplace=True)

# =============================================================================
# ~  Merge between sites and subsites to fetch site_id in subsites table
# =============================================================================

subsite_df_copy = subsite_df_copy.merge(df_sites_merged[["somos_code","id"]], on="somos_code", how="left")

subsite_df_copy = subsite_df_copy[subsite_df_copy['id'].notna()]

subsite_df_copy.rename(columns={ 'id':'site_id'},inplace=True)

subsite_df_copy['id'] = range(1, len(subsite_df_copy)+1)

# =============================================================================
# ~ # Cleaning empty format and drop somos_code column
# =============================================================================

subsite_df_copy = subsite_df_copy.replace(np.nan,None)
subsite_df_copy = subsite_df_copy.replace('None',None)
subsite_df_copy = subsite_df_copy.replace('nan',None)

# drop column
subsite_df_copy = subsite_df_copy.drop(['somos_code'], axis=1)

# change the format of column 'A' from float64 to int64
subsite_df_copy['site_id'] = subsite_df_copy['site_id'].astype('int64')



# =============================================================================
# ~ Complete columns of quotes table
# =============================================================================

df_customers_quotes = df_customers_concatenado[["quote_id","document_type","document_number","customer_type"]]

df_customers_quotes = df_customers_quotes.loc[df_customers_quotes["quote_id"].notna()]

df_customers_quotes["quote_id"] = df_customers_quotes["quote_id"].astype(np.int64)

df_customers_quotes.rename(columns = {'customer_type':'quote_type'}, inplace = True)

# =============================================================================
# ~ FETCH DATA FROM AIRTABLE UNITS TABLE
# =============================================================================
api_key = os.getenv("API_KEY")
units_base_id = os.getenv("UNITS_BASE_ID")
units_table_id = os.getenv("UNITS_TABLE_ID")
units_api = Api(api_key)


units_from_at = ['unit_number','recordId (from Torre [Sync])','lead_id (from cliente) (from suscripciones)']

units_df_airtable = pd.DataFrame(units_api.all(units_base_id,units_table_id,fields=units_from_at)).fields.apply(pd.Series)

units_df_copy = units_df_airtable.copy()

units_df_copy.rename(columns = {'unit_number':'unit_details',
                                 'recordId (from Torre [Sync])':'subsite_airtable_id',
                                 'lead_id (from cliente) (from suscripciones)': 'airtable_id'
                                 }, inplace = True)

units_df_copy = units_df_copy.astype({'subsite_airtable_id':str,
                                      'airtable_id':str})

units_df_copy["subsite_airtable_id"] = units_df_copy["subsite_airtable_id"].str.replace("[","").str.replace("]","").str.replace("'","")
units_df_copy["airtable_id"] = units_df_copy["airtable_id"].str.replace("[","").str.replace("]","").str.replace("'","")


units_df_copy = units_df_copy.replace('nan',None)


# =============================================================================
# ~ # Fetch quotes table from units, customers, sites and subsites
# ============================================================================

# Merge with customers
leads_df = leads_df.merge(df_customers_quotes, left_on = 'id', right_on = 'quote_id', how='left')

# Merge with units
leads_df = leads_df.merge(units_df_copy, on = 'airtable_id', how='left')

# Merge with site
df_sites_merged_quotes = df_sites_merged[["somos_code","id"]]

df_sites_merged_quotes = df_sites_merged_quotes.rename(columns={"id":"site_id"})

leads_df = leads_df.merge(df_sites_merged_quotes, on = 'somos_code', how='left')

leads_df["site_id"] = leads_df["site_id"].apply(lambda x: str(x).replace('.0',''))


# Merge with subsites 
subsite_df_copy_quotes = subsite_df_copy[["airtable_id","id"]]

subsite_df_copy_quotes = subsite_df_copy_quotes.rename(columns={"airtable_id":"subsite_airtable_id",
                                                                "id":"subsite_id"})

leads_df = leads_df.merge(subsite_df_copy_quotes, on = 'subsite_airtable_id', how='left')

# drop  columns of leads
leads_df = leads_df.drop(['subsite_airtable_id', 'somos_code', 'quote_id'], axis=1)

# Replace nan with None

leads_df = leads_df.copy()

leads_df = leads_df.astype({"quote_type":str,
                            "document_type":str})

leads_df["subsite_id"] = leads_df["subsite_id"].astype(str)

leads_df["subsite_id"] = leads_df["subsite_id"].astype(str).str.rstrip('.0')

columns_to_clean = ["quote_type","document_type"]

leads_df[columns_to_clean] = leads_df[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

leads_df = leads_df.replace('nan',None).replace(np.nan, None).replace('',None).replace('None',None)

leads_df = leads_df.drop_duplicates(subset=['id'])


# =============================================================================
# ~ # Fetch subscriptions table from units, customers, subsites and price
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

# Fetch data from units to gather unit details
units_df = fetch_mongo_table(table_name="units")
units_df["_id"] = units_df["_id"].apply(lambda x: str(x)) # coherce id into string format

# Fetch unit details from unit
final_subscriptions_df['unit_details'] = final_subscriptions_df['unit_id'].apply(
    lambda x: units_df.loc[units_df['_id']==x, 'detail'].iloc[0]
)

# drop unit id
final_subscriptions_df.drop(columns=['unit_id'],inplace=True)

# Get foreign key (customer_id) relation from customer
final_subscriptions_df["customer_id"] = final_subscriptions_df["mongo_customer_id"].apply(lambda x:df_customers_concatenado.loc[df_customers_concatenado["mongodb_id"]==x,"id"].iloc[0])
    
# Drop mongo customer id
final_subscriptions_df.drop(columns='mongo_customer_id',inplace=True)

################################### getting subsite_id by using airtable_id from subscriptions table

final_subscriptions_df = final_subscriptions_df.merge(subsite_df_copy[['airtable_id','id']],left_on='subsite_id_subscriptions',right_on='airtable_id',how='left')

final_subscriptions_df.rename(columns={"airtable_id_x":"airtable_id"},inplace=True)

final_subscriptions_df["subsite_id"] = final_subscriptions_df["id"]

final_subscriptions_df.drop(columns=['airtable_id_y','id','subsite_id_subscriptions'],inplace=True)


final_subscriptions_df["subsite_id"] = final_subscriptions_df["subsite_id"].apply(lambda x: str(x).replace('.0',''))

################################################### Merge between subscriptions and services

services_copy_2 = services_copy[["mongodb_id",'id','price_cents']]

services_copy_2.rename(columns={'mongodb_id':'service_id',
                                'id':'service_id_2'},inplace=True)

final_subscriptions_df = final_subscriptions_df.merge(services_copy_2, on='service_id', how='left')

final_subscriptions_df.drop("service_id", axis=1, inplace=True)

final_subscriptions_df.rename(columns={'service_id_2':'service_id'},inplace=True)

# add incremental unique identifier
final_subscriptions_df["id"] = range(1, len(final_subscriptions_df)+1)


final_subscriptions_df = final_subscriptions_df.replace('nan',None).replace(np.nan, None).replace('',None).replace('None',None)

# =============================================================================
#  create services_strata table
# =============================================================================
df_services_strata = services_copy[["id","created_at","updated_at","stratum"]]

# transform the column "stratum" in list to split
df_services_strata['stratum'] = df_services_strata['stratum'].apply(lambda x: str(x).split(';') if ';' in str(x) else [str(x)])

# Duplicate the rows based on stratum column
df_services_strata = df_services_strata.explode('stratum')

# rename columns
df_services_strata.rename(columns={"id":"service_id",
                                   "stratum":"stratum_id"},inplace=True)

# create the id column
df_services_strata["id"] = range(1, len(df_services_strata)+1)


# =============================================================================
#  Drop stratum column in services table
# =============================================================================
services_copy.drop("stratum",axis=1,inplace=True) 
# =============================================================================
#  Fill subsite_id to subscriptions with subsite_id = None
# =============================================================================

ultimo_id = str(subsite_df_copy['id'].iloc[-1])  # Obtener el último ID del dataframe subsites_df_copy

final_subscriptions_df['subsite_id'] = final_subscriptions_df['subsite_id'].fillna(ultimo_id)

# =============================================================================
#  add to subscriptions tax 0 or 0.19 depending on stratum
# =============================================================================
# copy of the dataframes
copy_subsites = subsite_df_copy.copy()
copy_sites = df_sites_merged.copy()
# rename and change format of subsite_id
copy_subsites.rename(columns={'id':'subsite_id'},inplace=True)
copy_subsites = copy_subsites.astype({"subsite_id":str})
# Merge between subscriptions and subsite to extract site_id
final_subscriptions_df = final_subscriptions_df.merge(copy_subsites[["subsite_id","site_id"]],on = 'subsite_id',how='left')
# rename site_id
copy_sites.rename(columns={'id':'site_id'},inplace=True)
# Merge between subscriptions and sites to extract stratum
final_subscriptions_df = final_subscriptions_df.merge(copy_sites[["site_id","stratum_id"]],on = 'site_id',how='left')

# function to apply the logic
def assign_tax(row):
    if row['stratum_id'] in ['1', '2', '3']:
        return 0
    elif row['stratum_id'] in ['4', '5', '6']:
        return 0.19

# create new column tax based on stratum
final_subscriptions_df['tax'] = final_subscriptions_df.apply(lambda row: assign_tax(row), axis=1)
# drop stratum and site_id columns
final_subscriptions_df.drop(["stratum_id","site_id"], axis=1, inplace=True)

# =============================================================================
#  Create subtotal_value column 
# =============================================================================
# function to apply the logic
def calculate_subtotal(row):
    if row['tax'] == 0.0:
        return row['price_cents']
    elif row['tax'] == 0.19:
        return row['price_cents'] * 0.81

# Apply the logic to create "subtotal_value" column
final_subscriptions_df['subtotal_value_cents'] = final_subscriptions_df.apply(lambda row: calculate_subtotal(row), axis=1)


# =============================================================================
#  Create cutoff_date 
# =============================================================================
import calendar

# Get the current date
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
# Create a function to calculate the cutoff_date
def calculate_cutoff_date(row):
    cutoff_day = int(row['cutoff_day'])
    _, last_day = calendar.monthrange(today.year, today.month)
    
    if today.day < cutoff_day <= last_day:
        cutoff_date = today.replace(day=cutoff_day)
    else:
        next_month = today.replace(day=28) + timedelta(days=4)
        _, last_day_next_month = calendar.monthrange(next_month.year, next_month.month)
        cutoff_date = next_month.replace(day=min(cutoff_day, last_day_next_month))
    
    return cutoff_date

# Apply the function to create the cutoff_date column
final_subscriptions_df.loc[final_subscriptions_df['status'] == '1', 'cutoff_date'] = final_subscriptions_df.loc[final_subscriptions_df['status'] == '1'].apply(calculate_cutoff_date, axis=1)

# Convert cutoff_date to string
final_subscriptions_df = final_subscriptions_df.astype({"cutoff_date": str})

# Replace 'NaT' with None
final_subscriptions_df["cutoff_date"] = final_subscriptions_df["cutoff_date"].replace('NaT', None)

'''
from datetime import datetime, timedelta

# Get the current date
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# Create a function to calculate the cutoff_date
def calculate_cutoff_date(row):
    cutoff_day = int(row['cutoff_day'])
    if today.day < cutoff_day:
        cutoff_date = today.replace(day=cutoff_day)
    else:
        next_month = today.replace(day=28) + timedelta(days=4)
        cutoff_date = next_month.replace(day=cutoff_day)
    return cutoff_date

# Apply the function to create the cutoff_date column
final_subscriptions_df.loc[final_subscriptions_df['status'] == '1', 'cutoff_date'] = final_subscriptions_df.loc[final_subscriptions_df['status'] == '1'].apply(calculate_cutoff_date, axis=1)

final_subscriptions_df = final_subscriptions_df.astype({"cutoff_date":str})

final_subscriptions_df["cutoff_date"] = final_subscriptions_df["cutoff_date"].replace('NaT',None)
'''
# =============================================================================
#  Create invoices table
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

# Fecth invoices data
bills_df_mongo = fetch_mongo_table(table_name="bills")

bills_df_mongo = bills_df_mongo[['_id','subscriptionId','description','total','taxes','billingDate','createdAt','updatedAt','success','verifiedInDian']]

bills_df_mongo["billingDate"] = bills_df_mongo["billingDate"]-pd.Timedelta(days=10) 

# Fecth invoices data
invoices_df_mongo = fetch_mongo_table(table_name="invoices")

invoices_df_mongo = invoices_df_mongo[['billId','prefix','number','invoiceAssets','cufe','invoicingDate','status']]

# Take only GENERATED invoices
invoices_df_mongo = invoices_df_mongo[invoices_df_mongo['status'] == 'GENERATED']

# Drop status column 
invoices_df_mongo.drop(["status"], axis=1, inplace=True)

# Create new column of status with value = 1 if cufe != nan and 0 if cufe = nan

invoices_df_mongo["status"] = invoices_df_mongo["cufe"].apply(lambda x: 0 if pd.isna(x) else 1)

# Join between invoices_df_mongo and bills_df_mongo to obtain all collumns
invoices_df = bills_df_mongo.merge(invoices_df_mongo,left_on="_id",right_on="billId",how="left")

invoices_df_copy = invoices_df.copy()

# Extract pdf and xml data from invoiceAssets
invoices_df["pdf"] = invoices_df_copy["invoiceAssets"].apply(lambda x: x.get('pdfUrl') if isinstance(x, dict) else None)
invoices_df["xml"] = invoices_df_copy["invoiceAssets"].apply(lambda x: x.get('xmlUrl')if isinstance(x, dict) else None)

#extract
invoices_df["success"] = invoices_df_copy["verifiedInDian"].apply(lambda x: x.get('success')if isinstance(x, dict) else None)
invoices_df["verified_by"] = invoices_df_copy["verifiedInDian"].apply(lambda x: x.get('verifiedBy')if isinstance(x, dict) else None)



#drop columns, clean column names

invoices_df.drop(["invoiceAssets", "billId"], axis=1, inplace=True)

invoices_df["status"] = invoices_df["status"].replace(np.nan,0)

invoices_df.rename(columns={'_id':'mongodb_id',
                           'subscriptionId':'subscription_mongodb_id',
                           'billingDate':'billing_date',
                           'number':'consecutive',
                           'createdAt':'created_at',
                           'updatedAt':'updated_at',
                           'total':'total_price_cents',
                           'taxes':'taxes_cents',
                           'invoicingDate':'sent_to_dian_at'},inplace=True)
# total_price

# create the send_to_dian column  
#invoices_df["send_to_dian"] = invoices_df["status"].apply(lambda x: 1 if x == 1 else 0)

# add verified_by column
invoices_df['verified_by'] = np.where(invoices_df['status'] == 1, 'mongodb', None)

# Generate invoice id
invoices_df["id"] = range(1, len(invoices_df)+1)

invoices_df.astype({"mongodb_id":str})

subs_invoices_copy = final_subscriptions_df[["mongodb_id","customer_id"]]

subs_invoices_copy.rename(columns={'mongodb_id':'subscription_mongodb_id'},inplace=True)

# Clean nan values
invoices_df= invoices_df.replace(np.nan,None)
invoices_df= invoices_df.replace("nan",None)

invoices_df = invoices_df.applymap(str)
subs_invoices_copy = subs_invoices_copy.applymap(str)

#ALTERNATIVE
'''
invoices_df["subscription_mongodb_id"] = invoices_df["subscription_mongodb_id"].astype(str)
subs_invoices_copy["subscription_mongodb_id"] = subs_invoices_copy["subscription_mongodb_id"].astype(str)
'''
# Merge between invoices and subscriptions to extract customer_id
invoices_df_final = invoices_df.merge(subs_invoices_copy, on='subscription_mongodb_id',how='left')

invoices_df_final = invoices_df_final.drop_duplicates(subset=["id"])

# Data wrangling

invoices_df_final["consecutive"] = invoices_df_final["consecutive"].apply(lambda x: str(x).replace('.0',''))

invoices_df_final["status"] = invoices_df_final["status"].apply(lambda x: str(x).replace('.0',''))


# drop column subscription_mongodb_id
invoices_df_final.drop(['subscription_mongodb_id'], axis=1, inplace=True)

invoices_df_final= invoices_df_final.replace("None",None)

# convert created_at and updated_at format to %Y-%m-%d %H:%M:%S
invoices_df_final['created_at'] = pd.to_datetime(invoices_df_final['created_at'])
invoices_df_final['created_at'] = invoices_df_final['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

invoices_df_final['updated_at'] = pd.to_datetime(invoices_df_final['updated_at'])
invoices_df_final['updated_at'] = invoices_df_final['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

invoices_df_final['sent_to_dian_at'] = pd.to_datetime(invoices_df_final['sent_to_dian_at'])
invoices_df_final['sent_to_dian_at'] = invoices_df_final['sent_to_dian_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

invoices_df_final[["description","prefix","consecutive","cufe","sent_to_dian_at","pdf","xml"]] = invoices_df_final[["description","prefix","consecutive","cufe","sent_to_dian_at","pdf","xml"]].replace("None",None).replace("nan",None).replace("",None).replace(np.nan,None)

invoices_df_final['taxes_cents'] = invoices_df_final['taxes_cents'].astype(int)
invoices_df_final["total_price_cents"] = invoices_df_final["total_price_cents"].astype(float)
invoices_df_final["total_price_cents"] = invoices_df_final["total_price_cents"].astype(int)

# change total_price and taxes to cents
invoices_df_final['taxes_cents'] = invoices_df_final['taxes_cents'].apply(lambda x: x * 100)
invoices_df_final['total_price_cents'] = invoices_df_final['total_price_cents'].apply(lambda x: x * 100)

# =============================================================================
#  Create subscriptions_invoices table
# =============================================================================


subscriptions_invoices = invoices_df[["id","subscription_mongodb_id","created_at","updated_at"]]

subscriptions_invoices.rename(columns={'id':'invoice_id'},inplace=True)


# Generate subscription_invoice id
# 

subs_invoices_copy_2 = final_subscriptions_df[["mongodb_id","id"]]

subs_invoices_copy_2.rename(columns={'mongodb_id':'subscription_mongodb_id',
                                     'id':'subscription_id'},inplace=True)

# Clean nan values
subscriptions_invoices= subscriptions_invoices.replace(np.nan,None)
subscriptions_invoices= subscriptions_invoices.replace("nan",None)

subscriptions_invoices = subscriptions_invoices.applymap(str)
subs_invoices_copy_2 = subs_invoices_copy_2.applymap(str)

def convert_to_utc(row,name):
    if isinstance(row[name],str) and row[name]!="None":

        # Create a datetime object from the string in Colombian time
        dt_col = pd.to_datetime(row[name])

        # Set the timezone to Colombian time
        colombian_tz = timezone('America/Bogota')
        dt_col_tz = dt_col.tz_localize(colombian_tz)

        # Convert the timezone to UTC
        return dt_col_tz.tz_convert('UTC')

    elif row[name]=="None":
        return None
        
    else:
        return None

# Merge between subscriptions_invoices and subscriptions to extract subscription_id
subscriptions_invoices_final = subscriptions_invoices.merge(subs_invoices_copy_2, on='subscription_mongodb_id',how='left')

subscriptions_invoices_final['updated_at'] = subscriptions_invoices_final.apply(lambda row: convert_to_utc(row,'updated_at'), axis=1)
subscriptions_invoices_final['created_at']   = subscriptions_invoices_final.apply(lambda row: convert_to_utc(row,'created_at'), axis=1)

#subscriptions_invoices_final = subscriptions_invoices_final.drop_duplicates(subset=["id"])

subscriptions_invoices_final = subscriptions_invoices_final.applymap(str)
# drop column subscription_mongodb_id
subscriptions_invoices_final.drop(['subscription_mongodb_id'], axis=1, inplace=True)

subscriptions_invoices_final= subscriptions_invoices_final.replace("None",None)

# convert created_at and updated_at format to %Y-%m-%d %H:%M:%S
subscriptions_invoices_final['created_at'] = pd.to_datetime(subscriptions_invoices_final['created_at'])
subscriptions_invoices_final['created_at'] = subscriptions_invoices_final['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

subscriptions_invoices_final['updated_at'] = pd.to_datetime(subscriptions_invoices_final['updated_at'])
subscriptions_invoices_final['updated_at'] = subscriptions_invoices_final['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

subscriptions_invoices_final["id"] = range(1, len(subscriptions_invoices_final)+1)

filtro = (final_subscriptions_df["id"] == 12666) | (final_subscriptions_df["id"] == 12665)
resultados = final_subscriptions_df[filtro]
# =============================================================================
# ~ Merge between payments and invoices to extract invoice_id in payments
# =============================================================================

copy_invoices = invoices_df_final.copy()
copy_invoices.rename(columns={'id':'invoice_id',
                              'mongodb_id':'invoice_mongodb_id'},inplace=True)
payments = payments.merge(copy_invoices[["invoice_mongodb_id","invoice_id"]],on='invoice_mongodb_id', how='left')

payments.drop_duplicates(subset='invoice_id', inplace=True)

payments.drop('invoice_mongodb_id', axis=1, inplace=True)

# =============================================================================
# ~ Merge between technicians and appointments
# =============================================================================
# Installer 1
df_appointments = df_instalaciones_filtrado.merge(technicians_df_copy[["name","id"]],left_on='technician1_id', right_on='name',how='left')

df_appointments.drop(["technician1_id","name"], axis=1, inplace=True)

df_appointments.rename(columns={'id':'technician1_id'},inplace=True)


# Installer 2
df_appointments = df_appointments.merge(technicians_df_copy[["name","id"]],left_on='technician2_id', right_on='name',how='left')

df_appointments.drop(["technician2_id","name"], axis=1, inplace=True)

df_appointments.rename(columns={'id':'technician2_id'},inplace=True)

# =============================================================================
# ~ Creating ID for df_appointments table and cleaning format in technician1_id and technician2_id
# =============================================================================

df_appointments = df_appointments.astype({"technician1_id":str,
                                      "technician2_id":str})

columns_to_clean = ["technician1_id","technician2_id"]

df_appointments[columns_to_clean] = df_appointments[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

df_appointments = df_appointments.replace('nan',None).replace(np.nan, None).replace('',None)

# elimina las rows de appointments si no tienen technician asociado
df_appointments.dropna(subset=['technician1_id', 'technician2_id'],how='all',inplace=True)

df_appointments['id'] = range(1, len(df_appointments)+1)

# =============================================================================
# ~ Technicians drop column "name" and create column "updated_at"
# =============================================================================
technicians_df_copy.drop("name", axis=1, inplace=True)

now = datetime.now()

formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")

technicians_df_copy["updated_at"] = formatted_datetime

technicians_df_copy["created_at"] = formatted_datetime
# =============================================================================
# ~ create the appointments_technicians table
# =============================================================================
'''
df_appointments_technicians['technician_id'] = df_appointments_technicians[['technician1_id', 'technician2_id']].apply(lambda x: ', '.join(filter(None, x)), axis=1)
'''
df_appointments_technicians = df_appointments[["id","technician1_id","technician2_id","created_at","updated_at"]]

# Crear el nuevo dataframe con la columna "technicians"
df_appointments_technicians = df_appointments_technicians.melt(id_vars=['id', 'created_at', 'updated_at'], value_vars=['technician1_id', 'technician2_id'], var_name='technicians', value_name='technician_id')

# Eliminar las filas donde technician_id es None
df_appointments_technicians = df_appointments_technicians.dropna(subset=['technician_id'])

# Ordenar el dataframe por ID
df_appointments_technicians = df_appointments_technicians.sort_values('id')

# Restablecer el índice
df_appointments_technicians = df_appointments_technicians.reset_index(drop=True)

df_appointments_technicians.rename(columns={"id":"appointment_id"},inplace=True)

df_appointments_technicians = df_appointments_technicians.drop("technicians",axis=1)

df_appointments_technicians['id'] = range(1, len(df_appointments_technicians)+1)

# =============================================================================
# ~ drop technician1_id and technician2_id
# =============================================================================
df_appointments = df_appointments.drop(["technician1_id","technician2_id"],axis=1)
# =============================================================================
# ~ Merge subscriptions + appointments
# =============================================================================
final_subscriptions_df = final_subscriptions_df.merge(df_appointments[["id","subscription_id"]],left_on='airtable_id',right_on='subscription_id',how='left')

final_subscriptions_df.rename(columns={'id_x':'id'},inplace=True)

final_subscriptions_df['appointment_id'] = final_subscriptions_df['id_y']

final_subscriptions_df.drop(["id_y","subscription_id"], axis=1, inplace=True)

final_subscriptions_df = final_subscriptions_df.astype({"appointment_id":str})

final_subscriptions_df["appointment_id"] = final_subscriptions_df["appointment_id"].astype(str).replace('\.0$', '', regex=True)

final_subscriptions_df = final_subscriptions_df.replace('nan',None).replace(np.nan, None).replace('',None)


# =============================================================================
# ~ Merge quotes + appointments
# =============================================================================

df_appointments = df_appointments.merge(leads_df[["id","airtable_id"]],left_on='quote_id',right_on='airtable_id',how='left')

df_appointments.rename(columns={'id_x':'id',
                                       'airtable_id_x':'airtable_id'},inplace=True)

df_appointments["quote_id"] = df_appointments["id_y"]

df_appointments.drop(["id_y","airtable_id_y"], axis=1, inplace=True)

df_appointments = df_appointments.astype({"quote_id":str})

df_appointments["quote_id"] = df_appointments["quote_id"].astype(str).replace('\.0$', '', regex=True)

df_appointments = df_appointments.replace('nan',None).replace(np.nan, None).replace('',None)

# =============================================================================
# ~ drop unless column in df_appointments
# =============================================================================
df_appointments.drop("subscription_id", axis=1, inplace=True)

df_quotes = leads_df.copy()
df_customers = df_customers_concatenado.copy()
df_subsites = subsite_df_copy.copy()
df_sites = df_sites_merged.copy()
df_addresses = df_addresses_merge.copy()
df_strata_2 = df_strata.copy()
df_services = services_copy.copy()
df_services_strata_2 = df_services_strata.copy()
df_technicians = technicians_df_copy.copy()
df_appointments_2 = df_appointments.copy()
df_appointments_technicians_2 = df_appointments_technicians.copy()
df_subscriptions = final_subscriptions_df.copy()
df_invoices = invoices_df_final.copy()
df_subscriptions_invoices = subscriptions_invoices_final.copy()
payments_2 = payments.copy()

'''
# =============================================================================
# ~ Initialize supabase credentials and client
# =============================================================================
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
client = supabase.create_client(supabase_url, supabase_key)


# =============================================================================
# ~  INSERT DATA INTO TABLES
# =============================================================================

def insert_df_to_table(df, table_name):
    rows = df.reset_index(drop=True).to_dict('records')
    inserts = []
    for i in range(len(rows)):
        inserts.append(rows[i])
        if len(inserts) == 1000:
            result = client.table(table_name).insert(inserts).execute()
            assert len(result.data) > 0
            inserts = []
    if len(inserts) > 0:
        result = client.table(table_name).insert(inserts).execute()
        assert len(result.data) > 0

# Insert data into tables
insert_df_to_table(df_addresses_merge, 'addresses')
insert_df_to_table(df_sites_merged, 'sites')
insert_df_to_table(subsite_df_copy, 'subsites')
insert_df_to_table(leads_df, 'quotes')
insert_df_to_table(services_copy, 'services')
insert_df_to_table(df_customers_concatenado, 'customers')
insert_df_to_table(technicians_df_copy, 'technicians')
insert_df_to_table(df_appointments, 'appointments')
insert_df_to_table(final_subscriptions_df, 'subscriptions')
insert_df_to_table(invoices_df_final, 'invoices')
insert_df_to_table(subscriptions_invoices_final, 'subscriptions_invoices')
insert_df_to_table(payments, 'payments')
'''

