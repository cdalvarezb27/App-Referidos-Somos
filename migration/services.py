import pymongo
import pandas as pd
import numpy as np
from pyairtable import Api, Base, Table
from datetime import datetime
#from subscriptions import final_subscriptions_df
from dotenv import load_dotenv
import os
import json


load_dotenv("credenciales.env")
# =============================================================================
# ~ # Fetch data from services table
# ============================================================================

data_types = {
    'status': str,
    'name': str,
    'max_transfer_rate': int,
    'code':str,
    'description':str,
    'features':str,
    'category':str,
    'stratum':str,
    'price_cents':int
}
# create services_df
services_df = pd.read_excel("services.xlsx", dtype=data_types)

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

#unique_services_df = final_subscriptions_df['service_id'].drop_duplicates().to_frame()

services_df_mongodb = fetch_mongo_table(table_name="services") 

services_df_mongodb = services_df_mongodb[["_id","code","createdAt","updatedAt"]]

services_df_mongodb.rename(columns={'_id':'mongodb_id',
                                      'createdAt':'created_at',
                                      'updatedAt':'updated_at'},inplace=True)

services_df_mongodb = services_df_mongodb.astype({'mongodb_id':str,
                                                  'created_at':str,
                                                  'updated_at':str})

services_df_mongodb[['created_at','updated_at']] = services_df_mongodb[['created_at','updated_at']].applymap(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S'))

# join between services excel and services from mongo
services_copy = services_df.merge(services_df_mongodb,on = 'code',how='left')

services_copy["created_at"] = services_copy["created_at"].fillna("2023-06-02 00:00:00")
services_copy["updated_at"] = services_copy["updated_at"].fillna("2023-06-02 00:00:00")


services_copy["active"] = services_copy["active"].map({
    'active': 1,
    'inactive': 0
})

# Mapeo de valores de category
services_copy["category"] = services_copy["category"].map({
    'device': 0,
    'tv': 1,
    'internet': 2
})


columns_to_clean = ["active","category"]

services_copy[columns_to_clean] = services_copy[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

services_copy = services_copy.replace('nan',None).replace(np.nan, None)

