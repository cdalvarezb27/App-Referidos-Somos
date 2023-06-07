## Libraries
import pymongo
import pandas as pd
from pyairtable import Api, Base, Table
import re
import numpy as np
import supabase
from pytz import timezone
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv("credenciales.env")

## Create function
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

## Fetch data
invoices = fetch_mongo_table(table_name="invoices")
bills = fetch_mongo_table(table_name="bills")

bills_copy = bills.copy()
bills_copy = bills_copy[["startDate","billingDate"]]
bills_copy = bills_copy.astype({"startDate":str,
                                "billingDate":str})
bills_copy["new_start_date"] = bills_copy["startDate"].str.slice(8,10) 
bills_copy["new_billing_date"] = bills_copy["billingDate"].str.slice(8,10) 


## take only status = GENERATED invoices
invoices = invoices[invoices['status'] == 'GENERATED']

## Modify and generate final columns
# created_at and updated_at
invoices.rename(columns={'createdAt':'created_at',
                                      'updatedAt':'updated_at'},inplace=True)

invoices = invoices.astype({'created_at':str,
                                  'updated_at':str,
                                  'active':str})


invoices[['created_at','updated_at']] = invoices[['created_at','updated_at']].applymap(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S'))


# mongodb_id
invoices["mongodb_id"] = invoices["_id"].apply(lambda x: str(x))
# payment_method
invoices["payment_method"] = None
# column to get invoice_id
invoices["invoice_mongodb_id"] = invoices["billId"].apply(lambda x: str(x))
# paid_at
invoices["paid_at"] = invoices["invoicingDate"] + " " + invoices["invoicingTime"]
# id
invoices["id"] = range(1, len(invoices)+1)

## Create payments
payment_cols = ["id","invoice_mongodb_id","mongodb_id","payment_method","paid_at","created_at","updated_at"]
payments = invoices[payment_cols]

## Replace 'None','nan','' with None in payments
payments = payments.replace('nan',None).replace(np.nan, None).replace('None',None).replace('',None)
