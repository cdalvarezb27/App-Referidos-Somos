from pyairtable import Api, Base, Table
import json
import pandas as pd
from datetime import datetime
import numpy as np



# AQUÍ ESTÁN LOS REFERIDOS


# Se extraen los datos de AirTable, LEADS
def TableDF_Leads(api_key: str, base_id: str, table: str):

    columns = {}
    table = Table('keyufJoR9jMxHMJ19', 'appCOsc9IIngeHhwO', 'Leads')
    table_data = table.all()
    if len(table_data) > 0:
 
        columns = {col: [] for col in table_data[0]["fields"].keys()}
        entries = [{col: row["fields"][col] for col in row["fields"].keys()} for row in table_data]
        df = pd.DataFrame(entries)
        return df
    else:
        return None

# Devuelve los datos extraídos como DataFrame
df = TableDF_Leads('keyufJoR9jMxHMJ19', 'appCOsc9IIngeHhwO', 'Leads')

# FILTROS AL DATAFRAME DE LEADS

df['Created'] = pd.to_datetime(df['Created'])

df_2 = df.loc[df['utm_source'] == 'referidos']

#df_2 = df_2.loc[df_2['Created'] > '2023-02-06']

df_2 = df_2[df_2['codigo_referidos'].notnull()]


df_2 = df_2[["phone","Name", "Created", "codigo_referidos","state_stepper (from Site)"]]

# Renombrar columnas de un dataframe:
df_2.rename({
             'phone': 'phone_referido','Name': 'Name_referido',
             'Created': 'Created_referido'}, axis=1, inplace=True)


# AQUÍ ESTÁN LOS QUE REFIEREN


# Se extraen los datos de AirTable
def TableDF_Mark2(api_key: str, base_id: str, table: str):

    columns = {}
    table = Table('keyufJoR9jMxHMJ19', 'appDdtDE2hIHGnU8k', 'Referidos')
    table_data = table.all()
    if len(table_data) > 0:
 
        columns = {col: [] for col in table_data[0]["fields"].keys()}
        entries = [{col: row["fields"][col] for col in row["fields"].keys()} for row in table_data]
        df = pd.DataFrame(entries)
        return df
    else:
        return None

# Devuelve los datos extraídos como DataFrame
df_referidores = TableDF_Mark2('keyufJoR9jMxHMJ19', 'appDdtDE2hIHGnU8k', 'Referidos')

df_referidores = df_referidores[["nombre_del_cliente","email","code","phone_number"]]

df_referidores.rename({'nombre_del_cliente': 'nombre_del_cliente_referidor','email': 'email_referidor',
                       'code':'code_referidor','phone_number':'phone_number_referidor'}, axis=1, inplace=True)


# AQUÍ EL MERGE ENTRE REFERIDOS Y LOS QUE REFIEREN



referidos_referidores = pd.merge(df_referidores, df_2, left_on='code_referidor',right_on='codigo_referidos', how='inner')


# Se extraen los datos de AirTable
def TableDF_Mark2_customers(api_key: str, base_id: str, table: str):

    columns = {}
    table = Table('keyufJoR9jMxHMJ19', 'appDdtDE2hIHGnU8k', 'customers')
    table_data = table.all()
    if len(table_data) > 0:
 
        columns = {col: [] for col in table_data[0]["fields"].keys()}
        entries = [{col: row["fields"][col] for col in row["fields"].keys()} for row in table_data]
        df = pd.DataFrame(entries)
        return df
    else:
        return None

# Devuelve los datos extraídos como DataFrame
df_customers = TableDF_Mark2_customers('keyufJoR9jMxHMJ19', 'appDdtDE2hIHGnU8k', 'customers')

# Instal valid

df_customers_2 = df_customers[["customer_phone","instalaciones_tiempo_kustomer","instal_valid (from suscripciones)","suscripcion_status"]]


df_customers_2['instalaciones_tiempo_kustomer'] = df_customers_2['instalaciones_tiempo_kustomer'].str.split(',').str[-1]


# Eliminamos los espacios vacíos de los registros en la columna
df_customers_2['instalaciones_tiempo_kustomer'] = df_customers_2['instalaciones_tiempo_kustomer'].str.strip()


df_customers_2['customer_phone'] = df_customers_2['customer_phone'].str.replace('\+57', '')


df_referidos_referidores_instal = pd.merge(referidos_referidores, df_customers_2, left_on='phone_referido',right_on='customer_phone', how='left')



df_referidos_referidores_instal_copy = df_referidos_referidores_instal.copy()


cols_to_convert = ["instal_valid (from suscripciones)","suscripcion_status","state_stepper (from Site)"]

df_referidos_referidores_instal_copy[cols_to_convert] = df_referidos_referidores_instal_copy[cols_to_convert].apply(lambda x: x.astype(str))

for col in cols_to_convert:
    df_referidos_referidores_instal_copy[col] = df_referidos_referidores_instal_copy[col].str.replace('[','').str.replace(']','').str.replace("'","")


#df_referidos_referidores_instal_copy=df_referidos_referidores_instal_copy.loc[df_referidos_referidores_instal_copy['instal_valid (from suscripciones)'] == 'completa']


# Convertir la columna de fecha a tipo datetime
df_referidos_referidores_instal_copy['instalaciones_tiempo_kustomer'] = pd.to_datetime(df_referidos_referidores_instal_copy['instalaciones_tiempo_kustomer'])

#df_referidos_referidores_instal_copy['Created_referido'] = pd.to_datetime(df_referidos_referidores_instal_copy['Created_referido'])

df_referidos_referidores_instal_copy['Created_referido'] = df_referidos_referidores_instal_copy['Created_referido'].dt.tz_convert(None).dt.strftime('%Y-%m-%d')

df_referidos_referidores_instal_copy['instalaciones_tiempo_kustomer'] = df_referidos_referidores_instal_copy['instalaciones_tiempo_kustomer'].dt.strftime('%Y-%m-%d')


df_referidos_referidores_instal_copy = df_referidos_referidores_instal_copy[['nombre_del_cliente_referidor', 'email_referidor',
       'phone_number_referidor','Created_referido',  'Name_referido', 'phone_referido',
       'codigo_referidos', 'instalaciones_tiempo_kustomer',
       'instal_valid (from suscripciones)', 'suscripcion_status','state_stepper (from Site)']]


writer = pd.ExcelWriter('referidos.xlsx', engine='xlsxwriter')
df_referidos_referidores_instal_copy.to_excel(writer, index=False)
writer.save()


