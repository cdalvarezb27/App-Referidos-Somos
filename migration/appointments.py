import pymongo
import pandas as pd
import numpy as np
from pytz import timezone
from pyairtable import Api, Base, Table
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv("credenciales.env")

# =============================================================================
# ~ FETCH DATA FROM AIRTABLE INSTALLATIONS TABLE
# =============================================================================

api_key = os.getenv("API_KEY")
instalaciones_base_id = os.getenv("INSTALACIONES_BASE_ID")
instalaciones_table_id = os.getenv("INSTALACIONES_TABLE_ID")
instalaciones_api = Api(api_key)


instalaciones_from_at = ['instalaciontiempo','estado_programacion','instalacion_tipo','start','end','instalation_id','suscripciones (from unidad)','Created','Instalador_1_string','Instalador_2_string','lead_id']

instalaciones_base_id_df_airtable = pd.DataFrame(instalaciones_api.all(instalaciones_base_id,instalaciones_table_id,fields=instalaciones_from_at)).fields.apply(pd.Series)

instalaciones_df_copy = instalaciones_base_id_df_airtable.copy()


instalaciones_df_copy.rename(columns={'instalaciontiempo':'installation_date',
                                      'estado_programacion':'status',
                                      'start':'start_at',
                                      'end':'end_at',
                                      'suscripciones (from unidad)':'subscription_id',
                                      'instalation_id':'airtable_id',
                                      'Instalador_1_string':'technician1_id',
                                      'Instalador_2_string':'technician2_id',
                                      'Created':'created_at',
                                      'lead_id':'quote_id'},inplace=True)


instalaciones_df_copy = instalaciones_df_copy.astype({'subscription_id':str,
                                                      'technician1_id':str,
                                                      'technician2_id':str,
                                                      'quote_id':str})


instalaciones_df_copy[["subscription_id", "technician1_id","technician2_id","quote_id"]] = instalaciones_df_copy[["subscription_id", "technician1_id","technician2_id","quote_id"]].applymap(lambda x: str(x).replace("[", "").replace("]", "").replace("'", ""))
# Crear una función que verifica la longitud de la cadena
def verificar_longitud(cadena):
    return len(cadena) < 18

# Aplicar la función al dataframe y crear una máscara booleana
mascara = instalaciones_df_copy['subscription_id'].apply(verificar_longitud)

# Filtrar el dataframe usando la máscara
df_instalaciones_filtrado = instalaciones_df_copy[mascara]

df_instalaciones_filtrado = df_instalaciones_filtrado[(df_instalaciones_filtrado['status'] == 'Terminada') &
                                                      (df_instalaciones_filtrado['instalacion_tipo'].isin(['programado', 'Programada']))]

# Agrupar y contar por la columna "document_number"
counts_instalaciones = df_instalaciones_filtrado.groupby(['subscription_id']).size().reset_index(name='counts')

# Filtrar los counts mayores que 1 para determinar los duplicados
duplicates_instalaciones  = counts_instalaciones[counts_instalaciones['counts'] > 1]

# Ordenar el DataFrame en orden descendente según la columna "installation_date"
df_instalaciones_filtrado = df_instalaciones_filtrado.sort_values(by='installation_date', ascending=False)

df_instalaciones_filtrado = df_instalaciones_filtrado.drop_duplicates(subset='subscription_id', keep='first')

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


df_instalaciones_filtrado[['created_at','installation_date']] = df_instalaciones_filtrado[['created_at','installation_date']].apply(lambda x: x.apply(lambda y: time_format(y)))

# Convertir las columnas a datetime
df_instalaciones_filtrado[['created_at','installation_date']] = df_instalaciones_filtrado[['created_at','installation_date']].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S'))

# Convertir las columnas start_at y end_at al formato deseado
df_instalaciones_filtrado['start_at'] = pd.to_datetime(df_instalaciones_filtrado['start_at'], format='%m/%d/%Y %I:%M %p').dt.strftime('%Y-%m-%d %H:%M:%S')

df_instalaciones_filtrado['end_at'] = pd.to_datetime(df_instalaciones_filtrado['end_at'], format='%m/%d/%Y %I:%M %p').dt.strftime('%Y-%m-%d %H:%M:%S')

df_instalaciones_filtrado = df_instalaciones_filtrado.astype({'created_at':str,
                                                      'installation_date':str,
                                                      'start_at':str,
                                                      'end_at':str})

df_instalaciones_filtrado.drop(['instalacion_tipo'], axis=1, inplace=True)

df_instalaciones_filtrado["appointment_type"] = 'installation'

# Hacer split de las columnas technician2_id e technician1_id
df_instalaciones_filtrado['technician2_id'] = df_instalaciones_filtrado['technician2_id'].str.split(',').str.get(0)
df_instalaciones_filtrado['technician1_id'] = df_instalaciones_filtrado['technician1_id'].str.split(',').str.get(0)

# Hacer filtro sobre la columna quote_id
df_instalaciones_filtrado=df_instalaciones_filtrado[df_instalaciones_filtrado['quote_id'] != 'nan']
df_instalaciones_filtrado = df_instalaciones_filtrado[df_instalaciones_filtrado['quote_id'].str.len() <= 17]

# elimina los technician2_id cuando existen en technician1_id
df_instalaciones_filtrado.loc[df_instalaciones_filtrado['technician2_id'] == df_instalaciones_filtrado['technician1_id'], 'technician2_id'] = None

df_instalaciones_filtrado = df_instalaciones_filtrado.replace('nan',None)

# elimina las rows de appointments si no tienen technician asociado
df_instalaciones_filtrado.dropna(subset=['technician1_id', 'technician2_id'],how='all',inplace=True)

# Mapeo 
df_instalaciones_filtrado["status"] = df_instalaciones_filtrado["status"].map({
    'Terminada': 1
})

df_instalaciones_filtrado["appointment_type"] = df_instalaciones_filtrado["appointment_type"].map({
    'installation': 0
})

columns_to_clean = ["status", "appointment_type"]

df_instalaciones_filtrado[columns_to_clean] = df_instalaciones_filtrado[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

df_instalaciones_filtrado = df_instalaciones_filtrado.replace('nan',None).replace(np.nan, None).replace('None',None).replace('',None)

# =============================================================================
# create updated_at column and replace null values with created_at
# =============================================================================

df_instalaciones_filtrado['updated_at'] = None
df_instalaciones_filtrado['updated_at'] = df_instalaciones_filtrado['updated_at'].fillna(df_instalaciones_filtrado['created_at'])



