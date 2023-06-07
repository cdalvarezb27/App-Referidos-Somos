from pytz import timezone
import psycopg2
import supabase
from pyairtable import Api, Base, Table
import re
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv("credenciales.env")
# =============================================================================
# ~ FETCH DATA FROM AIRTABLE INSTALLATIONS TABLE
# =============================================================================
api_key = os.getenv("API_KEY")
technicians_base_id = os.getenv("TECHNICIANS_BASE_ID")
technicians_table_id = os.getenv("TECHNICIANS_TABLE_ID")
technicians_api = Api(api_key)


technicians_from_at = ['primeros_nombres','apellidos','vinculacion','corre_google','carga','equipo_record_id','nombre_completado']

technicians_df_airtable = pd.DataFrame(technicians_api.all(technicians_base_id,technicians_table_id,fields=technicians_from_at)).fields.apply(pd.Series)

technicians_df_copy = technicians_df_airtable.copy()

# renombrando columnas
technicians_df_copy.rename(columns={'primeros_nombres':'first_name',
                                      'apellidos':'last_name',
                                      'vinculacion':'status',
                                      'corre_google':'email',
                                      'carga':'role',
                                      'equipo_record_id':'airtable_id',
                                      'nombre_completado':'name'},inplace=True)

technicians_df_copy = technicians_df_copy.astype({'airtable_id':str,
                                                  'name':str})

technicians_df_copy = technicians_df_copy[(technicians_df_copy["role"] == 'instalador') | (technicians_df_copy["role"] == 'Técnico de soporte')]
# Mapeo de valores de plan
technicians_df_copy["role"] = technicians_df_copy["role"].map({
    'Técnico de soporte': 0,
    'instalador': 1,

})

technicians_df_copy["status"] = technicians_df_copy["status"].map({
    'Activa': 1,
    'Inactiva': 0
})

columns_to_clean = ["role","status"]

# Crear columnas de created_at, updated_at y id
technicians_df_copy['id'] = range(1, len(technicians_df_copy)+1)
technicians_df_copy['created_at'] = None
technicians_df_copy['updated_at'] = None

technicians_df_copy[columns_to_clean] = technicians_df_copy[columns_to_clean].astype(str).apply(lambda x: x.str.replace('.0', ''))

technicians_df_copy = technicians_df_copy.replace('nan',None).replace(np.nan, None).replace('None',None).replace('',None)


