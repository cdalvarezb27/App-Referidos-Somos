
import pandas as pd
from pytz import timezone
from pyairtable import Api, Base, Table
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv("credenciales.env")

api_key = os.getenv("API_KEY")
leads_base_id = os.getenv("LEADS_BASE_ID")
leads_table_id = os.getenv("LEADS_TABLE_ID")
leads_api = Api(api_key)


leads_from_at = ['Created','first_name','last_name','country_code','phone','email','lead_id','last_modified_time_lead_status','utm_source','somos_code (from site)']

leads_df = pd.DataFrame(leads_api.all(leads_base_id,leads_table_id,fields=leads_from_at)).fields.apply(pd.Series)

# Borrar numeros de telefono sin importar la info del leads (solo necesito un numero de telefono max)
leads_df = leads_df[~leads_df["phone"].duplicated()]

# --- DATA WRANGLING ---

# Change time format for created
def time_format(row,col_name):
    # date format
    date_format = '%Y-%m-%d %H:%M:%S'
    if isinstance(row[col_name],str):
        # get year/month/date portion of the string
        ymd = row[col_name].split('T')[0]

        # get hour/minute/second portion of the string
        hms = row[col_name].split('T')[1].split('.')[0]

        # concatenate both
        actual_time = ymd + ' ' + hms
        
        #actual_time_datetime = datetime.strptime(actual_time,date_format)

        return actual_time
    
    else:
        return None
leads_df['Created'] = leads_df.apply(lambda row: time_format(row,'Created'), axis=1)

leads_df['last_modified_time_lead_status'] = leads_df.apply(lambda row: time_format(row,'last_modified_time_lead_status'), axis=1)

#Change column names to fit supabase table names
leads_df.rename(columns={'Created':'created_at',
                              'lead_id':'airtable_id',
                              'last_modified_time_lead_status':'updated_at',
                              'utm_source':'source',
                              'somos_code (from site)':'somos_code'},inplace=True)

# Coherce data types to assure type fit
leads_df = leads_df.astype({'airtable_id':str,
                            'created_at':str,
                            'first_name':str,
                            'last_name':str,
                            'country_code':str,
                            'phone':str,
                            'email':str,
                            'updated_at':str,
                            'source':str,
                            'somos_code':str})

# replace string nans with None
leads_df = leads_df.replace('nan',None)

# clean
leads_df['somos_code'] = leads_df['somos_code'].str.replace('[','').str.replace(']','').str.replace("'",'')

# drop columns where there is no name
leads_df = leads_df[~leads_df["first_name"].isna()].reset_index(drop=True)

# Take care of empty spaces at the end of first names and correct name structure
def correct_names(row):
    # remove empty spaces at the end of a string
    name = row["first_name"].rstrip()

    # Check two word names and always capital letter then lowercase
    if name.count(" ")!=0:
        name = ' '.join(name.capitalize() for name in name.split())
    else:
        name = name.capitalize()

    return name
leads_df["first_name"] = leads_df.apply(lambda row: correct_names(row),axis=1)

# Take care of empty spaces at the end of last names and correct name structure
def correct_last_name(row):
    if row["last_name"]!= ' ' and row["last_name"]!= None:

        # remove empty spaces at the end of a string
        name = row["last_name"].rstrip()

        # Check two word names and always capital letter then lowercase
        if name.count(" ")!=0:
            name = ' '.join(name.capitalize() for name in name.split())
        else:
            name = name.capitalize()
        
        return name
    else:
        return None
leads_df["last_name"] = leads_df.apply(lambda row: correct_last_name(row),axis=1)

# Set the timezone to UTC
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

leads_df["created_at"] = leads_df.apply(lambda row: convert_to_utc(row,"created_at"),axis=1)

leads_df["updated_at"] = leads_df.apply(lambda row: convert_to_utc(row,"updated_at"),axis=1)

leads_df = leads_df.astype({"created_at":str, "updated_at":str})

leads_df["updated_at"] = leads_df["updated_at"].replace("NaT",None)

# =============================================================================
# Replace null values in updated_at with created_at
# =============================================================================
leads_df['updated_at'] = leads_df['updated_at'].fillna(leads_df['created_at'])


leads_df['phone'] = leads_df['phone'].str.replace('(','') \
                                     .str.replace(')','') \
                                     .str.replace('-','') \
                                     .str.replace(' ','') \
                                     .str.replace('\+57','')

leads_df['country_code'] = leads_df['country_code'].str.replace('+','')

# Drop quotes duplicated by phone
leads_df = leads_df.drop_duplicates(subset=['phone'])

# Create lead id
leads_df["id"] = range(1, len(leads_df)+1)
