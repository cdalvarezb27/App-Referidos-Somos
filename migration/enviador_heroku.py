import pandas as pd
from integrador import df_quotes, df_customers, df_subsites, df_sites, df_addresses, df_services, df_technicians, df_appointments_2, df_subscriptions, df_invoices, df_subscriptions_invoices, payments_2, df_appointments_technicians_2,df_services_strata_2,df_strata_2

from sqlalchemy import create_engine
import psycopg2

engine = create_engine('postgresql+psycopg2://pkgplptxcoasph:f1044c330298ce191e58dc67765ec63e582563e489d9fd75b2e8dad3ce85bb79@ec2-3-234-204-26.compute-1.amazonaws.com:5432/d9gg37ud6998ra')


df_addresses.to_sql('addresses', con=engine, if_exists='append', index=False)

df_sites.to_sql('sites', con=engine, if_exists='append', index=False)

df_subsites.to_sql('subsites', con=engine, if_exists='append', index=False)

df_quotes.to_sql('quotes', con=engine, if_exists='append', index=False)

df_customers.to_sql('customers', con=engine, if_exists='append', index=False)

df_technicians.to_sql('technicians', con=engine, if_exists='append', index=False)

df_appointments_2.to_sql('appointments', con=engine, if_exists='append', index=False)

df_appointments_technicians_2.to_sql('appointments_technicians', con=engine, if_exists='append', index=False)

df_services.to_sql('services', con=engine, if_exists='append', index=False)

df_strata_2.to_sql('strata', con=engine, if_exists='append', index=False)

df_services_strata_2.to_sql('services_strata', con=engine, if_exists='append', index=False)

df_subscriptions.to_sql('subscriptions', con=engine, if_exists='append', index=False)

df_invoices.to_sql('invoices', con=engine, if_exists='append', index=False)

df_subscriptions_invoices.to_sql('subscriptions_invoices', con=engine, if_exists='append', index=False)

payments_2.to_sql('payments', con=engine, if_exists='append', index=False)

