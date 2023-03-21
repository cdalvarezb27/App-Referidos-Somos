import streamlit as st
import pandas as pd
import datetime
import base64
import io
import matplotlib.pyplot as plt


st.set_page_config(
    page_icon=":shark:"
)
st.sidebar.image("https://scontent-bog1-1.xx.fbcdn.net/v/t39.30808-6/286883597_158534336684733_9125584544552296429_n.jpg?_nc_cat=107&ccb=1-7&_nc_sid=09cbfe&_nc_ohc=yIRlt9XkYbMAX__pBUw&_nc_ht=scontent-bog1-1.xx&oh=00_AfAwT5h0Esx4KlJVPRgfrWIrQLAE6tkoWYXRnY4XGkBMqA&oe=64159770", width=50)


st.title("Control de Referidos")

'''
Esta aplicación permite conocer qué usuario es referido de quién 
'''

df = pd.read_excel("referidos.xlsx")
df['instalaciones_tiempo_kustomer'] = pd.to_datetime(df['instalaciones_tiempo_kustomer'], format='%Y-%m-%d')
df['Created_referido'] = pd.to_datetime(df['Created_referido'], format='%Y-%m-%d')

df['phone_referido'] = df['phone_referido'].astype(str)
df['codigo_referidos'] = df['codigo_referidos'].astype(str)


# Obtener los valores únicos de las columnas 'suscripcion status' y 'instal_valid'
unique_subscription_status = df['suscripcion_status'].unique()
unique_created_valid = df['instal_valid (from suscripciones)'].unique()
unique_state_stepper = df['state_stepper (from Site)'].unique()

# Verificar si las fechas son objetos de tipo date/datetime
min_date = df['Created_referido'].min()
max_date = df['Created_referido'].max()

if isinstance(min_date, (datetime.date, datetime.datetime)) and isinstance(max_date, (datetime.date, datetime.datetime)):

    
    
    #ff000050
    primaryColor="#F63366"
    backgroundColor="#FFFFFF"
    secondaryBackgroundColor="#F0F2F6"
    textColor="#262730"
    font="serif"

    st.markdown("""
    <style>
        [data-testid=stSidebar] {
            background-color: #F4F4F4;
        }
    </style>
    """, unsafe_allow_html=True)

    st.sidebar.write('**Filtro por fecha de Creación del referido**')
    # Mostrar los widgets de fecha en el sidebar
    start_date = pd.to_datetime(st.sidebar.date_input('Created_referido (Fecha de inicio)', min_date))
    end_date = pd.to_datetime(st.sidebar.date_input('Created_referido (Fecha de fin)', max_date))
    
    st.sidebar.write('**Otros filtros**')
    # Mostrar los widgets de los filtros en el sidebar
    selected_subscription_status = st.sidebar.multiselect("Selecciona un estado de suscripción", unique_subscription_status)
    selected_created_valid = st.sidebar.multiselect("Selecciona una instalación válida", unique_created_valid)
    selected_state_stepper = st.sidebar.multiselect("Selecciona el estado del site",unique_state_stepper)

    # Construir el filtro progresivamente
    filtered_data = df[df['Created_referido'].between(start_date, end_date)]
    
    if selected_subscription_status:
        filtered_data = filtered_data[filtered_data['suscripcion_status'].isin(selected_subscription_status)]
        
    if selected_created_valid:
        filtered_data = filtered_data[filtered_data['instal_valid (from suscripciones)'].isin(selected_created_valid)]
    
    if selected_state_stepper:
        filtered_data = filtered_data[filtered_data['state_stepper (from Site)'].isin(selected_state_stepper)]
    
        
    
    # Mostrar el dataframe filtrado
    st.write(filtered_data)
    
    # Agregar botón de descarga
    if st.button('Descargar XLSX'):
        # Codificar el dataframe en base64
        towrite = io.BytesIO()
        downloaded_file = filtered_data.to_excel(towrite, encoding='utf-8', index=False, header=True)
        towrite.seek(0)  # Asegurarse de que el cursor esté al inicio del archivo
        # Codificar el archivo en base64 para descargarlo como un archivo
        b64 = base64.b64encode(towrite.read()).decode()
        href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="referidos.xlsx">Descargar archivo XLSX</a>'
        st.markdown(href, unsafe_allow_html=True)

    
    filtered_data_2 = filtered_data.copy()
    
    filtered_data_2 = filtered_data_2[['nombre_del_cliente_referidor']]
    
    grouped_df = filtered_data_2.groupby('nombre_del_cliente_referidor').value_counts().to_frame().reset_index()
    
    grouped_df = grouped_df.rename(columns={0: 'cuenta'})


    st.title("Gráfico de usuarios top referidores")

    unique_referidos = grouped_df['cuenta'].unique()
    
    st.write('**filtro de # de referidos**')
    # Mostrar los widgets de los filtros en el sidebar
    selected_referidos = st.multiselect("Selecciona un # de referidos", unique_referidos)
    if selected_referidos:
        grouped_df = grouped_df[grouped_df['cuenta'].isin(selected_referidos)]
    

    import altair as alt
    # Create a bar chart using Altair
    bar_chart = alt.Chart(grouped_df).mark_bar().encode(
        x=alt.X('nombre_del_cliente_referidor', sort=alt.EncodingSortField(field='cuenta', order='descending')),
        y='cuenta'
    )
    
    st.altair_chart(bar_chart, use_container_width=True)
    
    
else:
    # Mostrar mensaje de error si las fechas no son válidas
    st.write('Error: las fechas no son válidas')




