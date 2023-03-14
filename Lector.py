
import streamlit as st
import pandas as pd
import datetime
import base64
import io

st.set_page_config(
    page_icon=":shark:"
)
st.sidebar.image("https://scontent-bog1-1.xx.fbcdn.net/v/t39.30808-6/286883597_158534336684733_9125584544552296429_n.jpg?_nc_cat=107&ccb=1-7&_nc_sid=09cbfe&_nc_ohc=yIRlt9XkYbMAX__pBUw&_nc_ht=scontent-bog1-1.xx&oh=00_AfAwT5h0Esx4KlJVPRgfrWIrQLAE6tkoWYXRnY4XGkBMqA&oe=64159770", width=50)


st.title("Control de Referidos")

'''
Esta aplicación permite conocer qué usuario es referido de quién 
'''
url = 'https://github.com/cdalvarezb27/App-Referidos-Somos/blob/9c147ce4c8cea9e2e22e09ab97c700ab274b4ec0/referidos.xlsx?raw=true'
df = pd.read_excel(url, engine='openpyxl')
df['instalaciones_tiempo_kustomer'] = pd.to_datetime(df['instalaciones_tiempo_kustomer'], format='%Y-%m-%d')
df['phone_referido'] = df['phone_referido'].astype(str)
df['codigo_referidos'] = df['codigo_referidos'].astype(str)


# Obtener los valores únicos de las columnas 'suscripcion status' y 'instal_valid'
unique_subscription_status = df['suscripcion_status'].unique()
unique_installation_valid = df['instal_valid (from suscripciones)'].unique()

# Verificar si las fechas son objetos de tipo date/datetime
min_date = df['instalaciones_tiempo_kustomer'].min()
max_date = df['instalaciones_tiempo_kustomer'].max()

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

    st.sidebar.write('**Filtro por fecha de instalación**')
    # Mostrar los widgets de fecha en el sidebar
    start_date = pd.to_datetime(st.sidebar.date_input('instalaciones (Fecha de inicio)', min_date))
    end_date = pd.to_datetime(st.sidebar.date_input('instalaciones (Fecha de fin)', max_date))
    
    st.sidebar.write('**Otros filtros**')
    # Mostrar los widgets de los filtros en el sidebar
    selected_subscription_status = st.sidebar.multiselect("Selecciona un estado de suscripción", unique_subscription_status)
    selected_installation_valid = st.sidebar.multiselect("Selecciona una instalación válida", unique_installation_valid)

    # Construir el filtro progresivamente
    filtered_data = df[df['instalaciones_tiempo_kustomer'].between(start_date, end_date)]
    
    if selected_subscription_status:
        filtered_data = filtered_data[filtered_data['suscripcion_status'].isin(selected_subscription_status)]
        
    if selected_installation_valid:
        filtered_data = filtered_data[filtered_data['instal_valid (from suscripciones)'].isin(selected_installation_valid)]

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

else:
    # Mostrar mensaje de error si las fechas no son válidas
    st.write('Error: las fechas no son válidas')

