import funciones as fn
from snowflake_utils import sf_check_snowflake_connection, st_query_to_snowflake_and_return_dataframe
# from snowflake_config import sf_config # Toca crear el archivo .toml
import docx # Para generar el reporte

import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objs as go
import folium
from streamlit_folium import folium_static

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer

# Configuración de pandas
pd.options.display.max_columns = None
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_colwidth', 0)

# # Archivos
# a_base, a_mun, a_ubic = "Tejido_Municipios.txt", "Base_municipios.txt", "DIVIPOLA_Municipios.xlsx"

# # Cargue bases de datos
# df_general = pd.read_csv( a_mun, decimal=',', sep='|', encoding ='utf-8', converters={'Cod. Municipio':str})
# df_base = pd.read_csv( a_base, sep="|", decimal=",", encoding ='utf-8', converters={'Cod. Depto':str,'Cod. Municipio':str, 'CIIU Rev 4 principal':str})
# df_ubicacion = pd.read_excel( a_ubic, skiprows=10, converters={'Código .1':str})

# Obtener los dataframes cargados desde funciones.py
df_general, df_base, df_ubicacion = fn.get_dataframes()

# Seleccionar de las bases lo que necesito
df_ubicacion = df_ubicacion[['Código .1', 'Nombre', 'Nombre.1', 'LATITUD', 'LONGITUD']]
df_ubicacion=df_ubicacion[df_ubicacion['Código .1'].notna()]
df_ubicacion.rename(columns={'Código .1': 'Cod. Municipio', 'Nombre': 'Departamento', 'Nombre.1': 'Municipio'}, inplace=True)

# Configuración página web
st.set_page_config(page_title="Perfil territorio", page_icon = '🌎', layout="wide",  initial_sidebar_state="expanded") 

# --------------- Sidebar -------------------------------------------

# Logo ProColombia
st.sidebar.image( "PRO_PRINCIPAL_HORZ_PNG.png", use_column_width=True)

st.sidebar.markdown("---") 

# Filtrar el municipio de interés
st.sidebar.title('Escoja el territorio de interés') 
depto0 = df_base[df_base['Departamento']!='No determinado']
depto = sorted(depto0['Departamento'].unique().tolist())
index1 = depto.index("Arauca")
depto_seleccionado = st.sidebar.selectbox("Seleccione el departamento", depto, index=index1)
mpio = sorted(df_base[df_base['Departamento']==depto_seleccionado]['Municipio'].unique().tolist())
mpio_seleccionado = st.sidebar.selectbox("Seleccione el municipio", mpio)

# Filtrar la información por el territorio de interés
cod_mpio_selec = df_base[(df_base['Departamento']==depto_seleccionado)&(df_base['Municipio']==mpio_seleccionado)]['Cod. Municipio'].values[0]
filtro1 = (df_base['Cod. Municipio']==cod_mpio_selec)
tejido = df_base[filtro1]
filtro2 = (df_general['Cod. Municipio']==cod_mpio_selec)
df_datos_mun = df_general[filtro2]

# Para verificar si hay información
print(df_datos_mun.head())

# Fuentes consultadas
st.sidebar.markdown('##')
st.sidebar.markdown('##')
st.sidebar.subheader("Fuentes consultadas:") 

st.sidebar.markdown("#### Información general:") 
st.sidebar.markdown("- Proyecciones de población municipal por área, sexo y edad para 2022, DANE.")
st.sidebar.markdown("- Censo Nacional de Población y Vivienda 2018, DANE.")
st.sidebar.markdown("- Medida de Pobreza Multidimensional Municipal 2018, DANE.")
st.sidebar.markdown("- Valor Agregado por municipio 2021, DANE.")
st.sidebar.markdown("- Divipola DANE.")

st.sidebar.markdown("#### Empresas:") 
st.sidebar.markdown("- Registro Único Empresarial y Social (RUES) con corte a mayo de 2023, que incluyó empresas con renovación de matrícula mercantil desde el año 2019 en adelante, clasificadas como 'sociedad o persona jurídica principal' y en estado 'activa'.") 
st.sidebar.markdown("- Directorio empresarial del DANE con corte a abril 2023.")
st.sidebar.markdown("- Las 10.000 empresas más grandes de Colombia, Superintendencia de Sociedades (2021).")
st.sidebar.markdown("- Base de exportaciones de bienes, DANE-DIAN (2013-2022).")
st.sidebar.markdown("-  CRM de ProColombia (2013-2022).")

st.sidebar.markdown('##')
st.sidebar.markdown('##')
st.sidebar.subheader("Elaborado por:") 
st.sidebar.markdown("####  Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.") 

st.sidebar.markdown("---") 

# Logo Ministerio
st.sidebar.image( "Logo MinCit_Mesa de trabajo 1 copia.png", use_column_width=True)

# ------------- Tablero -------------------------------

st.title(f'Perfil territorio: {mpio_seleccionado} - {depto_seleccionado}')

st.markdown("---") 

# Datos generales del municipio
st.header('🌐 **Ubicación geográfica**')

# Mapa del municipio
mapa = df_ubicacion[df_ubicacion['Cod. Municipio'] == cod_mpio_selec]
municipio_lat = mapa['LATITUD']
municipio_lon = mapa['LONGITUD']
colombia_center = [4.5709, -74.2973]
m = folium.Map(location=colombia_center, zoom_start=5)
folium.Marker(location=[municipio_lat, municipio_lon], popup=f'{mpio_seleccionado} - {depto_seleccionado}').add_to(m)
folium_static(m, width=1300, height=500)

st.header('🔍 **Información general**')

# Métricas PDET y ZOMAC
pdet, zomac = fn.crear_metricas_pdet_zomac(df_datos_mun)
c1,c2,c3 = st.columns(3)
with c1:
    st.markdown(f'#### {pdet}')
with c2:
    st.markdown(f'#### {zomac}')
with c3:
    pob = df_datos_mun['Población municipio'].values[0]
    st.markdown("#### Población 2022")
    st.subheader(f'{pob:,.0f} habitantes')

# Distribución de la población
st.subheader('Características de la población')

# Gráficos de torta
c1, c2, c3 = st.columns(3)

with c1:
    femenino = df_datos_mun['% mujeres municipio'].values[0]
    masculino = 100 - femenino
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['Femenino', 'Masculino'],
                                   [femenino, masculino],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)'],
                                   'Año 2022')

with c2:
    joven = df_datos_mun['% jóvenes municipio'].values[0]
    no_joven = 100 - joven
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['Jóvenes', 'Resto <br> de <br> población'],
                                   [joven, no_joven],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)'],
                                   'Año 2022')

with c3:
    etnico = df_datos_mun['% grupos étnicos municipio'].values[0]
    no_etnico = 100 - etnico
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['Grupos <br> étnicos', 'Resto <br> de <br> población'],
                                   [etnico, no_etnico],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)'],
                                   'Censo 2018')

c1, c2, c3 = st.columns(3)

with c1:
    discap = df_datos_mun['% grupos étnicos municipio'].values[0]
    no_discap = 100 - discap
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['Resto <br> de <br> población', 'Con <br> discapacidad'],
                                   [no_discap, discap],
                                   ['rgb(0, 109, 254)', 'rgb(255, 218, 0)'],
                                   'Censo 2018')

with c2:
    pobre = df_datos_mun['% pobreza municipio'].values[0]
    no_pobre = 100 - pobre
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['En <br> situación <br> de <br> pobreza', 'Resto <br> de <br> población'],
                                   [pobre, no_pobre],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)'],
                                   'Censo 2018')

with c3:
    informal = df_datos_mun['% informalidad municipio'].values[0]
    no_informal = 100 - informal
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   '',
                                   ['Ocupados <br> informales', 'Resto <br> de <br> ocupados'],
                                   [informal, no_informal],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)'],
                                   'Censo 2018')

# PIB y educación
c1, c2 = st.columns(2)

with c1:
    prim = df_datos_mun['% Act. primarias municipio'].values[0]
    sec = df_datos_mun['% Act. secundarias municipio'].values[0]
    ter = df_datos_mun['% Act. terciarias municipio'].values[0]
    
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   'Valor agregado',
                                   ['Actividades <br> primarias', 'Actividades <br> secundarias', 'Actividades <br> terciarias'],
                                   [prim, sec, ter],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)', 'rgb(252, 0, 81)'],
                                   'Año 2021')
    
    va = df_datos_mun['Valor agregado municipio'].values[0]
    st.subheader(f'COP {va:,.0f} miles de millones')

with c2:
    media = df_datos_mun['% pobl. con educación media municipio'].values[0]
    tecnica = df_datos_mun['% pobl. con edu. técnica/tecnología municipio'].values[0]
    pre = df_datos_mun['% pobl. con pregrado municipio'].values[0]
    pos = df_datos_mun['% pobl. con posgrado municipio'].values[0]
    resto = 100 - media - tecnica - pre - pos
    
    fn.mostrar_grafico_torta_datos(df_datos_mun,
                                   'Nivel educativo de la población',
                                   ['Educación <br> media', 'Educación <br> técnica/tecnología', 'Pregrado', 'Posgrado', 'Resto <br> de <br> población'],
                                   [media, tecnica, pre, pos, resto],
                                   ['rgb(255, 218, 0)', 'rgb(0, 109, 254)', 'rgb(252, 0, 81)', 'rgb(106, 124, 133)', 'rgb(69, 87, 108)'],
                                   'Censo 2018')

st.markdown("---")

# Tejido empresarial
st.header('🏭 **Empresas ubicadas en el territorio**')
st.markdown('##### Nota: Se enfoca en personas jurídicas con ubicación comercial en el territorio.')

fn.mostrar_empresas_por_categoria_unificada(tejido, 'Tamaño', '', 'Distribución según tamaño', 'rgb(69, 87, 108)')
fn.mostrar_empresas_por_categoria_unificada(tejido, 'Cadena productiva', '', 'Distribución según cadena productiva', 'rgb(69, 87, 108)', height=700)
fn.mostrar_empresas_por_categoria_unificada(tejido, 'Valor agregado empresa', '', 'Distribución según valor agregado', 'rgb(69, 87, 108)', height=700)

# Tejido exportador
exportadoras = tejido[tejido['Tipo* ult 10 años'] != "No exportó ult. 10 años"]
fn.mostrar_empresas_por_categoria_unificada(tejido, "Cadena* ult 10 años", 
                                            "Empresas ubicadas en el territorio que realizaron alguna exportación en los últimos 10 años (2013-2022)", 
                                            "Distribución según la cadena productiva por la que más exportó la empresa", "rgb(252, 0, 81)",
                                            df_filtrado=exportadoras)

# Instalados
ied = tejido[tejido['Sucursal sociedad extranjera'] == "Si"]
fn.mostrar_empresas_por_categoria_unificada(tejido, "Cadena productiva", 
                                            "Empresas ubicadas en el territorio identificadas como sucursal de sociedad extranjera", 
                                            "Distribución según cadena productiva", "rgb(0, 109, 254)",
                                            df_filtrado=ied)

# Turismo
fn.mostrar_empresas_turismo(tejido)
