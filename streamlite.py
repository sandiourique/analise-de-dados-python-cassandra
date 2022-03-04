# #############################  IMPORTANDO MODULOS ############################# #
import pathlib
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from dms2dec.dms_convert import dms2dec





# --- MAPA DE BARRAGENS NO BRASIL ------------------------------------------------ #

df = pd.read_csv('diego_autuacao_analise_2.csv',delimiter=',')
df.columns = ['id','lat','lon']
df.drop('id', axis=1, inplace=True)
df['lat'] = df['lat'].map(lambda x: x.rstrip('\\"'))
df['lon'] = df['lon'].map(lambda x: x.rstrip('\\"'))
df['lat'] = df.lat.apply(lambda x: x+"S" if x.startswith("-") else x+"N")
df['lon'] = df.lon.apply(lambda x: x+"W" if x.startswith("-") else x+"E")
df['lat'] = df['lat'].map(lambda x: x.lstrip('-'))
df['lon'] = df['lon'].map(lambda x: x.lstrip('-'))
df['lat'] = df.lat.apply(lambda x: dms2dec(x) if x.startswith("-") else dms2dec(x))
df['lon'] = df.lon.apply(lambda x: dms2dec(x) if x.startswith("-") else dms2dec(x))
df_barragens_brasil = df


# #############################  CORPO DA PAGINA ############################# #



# Titulo da pagina
st.title("Mineração / BC8")
# Divisor
st.markdown('---')
st.markdown('Mapa interativo mostra a distribuição de barragens no Brasil')

# Mapa de barragens
st.map(df_barragens_brasil)

    




#st.write(pydicom)