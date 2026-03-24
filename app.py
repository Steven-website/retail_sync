import streamlit as st
import pandas as pd
import json
import io
import os
from openpyxl import Workbook

st.set_page_config(page_title="Retail Sync", layout="wide")
st.title("🛒 Retail Sync")

RUTA_MASTER = "data/master.parquet"
RUTA_BD = "data/BD_ACTUALIZACION.parquet"


# =====================================================
# UTIL
# =====================================================
def normalizar_columnas(df):
    df.columns = df.columns.str.strip()
    return df


def detectar_pk(df):
    for c in df.columns:
        if c.upper().replace(" ", "") in ["PK_ARTICULOS", "PKARTICULOS"]:
            return c
    return None


# =====================================================
# CREAR MASTER
# =====================================================
def crear_master_vacio():

    if not os.path.exists(RUTA_BD):
        df = pd.DataFrame(columns=["PK_Articulos","ACTIVIDAD_COMERCIAL"])
        df.to_parquet(RUTA_MASTER,index=False)
        return

    bd = pd.read_parquet(RUTA_BD)
    bd = normalizar_columnas(bd)

    pk = detectar_pk(bd)

    columnas = bd.columns.tolist()
    columnas.insert(1,"ACTIVIDAD_COMERCIAL")

    columnas += [
        "MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO","PORC_AHORRO",
        "FECHA_INICIO","FECHA_FIN","ACCION","COMENTARIO"
    ]

    df = pd.DataFrame(columns=columnas)
    df.to_parquet(RUTA_MASTER,index=False)


if not os.path.exists(RUTA_MASTER):
    crear_master_vacio()


# =====================================================
# ACTUALIZAR MASTER
# =====================================================
def actualizar_master():

    if not os.path.exists(RUTA_MASTER):
        return

    if not os.path.exists(RUTA_BD):
        return

    master = pd.read_parquet(RUTA_MASTER)
    bd = pd.read_parquet(RUTA_BD)

    master = normalizar_columnas(master)
    bd = normalizar_columnas(bd)

    pk = detectar_pk(bd)
    if pk is None:
        st.error("No se detecta PK en BD_ACTUALIZACION")
        return

    if master.empty:
        return

    columnas_comerciales = [
        "MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO","PORC_AHORRO",
        "FECHA_INICIO","FECHA_FIN","ACCION","COMENTARIO"
    ]

    for col in columnas_comerciales:
        if col not in master.columns:
            master[col] = None

    lista = []

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    for ac in actividades:

        base_ac = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        nuevo = bd.copy()
        nuevo.insert(1,"ACTIVIDAD_COMERCIAL",ac)

        cols_merge = [pk] + [c for c in columnas_comerciales if c in base_ac.columns]

        nuevo = nuevo.merge(
            base_ac[cols_merge],
            on=pk,
            how="left"
        )

        lista.append(nuevo)

    master_nuevo = pd.concat(lista,ignore_index=True)

    master_nuevo.to_parquet(RUTA_MASTER,index=False)


# =====================================================
# CONSOLIDAR
# =====================================================
def consolidar(actividad):

    master = pd.read_parquet(RUTA_MASTER)
    master = normalizar_columnas(master)

    pk = detectar_pk(master)

    for archivo in os.listdir("data"):

        if archivo.startswith("trabajo_") and actividad in archivo:

            temp = pd.read_parquet(os.path.join("data",archivo))
            temp = normalizar_columnas(temp)

            master = master.merge(
                temp,
                on=[pk,"ACTIVIDAD_COMERCIAL"],
                how="left",
                suffixes=("","_NEW")
            )

            if "ACCION_NEW" in master.columns:
                master["ACCION"] = master["ACCION_NEW"].combine_first(master["ACCION"])

            if "COMENTARIO_NEW" in master.columns:
                master["COMENTARIO"] = master["COMENTARIO_NEW"].combine_first(master["COMENTARIO"])

            master = master.drop(columns=[c for c in master.columns if "_NEW" in c])

    master.to_parquet(RUTA_MASTER,index=False)


# =====================================================
# LOGIN
# =====================================================
if "login" not in st.session_state:
    st.session_state.login = False

if not st.session_state.login:

    user = st.text_input("Usuario")
    pwd = st.text_input("Password",type="password")

    if st.button("Ingresar"):

        with open("usuarios.json",encoding="utf-8") as f:
            usuarios = json.load(f)

        for u in usuarios:
            if u["usuario"] == user and u["password"] == pwd:
                st.session_state.login = True
                st.session_state.user = u
                st.rerun()

        st.error("Credenciales incorrectas")

else:

    actualizar_master()

    usuario = st.session_state.user["usuario"]
    rol = st.session_state.user["rol"]

    master = pd.read_parquet(RUTA_MASTER)
    master = normalizar_columnas(master)

    st.success(f"Bienvenido {usuario}")

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    if len(actividades) == 0 and rol == "MASTER":

        nueva = st.text_input("Crear Primera Actividad")

        if st.button("CREAR"):

            bd = pd.read_parquet(RUTA_BD)
            bd = normalizar_columnas(bd)

            bd.insert(1,"ACTIVIDAD_COMERCIAL",nueva)

            for c in ["MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO",
                      "PORC_AHORRO","FECHA_INICIO","FECHA_FIN",
                      "ACCION","COMENTARIO"]:
                bd[c]=None

            bd.to_parquet(RUTA_MASTER,index=False)
            st.rerun()

    else:

        actividad = st.selectbox("Actividad",actividades)

        df = master[master["ACTIVIDAD_COMERCIAL"]==actividad]

        st.dataframe(df,use_container_width=True)

        if rol in ["ADC","JEFE_ADC"]:

            archivo = st.file_uploader("Subir Excel")

            if archivo:

                df_user = pd.read_excel(archivo)
                df_user = normalizar_columnas(df_user)

                pk = detectar_pk(df_user)

                df_user["ACTIVIDAD_COMERCIAL"]=actividad

                ruta=f"data/trabajo_{usuario}_{actividad}.parquet"

                df_user[[pk,"ACTIVIDAD_COMERCIAL","ACCION","COMENTARIO"]].to_parquet(ruta,index=False)

                if st.button("CARGAR CAMBIOS"):
                    consolidar(actividad)
                    st.success("Cambios aplicados")

        if rol=="MASTER":

            col1,col2=st.columns(2)

            with col1:
                with open(RUTA_MASTER,"rb") as f:
                    st.download_button("MASTER",f,file_name="MASTER.parquet")

            with col2:
                buffer=io.BytesIO()
                df.to_parquet(buffer,index=False)
                buffer.seek(0)

                st.download_button("ACTIVIDAD",buffer,file_name=f"{actividad}.parquet")
