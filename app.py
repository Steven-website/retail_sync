import streamlit as st
import pandas as pd
import json
import io
import os
from datetime import datetime, timedelta
from openpyxl import Workbook

st.set_page_config(page_title="Retail Sync", layout="wide")
st.title("🛒 Retail Sync")

RUTA_MASTER = "data/master.parquet"
RUTA_BD = "data/BD_ACTUALIZACION.parquet"
CARPETA_VERSIONES = "data/versiones"
ARCHIVO_CONTROL = "data/control_consolidacion.json"


# ==========================================
# ACTUALIZAR MASTER DESDE BD_ACTUALIZACION
# ==========================================
def actualizar_master_desde_bd():

    if not os.path.exists(RUTA_BD):
        return

    try:
        master = pd.read_parquet(RUTA_MASTER)
        bd = pd.read_parquet(RUTA_BD)
    except:
        return

    columnas_update = [
        "FAMILIA","CATEGORIA","SUBCATEGORIA","NO_ARTI","DESCRIPCION","TIPO_CLASIF",
        "COMPRA_Q_2024","COMPRA_Q_2025","COMPRA_Q_2026",
        "VTA_YTD_2024","VTA_YTD_2025","VTA_YTD_2026",
        "VTA_Q_YTD_2024","VTA_Q_YTD_2025","VTA_Q_YTD_2026",
        "INVENTARIO_Q"
    ]

    master = master.merge(
        bd[["PK_ARTICULO"] + columnas_update],
        on="PK_ARTICULO",
        how="left",
        suffixes=("","_NEW")
    )

    for col in columnas_update:
        if col + "_NEW" in master.columns:
            master[col] = master[col + "_NEW"].combine_first(master[col])

    master = master.drop(columns=[c for c in master.columns if "_NEW" in c])

    master.to_parquet(RUTA_MASTER)


# ==========================================
# CONSOLIDAR MASTER
# ==========================================
def consolidar_master():

    master = pd.read_parquet(RUTA_MASTER)

    for archivo in os.listdir("data"):

        if archivo.startswith("trabajo_"):

            df_temp = pd.read_parquet(f"data/{archivo}")

            master = master.merge(
                df_temp,
                on="PK_ARTICULO",
                how="left",
                suffixes=("","_NEW")
            )

            master["ACCION"] = master["ACCION_NEW"].combine_first(master["ACCION"])
            master["COMENTARIO"] = master["COMENTARIO_NEW"].combine_first(master["COMENTARIO"])

            master = master.drop(columns=["ACCION_NEW","COMENTARIO_NEW"])

    if not os.path.exists(CARPETA_VERSIONES):
        os.makedirs(CARPETA_VERSIONES)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    master.to_parquet(f"{CARPETA_VERSIONES}/master_{timestamp}.parquet")

    master.to_parquet(RUTA_MASTER)


# ==========================================
# LOGIN STATE
# ==========================================
if "login" not in st.session_state:
    st.session_state.login = False


# ==========================================
# LOGIN
# ==========================================
if not st.session_state.login:

    user = st.text_input("Usuario")
    pwd = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        with open("usuarios.json", encoding="utf-8") as f:
            usuarios = json.load(f)

        for u in usuarios:
            if u["usuario"] == user and u["password"] == pwd:
                st.session_state.login = True
                st.session_state.user = u
                st.rerun()

        st.error("Credenciales incorrectas")


# ==========================================
# SISTEMA
# ==========================================
else:

    actualizar_master_desde_bd()

    usuario = st.session_state.user["usuario"]
    rol = st.session_state.user["rol"]

    st.success(f"Bienvenido {usuario}")

    df = pd.read_parquet(RUTA_MASTER)

    # ======================================
    # SELECTOR ACTIVIDAD COMERCIAL
    # ======================================
    actividades = df["ACTIVIDAD_COMERCIAL"].dropna().unique()

    actividad_sel = st.selectbox(
        "Seleccione Actividad Comercial",
        actividades
    )

    df = df[df["ACTIVIDAD_COMERCIAL"] == actividad_sel]

    # ======================================
    # FILTRO ADC
    # ======================================
    if rol == "ADC":
        familia_user = st.session_state.user["familia"]
        df = df[df["FAMILIA"] == familia_user]

    st.dataframe(df, use_container_width=True)


    # ======================================
    # DESCARGAR
    # ======================================
    st.markdown("### 📥 Descargar")

    buffer = io.BytesIO()
    wb = Workbook()
    ws = wb.active

    for c,col in enumerate(df.columns,1):
        ws.cell(row=1,column=c,value=col)

    for r,row in enumerate(df.itertuples(index=False),2):
        for c,val in enumerate(row,1):
            ws.cell(row=r,column=c,value=val)

    wb.save(buffer)
    buffer.seek(0)

    st.download_button(
        "Descargar Excel",
        data=buffer,
        file_name=f"trabajo_{usuario}.xlsx"
    )


    # ======================================
    # ROLES QUE PUEDEN SUBIR
    # ======================================
    if rol in ["ADC","JEFE_ADC"]:

        st.warning("Después de subir archivo debe presionar CARGAR CAMBIOS")

        archivo = st.file_uploader("Subir Excel", type=["xlsx"])

        if archivo is not None:

            df_user = pd.read_excel(archivo)

            columnas = ["PK_ARTICULO","ACCION","COMENTARIO"]

            if not all(col in df_user.columns for col in columnas):
                st.error("Archivo incorrecto")
                st.stop()

            df_user[columnas].to_parquet(f"data/trabajo_{usuario}.parquet")

            st.success("Archivo listo")

            if st.button("CARGAR CAMBIOS"):
                consolidar_master()
                st.success("Cambios aplicados")


    # ======================================
    # MASTER
    # ======================================
    if rol == "MASTER":

        st.markdown("### 📊 Analizar")

        if st.button("ANALIZAR"):
            st.write(df.describe())

        st.markdown("### 📦 Descargar Base Final")

        if st.button("DESCARGAR PARQUET"):
            st.download_button(
                "Descargar",
                data=open(RUTA_MASTER,"rb"),
                file_name="MASTER.parquet"
            )
