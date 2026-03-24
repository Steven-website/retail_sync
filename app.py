import streamlit as st
import pandas as pd
import json
import io
import os
from datetime import datetime
from openpyxl import Workbook

st.set_page_config(page_title="Retail Sync", layout="wide")
st.title("🛒 Retail Sync")

RUTA_MASTER = "data/master.parquet"
RUTA_BD = "data/BD_ACTUALIZACION.parquet"


# ==========================================================
# ACTUALIZAR MASTER DESDE BD_ACTUALIZACION
# ==========================================================
def actualizar_master():

    if not os.path.exists(RUTA_BD):
        return

    master = pd.read_parquet(RUTA_MASTER)
    bd = pd.read_parquet(RUTA_BD)

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
        master[col] = master[col+"_NEW"].combine_first(master[col])

    master = master.drop(columns=[c for c in master.columns if "_NEW" in c])

    master.to_parquet(RUTA_MASTER)


# ==========================================================
# CONSOLIDAR CAMBIOS USUARIOS
# ==========================================================
def consolidar(actividad):

    master = pd.read_parquet(RUTA_MASTER)

    for archivo in os.listdir("data"):

        if actividad in archivo and archivo.startswith("trabajo_"):

            df_temp = pd.read_parquet(f"data/{archivo}")

            master = master.merge(
                df_temp,
                on=["PK_ARTICULO","ACTIVIDAD_COMERCIAL"],
                how="left",
                suffixes=("","_NEW")
            )

            master["ACCION"] = master["ACCION_NEW"].combine_first(master["ACCION"])
            master["COMENTARIO"] = master["COMENTARIO_NEW"].combine_first(master["COMENTARIO"])

            master = master.drop(columns=["ACCION_NEW","COMENTARIO_NEW"])

    master.to_parquet(RUTA_MASTER)


# ==========================================================
# LOGIN STATE
# ==========================================================
if "login" not in st.session_state:
    st.session_state.login = False


# ==========================================================
# LOGIN
# ==========================================================
if not st.session_state.login:

    user = st.text_input("Usuario")
    pwd = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        with open("usuarios.json") as f:
            usuarios = json.load(f)

        for u in usuarios:
            if u["usuario"] == user and u["password"] == pwd:
                st.session_state.login = True
                st.session_state.user = u
                st.rerun()

        st.error("Credenciales incorrectas")


# ==========================================================
# SISTEMA
# ==========================================================
else:

    actualizar_master()

    usuario = st.session_state.user["usuario"]
    rol = st.session_state.user["rol"]

    st.success(f"Bienvenido {usuario}")

    master = pd.read_parquet(RUTA_MASTER)

    # ======================================================
    # CREAR ACTIVIDAD (MASTER)
    # ======================================================
    if rol == "MASTER":

        st.markdown("### 🎯 Crear Actividad Comercial")

        nueva_ac = st.text_input("Nombre Actividad")

        if st.button("CREAR ACTIVIDAD"):

            if nueva_ac == "":
                st.warning("Debe escribir nombre")
                st.stop()

            if nueva_ac in master["ACTIVIDAD_COMERCIAL"].unique():
                st.error("Actividad ya existe")
                st.stop()

            if not os.path.exists(RUTA_BD):
                st.error("No existe BD_ACTUALIZACION")
                st.stop()

            bd = pd.read_parquet(RUTA_BD)

            bd["ACTIVIDAD_COMERCIAL"] = nueva_ac
            bd["MUNDO_AC"] = None
            bd["PRECIO_PROMOCIONAL"] = None
            bd["DESCUENTO"] = None
            bd["PORC_AHORRO"] = None
            bd["FECHA_INICIO"] = None
            bd["FECHA_FIN"] = None
            bd["ACCION"] = None
            bd["COMENTARIO"] = None

            master = pd.concat([master, bd], ignore_index=True)

            master.to_parquet(RUTA_MASTER)

            st.success("Actividad creada")
            st.rerun()

    # ======================================================
    # SELECTOR ACTIVIDAD
    # ======================================================
    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    actividad = st.selectbox(
        "Seleccione Actividad Comercial",
        actividades
    )

    df = master[master["ACTIVIDAD_COMERCIAL"] == actividad]

    # filtro ADC
    if rol == "ADC":
        fam = st.session_state.user["familia"]
        df = df[df["FAMILIA"] == fam]

    st.dataframe(df, use_container_width=True)

    # ======================================================
    # DESCARGAR
    # ======================================================
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
        file_name=f"{actividad}_{usuario}.xlsx"
    )

    # ======================================================
    # SUBIR SOLO ADC Y JEFE
    # ======================================================
    if rol in ["ADC","JEFE_ADC"]:

        st.warning("Debe presionar CARGAR CAMBIOS después de subir archivo")

        archivo = st.file_uploader("Subir Excel", type=["xlsx"])

        if archivo is not None:

            df_user = pd.read_excel(archivo)

            columnas = ["PK_ARTICULO","ACCION","COMENTARIO"]

            if not all(c in df_user.columns for c in columnas):
                st.error("Archivo incorrecto")
                st.stop()

            df_user["ACTIVIDAD_COMERCIAL"] = actividad

            nombre = f"data/trabajo_{usuario}_{actividad}.parquet"

            df_user[["PK_ARTICULO","ACTIVIDAD_COMERCIAL","ACCION","COMENTARIO"]].to_parquet(nombre)

            st.success("Archivo listo")

            if st.button("CARGAR CAMBIOS"):
                consolidar(actividad)
                st.success("Cambios aplicados")

    # ======================================================
    # MASTER ANALIZA
    # ======================================================
    if rol == "MASTER":

        st.markdown("### 📊 Analizar")

        if st.button("ANALIZAR"):
            st.write(df.describe())

        st.markdown("### 📦 Descargar MASTER")

        if st.button("DESCARGAR PARQUET"):
            st.download_button(
                "Descargar",
                data=open(RUTA_MASTER,"rb"),
                file_name="MASTER.parquet"
            )
