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


# ======================================================
# CREAR MASTER VACIO SI NO EXISTE
# ======================================================
if not os.path.exists(RUTA_MASTER):

    columnas = [
        "PK_ARTICULO","ACTIVIDAD_COMERCIAL","FAMILIA","CATEGORIA","SUBCATEGORIA",
        "NO_ARTI","DESCRIPCION","TIPO_CLASIF",
        "COMPRA_Q_2024","COMPRA_Q_2025","COMPRA_Q_2026",
        "VTA_YTD_2024","VTA_YTD_2025","VTA_YTD_2026",
        "VTA_Q_YTD_2024","VTA_Q_YTD_2025","VTA_Q_YTD_2026",
        "INVENTARIO_Q",
        "MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO","PORC_AHORRO",
        "FECHA_INICIO","FECHA_FIN",
        "ACCION","COMENTARIO"
    ]

    pd.DataFrame(columns=columnas).to_parquet(RUTA_MASTER)


# ======================================================
# ACTUALIZAR MASTER DESDE BD
# ======================================================
def actualizar_master():

    master = pd.read_parquet(RUTA_MASTER)

    if master.empty:
        return

    if not os.path.exists(RUTA_BD):
        return

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
        if col+"_NEW" in master.columns:
            master[col] = master[col+"_NEW"].combine_first(master[col])

    master = master.drop(columns=[c for c in master.columns if "_NEW" in c])

    master.to_parquet(RUTA_MASTER)


# ======================================================
# CONSOLIDAR
# ======================================================
def consolidar(actividad):

    master = pd.read_parquet(RUTA_MASTER)

    for archivo in os.listdir("data"):

        if archivo.startswith("trabajo_") and actividad in archivo:

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


# ======================================================
# LOGIN
# ======================================================
if "login" not in st.session_state:
    st.session_state.login = False

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

else:

    actualizar_master()

    usuario = st.session_state.user["usuario"]
    rol = st.session_state.user["rol"]

    master = pd.read_parquet(RUTA_MASTER)

    st.success(f"Bienvenido {usuario}")

    # ==================================================
    # MASTER VACIO
    # ==================================================
    if master.empty:

        if rol != "MASTER":
            st.warning("No existen Actividades Comerciales creadas")
            st.stop()

        st.markdown("### 🎯 Crear Primera Actividad")

        nueva_ac = st.text_input("Nombre Actividad")

        if st.button("CREAR ACTIVIDAD"):

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

            estructura = pd.read_parquet(RUTA_MASTER).columns

            for col in estructura:
                if col not in bd.columns:
                    bd[col] = None

            bd = bd[estructura]

            bd.to_parquet(RUTA_MASTER)

            st.success("Actividad creada")
            st.rerun()

    # ==================================================
    # SISTEMA NORMAL
    # ==================================================
    else:

        if rol == "MASTER":

            st.markdown("### 🎯 Crear Actividad")

            nueva_ac = st.text_input("Nueva Actividad")

            if st.button("CREAR"):

                if nueva_ac in master["ACTIVIDAD_COMERCIAL"].unique():
                    st.error("Actividad ya existe")
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

        actividades = master["ACTIVIDAD_COMERCIAL"].unique()

        actividad = st.selectbox("Seleccione Actividad", actividades)

        df = master[master["ACTIVIDAD_COMERCIAL"] == actividad]

        if rol == "ADC":
            fam = st.session_state.user["familia"]
            df = df[df["FAMILIA"] == fam]

        st.dataframe(df, use_container_width=True)

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

        if rol in ["ADC","JEFE_ADC"]:

            archivo = st.file_uploader("Subir Excel", type=["xlsx"])

            if archivo:

                df_user = pd.read_excel(archivo)

                df_user["ACTIVIDAD_COMERCIAL"] = actividad

                df_user[
                    ["PK_ARTICULO","ACTIVIDAD_COMERCIAL","ACCION","COMENTARIO"]
                ].to_parquet(
                    f"data/trabajo_{usuario}_{actividad}.parquet"
                )

                if st.button("CARGAR CAMBIOS"):
                    consolidar(actividad)
                    st.success("Cambios aplicados")
