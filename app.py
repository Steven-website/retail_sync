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


# =========================================
# CREAR MASTER VACIO
# =========================================
def crear_master_vacio():

    columnas = [
        "PK_ARTICULO","ACTIVIDAD_COMERCIAL",
        "FAMILIA","CATEGORIA","SUBCATEGORIA",
        "NO_ARTI","DESCRIPCION","TIPO_CLASIF",
        "COMPRA_Q_2024","COMPRA_Q_2025","COMPRA_Q_2026",
        "VTA_YTD_2024","VTA_YTD_2025","VTA_YTD_2026",
        "VTA_Q_YTD_2024","VTA_Q_YTD_2025","VTA_Q_YTD_2026",
        "INVENTARIO_Q",
        "MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO","PORC_AHORRO",
        "FECHA_INICIO","FECHA_FIN",
        "ACCION","COMENTARIO"
    ]

    df = pd.DataFrame(columns=columnas)
    df.to_parquet(RUTA_MASTER,index=False)


if not os.path.exists(RUTA_MASTER):
    crear_master_vacio()


# =========================================
# ACTUALIZAR MASTER DESDE BD_ACTUALIZACION
# =========================================
def actualizar_master():

    if not os.path.exists(RUTA_MASTER):
        return

    if not os.path.exists(RUTA_BD):
        return

    master = pd.read_parquet(RUTA_MASTER)
    bd = pd.read_parquet(RUTA_BD)

    if master.empty:
        return

    columnas_comerciales = [
        "MUNDO_AC","PRECIO_PROMOCIONAL","DESCUENTO","PORC_AHORRO",
        "FECHA_INICIO","FECHA_FIN","ACCION","COMENTARIO"
    ]

    lista = []

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    for ac in actividades:

        base_ac = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        nuevo = bd.copy()
        nuevo["ACTIVIDAD_COMERCIAL"] = ac

        nuevo = nuevo.merge(
            base_ac[["PK_ARTICULO"] + columnas_comerciales],
            on="PK_ARTICULO",
            how="left"
        )

        lista.append(nuevo)

    master_nuevo = pd.concat(lista,ignore_index=True)

    master_nuevo.to_parquet(RUTA_MASTER,index=False)


# =========================================
# CONSOLIDAR CAMBIOS
# =========================================
def consolidar(actividad):

    master = pd.read_parquet(RUTA_MASTER)

    for archivo in os.listdir("data"):

        if archivo.startswith("trabajo_") and actividad in archivo:

            ruta = os.path.join("data",archivo)
            temp = pd.read_parquet(ruta)

            master = master.merge(
                temp,
                on=["PK_ARTICULO","ACTIVIDAD_COMERCIAL"],
                how="left",
                suffixes=("","_NEW")
            )

            if "ACCION_NEW" in master.columns:
                master["ACCION"] = master["ACCION_NEW"].combine_first(master["ACCION"])

            if "COMENTARIO_NEW" in master.columns:
                master["COMENTARIO"] = master["COMENTARIO_NEW"].combine_first(master["COMENTARIO"])

            master = master.drop(columns=[c for c in master.columns if "_NEW" in c])

    master.to_parquet(RUTA_MASTER,index=False)


# =========================================
# LOGIN
# =========================================
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

    st.success(f"Bienvenido {usuario}")

    # =====================================
    # MASTER VACIO
    # =====================================
    if master.empty:

        if rol != "MASTER":
            st.warning("No existen Actividades Comerciales")
            st.stop()

        st.markdown("### Crear Primera Actividad")

        nueva = st.text_input("Nombre Actividad")

        if st.button("CREAR ACTIVIDAD"):

            bd = pd.read_parquet(RUTA_BD)

            bd["ACTIVIDAD_COMERCIAL"] = nueva
            bd["MUNDO_AC"]=None
            bd["PRECIO_PROMOCIONAL"]=None
            bd["DESCUENTO"]=None
            bd["PORC_AHORRO"]=None
            bd["FECHA_INICIO"]=None
            bd["FECHA_FIN"]=None
            bd["ACCION"]=None
            bd["COMENTARIO"]=None

            bd.to_parquet(RUTA_MASTER,index=False)

            st.success("Actividad creada")
            st.rerun()

    else:

        if rol == "MASTER":

            st.markdown("### Crear nueva Actividad")

            nueva = st.text_input("Nueva AC")

            if st.button("CREAR"):

                bd = pd.read_parquet(RUTA_BD)

                bd["ACTIVIDAD_COMERCIAL"] = nueva
                bd["MUNDO_AC"]=None
                bd["PRECIO_PROMOCIONAL"]=None
                bd["DESCUENTO"]=None
                bd["PORC_AHORRO"]=None
                bd["FECHA_INICIO"]=None
                bd["FECHA_FIN"]=None
                bd["ACCION"]=None
                bd["COMENTARIO"]=None

                master = pd.concat([master,bd])
                master.to_parquet(RUTA_MASTER,index=False)

                st.success("Actividad creada")
                st.rerun()

        actividades = master["ACTIVIDAD_COMERCIAL"].unique()

        actividad = st.selectbox("Seleccione Actividad",actividades)

        df = master[master["ACTIVIDAD_COMERCIAL"] == actividad].copy()

        if rol == "ADC":
            fam = st.session_state.user.get("familia","ALL")
            if fam != "ALL":
                df = df[df["FAMILIA"] == fam]

        st.dataframe(df,use_container_width=True)

        # =====================================
        # DESCARGAR EXCEL
        # =====================================
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

        # =====================================
        # SUBIR
        # =====================================
        if rol in ["ADC","JEFE_ADC"]:

            archivo = st.file_uploader("Subir Excel")

            if archivo:

                df_user = pd.read_excel(archivo)
                df_user["ACTIVIDAD_COMERCIAL"] = actividad

                ruta = f"data/trabajo_{usuario}_{actividad}.parquet"

                df_user[["PK_ARTICULO","ACTIVIDAD_COMERCIAL","ACCION","COMENTARIO"]].to_parquet(ruta,index=False)

                st.success("Archivo cargado")

                if st.button("CARGAR CAMBIOS"):
                    consolidar(actividad)
                    st.success("Cambios aplicados")

        # =====================================
        # DESCARGA MASTER
        # =====================================
        if rol == "MASTER":

            st.markdown("### Descargar Parquet")

            col1,col2 = st.columns(2)

            with col1:
                with open(RUTA_MASTER,"rb") as f:
                    st.download_button(
                        "MASTER TOTAL",
                        f,
                        file_name="MASTER.parquet"
                    )

            with col2:
                df_ac = master[master["ACTIVIDAD_COMERCIAL"] == actividad]

                buffer_parquet = io.BytesIO()
                df_ac.to_parquet(buffer_parquet,index=False)
                buffer_parquet.seek(0)

                st.download_button(
                    "Solo Actividad",
                    buffer_parquet,
                    file_name=f"{actividad}.parquet"
                )
