import streamlit as st
import pandas as pd
import json
import os

st.set_page_config(layout="wide")
st.title("🛒 Retail Sync")

RUTA_BD = "data/BD_ACTUALIZACION.parquet"
RUTA_MASTER = "data/master.parquet"
RUTA_USERS = "usuarios.json"

PK = "PK_Articulos"

COLUMNAS_COMERCIALES = [
    "MUNDO_AC",
    "PRECIO_PROMOCIONAL",
    "DESCUENTO",
    "PORC_AHORRO",
    "FECHA_INICIO",
    "FECHA_FIN",
    "ACCION",
    "COMENTARIO"
]

# ================= LOGIN =================

if "login" not in st.session_state:
    st.session_state.login = False

if not st.session_state.login:

    with open(RUTA_USERS) as f:
        users = json.load(f)

    u = st.text_input("Usuario")
    p = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        for user in users:
            if user["usuario"] == u and user["password"] == p:
                st.session_state.login = True
                st.session_state.rol = user["rol"]
                st.session_state.usuario = u
                st.rerun()

    st.stop()

rol = st.session_state.rol

st.sidebar.success(st.session_state.usuario)
st.sidebar.info(rol)

# ================= BD =================

def cargar_bd():

    if not os.path.exists(RUTA_BD):
        st.error("No existe BD_ACTUALIZACION")
        st.stop()

    df = pd.read_parquet(RUTA_BD)

    if PK not in df.columns:
        st.error("BD sin PK_Articulos")
        st.stop()

    return df


# ================= MASTER =================

def crear_master():

    bd = cargar_bd()

    master = bd.copy()
    master.insert(1, "ACTIVIDAD_COMERCIAL", "")

    for c in COLUMNAS_COMERCIALES:
        master[c] = None

    master.to_parquet(RUTA_MASTER, index=False)

    return master


def cargar_master():

    if not os.path.exists(RUTA_MASTER):
        return crear_master()

    master = pd.read_parquet(RUTA_MASTER)

    for c in COLUMNAS_COMERCIALES:
        if c not in master.columns:
            master[c] = None

    return master


# ================= ACTUALIZAR =================

def actualizar_master():

    if not os.path.exists(RUTA_MASTER):
        return

    bd = cargar_bd()
    master = cargar_master()

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    nuevo_master = []

    for ac in actividades:

        if ac == "":
            continue

        base_ac = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        nuevo = bd.copy()
        nuevo.insert(1, "ACTIVIDAD_COMERCIAL", ac)

        # merge tolerante
        if PK in base_ac.columns:

            cols_usuario = [PK] + [c for c in COLUMNAS_COMERCIALES if c in base_ac.columns]

            base_usuario = base_ac[cols_usuario]

            nuevo = nuevo.merge(base_usuario, on=PK, how="left")

        nuevo_master.append(nuevo)

    if len(nuevo_master) > 0:
        master = pd.concat(nuevo_master)
        master.to_parquet(RUTA_MASTER, index=False)


# ================= CONSOLIDAR =================

def consolidar(ac):

    archivo = st.file_uploader("Subir base modificada", type="parquet")

    if archivo is not None:

        cambios = pd.read_parquet(archivo)
        master = cargar_master()

        mask = master["ACTIVIDAD_COMERCIAL"] == ac

        master_ac = master[mask]

        cols = [c for c in COLUMNAS_COMERCIALES if c in cambios.columns]

        master_ac = master_ac.set_index(PK)
        cambios = cambios.set_index(PK)

        master_ac.update(cambios[cols])

        master.update(master_ac)

        master.to_parquet(RUTA_MASTER, index=False)

        st.success("Cambios aplicados")


# ================= EJECUCION =================

actualizar_master()

master = cargar_master()

# ================= MASTER =================

if rol == "MASTER":

    st.header("ROL MASTER")

    nueva = st.text_input("Nueva Actividad Comercial")

    if st.button("Crear Actividad"):

        if nueva != "":

            bd = cargar_bd()

            nueva_base = bd.copy()
            nueva_base.insert(1, "ACTIVIDAD_COMERCIAL", nueva)

            for c in COLUMNAS_COMERCIALES:
                nueva_base[c] = None

            master = pd.concat([master, nueva_base])

            master.to_parquet(RUTA_MASTER, index=False)

            st.success("Actividad creada")
            st.rerun()

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()
    actividades = [a for a in actividades if a != ""]

    if len(actividades) > 0:

        ac = st.selectbox("Actividad", actividades)

        df = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        st.dataframe(df)

        st.download_button(
            "Descargar AC",
            df.to_parquet(index=False),
            file_name=f"{ac}.parquet"
        )

    st.download_button(
        "Descargar MASTER TOTAL",
        master.to_parquet(index=False),
        file_name="MASTER.parquet"
    )

# ================= USUARIOS =================

else:

    st.header("ROL USUARIO")

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()
    actividades = [a for a in actividades if a != ""]

    if len(actividades) == 0:
        st.warning("No existen actividades")
        st.stop()

    ac = st.selectbox("Actividad", actividades)

    df = master[master["ACTIVIDAD_COMERCIAL"] == ac]

    st.download_button(
        "Descargar Base",
        df.to_parquet(index=False),
        file_name=f"{ac}.parquet"
    )

    if rol in ["ADC","JEFE_ADC"]:
        consolidar(ac)
