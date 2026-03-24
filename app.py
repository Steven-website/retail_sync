import streamlit as st
import pandas as pd
import os
import io

st.set_page_config(layout="wide")
st.title("🛒 Retail Sync")

# =============================
# RUTAS
# =============================
RUTA_BD = "data/BD_ACTUALIZACION.parquet"
RUTA_MASTER = "data/master.parquet"

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

# =============================
# CARGAR BD UNIVERSO
# =============================
def cargar_bd():

    if not os.path.exists(RUTA_BD):
        st.error("❌ No existe BD_ACTUALIZACION")
        st.stop()

    df = pd.read_parquet(RUTA_BD)

    if PK not in df.columns:
        st.error("❌ BD_ACTUALIZACION no tiene PK_Articulos")
        st.stop()

    return df


# =============================
# CREAR MASTER VACIO
# =============================
def crear_master_vacio():

    bd = cargar_bd()

    master = bd.copy()
    master.insert(1, "ACTIVIDAD_COMERCIAL", "")

    for c in COLUMNAS_COMERCIALES:
        master[c] = None

    master.to_parquet(RUTA_MASTER, index=False)

    return master


def cargar_master():

    if not os.path.exists(RUTA_MASTER):
        return crear_master_vacio()

    return pd.read_parquet(RUTA_MASTER)


# =============================
# RECONSTRUIR MASTER
# =============================
def actualizar_master():

    if not os.path.exists(RUTA_MASTER):
        return

    bd = cargar_bd()
    master = cargar_master()

    if master.empty:
        return

    actividades = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    nuevo_master = []

    for ac in actividades:

        if ac == "":
            continue

        base_ac = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        nuevo = bd.copy()
        nuevo.insert(1, "ACTIVIDAD_COMERCIAL", ac)

        cols_usuario = [PK] + COLUMNAS_COMERCIALES

        base_usuario = base_ac[cols_usuario]

        nuevo = nuevo.merge(
            base_usuario,
            on=PK,
            how="left",
            suffixes=("","_old")
        )

        nuevo_master.append(nuevo)

    if len(nuevo_master) > 0:
        master = pd.concat(nuevo_master)
        master.to_parquet(RUTA_MASTER, index=False)


# =============================
# CONSOLIDAR CAMBIOS USUARIO
# =============================
def consolidar(actividad):

    archivo = st.file_uploader("Subir archivo modificado", type="parquet")

    if archivo is not None:

        cambios = pd.read_parquet(archivo)
        master = cargar_master()

        mask = master["ACTIVIDAD_COMERCIAL"] == actividad

        master_ac = master[mask]

        cols_editables = [c for c in cambios.columns if c in COLUMNAS_COMERCIALES]

        master_ac = master_ac.set_index(PK)
        cambios = cambios.set_index(PK)

        master_ac.update(cambios[cols_editables])

        master.update(master_ac)

        master.to_parquet(RUTA_MASTER, index=False)

        st.success("✅ Cambios aplicados")


# =============================
# LOGIN SIMPLE
# =============================
usuarios = {
    "steven":"MASTER",
    "adc":"ADC",
    "jefe":"JEFE_ADC",
    "precios":"PRECIOS",
    "marketing":"MARKETING"
}

usuario = st.sidebar.selectbox("Usuario", list(usuarios.keys()))
rol = usuarios[usuario]

st.sidebar.write("ROL:", rol)

# =============================
# ACTUALIZAR MASTER AL ENTRAR
# =============================
actualizar_master()

master = cargar_master()

# =============================
# ROL MASTER
# =============================
if rol == "MASTER":

    st.header("ROL MASTER")

    nueva = st.text_input("Crear nueva Actividad Comercial")

    if st.button("CREAR ACTIVIDAD"):

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
            "⬇️ Descargar esta AC",
            df.to_parquet(index=False),
            file_name=f"{ac}.parquet"
        )

    st.subheader("DESCARGA TOTAL")

    st.download_button(
        "⬇️ Descargar MASTER completo",
        master.to_parquet(index=False),
        file_name="MASTER_TOTAL.parquet"
    )

# =============================
# ROLES USUARIO
# =============================
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
        "⬇️ Descargar Base",
        df.to_parquet(index=False),
        file_name=f"{ac}.parquet"
    )

    if rol in ["ADC","JEFE_ADC"]:
        consolidar(ac)
