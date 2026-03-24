import streamlit as st
import pandas as pd
import json
import os
import io

st.set_page_config(layout="wide")
st.title("🛒 RETAIL SYNC")

# =========================
# RUTAS
# =========================

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

# =========================
# FUNCIONES GENERALES
# =========================

def df_to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()


def cargar_bd():
    if not os.path.exists(RUTA_BD):
        st.error("No existe BD_ACTUALIZACION")
        st.stop()

    df = pd.read_parquet(RUTA_BD)

    if PK not in df.columns:
        st.error("BD_ACTUALIZACION sin PK_Articulos")
        st.stop()

    return df


def cargar_master():
    if not os.path.exists(RUTA_MASTER):
        return pd.DataFrame()

    return pd.read_parquet(RUTA_MASTER)


def guardar_master(df):
    df.to_parquet(RUTA_MASTER, index=False)


# =========================
# LOGIN
# =========================

if "login" not in st.session_state:
    st.session_state.login = False

if not st.session_state.login:

    users = json.load(open(RUTA_USERS))

    u = st.text_input("Usuario")
    p = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        for user in users:

            if user["usuario"] == u and user["password"] == p:

                st.session_state.login = True
                st.session_state.rol = user["rol"]
                st.session_state.usuario = u
                st.session_state.familias = user.get("FAMILIAS", [])
                st.rerun()

    st.stop()

# =========================
# SIDEBAR
# =========================

st.sidebar.success(st.session_state.usuario)
st.sidebar.info(st.session_state.rol)

if st.sidebar.button("Cerrar sesión"):
    st.session_state.clear()
    st.rerun()

rol = st.session_state.rol

master = cargar_master()

# =========================
# MASTER
# =========================

if rol == "MASTER":

    st.header("ROL MASTER")

    st.subheader("1️⃣ Crear Actividad")

    nueva = st.text_input("Nombre Actividad")

    if st.button("Crear"):

        bd = cargar_bd()

        nueva_base = bd.copy()
        nueva_base.insert(1, "ACTIVIDAD_COMERCIAL", nueva)

        for c in COLUMNAS_COMERCIALES:
            nueva_base[c] = None

        master = pd.concat([master, nueva_base])
        guardar_master(master)

        st.success("Actividad creada")
        st.rerun()

    st.subheader("2️⃣ Regenerar bases")

    if st.button("Regenerar"):

        bd = cargar_bd()

        actividades = master["ACTIVIDAD_COMERCIAL"].unique()

        nuevo = []

        for ac in actividades:

            base_ac = master[master["ACTIVIDAD_COMERCIAL"] == ac]

            temp = bd.copy()
            temp.insert(1, "ACTIVIDAD_COMERCIAL", ac)

            cols_keep = [PK] + COLUMNAS_COMERCIALES

            temp = temp.merge(
                base_ac[cols_keep],
                on=PK,
                how="left"
            )

            nuevo.append(temp)

        master = pd.concat(nuevo)
        guardar_master(master)

        st.success("Bases regeneradas")

    st.subheader("3️⃣ Descargar MASTER")

    st.download_button(
        "⬇️ Descargar MASTER",
        master.to_parquet(index=False),
        "MASTER.parquet"
    )

# =========================
# JEFE ADC
# =========================

elif rol == "JEFE_ADC":

    st.header("ROL JEFE ADC")

    actividades = master["ACTIVIDAD_COMERCIAL"].unique()

    ac = st.selectbox("Actividad", actividades)

    if st.button("Consolidar"):

        df = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        familias = st.session_state.familias

        df = df[df["FAMILIA"].isin(familias)]

        excel = df_to_excel(df)

        st.download_button(
            "⬇️ Descargar Consolidado",
            excel,
            f"{ac}_CONSOLIDADO.xlsx"
        )

# =========================
# ADC
# =========================

elif rol == "ADC":

    st.header("ROL ADC")

    actividades = master["ACTIVIDAD_COMERCIAL"].unique()

    ac = st.selectbox("Actividad", actividades)

    df = master[
        (master["ACTIVIDAD_COMERCIAL"] == ac) &
        (master["FAMILIA"].isin(st.session_state.familias))
    ]

    excel = df_to_excel(df)

    st.download_button(
        "⬇️ Descargar Excel",
        excel,
        f"{ac}.xlsx"
    )

    archivo = st.file_uploader("Subir Excel trabajado")

    if archivo:

        cambios = pd.read_excel(archivo)

        mask = master["ACTIVIDAD_COMERCIAL"] == ac

        master_ac = master[mask].set_index(PK)
        cambios = cambios.set_index(PK)

        master_ac.update(cambios[COLUMNAS_COMERCIALES])

        master.update(master_ac)

        guardar_master(master)

        st.success("Actualizado")

# =========================
# PRECIOS Y MARKETING
# =========================

elif rol in ["PRECIOS", "MARKETING"]:

    st.header(f"ROL {rol}")

    actividades = master["ACTIVIDAD_COMERCIAL"].unique()

    ac = st.selectbox("Actividad", actividades)

    if st.button("Consolidar"):

        df = master[master["ACTIVIDAD_COMERCIAL"] == ac]

        excel = df_to_excel(df)

        st.download_button(
            "⬇️ Descargar Consolidado",
            excel,
            f"{ac}_CONSOLIDADO.xlsx"
        )
