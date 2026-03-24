import streamlit as st
import pandas as pd
import json
import os

# =========================
# RUTAS
# =========================

RUTA_MASTER = "data/master.parquet"
RUTA_BD = "data/BD_ACTUALIZACION.parquet"
RUTA_USERS = "usuarios.json"

st.set_page_config(layout="wide")

# =========================
# LOGIN
# =========================

with open(RUTA_USERS) as f:
    users = json.load(f)

if "login" not in st.session_state:
    st.session_state.login = False

if not st.session_state.login:

    st.title("Retail Sync")

    u = st.text_input("Usuario")
    p = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        if u in users and users[u]["password"] == p:

            st.session_state.login = True
            st.session_state.user = u
            st.session_state.rol = users[u]["rol"]
            st.rerun()

        else:
            st.error("Credenciales incorrectas")

    st.stop()

usuario = st.session_state.user
rol = st.session_state.rol

st.sidebar.write("Usuario:", usuario)
st.sidebar.write("Rol:", rol)

# =========================
# CARGAR MASTER
# =========================

if os.path.exists(RUTA_MASTER):
    master = pd.read_parquet(RUTA_MASTER)
else:
    st.error("No existe master.parquet")
    st.stop()

# =========================
# PANEL MASTER
# =========================

if rol == "MASTER":

    st.title("👑 PANEL MASTER")

    # CREAR ACTIVIDAD
    st.markdown("### 1 Crear Actividad")

    nombre = st.text_input("Nombre Actividad")

    if st.button("Crear Actividad"):

        if nombre.strip() != "":

            master["ACTIVIDAD_COMERCIAL"] = master["ACTIVIDAD_COMERCIAL"].fillna("")
            master.loc[
                master["ACTIVIDAD_COMERCIAL"] == "",
                "ACTIVIDAD_COMERCIAL"
            ] = nombre

            master.to_parquet(RUTA_MASTER)
            st.success("Actividad creada")
            st.rerun()

    # REGENERAR MASTER
    st.markdown("### 2 Regenerar bases")

    if st.button("Regenerar MASTER"):

        bd = pd.read_parquet(RUTA_BD)

        cols_trabajo = []

        if "ACTIVIDAD_COMERCIAL" in master.columns:
            cols_trabajo.append("ACTIVIDAD_COMERCIAL")

        master_aux = master[["PK_Articulos"] + cols_trabajo]

        nuevo = bd.merge(master_aux, how="left", on="PK_Articulos")

        nuevo.to_parquet(RUTA_MASTER)

        st.success("Master reconstruido")
        st.rerun()

    # DESCARGAR MASTER
    st.markdown("### 3 Descargar MASTER")

    st.download_button(
        "Descargar MASTER",
        master.to_csv(index=False),
        file_name="MASTER.csv"
    )

    # ELIMINAR ACTIVIDAD
    st.markdown("### 4 Eliminar Actividad")

    acts = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    if len(acts) > 0:

        act = st.selectbox("Seleccione actividad", acts)

        if st.button("Eliminar"):

            master = master[
                master["ACTIVIDAD_COMERCIAL"] != act
            ]

            master.to_parquet(RUTA_MASTER)

            st.success("Actividad eliminada")
            st.rerun()

# =========================
# PANEL USUARIOS
# =========================

else:

    st.title("Panel Usuario")

    acts = master["ACTIVIDAD_COMERCIAL"].dropna().unique()

    if len(acts) == 0:
        st.info("No hay actividades")
        st.stop()

    act = st.selectbox("Seleccione Actividad", acts)

    df = master[master["ACTIVIDAD_COMERCIAL"] == act]

    st.write("Registros:", len(df))

    # DESCARGAR EXCEL
    st.download_button(
        "Descargar Excel",
        df.to_csv(index=False),
        file_name=f"{usuario}_{act}.csv"
    )

    # SUBIR CAMBIOS
    st.markdown("### Subir Cambios")

    archivo = st.file_uploader("Cargar archivo trabajado", type=["csv"])

    if archivo:

        df_user = pd.read_csv(archivo)

        master = master.drop(
            master[master["ACTIVIDAD_COMERCIAL"] == act].index
        )

        master = pd.concat([master, df_user])

        master.to_parquet(RUTA_MASTER)

        st.success("Cambios aplicados")
        st.rerun()
