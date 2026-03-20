import streamlit as st
import pandas as pd
import json
import io
import os
from datetime import datetime, timedelta
from openpyxl import Workbook

# =============================
# CONFIG
# =============================
st.set_page_config(page_title="Retail Sync", layout="wide")
st.title("🛒 Retail Sync")

RUTA_MASTER = "data/master.parquet"
CARPETA_VERSIONES = "data/versiones"
ARCHIVO_CONTROL = "data/control_consolidacion.json"

# =============================
# LIMPIAR VERSIONES >7 DIAS
# =============================
def limpiar_versiones():

    if not os.path.exists(CARPETA_VERSIONES):
        return

    ahora = datetime.now()
    limite = ahora - timedelta(days=7)

    for archivo in os.listdir(CARPETA_VERSIONES):

        if archivo.startswith("master_"):

            ruta = os.path.join(CARPETA_VERSIONES, archivo)

            fecha_txt = archivo.replace("master_", "").replace(".parquet","")

            try:
                fecha_archivo = datetime.strptime(fecha_txt,"%Y%m%d_%H%M%S")

                if fecha_archivo < limite:
                    os.remove(ruta)

            except:
                pass

# =============================
# CONSOLIDAR MASTER
# =============================
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

    limpiar_versiones()

    master.to_parquet(RUTA_MASTER)

# =============================
# CONSOLIDACIÓN AUTOMÁTICA
# =============================
def consolidacion_automatica():

    hoy = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(ARCHIVO_CONTROL):

        with open(ARCHIVO_CONTROL) as f:
            data = json.load(f)

        if data.get("ultima_consolidacion") == hoy:
            return

    consolidar_master()

    control = {"ultima_consolidacion": hoy}

    with open(ARCHIVO_CONTROL,"w") as f:
        json.dump(control,f)

# =============================
# PANEL ADMIN
# =============================
def panel_admin():

    st.markdown("## 🎯 Retail Control Center")

    datos = []

    master = pd.read_parquet(RUTA_MASTER)
    total = len(master)

    for archivo in os.listdir("data"):

        if archivo.startswith("trabajo_"):

            usuario = archivo.replace("trabajo_", "").replace(".parquet","")

            df = pd.read_parquet(f"data/{archivo}")

            editados = df[
                (df["ACCION"].notna()) |
                (df["COMENTARIO"].notna())
            ].shape[0]

            pendientes = total - editados
            avance = round(editados / total * 100, 2)

            # semaforo
            if avance < 30:
                estado = "🔴"
            elif avance < 70:
                estado = "🟡"
            else:
                estado = "🟢"

            datos.append({
                "Usuario": usuario,
                "Editados": editados,
                "Pendientes": pendientes,
                "Avance %": avance,
                "Estado": estado
            })

    if len(datos) == 0:
        st.info("No hay trabajos aún")
        return

    df_dash = pd.DataFrame(datos)

    # =========================
    # KPIs
    # =========================
    col1, col2, col3 = st.columns(3)

    col1.metric(
        "👥 Usuarios trabajando",
        len(df_dash)
    )

    col2.metric(
        "📦 Artículos totales",
        total
    )

    avance_global = round(df_dash["Editados"].sum() / (total * len(df_dash)) * 100, 2)

    col3.metric(
        "🚀 Avance Global %",
        avance_global
    )

    st.divider()

    # =========================
    # RANKING
    # =========================
    st.markdown("### 🏆 Ranking avance")

    ranking = df_dash.sort_values("Avance %", ascending=False)

    st.dataframe(ranking, use_container_width=True)

    # =========================
    # BARRAS
    # =========================
    st.markdown("### 📊 Avance por usuario")

    st.bar_chart(
        ranking.set_index("Usuario")["Avance %"]
    )

    # =========================
    # PROGRESO VISUAL
    # =========================
    st.markdown("### 📈 Progreso detallado")

    for _, row in ranking.iterrows():

        st.write(f"{row['Estado']} {row['Usuario']}")

        st.progress(
            min(row["Avance %"] / 100, 1.0)
        )

    # =========================
    # PENDIENTES
    # =========================
    st.markdown("### ⏳ Pendientes por usuario")

    st.bar_chart(
        ranking.set_index("Usuario")["Pendientes"]
    )

# =============================
# LOGIN STATE
# =============================
if "login" not in st.session_state:
    st.session_state.login = False

# =============================
# LOGIN
# =============================
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

# =============================
# SISTEMA
# =============================
else:

    consolidacion_automatica()

    usuario = st.session_state.user["usuario"]
    rol = st.session_state.user["rol"]

    st.success(f"Bienvenido {usuario}")

    df = pd.read_parquet(RUTA_MASTER)

    if st.session_state.user["filtro"] != "ALL":
        df = df[df["SUBCATEGORIA"] == st.session_state.user["filtro"]]

    st.dataframe(df)

    # =============================
    # DESCARGAR
    # =============================
    st.markdown("### 📥 Descargar archivo de trabajo")

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

    # =============================
    # SUBIR TRABAJO
    # =============================
    st.markdown("### 📤 Subir trabajo")

    archivo = st.file_uploader("Subir Excel", type=["xlsx"])

    if archivo is not None:

        df_user = pd.read_excel(archivo)

        columnas = ["PK_ARTICULO","ACCION","COMENTARIO"]

        if not all(col in df_user.columns for col in columnas):
            st.error("Archivo incorrecto")
            st.stop()

        df_user[columnas].to_parquet(f"data/trabajo_{usuario}.parquet")

        st.success("Trabajo guardado")

    # =============================
    # PANEL + CONSOLIDACIÓN MASTER
    # =============================
    if rol == "MASTER":

        panel_admin()

        st.markdown("### 🔧 Consolidación")

        if st.button("Consolidar ahora"):
            consolidar_master()
            st.success("Master consolidado manualmente")