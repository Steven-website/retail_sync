import streamlit as st
import pandas as pd

from data_manager import *
from config import *


def adc_view():

    st.header("🧑‍💻 ROL ADC")

    familias_usuario = st.session_state.familias

    actividades = obtener_actividades()

    if not actividades:
        st.warning("No existen actividades")
        return

    # ===============================
    # SELECCIONAR ACTIVIDAD
    # ===============================
    ac = st.selectbox(
        "Seleccione actividad",
        actividades
    )

    df = dataset_actividad(ac)

    # ===============================
    # FILTRO FAMILIAS
    # ===============================
    df = filtrar_familias(df, familias_usuario)

    st.subheader("Base operativa")

    st.dataframe(df, width="stretch")

    # ===============================
    # DESCARGAR EXCEL
    # ===============================
    st.subheader("Descargar")

    st.download_button(
        "⬇ Descargar Excel",
        data=df.to_csv(index=False).encode(),
        file_name=f"{ac}_ADC.csv"
    )

    st.divider()

    # ===============================
    # SUBIR EXCEL
    # ===============================
    st.subheader("Subir archivo trabajado")

    file = st.file_uploader(
        "Subir Excel",
        type=["xlsx"]
    )

    if file:

        if st.button("Actualizar"):

            try:
                actualizar_desde_excel(file, ac)
                st.success("Actualización aplicada")
            except Exception as e:
                st.error(e)