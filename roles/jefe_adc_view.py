import streamlit as st
import pandas as pd

from data_manager import *
from config import *


def jefe_adc_view():

    st.header("🧠 ROL JEFE ADC")

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
    # CONSOLIDAR
    # ===============================
    if st.button("Consolidar"):

        df = consolidar(ac)
        st.success("Consolidación realizada")

    # ===============================
    # FILTRO VISUAL FAMILIAS
    # ===============================
    familias_existentes = sorted(df["FAMILIA"].unique())

    familias_sel = st.multiselect(
        "Filtrar familias",
        familias_existentes,
        default=familias_usuario
    )

    if familias_sel:
        df = df[df["FAMILIA"].isin(familias_sel)]

    st.dataframe(df, width="stretch")

    # ===============================
    # DESCARGAR
    # ===============================
    st.download_button(
        "⬇ Descargar Excel",
        data=df.to_csv(index=False).encode(),
        file_name=f"{ac}_JEFE_ADC.csv"
    )