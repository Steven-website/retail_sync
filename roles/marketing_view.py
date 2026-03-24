import streamlit as st
import pandas as pd

from data_manager import *
from config import *


def marketing_view():

    st.header("📢 ROL MARKETING")

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

    # ===============================
    # CONSOLIDAR
    # ===============================
    if st.button("Consolidar"):

        df = consolidar(ac)
        st.success("Actividad consolidada")

    else:
        df = dataset_actividad(ac)

    st.dataframe(df, width="stretch")

    # ===============================
    # DESCARGAR
    # ===============================
    st.download_button(
        "⬇ Descargar Excel consolidado",
        data=df.to_csv(index=False).encode(),
        file_name=f"{ac}_MARKETING.csv"
    )