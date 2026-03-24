import streamlit as st
import pandas as pd
from io import BytesIO

from data_manager import dataset_actividad, obtener_actividades, consolidar


# ===============================
# NORMALIZAR TEXTO
# ===============================
def normalizar(txt):

    if txt is None:
        return ""

    return (
        str(txt)
        .upper()
        .strip()
        .replace("Á","A")
        .replace("É","E")
        .replace("Í","I")
        .replace("Ó","O")
        .replace("Ú","U")
    )


def jefe_adc_view():

    st.header("🧠 Rol JEFE ADC")

    # ===============================
    # VALIDAR SESSION
    # ===============================
    if "familias" not in st.session_state:
        st.error("Usuario sin familias asignadas")
        return

    familias_usuario = [normalizar(x) for x in st.session_state.familias]

    # ===============================
    # ACTIVIDADES
    # ===============================
    actividades = obtener_actividades()

    if not actividades:
        st.warning("No existen actividades comerciales")
        return

    ac = st.selectbox(
        "Seleccione Actividad Comercial",
        actividades
    )

    # ===============================
    # CONSOLIDAR
    # ===============================
    if st.button("🔄 Consolidar actividad"):

        try:
            consolidar(ac)
            st.success("Consolidación realizada")
            st.rerun()
        except Exception as e:
            st.error(e)

    # ===============================
    # CARGAR DATASET
    # ===============================
    df = dataset_actividad(ac)

    if df is None or df.empty:
        st.warning("Actividad sin datos")
        return

    # ===============================
    # NORMALIZAR FAMILIA DATASET
    # ===============================
    df["FAMILIA"] = df["FAMILIA"].apply(normalizar)

    # ===============================
    # FILTRO VISUAL
    # ===============================
    familias_existentes = sorted(df["FAMILIA"].unique())

    familias_sel = st.multiselect(
        "Filtrar familias",
        familias_existentes,
        default=familias_usuario
    )

    if familias_sel:
        df = df[df["FAMILIA"].isin(familias_sel)]

    st.subheader("Vista consolidada")

    st.dataframe(df, use_container_width=True)

    st.divider()

    # ===============================
    # DESCARGAR EXCEL REAL
    # ===============================
    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    st.download_button(
        label="⬇ Descargar Excel",
        data=buffer,
        file_name=f"{ac}_JEFE_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
