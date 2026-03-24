import streamlit as st
import pandas as pd
from io import BytesIO

from data_manager import (
    dataset_actividad,
    obtener_actividades,
    consolidar,
    filtrar_familias
)


def jefe_adc_view():

    st.header("🧠 Rol JEFE ADC")

    familias_usuario = st.session_state.familias

    if not familias_usuario:
        st.error("Usuario sin familias asignadas")
        return

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
            with st.spinner("Consolidando actividad..."):
                consolidar(ac)

            st.success("Consolidación realizada")
            st.rerun()

        except Exception as e:
            st.error(e)

    # ===============================
    # DATASET
    # ===============================
    df = dataset_actividad(ac)

    if df.empty:
        st.warning("Actividad sin datos")
        return

    # 🔥 límite superior = familias del JEFE
    df = filtrar_familias(df, familias_usuario)

    if df.empty:
        st.warning("No hay datos para sus familias")
        return

    # ===============================
    # FILTRO VISUAL
    # ===============================
    familias_existentes = sorted(df["FAMILIA"].dropna().unique())

    familias_sel = st.multiselect(
        "Filtrar familias",
        familias_existentes,
        default=familias_existentes
    )

    if familias_sel:
        df = df[df["FAMILIA"].isin(familias_sel)]

    st.subheader(f"Vista consolidada — {ac}")
    st.caption(f"Registros: {len(df):,}")

    st.dataframe(df, use_container_width=True)

    st.divider()

    # ===============================
    # DESCARGAR
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
