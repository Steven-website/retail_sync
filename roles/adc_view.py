import streamlit as st
import pandas as pd
from io import BytesIO

from data_manager import (
    dataset_actividad,
    filtrar_familias,
    actualizar_actividad_desde_excel,
    obtener_actividades
)


def adc_view():

    st.header("🧑‍💻 Rol ADC")

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
    # DATASET
    # ===============================
    df = dataset_actividad(ac)

    if df.empty:
        st.warning("Actividad sin datos")
        return

    df = filtrar_familias(df, familias_usuario)

    if df.empty:
        st.warning("No hay artículos para sus familias")
        return

    st.subheader(f"Base operativa — {ac}")
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
        file_name=f"{ac}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()

    # ===============================
    # SUBIR
    # ===============================
    st.subheader("Subir archivo trabajado")

    file = st.file_uploader(
        "Subir Excel trabajado",
        type=["xlsx"]
    )

    if file:

        if st.button("Aplicar actualización"):

            try:

                with st.spinner("Procesando actualización..."):

                    base_excel = pd.read_excel(file)

                    actualizar_actividad_desde_excel(
                        nombre=ac,
                        base_excel=base_excel,
                        familias_permitidas=familias_usuario
                    )

                st.success("Actualización aplicada correctamente")
                st.rerun()

            except Exception as e:
                st.error(f"Error al actualizar: {e}")
