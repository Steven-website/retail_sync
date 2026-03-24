import streamlit as st
import pandas as pd
from io import BytesIO

from data_manager import dataset_actividad, filtrar_familias, actualizar_desde_excel, obtener_actividades


def adc_view():

    st.header("🧑‍💻 Rol ADC")

    # ===============================
    # VALIDAR SESSION
    # ===============================
    if "familias" not in st.session_state:
        st.error("No hay familias asignadas al usuario")
        return

    familias_usuario = st.session_state.familias
    if isinstance(familias_usuario, str):
        familias_usuario = [familias_usuario]

    familias_usuario = [str(x).strip() for x in familias_usuario if str(x).strip()]
    if not familias_usuario:
        st.error("Usuario ADC sin familias válidas asignadas")
        return

    # ===============================
    # OBTENER ACTIVIDADES
    # ===============================
    actividades = obtener_actividades()

    if not actividades:
        st.warning("No existen actividades comerciales")
        return

    # ===============================
    # SELECCIONAR ACTIVIDAD
    # ===============================
    ac = st.selectbox(
        "Seleccione Actividad Comercial",
        actividades
    )

    # ===============================
    # CARGAR DATASET
    # ===============================
    df = dataset_actividad(ac)

    if df is None or df.empty:
        st.warning("La actividad no tiene datos")
        return

    # ===============================
    # FILTRAR POR FAMILIAS
    # ===============================
    df = filtrar_familias(df, familias_usuario)

    if df.empty:
        st.warning("No hay datos para sus familias asignadas")
        return

    st.subheader("Base operativa")

    st.dataframe(df, use_container_width=True)

    st.divider()

    # ===============================
    # DESCARGAR EXCEL REAL
    # ===============================
    st.subheader("Descargar base")

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
    # SUBIR ARCHIVO TRABAJADO
    # ===============================
    st.subheader("Subir archivo trabajado")

    file = st.file_uploader(
        "Subir Excel",
        type=["xlsx"]
    )

    if file is not None:

        if st.button("Actualizar información"):

            try:
                actualizar_desde_excel(file, ac, familias_usuario)
                st.success("Actualización aplicada correctamente")
                st.rerun()

            except Exception as e:
                st.error(f"Error al actualizar: {e}")
