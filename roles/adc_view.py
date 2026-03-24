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

    familias_usuario = st.session_state.get("familias", [])
    if not familias_usuario:
        st.error("⚠️ Usuario sin familias asignadas. Contacte al administrador.")
        return

    # ===============================
    # ACTIVIDADES
    # ===============================
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No existen actividades comerciales disponibles.")
        return

    ac = st.selectbox("Seleccione Actividad Comercial", actividades)

    # ===============================
    # DATASET
    # ===============================
    with st.spinner("Cargando información..."):
        df = dataset_actividad(ac)

    if df is None or df.empty:
        st.warning("La actividad seleccionada no tiene datos.")
        return

    df = filtrar_familias(df, familias_usuario)
    if df.empty:
        st.warning("No existen artículos asignados a sus familias.")
        return

    st.subheader(f"📊 Base operativa — {ac}")
    st.caption(f"Registros disponibles: {len(df):,}")
    st.dataframe(df, use_container_width=True, height=500)
    st.divider()

    # ===============================
    # DESCARGAR EXCEL
    # ===============================
    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    st.download_button(
        label="⬇ Descargar base en Excel",
        data=buffer,
        file_name=f"{ac}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    st.divider()

    # ===============================
    # SUBIR ARCHIVO
    # CORRECCIÓN: key dinámica para poder resetear el uploader
    # ===============================
    st.subheader("📤 Subir archivo trabajado")

    # Inicializar contador de uploader en session_state
    if "adc_upload_key" not in st.session_state:
        st.session_state.adc_upload_key = 0

    file = st.file_uploader(
        "Seleccione archivo Excel",
        type=["xlsx"],
        key=f"adc_uploader_{st.session_state.adc_upload_key}"
    )

    if file is not None:
        st.info("Archivo listo para actualizar")

        if st.button("✅ Aplicar actualización"):
            try:
                with st.spinner("Aplicando cambios en la actividad..."):
                    base_excel = pd.read_excel(file)
                    if base_excel.empty:
                        st.warning("El archivo está vacío.")
                        return
                    actualizar_actividad_desde_excel(
                        nombre=ac,
                        base_excel=base_excel,
                        familias_permitidas=familias_usuario
                    )
                st.success("✔ Información actualizada correctamente")
                # CORRECCIÓN: incrementar key para resetear uploader limpiamente
                st.session_state.adc_upload_key += 1
                st.rerun()
            except Exception as e:
                st.error("❌ Error al procesar el archivo")
                st.exception(e)
