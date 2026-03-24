import streamlit as st
import pandas as pd
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    filtrar_familias,
    actualizar_actividad_desde_excel,
    df_to_excel_bytes,
)

def adc_view():
    st.header("🧑‍💻 Rol ADC")

    familias_usuario = st.session_state.get("familias", [])
    if not familias_usuario:
        st.error("⚠️ Usuario sin familias asignadas. Contacte al administrador.")
        return

    # =========================================================
    # Paso 1: Seleccionar actividad — filtra por familias del ADC
    # =========================================================
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No existen actividades comerciales disponibles.")
        return

    ac = st.selectbox("Seleccione Actividad Comercial", actividades)

    with st.spinner("Cargando base operativa..."):
        df = dataset_actividad(ac)

    if df is None or df.empty:
        st.warning("La actividad seleccionada no tiene datos.")
        return

    # Filtrar solo las familias del ADC
    df = filtrar_familias(df, familias_usuario)
    if df.empty:
        st.warning("No existen artículos asignados a sus familias en esta actividad.")
        return

    st.subheader(f"📊 Base operativa — {ac}")
    st.caption(f"Registros disponibles: {len(df):,}")
    st.dataframe(df, width="stretch", height=450)
    st.divider()

    # =========================================================
    # Paso 2: Descargar Excel — base operativa para trabajar offline
    # =========================================================
    st.download_button(
        label="⬇️ Descargar base en Excel",
        data=df_to_excel_bytes(df),
        file_name=f"{ac}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()

    # =========================================================
    # Pasos 3, 4 y 5: Trabajar offline → Subir Excel → Actualizar
    # El sistema hace merge por PK y respeta otras familias (Paso 6)
    # =========================================================
    st.subheader("📤 Subir archivo trabajado")
    st.caption("Solo se actualizan los artículos de sus familias. El resto no se toca.")

    if "adc_upload_key" not in st.session_state:
        st.session_state.adc_upload_key = 0

    file = st.file_uploader(
        "Seleccione archivo Excel trabajado",
        type=["xlsx"],
        key=f"adc_uploader_{st.session_state.adc_upload_key}"
    )

    if file is not None:
        st.info("✅ Archivo cargado. Revise y presione Aplicar.")

        # Vista previa del archivo antes de aplicar
        try:
            preview = pd.read_excel(file)
            st.caption(f"Vista previa: {len(preview):,} filas")
            st.dataframe(preview.head(10), width="stretch")
            file.seek(0)  # resetear puntero para leer de nuevo al aplicar
        except Exception:
            pass

        if st.button("✅ Aplicar actualización"):
            try:
                with st.spinner("Aplicando cambios..."):
                    base_excel = pd.read_excel(file)
                    if base_excel.empty:
                        st.warning("El archivo está vacío.")
                        return
                    actualizar_actividad_desde_excel(
                        nombre=ac,
                        base_excel=base_excel,
                        familias_permitidas=familias_usuario
                    )
                st.success("✔ Información actualizada correctamente. Solo sus familias fueron modificadas.")
                st.session_state.adc_upload_key += 1
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error al procesar el archivo: {e}")
