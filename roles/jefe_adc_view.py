import streamlit as st
from io import BytesIO
from config import CAMPO_FAMILIA
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    regenerar_actividad,
    filtrar_familias,
    df_to_excel_bytes,
)

def jefe_adc_view():
    st.header("🧠 Rol JEFE ADC")

    familias_usuario = st.session_state.get("familias", [])
    if not familias_usuario:
        st.error("⚠️ Usuario sin familias asignadas. Contacte al administrador.")
        return

    # =========================================================
    # Paso 1: Seleccionar actividad — carga dataset completo
    # =========================================================
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No existen actividades comerciales disponibles.")
        return

    ac = st.selectbox("Seleccione Actividad Comercial", actividades)

    # =========================================================
    # Paso 2: Consolidar — integra todos los cambios de ADC
    # =========================================================
    if st.button("🔄 Consolidar actividad"):
        try:
            with st.spinner("Integrando cambios de ADC..."):
                regenerar_actividad(ac)
            st.success("✔ Consolidación realizada. Cambios de ADC integrados.")
            st.rerun()
        except Exception as e:
            st.error(f"❌ {e}")

    # Cargar dataset filtrado por familias del JEFE
    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    df = filtrar_familias(df, familias_usuario)
    if df.empty:
        st.warning("No hay datos para sus familias asignadas.")
        return

    # =========================================================
    # Paso 3: Filtrar familias — filtro visual adicional
    # =========================================================
    if CAMPO_FAMILIA in df.columns:
        familias_disponibles = sorted(df[CAMPO_FAMILIA].dropna().unique())
        familias_sel = st.multiselect(
            "Filtrar por familia",
            familias_disponibles,
            default=familias_disponibles
        )
        if familias_sel:
            df = df[df[CAMPO_FAMILIA].isin(familias_sel)]

    st.subheader(f"Vista consolidada — {ac}")
    st.caption(f"Registros: {len(df):,}")
    st.dataframe(df, width="stretch")
    st.divider()

    # =========================================================
    # Paso 4: Descargar Excel consolidado
    # =========================================================
    st.download_button(
        label="⬇️ Descargar Excel consolidado",
        data=df_to_excel_bytes(df),
        file_name=f"{ac}_JEFE_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
