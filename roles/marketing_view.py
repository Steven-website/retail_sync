import streamlit as st
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    regenerar_actividad,
    df_to_excel_bytes,
)

def marketing_view():
    st.header("📣 Rol MARKETING")

    # =========================================================
    # Paso 1: Seleccionar actividad
    # =========================================================
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No existen actividades comerciales disponibles.")
        return

    ac = st.selectbox("Seleccione Actividad Comercial", actividades)

    # =========================================================
    # Paso 2: Consolidar — integra trabajo de ADC
    # =========================================================
    if st.button("🔄 Consolidar actividad"):
        try:
            with st.spinner("Integrando trabajo de ADC..."):
                regenerar_actividad(ac)
            st.success("✔ Consolidación realizada.")
            st.rerun()
        except Exception as e:
            st.error(f"❌ {e}")

    df = dataset_actividad(ac)

    if df is None or df.empty:
        st.warning("La actividad no tiene datos.")
        return

    st.subheader(f"Vista MARKETING — {ac}")
    st.caption(f"Registros: {len(df):,}")
    st.dataframe(df, use_container_width=True)
    st.divider()

    # =========================================================
    # Paso 3: Descargar consolidado
    # =========================================================
    st.download_button(
        label="⬇️ Descargar Excel consolidado",
        data=df_to_excel_bytes(df),
        file_name=f"{ac}_MARKETING.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
