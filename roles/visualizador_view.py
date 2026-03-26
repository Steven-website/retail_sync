import streamlit as st
from data_manager import obtener_actividades, dataset_actividad, a_excel

def visualizador_view():
    st.header("👁️ Panel VISUALIZADOR")

    actividades = obtener_actividades()
    if not actividades:
        st.warning("No hay actividades disponibles.")
        return

    # Paso 1: Seleccionar actividad
    ac = st.selectbox("Seleccione actividad", actividades)

    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    st.caption(f"Registros: {len(df):,}")
    st.dataframe(df, use_container_width=True, height=450)
    st.divider()

    # Paso 2: Descargar Excel completo
    st.download_button(
        "⬇️ Descargar Excel completo",
        data=a_excel(df),
        file_name=f"{ac}_VISUALIZADOR.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
