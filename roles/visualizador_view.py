import streamlit as st
from data_manager import obtener_actividades, dataset_actividad, a_csv

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

    # Paso 2: Descargar CSV completo sin filtro
    st.download_button(
        "⬇️ Descargar CSV completo",
        data=a_csv(df),
        file_name=f"{ac}_VISUALIZADOR.csv",
        mime="text/csv"
    )
