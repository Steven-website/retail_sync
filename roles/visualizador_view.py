import streamlit as st
import io
import pandas as pd
from data_manager import obtener_actividades, dataset_actividad

def visualizador_view():
    st.header("👁️ Panel VISUALIZADOR")

    actividades = obtener_actividades()
    if not actividades:
        st.warning("No hay actividades disponibles.")
        return

    ac = st.selectbox("Seleccione actividad", actividades)

    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    st.caption(f"Registros: {len(df):,}")
    st.dataframe(df, use_container_width=True, height=450)
    st.divider()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="BASE")
    buf.seek(0)

    st.download_button(
        "⬇️ Descargar Excel",
        data=buf.getvalue(),
        file_name=f"{ac}_VISUALIZADOR.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
