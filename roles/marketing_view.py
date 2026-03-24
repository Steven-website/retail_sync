import streamlit as st
from io import BytesIO

from data_manager import (
    obtener_actividades,
    consolidar,
    dataset_actividad
)


def marketing_view():

    st.header("📢 Rol MARKETING")

    # ===============================
    # ACTIVIDADES
    # ===============================
    actividades = obtener_actividades()

    if not actividades:
        st.warning("No existen actividades")
        return

    ac = st.selectbox(
        "Seleccione actividad",
        actividades
    )

    # ===============================
    # CONSOLIDAR
    # ===============================
    if st.button("🔄 Consolidar actividad"):

        with st.spinner("Consolidando actividad..."):
            df = consolidar(ac)

    else:
        df = dataset_actividad(ac)

    # ===============================
    # VALIDAR DATA
    # ===============================
    if df is None or df.empty:
        st.warning("Actividad sin datos")
        return

    st.subheader(f"Vista MARKETING — {ac}")
    st.caption(f"Registros: {len(df):,}")

    st.dataframe(df, use_container_width=True)

    st.divider()

    # ===============================
    # DESCARGAR EXCEL REAL
    # ===============================
    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    st.download_button(
        label="⬇ Descargar Excel",
        data=buffer,
        file_name=f"{ac}_MARKETING.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
