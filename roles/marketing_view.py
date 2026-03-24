import streamlit as st
from io import BytesIO
from data_manager import (
    obtener_actividades,
    regenerar_actividad,   # FIX: era consolidar(), que no hacía nada útil
    dataset_actividad
)

def marketing_view():
    st.header("📣 Rol MARKETING")

    # ===============================
    # ACTIVIDADES
    # ===============================
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No existen actividades")
        return

    ac = st.selectbox("Seleccione actividad", actividades)

    # ===============================
    # CONSOLIDAR
    # FIX: igual que precios_view, se usa session_state para persistir estado
    # ===============================
    if "marketing_consolidado" not in st.session_state:
        st.session_state.marketing_consolidado = False

    if st.button("🔄 Consolidar actividad"):
        try:
            with st.spinner("Consolidando actividad..."):
                regenerar_actividad(ac)
            st.session_state.marketing_consolidado = True
            st.success("Consolidación realizada")
            st.rerun()
        except Exception as e:
            st.error(e)
            return

    df = dataset_actividad(ac)

    # ===============================
    # VALIDAR DATA
    # ===============================
    if df is None or df.empty:
        st.warning("Actividad sin datos")
        return

    st.subheader(f"Vista MARKETING — {ac}")
    st.caption(f"Registros: {len(df):,}")
    st.dataframe(df, width="stretch")
    st.divider()

    # ===============================
    # DESCARGAR EXCEL
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
