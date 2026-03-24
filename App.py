import streamlit as st
from auth import init_session, login_view, sidebar_usuario
from config import ROL_MASTER, ROL_ADC, ROL_VISUALIZADOR
from roles.master_view      import master_view
from roles.adc_view         import adc_view
from roles.visualizador_view import visualizador_view

st.set_page_config(page_title="Retail Sync", layout="wide")
init_session()

if not st.session_state.login:
    login_view()
    st.stop()

st.title("🛒 Retail Sync")
st.divider()
sidebar_usuario()

ROUTER = {
    ROL_MASTER:      master_view,
    ROL_ADC:         adc_view,
    ROL_VISUALIZADOR: visualizador_view,
}

vista = ROUTER.get(st.session_state.rol)
if vista:
    vista()
else:
    st.error("⚠️ Rol no reconocido.")
