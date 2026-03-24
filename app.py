import streamlit as st
from auth import init_session, login_view, sidebar_usuario, cerrar_sesion
from config import (
    ROL_MASTER,
    ROL_ADC,
    ROL_JEFE_ADC,
    ROL_PRECIOS,
    ROL_MARKETING,
    DEBUG_APP
)
from roles.master_view    import master_view
from roles.adc_view       import adc_view
from roles.jefe_adc_view  import jefe_adc_view
from roles.precios_view   import precios_view
from roles.marketing_view import marketing_view

st.set_page_config(page_title="Retail Sync", layout="wide")

init_session()

if not st.session_state.login:
    login_view()
    st.stop()

st.title("🛒 Retail Sync")
st.divider()

sidebar_usuario()
rol = st.session_state.rol

ROUTER = {
    ROL_MASTER:    master_view,
    ROL_ADC:       adc_view,
    ROL_JEFE_ADC:  jefe_adc_view,
    ROL_PRECIOS:   precios_view,
    ROL_MARKETING: marketing_view,
}

vista = ROUTER.get(rol)
if vista:
    vista()
else:
    st.error("⚠️ Rol no reconocido en el sistema.")
    if st.button("Cerrar sesión"):
        cerrar_sesion()

if DEBUG_APP:
    with st.expander("DEBUG SESSION"):
        st.write(st.session_state)
