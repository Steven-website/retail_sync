import streamlit as st

from auth import init_session, login_view, sidebar_usuario
from config import *

from roles.master_view import master_view
from roles.adc_view import adc_view
from roles.jefe_adc_view import jefe_adc_view
from roles.precios_view import precios_view
from roles.marketing_view import marketing_view


# ==========================================
# CONFIG PAGE
# ==========================================
st.set_page_config(
    page_title="Retail Sync",
    layout="wide"
)

# ==========================================
# INIT SESSION
# ==========================================
init_session()

# ==========================================
# LOGIN
# ==========================================
if not st.session_state.login:
    login_view()
    st.stop()

# ==========================================
# SIDEBAR
# ==========================================
sidebar_usuario()

rol = st.session_state.rol

# ==========================================
# ROUTER POR ROL
# ==========================================
if rol == ROL_MASTER:
    master_view()

elif rol == ROL_ADC:
    adc_view()

elif rol == ROL_JEFE_ADC:
    jefe_adc_view()

elif rol == ROL_PRECIOS:
    precios_view()

elif rol == ROL_MARKETING:
    marketing_view()

else:
    st.error("Rol no reconocido")
