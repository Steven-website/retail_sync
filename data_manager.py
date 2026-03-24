import json
import streamlit as st
from config import SESSION_DEFAULTS, RUTA_USERS


# ==========================================
# INICIALIZAR SESSION STATE
# ==========================================
def init_session():
    for k, v in SESSION_DEFAULTS.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ==========================================
# CARGAR USUARIOS
# ==========================================
def cargar_usuarios():
    try:
        with open(RUTA_USERS, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []


# ==========================================
# LOGIN
# ==========================================
def login_view():

    st.title("🛒 Retail Sync")

    usuario = st.text_input("Usuario")
    password = st.text_input("Password", type="password")

    if st.button("Ingresar"):

        usuarios = cargar_usuarios()

        user_ok = None
        for u in usuarios:
            if u["usuario"] == usuario and u["password"] == password:
                user_ok = u
                break

        if user_ok:

            st.session_state.login = True
            st.session_state.usuario = user_ok["usuario"]
            st.session_state.rol = user_ok["rol"]

            # Familias puede ser string o lista
            familias = user_ok.get("FAMILIA", [])

            if isinstance(familias, str):
                familias = [familias]

            st.session_state.familias = familias

            st.rerun()

        else:
            st.error("Credenciales incorrectas")


# ==========================================
# SIDEBAR INFO
# ==========================================
def sidebar_usuario():

    with st.sidebar:

        st.success(f"Usuario: {st.session_state.usuario}")
        st.info(f"Rol: {st.session_state.rol}")

        if st.button("Cerrar sesión"):
            cerrar_sesion()


# ==========================================
# LOGOUT
# ==========================================
def cerrar_sesion():

    for k in SESSION_DEFAULTS.keys():
        st.session_state[k] = SESSION_DEFAULTS[k]

    st.rerun()
