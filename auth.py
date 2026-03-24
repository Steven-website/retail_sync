import json
import streamlit as st
from config import (
    SESSION_DEFAULTS,
    RUTA_USERS,
    ROL_MASTER,
    FILTRO_FAMILIAS_ACTIVO,
    ROLES_SIN_FILTRO_FAMILIA
)

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
    if not st.session_state.get("_users_cache"):
        try:
            with open(RUTA_USERS, "r", encoding="utf-8") as f:
                st.session_state._users_cache = json.load(f)
        except Exception as e:
            st.error(f"Error leyendo usuarios.json: {e}")
            st.stop()
    return st.session_state._users_cache

# ==========================================
# NORMALIZAR FAMILIAS
# ==========================================
def _normalizar_familias(familias):
    if familias is None:
        return []
    if isinstance(familias, str):
        familias = familias.split(",")
    familias_ok = []
    for f in familias:
        if f is None:
            continue
        f = str(f).strip().upper()
        if f:
            familias_ok.append(f)
    return familias_ok

# ==========================================
# LOGIN
# ==========================================
def login_view():
    st.title("🛒 Retail Sync")
    usuario = st.text_input("Usuario")
    password = st.text_input("Password", type="password")

    if st.button("Ingresar"):
        if not usuario or not password:
            st.warning("Debe ingresar usuario y contraseña")
            return

        usuarios = cargar_usuarios()
        user_ok = None

        for u in usuarios:
            if (
                str(u.get("usuario", "")).strip().lower()
                == usuario.strip().lower()
                and str(u.get("password", "")).strip() == password.strip()
            ):
                user_ok = u
                break

        if user_ok:
            rol = user_ok.get("rol")
            st.session_state.login = True
            st.session_state.usuario = user_ok["usuario"]
            st.session_state.rol = rol

            if FILTRO_FAMILIAS_ACTIVO and rol not in ROLES_SIN_FILTRO_FAMILIA:
                familias = _normalizar_familias(user_ok.get("FAMILIA"))
            else:
                familias = []

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
        if st.session_state.rol != ROL_MASTER:
            familias = st.session_state.familias
            # CORRECCIÓN: evita crash si familias es None
            if familias:
                st.caption(f"Familias: {', '.join(familias)}")
            else:
                st.caption("Familias: (sin asignar)")
        if st.button("Cerrar sesión"):
            cerrar_sesion()

# ==========================================
# LOGOUT
# ==========================================
def cerrar_sesion():
    for k in SESSION_DEFAULTS.keys():
        st.session_state[k] = SESSION_DEFAULTS[k]
    # CORRECCIÓN: limpiar cache de usuarios para que se relean cambios
    if "_users_cache" in st.session_state:
        del st.session_state["_users_cache"]
    st.rerun()
