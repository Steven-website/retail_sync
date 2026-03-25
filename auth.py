import json
import streamlit as st
from config import SESSION_DEFAULTS, RUTA_USERS, ROL_MASTER

def init_session():
    for k, v in SESSION_DEFAULTS.items():
        if k not in st.session_state:
            st.session_state[k] = v

def cargar_usuarios() -> list:
    try:
        with open(RUTA_USERS, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        st.error(f"Error leyendo usuarios: {e}")
        st.stop()

def guardar_usuarios(usuarios: list):
    with open(RUTA_USERS, "w", encoding="utf-8") as f:
        json.dump(usuarios, f, ensure_ascii=False, indent=2)

def login_view():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("🛒 Retail Sync")
        st.divider()
        usuario  = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")
        if st.button("Ingresar", use_container_width=True):
            if not usuario or not password:
                st.warning("Complete usuario y contraseña.")
                return
            usuarios  = cargar_usuarios()
            encontrado = None
            for u in usuarios:
                if (
                    u.get("usuario", "").strip().lower() == usuario.strip().lower()
                    and u.get("password", "").strip() == password.strip()
                ):
                    encontrado = u
                    break
            if encontrado:
                st.session_state.login    = True
                st.session_state.usuario  = encontrado["usuario"]
                st.session_state.rol      = encontrado["rol"]
                st.session_state.familias = encontrado.get("familias", [])
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")

def sidebar_usuario():
    with st.sidebar:
        st.markdown(f"**👤 {st.session_state.usuario}**")
        st.caption(f"Rol: {st.session_state.rol}")
        if st.session_state.familias:
            st.caption(f"Familias: {', '.join(sorted(st.session_state.familias))}")
        st.divider()
        if st.button("🚪 Cerrar sesión", use_container_width=True):
            st.session_state["confirmar_logout"] = True
        if st.session_state.get("confirmar_logout"):
            st.warning("¿Seguro que desea cerrar sesión?")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("✅ Sí, salir", use_container_width=True):
                    cerrar_sesion()
            with col_b:
                if st.button("❌ Cancelar", use_container_width=True):
                    st.session_state["confirmar_logout"] = False
                    st.rerun()

def cerrar_sesion():
    for k in SESSION_DEFAULTS.keys():
        st.session_state[k] = SESSION_DEFAULTS[k]
    st.rerun()
