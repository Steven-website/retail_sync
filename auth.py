import json
import streamlit as st
from config import SESSION_DEFAULTS, RUTA_USERS, ROL_MASTER
from github_storage import push_json

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
    texto = json.dumps(usuarios, ensure_ascii=False, indent=2)
    with open(RUTA_USERS, "w", encoding="utf-8") as f:
        f.write(texto)
    push_json(texto, "usuarios.json", "update usuarios")

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
        familias = st.session_state.get("familias", [])
        fam_html = (
            f'<div class="u-families">📂 {", ".join(sorted(familias))}</div>'
            if familias else ""
        )
        st.markdown(
            f"""
            <div class="user-card">
                <div class="u-name">👤 {st.session_state.usuario}</div>
                <div class="u-role">Rol: {st.session_state.rol}</div>
                {fam_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

def cerrar_sesion():
    for k in SESSION_DEFAULTS.keys():
        st.session_state[k] = SESSION_DEFAULTS[k]
    st.rerun()
