import streamlit as st
from auth import init_session, login_view, sidebar_usuario
from config import ROL_MASTER, ROL_ADC, ROL_VISUALIZADOR, ROL_VM
from roles.master_view       import master_view
from roles.adc_view          import adc_view
from roles.visualizador_view import visualizador_view
from roles.vm_view           import vm_view

st.set_page_config(
    page_title="Gestión de Actividades Comerciales",
    page_icon="🏬",
    layout="wide",
)
init_session()

if not st.session_state.login:
    login_view()
    st.stop()

# ── CSS GLOBAL ────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Sidebar base ── */
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 24px;
    display: flex;
    flex-direction: column;
    height: 100vh;
}

/* ── Tarjeta de usuario ── */
.user-card {
    background: rgba(255, 255, 255, 0.06);
    border: 1px solid rgba(255, 255, 255, 0.10);
    border-radius: 12px;
    padding: 14px 16px;
    margin: 0 0 8px 0;
}
.user-card .u-name {
    font-size: 16px;
    font-weight: 700;
    margin-bottom: 3px;
}
.user-card .u-role {
    font-size: 11px;
    opacity: 0.60;
    text-transform: uppercase;
    letter-spacing: 0.8px;
}
.user-card .u-families {
    font-size: 11px;
    opacity: 0.55;
    margin-top: 4px;
    line-height: 1.4;
}

/* ── Espaciador que empuja logout al fondo ── */
.sidebar-spacer { flex: 1; }

/* ── Botón cerrar sesión ── */
.logout-wrap {
    padding: 12px 0 8px 0;
    border-top: 1px solid rgba(255, 255, 255, 0.10);
    margin-top: 8px;
}
section[data-testid="stSidebar"] .logout-wrap button {
    background: rgba(220, 53, 69, 0.15) !important;
    border: 1px solid rgba(220, 53, 69, 0.35) !important;
    color: #ff6b7a !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    transition: background 0.2s !important;
}
section[data-testid="stSidebar"] .logout-wrap button:hover {
    background: rgba(220, 53, 69, 0.28) !important;
}
</style>
""", unsafe_allow_html=True)

# ── SIDEBAR: usuario + logout (antes de vista para que siempre se renderice) ──
sidebar_usuario()
with st.sidebar:
    st.markdown('<div class="sidebar-spacer"></div>', unsafe_allow_html=True)
    st.markdown('<div class="logout-wrap">', unsafe_allow_html=True)
    if st.button("🚪 Cerrar sesión", use_container_width=True, key="logout_btn"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

# ── CONTENIDO PRINCIPAL ───────────────────────────────────────
st.title("🏬 Gestión de Actividades Comerciales")
st.caption(f"Bienvenido, **{st.session_state.usuario}** · Rol: {st.session_state.rol}")
st.divider()

ROUTER = {
    ROL_MASTER:       master_view,
    ROL_ADC:          adc_view,
    ROL_VISUALIZADOR: visualizador_view,
    ROL_VM:           vm_view,
}

vista = ROUTER.get(st.session_state.rol)
if vista:
    vista()
else:
    st.error("⚠️ Rol no reconocido.")
