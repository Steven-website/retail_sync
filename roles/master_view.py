import json
import streamlit as st
from auth import cargar_usuarios, guardar_usuarios
from config import ROLES_DISPONIBLES, FAMILIAS_DISPONIBLES
from data_manager import (
    leer_bd, subir_bd, leer_base,
    obtener_actividades,
    crear_actividad, eliminar_actividad, regenerar_actividad,
    dataset_actividad, a_parquet,
)

def master_view():
    st.header("👑 Panel MASTER")

    tab_bd, tab_actividades, tab_usuarios, tab_descargas = st.tabs([
        "📂 BD", "⚙️ Actividades", "👥 Usuarios", "⬇️ Descargas"
    ])

    # ── TAB BD ────────────────────────────────────────────
    with tab_bd:
        st.subheader("BD_ACTUALIZACION")
        bd = leer_bd()
        if not bd.empty:
            st.success(f"✔ BD cargada — {len(bd):,} filas · {len(bd.columns)} columnas")
        else:
            st.warning("⚠️ No hay BD cargada. Suba un archivo .parquet para comenzar.")

        archivo = st.file_uploader("Subir BD (.parquet)", type=["parquet"])
        if archivo:
            if st.button("💾 Guardar BD"):
                try:
                    df = subir_bd(archivo)
                    st.success(f"✔ BD guardada — {len(df):,} filas")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    # ── TAB ACTIVIDADES ───────────────────────────────────
    with tab_actividades:
        st.subheader("Crear actividad comercial")
        nombre = st.text_input("Nombre de la actividad")
        if st.button("➕ Crear"):
            if not nombre.strip():
                st.warning("Escriba un nombre.")
            else:
                try:
                    crear_actividad(nombre)
                    st.success(f"✔ Actividad '{nombre}' creada.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

        st.divider()
        actividades = obtener_actividades()
        if not actividades:
            st.info("No hay actividades creadas aún.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="ac_gestion")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Regenerar"):
                    try:
                        regenerar_actividad(ac)
                        st.success(f"✔ '{ac}' regenerada. Artículos nuevos agregados, trabajo previo conservado.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")
            with col2:
                confirmar = st.checkbox("Confirmar eliminación")
                if st.button("🗑️ Eliminar"):
                    if not confirmar:
                        st.warning("Marque la casilla de confirmación primero.")
                    else:
                        try:
                            eliminar_actividad(ac)
                            st.success(f"✔ '{ac}' eliminada.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")

    # ── TAB USUARIOS ──────────────────────────────────────
    with tab_usuarios:
        usuarios = cargar_usuarios()

        st.subheader("Usuarios existentes")
        if not usuarios:
            st.info("No hay usuarios.")
        else:
            for i, u in enumerate(usuarios):
                with st.expander(f"👤 {u['usuario']} — {u['rol']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        nuevo_pwd = st.text_input(
                            "Nueva contraseña",
                            key=f"pwd_{i}",
                            placeholder="Dejar vacío para no cambiar"
                        )
                        nuevo_rol = st.selectbox(
                            "Rol",
                            ROLES_DISPONIBLES,
                            index=ROLES_DISPONIBLES.index(u["rol"]) if u["rol"] in ROLES_DISPONIBLES else 0,
                            key=f"rol_{i}"
                        )
                    with col2:
                        nuevas_fam = st.multiselect(
                            "Familias",
                            FAMILIAS_DISPONIBLES,
                            default=[f for f in u.get("familias", []) if f in FAMILIAS_DISPONIBLES],
                            key=f"fam_{i}"
                        )
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("💾 Guardar", key=f"save_{i}"):
                            usuarios[i]["rol"]      = nuevo_rol
                            usuarios[i]["familias"] = nuevas_fam
                            if nuevo_pwd.strip():
                                usuarios[i]["password"] = nuevo_pwd.strip()
                            guardar_usuarios(usuarios)
                            st.success("✔ Cambios guardados.")
                            st.rerun()
                    with c2:
                        if u["usuario"] != "admin":
                            if st.button("🗑️ Eliminar usuario", key=f"del_{i}"):
                                usuarios.pop(i)
                                guardar_usuarios(usuarios)
                                st.success("✔ Usuario eliminado.")
                                st.rerun()

        st.divider()
        st.subheader("Crear nuevo usuario")
        nu  = st.text_input("Usuario", key="nu")
        np_ = st.text_input("Contraseña", type="password", key="np")
        nr  = st.selectbox("Rol", ROLES_DISPONIBLES, key="nr")
        nf  = st.multiselect("Familias", FAMILIAS_DISPONIBLES, key="nf")

        if st.button("➕ Crear usuario"):
            if not nu.strip() or not np_.strip():
                st.warning("Complete usuario y contraseña.")
            else:
                usuarios = cargar_usuarios()
                if any(u["usuario"].lower() == nu.strip().lower() for u in usuarios):
                    st.error("Ya existe un usuario con ese nombre.")
                else:
                    usuarios.append({
                        "usuario":  nu.strip(),
                        "password": np_.strip(),
                        "rol":      nr,
                        "familias": nf,
                    })
                    guardar_usuarios(usuarios)
                    st.success(f"✔ Usuario '{nu}' creado.")
                    st.rerun()

        st.divider()
        st.subheader("Respaldo de usuarios")
        usuarios_actuales = cargar_usuarios()
        st.download_button(
            label="⬇️ Descargar usuarios.json",
            data=json.dumps(usuarios_actuales, ensure_ascii=False, indent=2).encode("utf-8"),
            file_name="usuarios.json",
            mime="application/json",
            key="dl_usuarios"
        )

    # ── TAB DESCARGAS ─────────────────────────────────────
    with tab_descargas:
        st.subheader("Descargas en parquet")
        actividades = obtener_actividades()

        if actividades:
            st.markdown("**Por actividad**")
            ac_dl = st.selectbox("Seleccione actividad", actividades, key="ac_dl")
            df_ac = dataset_actividad(ac_dl)
            if not df_ac.empty:
                st.download_button(
                    "⬇️ Descargar actividad (.parquet)",
                    data=a_parquet(df_ac),
                    file_name=f"{ac_dl}.parquet",
                    mime="application/octet-stream",
                    key="dl_ac"
                )
            st.divider()

        st.markdown("**BASE completa**")
        base = leer_base()
        if not base.empty:
            st.download_button(
                "⬇️ Descargar BASE completa (.parquet)",
                data=a_parquet(base),
                file_name="BASE_COMPLETA.parquet",
                mime="application/octet-stream",
                key="dl_base"
            )
        else:
            st.info("No hay BASE generada aún.")
