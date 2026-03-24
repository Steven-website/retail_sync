import streamlit as st
from auth import cargar_usuarios, guardar_usuarios
from config import ROLES_DISPONIBLES, FAMILIAS_DISPONIBLES, ROL_MASTER
from data_manager import (
    leer_bd, subir_bd,
    leer_base, obtener_actividades,
    crear_actividad, eliminar_actividad, regenerar_actividad,
    descargar_actividad_parquet, descargar_base_completa_parquet,
)

def master_view():
    st.header("👑 Panel MASTER")

    tab_bd, tab_actividades, tab_usuarios, tab_descargas = st.tabs([
        "📂 BD", "⚙️ Actividades", "👥 Usuarios", "⬇️ Descargas"
    ])

    # ─────────────────────────────────────────
    # TAB BD
    # ─────────────────────────────────────────
    with tab_bd:
        st.subheader("BD_ACTUALIZACION")

        bd = leer_bd()
        if not bd.empty:
            st.success(f"✔ BD cargada — {len(bd):,} filas · {len(bd.columns)} columnas")
        else:
            st.warning("⚠️ No hay BD cargada.")

        archivo = st.file_uploader("Subir nueva BD (.parquet)", type=["parquet"])
        if archivo:
            if st.button("💾 Guardar BD"):
                try:
                    df = subir_bd(archivo)
                    st.success(f"✔ BD guardada — {len(df):,} filas")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    # ─────────────────────────────────────────
    # TAB ACTIVIDADES
    # ─────────────────────────────────────────
    with tab_actividades:

        # Crear
        st.subheader("Crear actividad")
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
            st.info("No hay actividades creadas.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="ac_gestion")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Regenerar"):
                    try:
                        regenerar_actividad(ac)
                        st.success(f"✔ '{ac}' regenerada.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")
            with col2:
                confirmar = st.checkbox("Confirmar eliminación")
                if st.button("🗑️ Eliminar"):
                    if not confirmar:
                        st.warning("Marque la casilla primero.")
                    else:
                        try:
                            eliminar_actividad(ac)
                            st.success(f"✔ '{ac}' eliminada.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")

    # ─────────────────────────────────────────
    # TAB USUARIOS
    # ─────────────────────────────────────────
    with tab_usuarios:

        usuarios = cargar_usuarios()

        # Tabla actual
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
                        nuevas_familias = st.multiselect(
                            "Familias",
                            FAMILIAS_DISPONIBLES,
                            default=u.get("familias", []),
                            key=f"fam_{i}"
                        )

                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("💾 Guardar cambios", key=f"save_{i}"):
                            usuarios[i]["rol"]      = nuevo_rol
                            usuarios[i]["familias"] = nuevas_familias
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

        # Crear usuario
        st.subheader("Crear nuevo usuario")
        nuevo_usuario  = st.text_input("Usuario")
        nuevo_password = st.text_input("Contraseña", type="password")
        nuevo_rol      = st.selectbox("Rol", ROLES_DISPONIBLES, key="nuevo_rol")
        nuevas_familias = st.multiselect(
            "Familias asignadas",
            FAMILIAS_DISPONIBLES,
            key="nuevas_familias"
        )

        if st.button("➕ Crear usuario"):
            if not nuevo_usuario.strip() or not nuevo_password.strip():
                st.warning("Complete usuario y contraseña.")
            else:
                usuarios = cargar_usuarios()
                existe   = any(u["usuario"].lower() == nuevo_usuario.strip().lower() for u in usuarios)
                if existe:
                    st.error("Ya existe un usuario con ese nombre.")
                else:
                    usuarios.append({
                        "usuario":  nuevo_usuario.strip(),
                        "password": nuevo_password.strip(),
                        "rol":      nuevo_rol,
                        "familias": nuevas_familias,
                    })
                    guardar_usuarios(usuarios)
                    st.success(f"✔ Usuario '{nuevo_usuario}' creado.")
                    st.rerun()

    # ─────────────────────────────────────────
    # TAB DESCARGAS
    # ─────────────────────────────────────────
    with tab_descargas:
        st.subheader("Descargas")

        actividades = obtener_actividades()

        if actividades:
            st.markdown("**Por actividad**")
            ac_dl = st.selectbox("Seleccione actividad", actividades, key="ac_dl")
            if st.button("⬇️ Descargar actividad (.parquet)"):
                try:
                    data = descargar_actividad_parquet(ac_dl)
                    st.download_button(
                        "📥 Haga clic para descargar",
                        data=data,
                        file_name=f"{ac_dl}.parquet",
                        mime="application/octet-stream",
                        key="dl_ac"
                    )
                except Exception as e:
                    st.error(f"❌ {e}")

        st.divider()
        st.markdown("**BASE completa**")
        if st.button("⬇️ Descargar BASE completa (.parquet)"):
            try:
                data = descargar_base_completa_parquet()
                st.download_button(
                    "📥 Haga clic para descargar",
                    data=data,
                    file_name="BASE_COMPLETA.parquet",
                    mime="application/octet-stream",
                    key="dl_base"
                )
            except Exception as e:
                st.error(f"❌ {e}")
