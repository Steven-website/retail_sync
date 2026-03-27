import io
import json
import pandas as pd
import streamlit as st
from auth import cargar_usuarios, guardar_usuarios
from config import ROLES_DISPONIBLES, FAMILIAS_DISPONIBLES
from data_manager import (
    leer_bd, subir_bd, leer_base,
    obtener_actividades,
    crear_actividad, eliminar_actividad, regenerar_actividad,
    dataset_actividad, a_excel, a_parquet,
    actualizar_desde_csv,
    leer_filtro_act, subir_filtro_act, filtrar_por_ac,
)
from queue_manager import handle_queue, submit_op
import historial as hist


def _leer_excel(archivo) -> "pd.DataFrame":
    archivo.seek(0)
    try:
        df = pd.read_excel(archivo, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
        return df
    except Exception as e:
        raise Exception(f"No se pudo leer el archivo Excel. Verifique el archivo. ({e})")


def _bd_subir_bytes(data_bytes):
    return subir_bd(io.BytesIO(data_bytes))


# ── HANDLERS CON HISTORIAL ────────────────────────────────

def _h_bd_subir(data_bytes):
    df = _bd_subir_bytes(data_bytes)
    hist.registrar(st.session_state.get("usuario", "?"), "Subió BD", f"{len(df):,} filas")
    return df

def _h_crear(nombre):
    result = crear_actividad(nombre)
    hist.registrar(st.session_state.get("usuario", "?"), "Creó actividad", nombre)
    return result

def _h_eliminar(nombre):
    eliminar_actividad(nombre)
    hist.registrar(st.session_state.get("usuario", "?"), "Eliminó actividad", nombre)

def _h_regenerar(nombre):
    result = regenerar_actividad(nombre)
    hist.registrar(st.session_state.get("usuario", "?"), "Regeneró actividad", nombre)
    return result

def _h_actualizar_master(ac, datos):
    actualizar_desde_csv(ac, datos, familias_permitidas=None)
    hist.registrar(st.session_state.get("usuario", "?"), "Actualizó datos (MASTER)", ac)

def _h_regenerar_todas(nombres):
    errores = []
    for nombre in nombres:
        try:
            regenerar_actividad(nombre)
        except Exception as e:
            errores.append(f"{nombre}: {e}")
    msg = f"{len(nombres)} actividades regeneradas"
    if errores:
        msg += f" ({len(errores)} con error)"
    hist.registrar(st.session_state.get("usuario", "?"), "Regeneró TODAS las actividades", msg)


def master_view():
    st.header("👑 Panel MASTER")

    # ── MENSAJES DE COLA ───────────────────────────────────
    if st.session_state.pop("_q_cancelled", False):
        st.warning(
            "⚠️ Operación cancelada. "
            "Si el sistema sigue ocupado, espere unos segundos y vuélvala a intentar."
        )

    # ── COLA ───────────────────────────────────────────
    try:
        completed, _ = handle_queue({
            "bd_subir":              _h_bd_subir,
            "actividad_crear":       _h_crear,
            "actividad_eliminar":    _h_eliminar,
            "actividad_regenerar":   _h_regenerar,
            "actividad_regen_todas": _h_regenerar_todas,
            "master_actualizar":     _h_actualizar_master,
        })
    except Exception as e:
        st.error(f"❌ {e}")
        st.info("Si el problema persiste, recargue la página e intente de nuevo.")
        completed = False

    if completed:
        st.success("✔ Operación completada.")

    # ── TABS ─────────────────────────────────────────────
    tab_bd, tab_fac, tab_act, tab_usr, tab_edit, tab_dl, tab_hist = st.tabs([
        "📂 BD", "🗂️ Filtro AC", "⚙️ Actividades", "👥 Usuarios", "✏️ Edición", "⬇️ Descargas", "📋 Historial"
    ])

    # ── TAB FILTRO AC ───────────────────────────────────
    with tab_fac:
        st.subheader("Filtro por Actividad Comercial")
        st.caption("Seleccione la actividad y cargue su Excel con columnas: Familia · Categoría · Subcategoría")

        actividades_fac = obtener_actividades()
        if not actividades_fac:
            st.warning("No hay actividades disponibles.")
        else:
            ac_fac = st.selectbox("Seleccione actividad", actividades_fac, key="ac_fac")

            filtro_actual = leer_filtro_act(ac_fac)
            if not filtro_actual.empty:
                st.success(f"✔ Filtro cargado — {len(filtro_actual):,} combinaciones")
                st.dataframe(filtro_actual, use_container_width=True, height=300)
                st.download_button(
                    "⬇️ Descargar filtro actual",
                    data=a_excel(filtro_actual),
                    file_name=f"FILTRO_{ac_fac}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dl_filtro"
                )
            else:
                st.warning("⚠️ Esta actividad no tiene filtro. Se muestran todos los artículos.")

            st.divider()
            st.subheader("📤 Subir filtro")

            if "upload_key_fac" not in st.session_state:
                st.session_state.upload_key_fac = 0

            archivo_fac = st.file_uploader(
                "Seleccione Excel (.xlsx)", type=["xlsx"],
                key=f"fac_uploader_{st.session_state.upload_key_fac}"
            )
            if archivo_fac:
                try:
                    archivo_fac.seek(0)
                    df_fac_prev = pd.read_excel(archivo_fac, engine="openpyxl")
                    st.caption(f"Vista previa: {len(df_fac_prev):,} filas · {len(df_fac_prev.columns)} columnas")
                    st.dataframe(df_fac_prev.head(5), use_container_width=True)
                except Exception as e:
                    st.error(f"❌ {e}")
                else:
                    if st.button("💾 Guardar filtro"):
                        try:
                            archivo_fac.seek(0)
                            df_saved = subir_filtro_act(ac_fac, archivo_fac)
                            hist.registrar(st.session_state.get("usuario", "?"),
                                           "Subió Filtro AC", f"{ac_fac} — {len(df_saved):,} combinaciones")
                            st.session_state.upload_key_fac += 1
                            st.success("✔ Filtro guardado correctamente.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")

    # ── TAB BD ──────────────────────────────────────────
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
                submit_op("bd_subir", "Guardar BD_ACTUALIZACION", {"data_bytes": archivo.read()})
                st.rerun()

    # ── TAB ACTIVIDADES ─────────────────────────────────
    with tab_act:
        st.subheader("Crear actividad comercial")
        nombre = st.text_input("Nombre de la actividad")
        if st.button("➕ Crear"):
            if not nombre.strip():
                st.warning("Escriba un nombre.")
            else:
                submit_op("actividad_crear", f"Crear actividad '{nombre}'", {"nombre": nombre})
                st.rerun()

        st.divider()
        actividades = obtener_actividades()
        if not actividades:
            st.info("No hay actividades creadas aún.")
        else:
            st.info(f"📋 {len(actividades)} actividad(es): {', '.join(actividades)}")
            if st.button("🔄 Regenerar TODAS las actividades"):
                submit_op("actividad_regen_todas",
                          f"Regenerar todas ({len(actividades)} actividades)",
                          {"nombres": actividades})
                st.rerun()
            st.caption("Regenerar actualiza todas las actividades con la BD actual, conservando los datos comerciales.")
            st.divider()
            ac = st.selectbox("Seleccione actividad", actividades, key="ac_gestion")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Regenerar"):
                    submit_op("actividad_regenerar", f"Regenerar actividad '{ac}'", {"nombre": ac})
                    st.rerun()
            with col2:
                confirmar = st.checkbox(f"Confirmar eliminación de '{ac}'", key=f"chk_elim_{ac}")
                if st.button("🗑️ Eliminar"):
                    if not confirmar:
                        st.warning("⚠️ Marque la casilla para confirmar. Esta acción no se puede deshacer.")
                    else:
                        submit_op("actividad_eliminar", f"Eliminar actividad '{ac}'", {"nombre": ac})
                        st.rerun()

    # ── TAB USUARIOS ───────────────────────────────────
    with tab_usr:
        usuarios = cargar_usuarios()
        st.subheader("Usuarios existentes")
        if not usuarios:
            st.info("No hay usuarios.")
        else:
            from collections import Counter
            conteo = Counter(u["rol"] for u in usuarios)
            cols = st.columns(len(conteo))
            for col, (rol, cantidad) in zip(cols, conteo.items()):
                col.metric(rol, cantidad)
            for i, u in enumerate(usuarios):
                with st.expander(f"👤 {u['usuario']} — {u['rol']} — 🔑 {'•' * len(u.get('password', ''))}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        nuevo_pwd = st.text_input("Nueva contraseña", key=f"pwd_{i}",
                                                   placeholder="Dejar vacío para no cambiar")
                        nuevo_rol = st.selectbox("Rol", ROLES_DISPONIBLES,
                            index=ROLES_DISPONIBLES.index(u["rol"]) if u["rol"] in ROLES_DISPONIBLES else 0,
                            key=f"rol_{i}")
                    with col2:
                        nuevas_fam = st.multiselect("Familias", FAMILIAS_DISPONIBLES,
                            default=[f for f in u.get("familias", []) if f in FAMILIAS_DISPONIBLES],
                            key=f"fam_{i}")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("💾 Guardar", key=f"save_{i}"):
                            if nuevo_pwd.strip() and len(nuevo_pwd.strip()) < 6:
                                st.warning("⚠️ La contraseña debe tener al menos 6 caracteres.")
                            else:
                                usuarios[i]["rol"]      = nuevo_rol
                                usuarios[i]["familias"] = nuevas_fam
                                if nuevo_pwd.strip():
                                    usuarios[i]["password"] = nuevo_pwd.strip()
                                guardar_usuarios(usuarios)
                                hist.registrar(st.session_state.get("usuario", "?"),
                                               "Editó usuario", u["usuario"])
                                st.success("✔ Cambios guardados.")
                                st.rerun()
                    with c2:
                        if u["usuario"] != "admin":
                            if st.button("🗑️ Eliminar usuario", key=f"del_{i}"):
                                usuarios.pop(i)
                                guardar_usuarios(usuarios)
                                hist.registrar(st.session_state.get("usuario", "?"),
                                               "Eliminó usuario", u["usuario"])
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
            elif len(np_.strip()) < 6:
                st.warning("⚠️ La contraseña debe tener al menos 6 caracteres.")
            else:
                usuarios = cargar_usuarios()
                if any(u["usuario"].lower() == nu.strip().lower() for u in usuarios):
                    st.error("Ya existe un usuario con ese nombre.")
                else:
                    usuarios.append({"usuario": nu.strip(), "password": np_.strip(),
                                     "rol": nr, "familias": nf})
                    guardar_usuarios(usuarios)
                    hist.registrar(st.session_state.get("usuario", "?"),
                                   "Creó usuario", nu.strip())
                    st.success(f"✔ Usuario '{nu}' creado.")
                    st.rerun()

    # ── TAB EDICIÓN ────────────────────────────────────
    with tab_edit:
        actividades_edit = obtener_actividades()
        if not actividades_edit:
            st.warning("No hay actividades disponibles.")
        else:
            ac_e = st.selectbox("Seleccione actividad", actividades_edit, key="ac_edit")
            df_e = dataset_actividad(ac_e)
            if df_e.empty:
                st.warning("La actividad no tiene datos.")
            else:
                df_e = filtrar_por_ac(df_e, ac_e)
                st.caption(f"Registros: {len(df_e):,}")
                st.dataframe(df_e, use_container_width=True, height=400)
                st.divider()

                st.download_button(
                    "⬇️ Descargar Excel para trabajar",
                    data=a_excel(df_e),
                    file_name=f"{ac_e}_MASTER.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.divider()
                st.subheader("📤 Subir archivo trabajado")
                st.caption("Reemplaza los datos comerciales de todos los artículos de la actividad.")

                if "upload_key_master" not in st.session_state:
                    st.session_state.upload_key_master = 0

                archivo_e = st.file_uploader(
                    "Seleccione Excel trabajado", type=["xlsx"],
                    key=f"uploader_master_{st.session_state.upload_key_master}"
                )

                if archivo_e:
                    try:
                        preview_e = _leer_excel(archivo_e)
                        st.caption(f"Vista previa: {len(preview_e):,} filas · {len(preview_e.columns)} columnas")
                        st.dataframe(preview_e.head(5), use_container_width=True)
                    except Exception as e:
                        st.error(f"❌ {e}")
                    else:
                        if st.button("✅ Actualizar BASE"):
                            try:
                                datos_e = _leer_excel(archivo_e)
                                submit_op(
                                    "master_actualizar",
                                    f"Actualizar {ac_e} (MASTER — todas las familias)",
                                    {"ac": ac_e, "datos": datos_e},
                                )
                                st.session_state.upload_key_master += 1
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ {e}")

    # ── TAB DESCARGAS ──────────────────────────────────
    with tab_dl:
        st.subheader("Descargas en parquet")
        actividades = obtener_actividades()
        if actividades:
            st.markdown("**Por actividad**")
            ac_dl = st.selectbox("Seleccione actividad", actividades, key="ac_dl")
            df_ac = dataset_actividad(ac_dl)
            if not df_ac.empty:
                st.download_button(
                    "⬇️ Descargar actividad (.parquet)",
                    data=a_parquet(df_ac), file_name=f"{ac_dl}.parquet",
                    mime="application/octet-stream", key="dl_ac"
                )
            st.divider()
        st.markdown("**BASE completa**")
        base = leer_base()
        if not base.empty:
            st.download_button(
                "⬇️ Descargar BASE completa (.parquet)",
                data=a_parquet(base), file_name="BASE_COMPLETA.parquet",
                mime="application/octet-stream", key="dl_base"
            )
        else:
            st.info("No hay BASE generada aún.")

    # ── TAB HISTORIAL ─────────────────────────────────
    with tab_hist:
        st.subheader("Historial de cambios")
        entradas = hist.leer_historial()
        if not entradas:
            st.info("No hay cambios registrados aún.")
        else:
            df_h = pd.DataFrame(entradas)
            df_h.columns = ["Fecha/Hora", "Usuario", "Acción", "Detalle"]
            df_h["_fecha"] = pd.to_datetime(df_h["Fecha/Hora"], errors="coerce").dt.date

            # ── FILTROS ──────────────────────────────────────
            col_f1, col_f2, col_f3 = st.columns(3)

            with col_f1:
                usuarios_hist = ["Todos"] + sorted(df_h["Usuario"].dropna().unique().tolist())
                filtro_usr = st.selectbox("Filtrar por usuario", usuarios_hist, key="h_usr")

            fechas_validas = df_h["_fecha"].dropna()
            fecha_min = fechas_validas.min() if not fechas_validas.empty else None
            fecha_max = fechas_validas.max() if not fechas_validas.empty else None

            with col_f2:
                desde = st.date_input("Desde", value=fecha_min, min_value=fecha_min,
                                      max_value=fecha_max, key="h_desde")
            with col_f3:
                hasta = st.date_input("Hasta", value=fecha_max, min_value=fecha_min,
                                      max_value=fecha_max, key="h_hasta")

            # ── APLICAR FILTROS ───────────────────────────────
            df_fil = df_h.copy()
            if filtro_usr != "Todos":
                df_fil = df_fil[df_fil["Usuario"] == filtro_usr]
            if fecha_min is not None:
                df_fil = df_fil[
                    (df_fil["_fecha"] >= desde) &
                    (df_fil["_fecha"] <= hasta)
                ]

            df_mostrar = df_fil.drop(columns=["_fecha"])
            st.caption(f"{len(df_mostrar)} entrada(s) — más reciente primero")
            st.dataframe(df_mostrar, use_container_width=True, hide_index=True, height=400)
            st.download_button(
                "⬇️ Descargar historial filtrado (.csv)",
                data=df_mostrar.to_csv(index=False).encode("utf-8-sig"),
                file_name="historial.csv", mime="text/csv"
            )
