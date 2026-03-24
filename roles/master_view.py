import streamlit as st
from data_manager import (
    guardar_bd_actualizacion_desde_upload,
    leer_bd_actualizacion,
    obtener_actividades,
    crear_actividad,
    eliminar_actividad,
    regenerar_actividad,
    regenerar_todas_las_actividades,
    dataset_actividad,
    analizar_actividad,
    leer_master,
    df_to_excel_bytes,
)

def master_view():
    st.title("👑 MASTER")

    tab_bd, tab_actividades, tab_resumen, tab_consolidacion = st.tabs([
        "📂 BD_ACTUALIZACION",
        "⚙️ Actividades",
        "📊 Resumen",
        "📋 Consolidación",
    ])

    # =========================================================
    # TAB 1 — BD_ACTUALIZACION
    # Paso 1: Cargar BD | Paso 2: Validación automática
    # =========================================================
    with tab_bd:
        st.subheader("Cargar BD_ACTUALIZACION")
        st.caption("Sube el parquet base. El sistema valida estructura automáticamente.")

        bd_actual = leer_bd_actualizacion()
        if not bd_actual.empty:
            st.success(f"✔ BD activa — {len(bd_actual):,} filas · {len(bd_actual.columns)} columnas")
            with st.expander("Vista previa BD actual"):
                st.dataframe(bd_actual.head(100), use_container_width=True)
        else:
            st.warning("⚠️ No hay BD cargada. Sube un archivo .parquet para comenzar.")

        archivo = st.file_uploader("Subir nuevo parquet", type=["parquet"])
        if archivo:
            if st.button("💾 Guardar BD"):
                try:
                    with st.spinner("Validando y guardando BD..."):
                        df_bd = guardar_bd_actualizacion_desde_upload(archivo)
                    st.success(f"✔ BD cargada correctamente — {len(df_bd):,} filas · {len(df_bd.columns)} columnas")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    # =========================================================
    # TAB 2 — ACTIVIDADES
    # Paso 3: Crear | Paso 4: Generar base | Paso 6: Regenerar
    # Paso 7: Sincronizar | Paso 8: Mantener trabajo previo
    # =========================================================
    with tab_actividades:

        # --- Crear actividad (Pasos 3 y 4) ---
        st.subheader("Crear actividad comercial")
        st.caption("Al crear, el sistema copia la BD_ACTUALIZACION con columnas comerciales vacías.")

        nueva = st.text_input("Nombre de la actividad")
        if st.button("➕ Crear actividad"):
            if not nueva.strip():
                st.warning("Debe escribir un nombre.")
            else:
                try:
                    with st.spinner("Creando actividad..."):
                        crear_actividad(nueva)
                    st.success(f"✔ Actividad '{nueva}' creada correctamente.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

        st.divider()

        # --- Gestionar actividades existentes ---
        actividades = obtener_actividades()
        if not actividades:
            st.info("No hay actividades creadas aún. Crea una arriba.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="act_ac")

            col1, col2, col3 = st.columns(3)

            # Paso 6 y 7: Regenerar — sincroniza con BD nueva, mantiene trabajo previo
            with col1:
                if st.button("🔄 Regenerar actividad"):
                    try:
                        with st.spinner("Sincronizando con BD_ACTUALIZACION..."):
                            regenerar_actividad(ac)
                        st.success("✔ Actividad regenerada. Artículos nuevos agregados, eliminados quitados, trabajo comercial conservado.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")

            # Regenerar todas
            with col2:
                if st.button("🔄 Regenerar TODAS"):
                    try:
                        with st.spinner("Regenerando todas las actividades..."):
                            regenerar_todas_las_actividades()
                        st.success("✔ Todas las actividades regeneradas.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")

            # Eliminar
            with col3:
                confirmar = st.checkbox("Confirmar eliminación")
                if st.button("🗑️ Eliminar actividad"):
                    if confirmar:
                        try:
                            eliminar_actividad(ac)
                            st.success("✔ Actividad eliminada.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")
                    else:
                        st.warning("Marque la casilla de confirmación primero.")

    # =========================================================
    # TAB 3 — RESUMEN
    # Paso 9: Analizar | Paso 10: Métricas automáticas
    # =========================================================
    with tab_resumen:
        st.subheader("Análisis de actividad")
        st.caption("Revisa avance, familias trabajadas y pendientes.")

        actividades = obtener_actividades()
        if not actividades:
            st.info("⚠️ No hay actividades. Ve a ⚙️ Actividades para crear una.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="resumen_ac")

            info = analizar_actividad(ac)

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total filas",   f"{info['filas']:,}")
            c2.metric("Familias",      info["familias"])
            c3.metric("Trabajadas",    info["trabajadas"])
            c4.metric("Pendientes",    info["pendientes"])
            c5.metric("Avance",        f"{info['avance_pct']}%")

            st.divider()
            df = dataset_actividad(ac)
            st.caption(f"Mostrando hasta 2,000 registros de {len(df):,}")
            st.dataframe(df.head(2000), use_container_width=True)

    # =========================================================
    # TAB 4 — CONSOLIDACION
    # Paso 11: Consolidar | Paso 12: Armar consolidado
    # Paso 13: Descargar Excel | Paso 14: Descargar parquet
    # =========================================================
    with tab_consolidacion:
        st.subheader("Consolidación final")
        st.caption("Genera la foto oficial de la actividad y descarga el master completo.")

        actividades = obtener_actividades()
        if not actividades:
            st.info("⚠️ No hay actividades. Ve a ⚙️ Actividades para crear una.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="consol_ac")

            # Paso 11 y 12: Consolidar
            if st.button("📋 Consolidar actividad"):
                try:
                    with st.spinner("Armando consolidado..."):
                        regenerar_actividad(ac)
                    st.success("✔ Consolidado generado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

            df = dataset_actividad(ac)

            if df.empty:
                st.warning("La actividad no tiene datos.")
            else:
                st.caption(f"Registros: {len(df):,}")
                st.dataframe(df.head(2000), use_container_width=True)
                st.divider()

                col1, col2 = st.columns(2)

                # Paso 13: Descargar Excel consolidado
                with col1:
                    st.download_button(
                        "⬇️ Descargar Excel consolidado",
                        data=df_to_excel_bytes(df),
                        file_name=f"{ac}_CONSOLIDADO.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="dl_excel_consol"
                    )

                # Paso 14: Descargar parquet total (master completo)
                with col2:
                    master = leer_master()
                    if not master.empty:
                        import io
                        buf = io.BytesIO()
                        master.to_parquet(buf, index=False)
                        buf.seek(0)
                        st.download_button(
                            "⬇️ Descargar MASTER parquet",
                            data=buf.getvalue(),
                            file_name="MASTER_TOTAL.parquet",
                            mime="application/octet-stream",
                            key="dl_parquet_master"
                        )
                        st.download_button(
                            "⬇️ Descargar MASTER Excel",
                            data=df_to_excel_bytes(master),
                            file_name="MASTER_TOTAL.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="dl_excel_master"
                        )
