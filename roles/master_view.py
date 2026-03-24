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

    # Tabs reordenadas: BD primero (paso 1), luego Actividades (paso 2), etc.
    tab_bd, tab_actividades, tab_inicio, tab_consolidacion = st.tabs(
        ["📂 BD_ACTUALIZACION", "⚙️ Actividades", "📊 Resumen", "📋 Consolidación"]
    )

    # =====================================================
    # TAB BD — Paso 1 obligatorio
    # =====================================================
    with tab_bd:

        st.subheader("Cargar BD_ACTUALIZACION")
        st.caption("Paso 1: carga el archivo base antes de crear actividades.")

        bd_actual = leer_bd_actualizacion()
        if not bd_actual.empty:
            st.success(f"✔ BD activa — {len(bd_actual):,} filas · {len(bd_actual.columns)} columnas")
        else:
            st.warning("⚠️ No hay BD cargada. Sube un archivo .parquet para comenzar.")

        archivo = st.file_uploader("Subir parquet", type=["parquet"])

        if archivo:
            if st.button("💾 Guardar BD"):
                try:
                    df_bd = guardar_bd_actualizacion_desde_upload(archivo)
                    st.success("BD cargada correctamente")
                    st.write(f"Filas: {len(df_bd):,}")
                    st.write(f"Columnas: {len(df_bd.columns)}")
                    st.rerun()
                except Exception as e:
                    st.error(e)

    # =====================================================
    # TAB ACTIVIDADES — Paso 2
    # =====================================================
    with tab_actividades:

        st.subheader("Crear actividad")
        st.caption("Paso 2: crea una actividad para comenzar a trabajar.")

        nueva = st.text_input("Nombre actividad")

        if st.button("➕ Crear"):
            if not nueva.strip():
                st.warning("Debe escribir un nombre")
            else:
                try:
                    crear_actividad(nueva)
                    st.success("Actividad creada")
                    st.rerun()
                except Exception as e:
                    st.error(e)

        st.divider()

        actividades = obtener_actividades()

        if not actividades:
            st.info("No hay actividades creadas aún.")
        else:
            ac = st.selectbox("Seleccione actividad", actividades, key="actividades_ac")

            col1, col2 = st.columns(2)

            with col1:
                if st.button("🔄 Regenerar"):
                    try:
                        with st.spinner("Regenerando actividad..."):
                            regenerar_actividad(ac)
                        st.success("Regenerada correctamente")
                        st.rerun()
                    except Exception as e:
                        st.error(e)

            with col2:
                confirmar = st.checkbox("Confirmar eliminación")
                if st.button("🗑️ Eliminar"):
                    if confirmar:
                        try:
                            eliminar_actividad(ac)
                            st.success("Eliminada")
                            st.rerun()
                        except Exception as e:
                            st.error(e)
                    else:
                        st.warning("Marque la casilla de confirmación primero")

            st.divider()

            if st.button("🔄 Regenerar TODAS las actividades"):
                try:
                    with st.spinner("Regenerando todas las actividades..."):
                        regenerar_todas_las_actividades()
                    st.success("Todas regeneradas correctamente")
                    st.rerun()
                except Exception as e:
                    st.error(e)

    # =====================================================
    # TAB RESUMEN
    # =====================================================
    with tab_inicio:

        actividades = obtener_actividades()

        if not actividades:
            st.info("⚠️ No existen actividades todavía. Ve a la tab **⚙️ Actividades** para crear una.")
        else:
            ac = st.selectbox("Actividad", actividades, key="resumen_ac")

            info = analizar_actividad(ac)

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Filas", info["filas"])
            c2.metric("Familias", info["familias"])
            c3.metric("Trabajadas", info["trabajadas"])
            c4.metric("Pendientes", info["pendientes"])
            c5.metric("Avance %", f"{info['avance_pct']}%")

            df = dataset_actividad(ac)
            st.caption(f"Registros: {len(df):,}")
            st.dataframe(df.head(2000), width="stretch")

    # =====================================================
    # TAB CONSOLIDACION
    # =====================================================
    with tab_consolidacion:

        actividades = obtener_actividades()

        if not actividades:
            st.info("⚠️ No hay actividades. Ve a la tab **⚙️ Actividades** para crear una.")
        else:
            ac = st.selectbox("Actividad", actividades, key="consolidacion_ac")

            df = dataset_actividad(ac)

            if df.empty:
                st.warning("La actividad no tiene datos.")
            else:
                st.caption(f"Registros: {len(df):,}")
                st.dataframe(df.head(2000), width="stretch")

                st.download_button(
                    "⬇ Descargar Excel actividad",
                    data=df_to_excel_bytes(df),
                    file_name=f"{ac}_CONSOLIDADO.xlsx",
                    key="dl_consolidado"
                )

            st.divider()

            master = leer_master()
            if not master.empty:
                st.download_button(
                    "⬇ Descargar MASTER completo",
                    data=df_to_excel_bytes(master),
                    file_name="MASTER_TOTAL.xlsx",
                    key="dl_master"
                )
