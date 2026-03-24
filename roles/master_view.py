import streamlit as st
from data_manager import (
    guardar_bd_actualizacion_desde_upload,
    obtener_actividades,
    crear_actividad,
    eliminar_actividad,
    regenerar_actividad,
    regenerar_todas_las_actividades,
    dataset_actividad,
    consolidar,
    analizar_actividad,
    leer_master,
    df_to_excel_bytes,
    df_to_parquet_bytes,
)


def master_view():
    st.title("👑 MASTER")

    tab_inicio, tab_actividades, tab_bd, tab_consolidacion = st.tabs(
        ["Resumen", "Actividades", "BD_ACTUALIZACION", "Consolidación"]
    )

    # =====================================================
    # TAB 1: RESUMEN
    # =====================================================
    with tab_inicio:
        st.subheader("Estado general")

        actividades = obtener_actividades()

        if not actividades:
            st.info("No existen actividades todavía.")
        else:
            actividad_resumen = st.selectbox(
                "Seleccionar actividad para análisis",
                actividades,
                key="master_tab_resumen_actividad"
            )

            info = analizar_actividad(actividad_resumen)
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Filas", info["filas"])
            c2.metric("Familias", info["familias"])
            c3.metric("Trabajadas", info["trabajadas"])
            c4.metric("Pendientes", info["pendientes"])
            c5.metric("Avance %", info["avance_pct"])

            df = dataset_actividad(actividad_resumen)
            st.dataframe(df, width="stretch")

    # =====================================================
    # TAB 2: ACTIVIDADES
    # =====================================================
    with tab_actividades:
        st.subheader("Crear actividad")

        nueva = st.text_input("Nombre nueva actividad", key="master_nueva_actividad")

        if st.button("Crear actividad", key="btn_crear_actividad"):
            try:
                crear_actividad(nueva)
                st.success("Actividad creada correctamente")
                st.rerun()
            except Exception as e:
                st.error(str(e))

        st.divider()

        actividades = obtener_actividades()

        if actividades:
            st.subheader("Gestionar actividades")

            ac = st.selectbox(
                "Seleccione actividad",
                actividades,
                key="master_gestion_actividad"
            )

            c1, c2 = st.columns(2)

            with c1:
                if st.button("Regenerar actividad", key="btn_regenerar_una"):
                    try:
                        regenerar_actividad(ac)
                        st.success("Actividad regenerada")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

            with c2:
                if st.button("Eliminar actividad", key="btn_eliminar_actividad"):
                    try:
                        eliminar_actividad(ac)
                        st.success("Actividad eliminada")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

            st.divider()

            if st.button("Regenerar todas las actividades", key="btn_regenerar_todas"):
                try:
                    regenerar_todas_las_actividades()
                    st.success("Todas las actividades fueron regeneradas")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        else:
            st.info("No hay actividades para gestionar.")

    # =====================================================
    # TAB 3: BD_ACTUALIZACION
    # =====================================================
    with tab_bd:
        st.subheader("Cargar nueva BD_ACTUALIZACION")

        archivo_bd = st.file_uploader(
            "Subir parquet oficial",
            type=["parquet"],
            key="uploader_bd_actualizacion"
        )

        if archivo_bd is not None:
            if st.button("Guardar BD_ACTUALIZACION", key="btn_guardar_bd"):
                try:
                    df_bd = guardar_bd_actualizacion_desde_upload(archivo_bd)
                    st.success("BD_ACTUALIZACION cargada correctamente")
                    st.write(f"Filas: {len(df_bd):,}")
                    st.write(f"Columnas: {len(df_bd.columns)}")
                except Exception as e:
                    st.error(str(e))

    # =====================================================
    # TAB 4: CONSOLIDACION
    # =====================================================
    with tab_consolidacion:
        st.subheader("Consolidación y salidas")

        actividades = obtener_actividades()

        if not actividades:
            st.info("No existen actividades para consolidar.")
        else:
            ac_cons = st.selectbox(
                "Actividad",
                actividades,
                key="master_consolidacion_actividad"
            )

            if st.button("Consolidar final", key="btn_consolidar_final"):
                st.success("Consolidado final listo")

            df_cons = consolidar(ac_cons)

            st.dataframe(df_cons, width="stretch")

            st.download_button(
                "⬇ Descargar Excel consolidado",
                data=df_to_excel_bytes(df_cons),
                file_name=f"{ac_cons}_CONSOLIDADO.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            master = leer_master()

            st.download_button(
                "⬇ Descargar parquet total",
                data=df_to_parquet_bytes(master),
                file_name="MASTER_TOTAL.parquet",
                mime="application/octet-stream"
            )
