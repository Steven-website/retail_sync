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
)

def master_view():

    st.title("👑 MASTER")

    tab_inicio, tab_actividades, tab_bd, tab_consolidacion = st.tabs(
        ["Resumen", "Actividades", "BD_ACTUALIZACION", "Consolidación"]
    )

    # =====================================================
    # TAB RESUMEN
    # =====================================================
    with tab_inicio:

        actividades = obtener_actividades()

        if not actividades:
            st.info("No existen actividades todavía.")
            return

        ac = st.selectbox("Actividad", actividades)

        info = analizar_actividad(ac)

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Filas", info["filas"])
        c2.metric("Familias", info["familias"])
        c3.metric("Trabajadas", info["trabajadas"])
        c4.metric("Pendientes", info["pendientes"])
        c5.metric("Avance %", info["avance_pct"])

        df = dataset_actividad(ac)

        st.caption(f"Registros: {len(df):,}")

        st.dataframe(df.head(2000), use_container_width=True)

    # =====================================================
    # TAB ACTIVIDADES
    # =====================================================
    with tab_actividades:

        st.subheader("Crear actividad")

        nueva = st.text_input("Nombre actividad")

        if st.button("Crear"):

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
            st.info("No hay actividades")
            return

        ac = st.selectbox("Actividad", actividades)

        col1, col2 = st.columns(2)

        with col1:

            if st.button("Regenerar"):

                try:
                    with st.spinner("Regenerando actividad..."):
                        regenerar_actividad(ac)

                    st.success("Regenerada")
                    st.rerun()

                except Exception as e:
                    st.error(e)

        with col2:

            if st.button("Eliminar"):

                if st.checkbox("Confirmar eliminación"):
                    try:
                        eliminar_actividad(ac)
                        st.success("Eliminada")
                        st.rerun()
                    except Exception as e:
                        st.error(e)

        st.divider()

        if st.button("Regenerar TODAS"):

            try:
                with st.spinner("Regenerando todas las actividades..."):
                    regenerar_todas_las_actividades()

                st.success("Todas regeneradas")
                st.rerun()

            except Exception as e:
                st.error(e)

    # =====================================================
    # TAB BD
    # =====================================================
    with tab_bd:

        st.subheader("Cargar BD_ACTUALIZACION")

        archivo = st.file_uploader("Subir parquet", type=["parquet"])

        if archivo:

            if st.button("Guardar BD"):

                try:
                    df_bd = guardar_bd_actualizacion_desde_upload(archivo)

                    st.success("BD cargada")
                    st.write(f"Filas: {len(df_bd):,}")
                    st.write(f"Columnas: {len(df_bd.columns)}")

                except Exception as e:
                    st.error(e)

    # =====================================================
    # TAB CONSOLIDACION
    # =====================================================
    with tab_consolidacion:

        actividades = obtener_actividades()

        if not actividades:
            st.info("No hay actividades")
            return

        ac = st.selectbox("Actividad", actividades)

        df = consolidar(ac)

        st.dataframe(df.head(2000), use_container_width=True)

        st.download_button(
            "⬇ Descargar Excel",
            data=df_to_excel_bytes(df),
            file_name=f"{ac}_CONSOLIDADO.xlsx"
        )

        master = leer_master()

        st.download_button(
            "⬇ Descargar MASTER",
            data=df_to_excel_bytes(master),
            file_name="MASTER_TOTAL.xlsx"
        )
