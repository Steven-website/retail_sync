import streamlit as st
import pandas as pd
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    filtrar_por_familias,
    actualizar_desde_csv,
    a_csv,
)

def _leer_csv(archivo) -> pd.DataFrame:
    """Lee CSV manejando comas dentro de campos y distintas codificaciones."""
    for encoding in ["utf-8-sig", "utf-8", "latin-1"]:
        try:
            archivo.seek(0)
            return pd.read_csv(
                archivo,
                encoding=encoding,
                sep=",",
                quotechar='"',
                quoting=0,        # QUOTE_MINIMAL — respeta comillas dobles
                on_bad_lines="skip"
            )
        except Exception:
            continue
    raise Exception("No se pudo leer el CSV. Verifique que el archivo no esté corrupto.")

def adc_view():
    st.header("🧑‍💻 Panel ADC")

    familias = st.session_state.get("familias", [])
    if not familias:
        st.error("⚠️ No tiene familias asignadas. Contacte al administrador.")
        return

    actividades = obtener_actividades()
    if not actividades:
        st.warning("No hay actividades disponibles.")
        return

    # Paso 1: Seleccionar actividad
    ac = st.selectbox("Seleccione actividad", actividades)

    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    df_filtrado = filtrar_por_familias(df, familias)
    if df_filtrado.empty:
        st.warning("No hay artículos para sus familias en esta actividad.")
        return

    st.caption(f"Registros de sus familias: {len(df_filtrado):,}")
    st.dataframe(df_filtrado, use_container_width=True, height=400)
    st.divider()

    # Paso 2: Descargar CSV para trabajar offline
    st.download_button(
        "⬇️ Descargar CSV para trabajar",
        data=a_csv(df_filtrado),
        file_name=f"{ac}_ADC.csv",
        mime="text/csv"
    )

    st.divider()

    # Paso 3: Subir CSV trabajado y actualizar
    st.subheader("📤 Subir archivo trabajado")
    st.caption("Solo se actualizan sus familias. El resto permanece intacto.")

    if "upload_key" not in st.session_state:
        st.session_state.upload_key = 0

    archivo = st.file_uploader(
        "Seleccione CSV trabajado",
        type=["csv"],
        key=f"uploader_{st.session_state.upload_key}"
    )

    if archivo:
        try:
            preview = _leer_csv(archivo)
            st.caption(f"Vista previa: {len(preview):,} filas")
            st.dataframe(preview.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"❌ No se pudo leer el archivo: {e}")
            return

        if st.button("✅ Actualizar BASE"):
            try:
                with st.spinner("Aplicando cambios..."):
                    datos = _leer_csv(archivo)
                    actualizar_desde_csv(ac, datos, familias)
                st.success("✔ BASE actualizada correctamente.")
                st.session_state.upload_key += 1
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
