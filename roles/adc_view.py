import streamlit as st
import pandas as pd
import io
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    filtrar_por_familias,
    actualizar_desde_excel,
)

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

    ac = st.selectbox("Seleccione actividad", actividades)

    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    df_filtrado = filtrar_por_familias(df, familias)
    if df_filtrado.empty:
        st.warning("No hay artículos para sus familias en esta actividad.")
        return

    st.caption(f"Registros: {len(df_filtrado):,}")
    st.dataframe(df_filtrado, use_container_width=True, height=400)
    st.divider()

    # ── Descargar Excel filtrado por familias ──
    buf = io.BytesIO()
    df_filtrado.to_excel(buf, index=False, engine="xlsxwriter")
    buf.seek(0)

    st.download_button(
        "⬇️ Descargar Excel de mis familias",
        data=buf.getvalue(),
        file_name=f"{ac}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()

    # ── Subir y actualizar ──
    st.subheader("📤 Subir archivo trabajado")
    st.caption("Solo se actualizan sus familias. El resto no se toca.")

    if "upload_key" not in st.session_state:
        st.session_state.upload_key = 0

    archivo = st.file_uploader(
        "Seleccione Excel trabajado",
        type=["xlsx"],
        key=f"uploader_{st.session_state.upload_key}"
    )

    if archivo:
        try:
            preview = pd.read_excel(archivo)
            st.caption(f"Vista previa: {len(preview):,} filas")
            st.dataframe(preview.head(5), use_container_width=True)
            archivo.seek(0)
        except Exception:
            pass

        if st.button("✅ Actualizar"):
            try:
                with st.spinner("Actualizando..."):
                    base_excel = pd.read_excel(archivo)
                    actualizar_desde_excel(ac, base_excel, familias)
                st.success("✔ Actualización aplicada correctamente.")
                st.session_state.upload_key += 1
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
