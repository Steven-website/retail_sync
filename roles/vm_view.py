import streamlit as st
import pandas as pd
from data_manager import obtener_actividades, leer_filtro_act, actualizar_filtro_vm, a_excel
import historial as hist


def _leer_excel(archivo) -> pd.DataFrame:
    archivo.seek(0)
    try:
        df = pd.read_excel(archivo, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
        return df
    except Exception as e:
        raise Exception(f"No se pudo leer el archivo Excel. ({e})")


def vm_view():
    st.header("🏪 Panel Visual Merchandising")

    actividades = obtener_actividades()
    if not actividades:
        st.warning("No hay actividades disponibles.")
        return

    ac = st.selectbox("Seleccione actividad", actividades)

    filtro = leer_filtro_act(ac)
    if filtro.empty:
        st.warning("⚠️ Esta actividad no tiene filtro cargado. El Master debe cargarlo primero.")
        return

    st.caption(f"Registros: {len(filtro):,}")
    st.dataframe(filtro, use_container_width=True, height=400)
    st.divider()

    st.download_button(
        "⬇️ Descargar Excel para trabajar",
        data=a_excel(filtro),
        file_name=f"{ac}_VM.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()
    st.subheader("📤 Subir archivo trabajado")
    st.caption("Solo se actualizan las columnas R01-R40. El resto permanece intacto.")

    if "upload_key_vm" not in st.session_state:
        st.session_state.upload_key_vm = 0

    archivo = st.file_uploader(
        "Seleccione Excel trabajado", type=["xlsx"],
        key=f"vm_uploader_{st.session_state.upload_key_vm}"
    )

    if archivo:
        try:
            preview = _leer_excel(archivo)
            st.caption(f"Vista previa: {len(preview):,} filas · {len(preview.columns)} columnas")
            st.dataframe(preview.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"❌ {e}")
            return

        if st.button("✅ Guardar"):
            try:
                datos = _leer_excel(archivo)
                actualizar_filtro_vm(ac, datos)
                hist.registrar(
                    st.session_state.get("usuario", "?"),
                    "Actualizó VM",
                    ac
                )
                st.session_state.upload_key_vm += 1
                st.success("✔ Datos guardados correctamente.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
