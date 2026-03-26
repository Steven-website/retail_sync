import streamlit as st
import pandas as pd
from data_manager import leer_vm, subir_vm, a_excel
import historial as hist


def vm_view():
    st.header("🏪 Panel Visual Merchandising")

    vm = leer_vm()
    if not vm.empty:
        st.success(f"✔ Datos cargados — {len(vm):,} filas · {len(vm.columns)} columnas")
        st.dataframe(vm, use_container_width=True, height=400)
        st.divider()
        st.download_button(
            "⬇️ Descargar Excel",
            data=a_excel(vm),
            file_name="VM_MERCHANDISING.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("⚠️ No hay datos cargados aún.")

    st.divider()
    st.subheader("📤 Subir archivo Excel")

    archivo = st.file_uploader("Seleccione archivo Excel (.xlsx)", type=["xlsx"])
    if archivo:
        try:
            df_preview = pd.read_excel(archivo, engine="openpyxl")
            df_preview.columns = df_preview.columns.str.strip()
            st.caption(f"Vista previa: {len(df_preview):,} filas · {len(df_preview.columns)} columnas")
            st.dataframe(df_preview.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"❌ {e}")
            return

        if st.button("💾 Guardar"):
            try:
                archivo.seek(0)
                df = subir_vm(archivo)
                hist.registrar(
                    st.session_state.get("usuario", "?"),
                    "Subió VM Merchandising",
                    f"{len(df):,} filas"
                )
                st.success("✔ Datos guardados correctamente.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
