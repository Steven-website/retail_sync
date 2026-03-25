import streamlit as st
import pandas as pd
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    filtrar_por_familias,
    actualizar_desde_csv,
    a_csv,
)
from queue_manager import handle_queue, submit_op


def _leer_csv(archivo) -> pd.DataFrame:
    """Lee CSV detectando separador automáticamente y limpiando nombres de columnas."""
    for encoding in ["utf-8-sig", "utf-8", "latin-1"]:
        for sep in [",", ";", "\t"]:
            try:
                archivo.seek(0)
                df = pd.read_csv(
                    archivo,
                    encoding=encoding,
                    sep=sep,
                    quotechar='"',
                    quoting=0,
                    on_bad_lines="skip"
                )
                if len(df.columns) <= 1:
                    continue
                df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
                return df
            except Exception:
                continue
    raise Exception("No se pudo leer el CSV. Verifique el archivo.")


def adc_view():
    st.header("🧑‍💻 Panel ADC")

    familias = st.session_state.get("familias", [])
    if not familias:
        st.error("⚠️ No tiene familias asignadas. Contacte al administrador.")
        return

    # ── MENSAJES DE ESTADO DE COLA ────────────────────────
    if st.session_state.pop("_q_cancelled", False):
        st.warning(
            "⚠️ Operación cancelada. "
            "Si el sistema sigue ocupado, espere unos segundos y vuélvala a intentar."
        )

    # ── PROCESAR COLA ─────────────────────────────────────
    try:
        completed, _ = handle_queue({
            "actualizar_csv": lambda ac, datos, familias: actualizar_desde_csv(ac, datos, familias),
        })
    except Exception as e:
        st.error(f"❌ {e}")
        st.info("Si el problema persiste, recargue la página e intente de nuevo.")
        completed = False

    if completed:
        st.success("✔ BASE actualizada correctamente.")
        if "upload_key" not in st.session_state:
            st.session_state.upload_key = 0
        st.session_state.upload_key += 1
        st.rerun()
        return

    # ── VISTA NORMAL ──────────────────────────────────────
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

    st.caption(f"Registros de sus familias: {len(df_filtrado):,}")
    st.dataframe(df_filtrado, use_container_width=True, height=400)
    st.divider()

    st.download_button(
        "⬇️ Descargar CSV para trabajar",
        data=a_csv(df_filtrado),
        file_name=f"{ac}_ADC.csv",
        mime="text/csv"
    )

    st.divider()
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
            st.caption(f"Vista previa: {len(preview):,} filas · {len(preview.columns)} columnas")
            st.dataframe(preview.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"❌ {e}")
            return

        if st.button("✅ Actualizar BASE"):
            try:
                datos = _leer_csv(archivo)
                fam_label = ', '.join(familias[:3]) + ('...' if len(familias) > 3 else '')
                submit_op(
                    "actualizar_csv",
                    f"Actualizar {ac} — familias: {fam_label}",
                    {"ac": ac, "datos": datos, "familias": familias},
                )
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
