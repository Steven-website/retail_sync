import streamlit as st
import pandas as pd
from data_manager import (
    obtener_actividades,
    dataset_actividad,
    filtrar_por_familias,
    filtrar_por_ac,
    actualizar_desde_csv,
    a_excel,
)
from queue_manager import handle_queue, submit_op
import historial as hist


def _leer_archivo(archivo) -> pd.DataFrame:
    archivo.seek(0)
    try:
        df = pd.read_excel(archivo, engine="openpyxl")
        df.columns = df.columns.str.strip().str.replace('\ufeff', '', regex=False)
        return df
    except Exception as e:
        raise Exception(f"No se pudo leer el archivo Excel. Verifique el archivo. ({e})")


def _h_actualizar(ac, datos, familias):
    actualizar_desde_csv(ac, datos, familias)
    fam_label = ', '.join(familias[:3]) + ('...' if len(familias) > 3 else '')
    hist.registrar(
        st.session_state.get("usuario", "?"),
        "Actualizó datos",
        f"{ac} — familias: {fam_label}"
    )


def adc_view():
    st.header("🧑‍💻 Panel ADC")

    familias = st.session_state.get("familias", [])
    if not familias:
        st.error("⚠️ No tiene familias asignadas. Contacte al administrador.")
        return

    # ── MENSAJES DE COLA ─────────────────────────────────
    if st.session_state.pop("_q_cancelled", False):
        st.warning(
            "⚠️ Operación cancelada. "
            "Si el sistema sigue ocupado, espere unos segundos y vuélvala a intentar."
        )

    # ── COLA ──────────────────────────────────────────
    try:
        completed, _ = handle_queue({
            "actualizar_csv": _h_actualizar,
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

    # ── VISTA ────────────────────────────────────────────
    actividades = obtener_actividades()
    if not actividades:
        st.warning("No hay actividades disponibles.")
        return

    ac = st.selectbox("Seleccione actividad", actividades)
    df = dataset_actividad(ac)
    if df.empty:
        st.warning("La actividad no tiene datos.")
        return

    df = filtrar_por_ac(df, ac)
    df_filtrado = filtrar_por_familias(df, familias)
    if df_filtrado.empty:
        st.warning("No hay artículos para sus familias en esta actividad.")
        return

    st.caption(f"Registros de sus familias: {len(df_filtrado):,}")
    st.dataframe(df_filtrado, use_container_width=True, height=400)
    st.divider()

    st.download_button(
        "⬇️ Descargar Excel para trabajar",
        data=a_excel(df_filtrado),
        file_name=f"{ac}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.divider()
    st.subheader("📤 Subir archivo trabajado")
    st.caption("Solo se actualizan sus familias. El resto permanece intacto.")

    if "upload_key" not in st.session_state:
        st.session_state.upload_key = 0

    archivo = st.file_uploader(
        "Seleccione Excel trabajado", type=["xlsx"],
        key=f"uploader_{st.session_state.upload_key}"
    )

    if archivo:
        try:
            preview = _leer_archivo(archivo)
            st.caption(f"Vista previa: {len(preview):,} filas · {len(preview.columns)} columnas")
            st.dataframe(preview.head(5), use_container_width=True)
        except Exception as e:
            st.error(f"❌ {e}")
            return

        if st.button("✅ Actualizar BASE"):
            try:
                datos = _leer_archivo(archivo)
                fam_label = ', '.join(familias[:3]) + ('...' if len(familias) > 3 else '')
                submit_op(
                    "actualizar_csv",
                    f"Actualizar {ac} — familias: {fam_label}",
                    {"ac": ac, "datos": datos, "familias": familias},
                )
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")
