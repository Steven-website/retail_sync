import io
import json
import os
from typing import List, Optional

import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")
st.title("🛒 Retail Sync")

# =========================
# CONFIG
# =========================
DATA_DIR = "data"
RUTA_BD = os.path.join(DATA_DIR, "BD_ACTUALIZACION.parquet")
RUTA_MASTER = os.path.join(DATA_DIR, "master.parquet")
RUTA_USERS = "usuarios.json"

PK = "PK_Articulos"

COLUMNAS_COMERCIALES = [
    "MUNDO_AC",
    "PRECIO_PROMOCIONAL",
    "DESCUENTO",
    "PORC_AHORRO",
    "FECHA_INICIO",
    "FECHA_FIN",
    "ACCION",
    "COMENTARIO",
]

ROLES_CONSOLIDAN = {"MASTER", "JEFE_ADC", "PRECIOS", "MARKETING"}


# =========================
# HELPERS
# =========================
def asegurar_directorio() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


def normalizar_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="BASE")
    output.seek(0)
    return output.getvalue()


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)
    return output.getvalue()


def limpiar_texto(x) -> str:
    return str(x).strip()


def parse_familias(user: dict) -> List[str]:
    if "FAMILIAS" in user and isinstance(user["FAMILIAS"], list):
        return [limpiar_texto(x) for x in user["FAMILIAS"] if limpiar_texto(x)]

    if "FAMILIAS" in user and isinstance(user["FAMILIAS"], str):
        return [x.strip() for x in user["FAMILIAS"].split(",") if x.strip()]

    if "FAMILIA" in user and isinstance(user["FAMILIA"], list):
        return [limpiar_texto(x) for x in user["FAMILIA"] if limpiar_texto(x)]

    if "FAMILIA" in user and isinstance(user["FAMILIA"], str):
        txt = user["FAMILIA"].strip()
        if not txt:
            return []
        if "," in txt:
            return [x.strip() for x in txt.split(",") if x.strip()]
        return [txt]

    return []


def cargar_usuarios() -> List[dict]:
    if not os.path.exists(RUTA_USERS):
        st.error("No existe usuarios.json")
        st.stop()

    with open(RUTA_USERS, "r", encoding="utf-8") as f:
        users = json.load(f)

    if not isinstance(users, list):
        st.error("usuarios.json debe ser una lista")
        st.stop()

    return users


def autenticar(usuario_input: str, password_input: str) -> Optional[dict]:
    users = cargar_usuarios()

    usuario_input_norm = usuario_input.strip().lower()
    password_input_norm = password_input.strip()

    for user in users:
        usuario_json = str(user.get("usuario", "")).strip().lower()
        password_json = str(user.get("password", "")).strip()

        if usuario_input_norm == usuario_json and password_input_norm == password_json:
            return user

    return None


def cargar_bd() -> pd.DataFrame:
    if not os.path.exists(RUTA_BD):
        st.error("No existe BD_ACTUALIZACION.parquet")
        st.stop()

    df = pd.read_parquet(RUTA_BD)
    df = normalizar_cols(df)

    if PK not in df.columns:
        st.error(f"BD_ACTUALIZACION no tiene la columna {PK}")
        st.stop()

    return df


def crear_master_desde_bd() -> pd.DataFrame:
    bd = cargar_bd().copy()

    if "ACTIVIDAD_COMERCIAL" not in bd.columns:
        bd.insert(1, "ACTIVIDAD_COMERCIAL", "")

    for c in COLUMNAS_COMERCIALES:
        if c not in bd.columns:
            bd[c] = None

    bd.to_parquet(RUTA_MASTER, index=False)
    return bd


def cargar_master() -> pd.DataFrame:
    if not os.path.exists(RUTA_MASTER):
        return crear_master_desde_bd()

    master = pd.read_parquet(RUTA_MASTER)
    master = normalizar_cols(master)

    if PK not in master.columns:
        return crear_master_desde_bd()

    if "ACTIVIDAD_COMERCIAL" not in master.columns:
        master.insert(1, "ACTIVIDAD_COMERCIAL", "")

    for c in COLUMNAS_COMERCIALES:
        if c not in master.columns:
            master[c] = None

    return master


def guardar_master(df: pd.DataFrame) -> None:
    df = normalizar_cols(df)
    for c in COLUMNAS_COMERCIALES:
        if c not in df.columns:
            df[c] = None
    df.to_parquet(RUTA_MASTER, index=False)


def actividades_disponibles(master: pd.DataFrame) -> List[str]:
    if "ACTIVIDAD_COMERCIAL" not in master.columns:
        return []
    vals = master["ACTIVIDAD_COMERCIAL"].dropna().astype(str).str.strip().unique().tolist()
    return [x for x in vals if x != ""]


def crear_actividad(master: pd.DataFrame, nombre_ac: str) -> pd.DataFrame:
    nombre_ac = nombre_ac.strip()
    if not nombre_ac:
        st.warning("Debe escribir un nombre de actividad.")
        return master

    existentes = set(actividades_disponibles(master))
    if nombre_ac in existentes:
        st.warning("La actividad ya existe.")
        return master

    bd = cargar_bd().copy()
    bd.insert(1, "ACTIVIDAD_COMERCIAL", nombre_ac)

    for c in COLUMNAS_COMERCIALES:
        if c not in bd.columns:
            bd[c] = None

    # Alinear columnas con master actual
    for col in master.columns:
        if col not in bd.columns:
            bd[col] = None

    for col in bd.columns:
        if col not in master.columns:
            master[col] = None

    bd = bd[master.columns]
    master = pd.concat([master, bd], ignore_index=True)
    guardar_master(master)
    return master


def regenerar_bases(master: pd.DataFrame) -> pd.DataFrame:
    bd = cargar_bd()
    acts = actividades_disponibles(master)

    if not acts:
        st.warning("No hay actividades para regenerar.")
        return master

    nuevos = []
    for ac in acts:
        base_ac = master[master["ACTIVIDAD_COMERCIAL"].astype(str).str.strip() == ac].copy()

        # asegurar columnas comerciales
        for c in COLUMNAS_COMERCIALES:
            if c not in base_ac.columns:
                base_ac[c] = None

        keep_cols = [PK] + COLUMNAS_COMERCIALES
        keep_cols = [c for c in keep_cols if c in base_ac.columns]

        temp = bd.copy()
        temp.insert(1, "ACTIVIDAD_COMERCIAL", ac)
        temp = temp.merge(base_ac[keep_cols], on=PK, how="left")

        # alinear columnas contra master
        for col in master.columns:
            if col not in temp.columns:
                temp[col] = None
        temp = temp[master.columns]
        nuevos.append(temp)

    if nuevos:
        master_nuevo = pd.concat(nuevos, ignore_index=True)
        guardar_master(master_nuevo)
        return master_nuevo

    return master


def consolidado_actividad(master: pd.DataFrame, actividad: str) -> pd.DataFrame:
    df = master[master["ACTIVIDAD_COMERCIAL"].astype(str).str.strip() == actividad].copy()
    return df


def filtrar_por_familias(df: pd.DataFrame, familias: List[str]) -> pd.DataFrame:
    if not familias:
        return df.copy()

    if "FAMILIA" not in df.columns:
        return df.copy()

    familias_norm = {x.strip() for x in familias if x.strip()}
    if not familias_norm:
        return df.copy()

    return df[df["FAMILIA"].astype(str).str.strip().isin(familias_norm)].copy()


def analizar_actividad(df: pd.DataFrame) -> dict:
    total = len(df)

    if total == 0:
        return {
            "total": 0,
            "trabajados": 0,
            "pendientes": 0,
            "avance_pct": 0.0,
            "familias": 0,
        }

    cols = [c for c in COLUMNAS_COMERCIALES if c in df.columns]
    if cols:
        trabajados_mask = df[cols].notna().any(axis=1)
        trabajados = int(trabajados_mask.sum())
    else:
        trabajados = 0

    pendientes = total - trabajados
    familias = df["FAMILIA"].nunique() if "FAMILIA" in df.columns else 0
    avance_pct = round((trabajados / total) * 100, 2) if total > 0 else 0.0

    return {
        "total": total,
        "trabajados": trabajados,
        "pendientes": pendientes,
        "avance_pct": avance_pct,
        "familias": int(familias),
    }


def actualizar_parcial_adc(master: pd.DataFrame, actividad: str, cambios: pd.DataFrame) -> pd.DataFrame:
    cambios = normalizar_cols(cambios)

    if PK not in cambios.columns:
        st.error(f"El archivo no tiene la columna {PK}")
        return master

    if cambios[PK].duplicated().any():
        st.error(f"El archivo tiene {PK} duplicados.")
        return master

    cols_presentes = [c for c in COLUMNAS_COMERCIALES if c in cambios.columns]
    if not cols_presentes:
        st.error("El archivo no trae columnas comerciales para actualizar.")
        return master

    cambios = cambios[[PK] + cols_presentes].copy()

    mask = master["ACTIVIDAD_COMERCIAL"].astype(str).str.strip() == actividad
    master_ac = master.loc[mask].copy()

    if master_ac.empty:
        st.warning("No hay registros para esa actividad.")
        return master

    master_ac = master_ac.set_index(PK)
    cambios = cambios.set_index(PK)

    master_ac.update(cambios)

    master.loc[mask] = master_ac.reset_index()
    guardar_master(master)
    return master


# =========================
# INICIO
# =========================
asegurar_directorio()

if "login" not in st.session_state:
    st.session_state.login = False

if not st.session_state.login:
    u = st.text_input("Usuario")
    p = st.text_input("Password", type="password")

    if st.button("Ingresar"):
        user = autenticar(u, p)
        if user is None:
            st.error("Credenciales incorrectas")
            st.stop()

        st.session_state.login = True
        st.session_state.usuario = str(user.get("usuario", "")).strip()
        st.session_state.rol = str(user.get("rol", "")).strip().upper()
        st.session_state.familias = parse_familias(user)
        st.rerun()

    st.stop()

# =========================
# SESION
# =========================
usuario = st.session_state.usuario
rol = st.session_state.rol
familias_usuario = st.session_state.familias

st.sidebar.success(f"Usuario: {usuario}")
st.sidebar.info(f"Rol: {rol}")

if familias_usuario:
    st.sidebar.write("Familias:")
    for fam in familias_usuario:
        st.sidebar.write(f"- {fam}")

if st.sidebar.button("Cerrar sesión"):
    st.session_state.clear()
    st.rerun()

master = cargar_master()

# =========================
# MASTER
# =========================
if rol == "MASTER":
    st.header("ROL MASTER")

    st.subheader("1. Cargar BD_ACTUALIZACION")
    archivo_bd = st.file_uploader("Subir nuevo parquet oficial", type=["parquet"], key="uploader_bd")
    if archivo_bd is not None:
        bd_nueva = pd.read_parquet(archivo_bd)
        bd_nueva = normalizar_cols(bd_nueva)

        if PK not in bd_nueva.columns:
            st.error("El parquet no tiene PK_Articulos")
        else:
            bd_nueva.to_parquet(RUTA_BD, index=False)
            st.success("BD_ACTUALIZACION cargada correctamente")

    st.subheader("2. Crear Actividad Comercial")
    nueva_ac = st.text_input("Nombre nueva actividad")
    if st.button("Crear actividad"):
        master = crear_actividad(master, nueva_ac)
        st.success("Actividad creada")
        st.rerun()

    st.subheader("3. Regenerar bases")
    if st.button("Regenerar bases"):
        master = regenerar_bases(master)
        st.success("Bases regeneradas")

    acts = actividades_disponibles(master)

    if acts:
        st.subheader("4. Analizar actividad")
        ac_sel = st.selectbox("Actividad", acts, key="master_actividad")
        df_ac = consolidado_actividad(master, ac_sel)

        analisis = analizar_actividad(df_ac)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Filas", analisis["total"])
        c2.metric("Trabajadas", analisis["trabajados"])
        c3.metric("Pendientes", analisis["pendientes"])
        c4.metric("Avance %", analisis["avance_pct"])

        st.dataframe(df_ac, use_container_width=True)

        st.subheader("5. Consolidar final")
        if st.button("Consolidar final"):
            st.success("Consolidado final listo para descarga")

        st.download_button(
            "⬇ Descargar Excel consolidado",
            data=df_to_excel_bytes(df_ac),
            file_name=f"{ac_sel}_CONSOLIDADO.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.subheader("6. Descargar parquet total")
    st.download_button(
        "⬇ Descargar parquet total",
        data=df_to_parquet_bytes(master),
        file_name="MASTER_TOTAL.parquet",
        mime="application/octet-stream"
    )

# =========================
# JEFE ADC
# =========================
elif rol == "JEFE_ADC":
    st.header("ROL JEFE ADC")

    acts = actividades_disponibles(master)
    if not acts:
        st.warning("No existen actividades")
        st.stop()

    ac_sel = st.selectbox("Seleccionar actividad", acts, key="jefe_actividad")

    if st.button("Consolidar"):
        df_cons = consolidado_actividad(master, ac_sel)

        # primero consolida, luego filtra familias
        familias_disponibles = sorted(df_cons["FAMILIA"].dropna().astype(str).unique().tolist()) if "FAMILIA" in df_cons.columns else []
        familias_permitidas = [f for f in familias_disponibles if f in familias_usuario] if familias_usuario else familias_disponibles

        familias_sel = st.multiselect(
            "Filtrar familias",
            options=familias_permitidas,
            default=familias_permitidas,
            key="jefe_familias"
        )

        df_cons = filtrar_por_familias(df_cons, familias_sel)

        st.dataframe(df_cons, use_container_width=True)

        st.download_button(
            "⬇ Descargar Excel",
            data=df_to_excel_bytes(df_cons),
            file_name=f"{ac_sel}_JEFE_ADC.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# =========================
# ADC
# =========================
elif rol == "ADC":
    st.header("ROL ADC")

    acts = actividades_disponibles(master)
    if not acts:
        st.warning("No existen actividades")
        st.stop()

    ac_sel = st.selectbox("Seleccionar actividad", acts, key="adc_actividad")

    df_adc = consolidado_actividad(master, ac_sel)
    df_adc = filtrar_por_familias(df_adc, familias_usuario)

    st.dataframe(df_adc, use_container_width=True)

    st.download_button(
        "⬇ Descargar Excel",
        data=df_to_excel_bytes(df_adc),
        file_name=f"{ac_sel}_ADC.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    archivo_excel = st.file_uploader("Subir Excel trabajado", type=["xlsx"], key="adc_upload")
    if archivo_excel is not None:
        cambios = pd.read_excel(archivo_excel)

        if st.button("Actualizar"):
            master = actualizar_parcial_adc(master, ac_sel, cambios)
            st.success("Actualización aplicada")

# =========================
# PRECIOS / MARKETING
# =========================
elif rol in {"PRECIOS", "MARKETING"}:
    st.header(f"ROL {rol}")

    acts = actividades_disponibles(master)
    if not acts:
        st.warning("No existen actividades")
        st.stop()

    ac_sel = st.selectbox("Seleccionar actividad", acts, key=f"{rol}_actividad")

    if st.button("Consolidar"):
        df_cons = consolidado_actividad(master, ac_sel)
        st.dataframe(df_cons, use_container_width=True)

        st.download_button(
            "⬇ Descargar consolidado",
            data=df_to_excel_bytes(df_cons),
            file_name=f"{ac_sel}_{rol}_CONSOLIDADO.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

else:
    st.error("Rol no reconocido en usuarios.json")
