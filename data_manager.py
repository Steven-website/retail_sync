import io
import os
import re
import unicodedata
from typing import Optional, List
import pandas as pd
from config import (
    RUTA_BD, RUTA_BASE, RUTA_ACTIVIDADES, PK,
    CAMPO_ACTIVIDAD, CAMPO_FAMILIA,
    COLUMNAS_COMERCIALES,
)
from github_storage import push_parquet, delete_file

# ─── HELPERS ────────────────────────────────────────────────────────

def _safe_name(nombre: str) -> str:
    """Convierte nombre de actividad a nombre de archivo válido."""
    safe = re.sub(r'[^\w\s-]', '_', nombre.strip())
    safe = re.sub(r'\s+', '_', safe)
    return safe[:80] or "actividad"

def _ruta_act(nombre: str) -> str:
    return os.path.join(RUTA_ACTIVIDADES, f"{_safe_name(nombre)}.parquet")

def _github_path_act(nombre: str) -> str:
    return f"data/actividades/{_safe_name(nombre)}.parquet"

def _leer_parquet(ruta: str) -> pd.DataFrame:
    if not os.path.exists(ruta):
        return pd.DataFrame()
    try:
        return pd.read_parquet(ruta)
    except Exception as e:
        raise Exception(f"Error leyendo archivo: {e}")

def _normalizar(valor) -> str:
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        pass
    txt = str(valor).strip().upper()
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(c for c in txt if not unicodedata.combining(c))

def _limpiar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.replace('\ufeff', '', regex=False)
        .str.replace('\u200b', '', regex=False)
    )
    return df

def _validar_pk(df: pd.DataFrame) -> pd.DataFrame:
    df = _limpiar_columnas(df)
    if PK not in df.columns:
        raise Exception(f"El archivo no tiene la columna '{PK}'. Columnas encontradas: {list(df.columns[:5])}")
    df[PK] = (
        df[PK]
        .apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() not in ["", "nan"] else "")
        .str.strip()
    )
    return df

def _agregar_cols_comerciales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in COLUMNAS_COMERCIALES:
        if col not in df.columns:
            df[col] = None
    return df

def a_csv(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    return buf.getvalue()

def a_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf.getvalue()

def a_parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf.getvalue()

# ─── POR ACTIVIDAD ──────────────────────────────────────────────────

def _leer_actividad(nombre: str) -> pd.DataFrame:
    """Lee el parquet de UNA sola actividad."""
    ruta = _ruta_act(nombre)
    if not os.path.exists(ruta):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(ruta)
        df = _limpiar_columnas(df)
        df = _agregar_cols_comerciales(df)
        df = _validar_pk(df)
        if CAMPO_ACTIVIDAD not in df.columns:
            df[CAMPO_ACTIVIDAD] = nombre
        df[CAMPO_ACTIVIDAD] = df[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
        return df
    except Exception as e:
        raise Exception(f"Error leyendo actividad '{nombre}': {e}")

def _guardar_actividad(nombre: str, df: pd.DataFrame):
    """Guarda UNA actividad en su propio archivo y lo sube a GitHub."""
    os.makedirs(RUTA_ACTIVIDADES, exist_ok=True)
    df = _limpiar_columnas(df)
    df = _agregar_cols_comerciales(df)
    df = _validar_pk(df)
    df[CAMPO_ACTIVIDAD] = nombre
    df[CAMPO_ACTIVIDAD] = df[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    df.to_parquet(_ruta_act(nombre), index=False)
    push_parquet(df, _github_path_act(nombre), f"update actividad {nombre}")

def _eliminar_actividad_archivo(nombre: str):
    """Elimina el archivo local y en GitHub de UNA actividad."""
    ruta = _ruta_act(nombre)
    if os.path.exists(ruta):
        os.remove(ruta)
    delete_file(_github_path_act(nombre), f"eliminar actividad {nombre}")

# ─── MIGRACIÓN DESDE BASE.parquet ───────────────────────────────────────

def _migrar():
    """
    Migra BASE.parquet al nuevo formato (un archivo por actividad).
    Solo se ejecuta si no existen archivos en data/actividades/.
    """
    if not os.path.exists(RUTA_BASE):
        return
    try:
        base = pd.read_parquet(RUTA_BASE)
        if base.empty or CAMPO_ACTIVIDAD not in base.columns:
            return
        base = _limpiar_columnas(base)
        base = _agregar_cols_comerciales(base)
        os.makedirs(RUTA_ACTIVIDADES, exist_ok=True)
        for nombre, grupo in base.groupby(CAMPO_ACTIVIDAD):
            nombre = str(nombre).strip()
            if nombre:
                _guardar_actividad(nombre, grupo.copy())
    except Exception:
        pass

def _archivos_actividades() -> list:
    os.makedirs(RUTA_ACTIVIDADES, exist_ok=True)
    return [f for f in os.listdir(RUTA_ACTIVIDADES) if f.endswith('.parquet')]

def _asegurar_actividades():
    """Migra si no hay archivos por actividad todavía."""
    if not _archivos_actividades():
        _migrar()

# ─── BD_ACTUALIZACION ──────────────────────────────────────────────────

def leer_bd() -> pd.DataFrame:
    df = _leer_parquet(RUTA_BD)
    if df.empty:
        return df
    return _validar_pk(df)

def subir_bd(file) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file)
    except Exception as e:
        raise Exception(f"No se pudo leer el parquet: {e}")
    df = _validar_pk(df)
    df = df.drop_duplicates(subset=[PK], keep="last")
    df.to_parquet(RUTA_BD, index=False)
    push_parquet(df, "data/BD_ACTUALIZACION.parquet", "update BD_ACTUALIZACION")
    return df

# ─── BASE (vista completa) ───────────────────────────────────────────────

def leer_base() -> pd.DataFrame:
    """Concatena todas las actividades. Usar solo para descargas o vistas globales."""
    _asegurar_actividades()
    archivos = _archivos_actividades()
    if not archivos:
        return pd.DataFrame()
    dfs = []
    for f in archivos:
        try:
            df = pd.read_parquet(os.path.join(RUTA_ACTIVIDADES, f))
            df = _limpiar_columnas(df)
            df = _agregar_cols_comerciales(df)
            dfs.append(df)
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    base = pd.concat(dfs, ignore_index=True)
    base = _validar_pk(base)
    if CAMPO_ACTIVIDAD not in base.columns:
        base[CAMPO_ACTIVIDAD] = ""
    base[CAMPO_ACTIVIDAD] = base[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    return base

# ─── ACTIVIDADES ─────────────────────────────────────────────────────

def obtener_actividades() -> list:
    """Lista actividades leyendo sólo la primera fila de cada archivo."""
    _asegurar_actividades()
    actividades = set()
    for f in _archivos_actividades():
        try:
            df = pd.read_parquet(
                os.path.join(RUTA_ACTIVIDADES, f),
                columns=[CAMPO_ACTIVIDAD]
            )
            if not df.empty and CAMPO_ACTIVIDAD in df.columns:
                nombre = df[CAMPO_ACTIVIDAD].iloc[0]
                if pd.notna(nombre) and str(nombre).strip():
                    actividades.add(str(nombre).strip())
        except Exception:
            pass
    return sorted(actividades)

def crear_actividad(nombre: str) -> pd.DataFrame:
    nombre = nombre.strip()
    if not nombre:
        raise Exception("El nombre no puede estar vacío.")
    bd = leer_bd()
    if bd.empty:
        raise Exception("No hay BD_ACTUALIZACION cargada. Súbala primero en la tab BD.")
    if nombre in obtener_actividades():
        raise Exception(f"La actividad '{nombre}' ya existe.")

    # Obtener columnas de referencia de una actividad existente
    cols_ref = None
    for f in _archivos_actividades():
        try:
            df_ref = pd.read_parquet(os.path.join(RUTA_ACTIVIDADES, f))
            cols_ref = list(_limpiar_columnas(df_ref).columns)
            break
        except Exception:
            pass

    nueva = bd.copy()
    nueva[CAMPO_ACTIVIDAD] = nombre
    nueva = _agregar_cols_comerciales(nueva)
    nueva = _validar_pk(nueva)
    nueva = nueva.drop_duplicates(subset=[PK], keep="last")

    if cols_ref:
        for col in cols_ref:
            if col not in nueva.columns:
                nueva[col] = None
        # Reordenar para que CAMPO_ACTIVIDAD quede al inicio
        resto = [c for c in cols_ref if c in nueva.columns]
        extra = [c for c in nueva.columns if c not in cols_ref]
        nueva = nueva[resto + extra]

    _guardar_actividad(nombre, nueva)
    return nueva

def eliminar_actividad(nombre: str):
    if nombre not in obtener_actividades():
        raise Exception(f"La actividad '{nombre}' no existe.")
    _eliminar_actividad_archivo(nombre)

def regenerar_actividad(nombre: str) -> pd.DataFrame:
    nombre = nombre.strip()
    bd = leer_bd()
    if bd.empty:
        raise Exception("No hay BD_ACTUALIZACION.")
    ac = _leer_actividad(nombre)
    if ac.empty:
        raise Exception(f"La actividad '{nombre}' no existe.")

    ac = _validar_pk(ac)
    ac = _agregar_cols_comerciales(ac)
    ac = ac.drop_duplicates(subset=[PK], keep="last")
    keep = [c for c in [PK] + COLUMNAS_COMERCIALES if c in ac.columns]

    nueva = bd.copy()
    nueva[CAMPO_ACTIVIDAD] = nombre
    nueva = nueva.merge(ac[keep], on=PK, how="left")
    nueva = _agregar_cols_comerciales(nueva)
    nueva = _validar_pk(nueva)

    _guardar_actividad(nombre, nueva)
    return nueva

def dataset_actividad(nombre: str) -> pd.DataFrame:
    """Lee datos de UNA actividad sin tocar las demás."""
    return _leer_actividad(nombre)

# ─── FILTROS ────────────────────────────────────────────────────────

def filtrar_por_familias(df: pd.DataFrame, familias: list) -> pd.DataFrame:
    if df.empty or not familias:
        return df.iloc[0:0].copy()
    if CAMPO_FAMILIA not in df.columns:
        return df.iloc[0:0].copy()
    norm = {_normalizar(x) for x in familias if _normalizar(x)}
    return df[df[CAMPO_FAMILIA].apply(_normalizar).isin(norm)].copy()

# ─── ACTUALIZAR DESDE CSV ──────────────────────────────────────────────

def actualizar_desde_csv(
    nombre: str,
    archivo: pd.DataFrame,
    familias_permitidas: Optional[List[str]] = None
):
    nombre = nombre.strip()
    if archivo is None or archivo.empty:
        raise Exception("El archivo está vacío.")

    actividad = _leer_actividad(nombre)
    if actividad.empty:
        raise Exception(f"La actividad '{nombre}' no existe.")

    archivo = _limpiar_columnas(archivo)
    archivo = _validar_pk(archivo.copy())

    cols = [c for c in COLUMNAS_COMERCIALES if c in archivo.columns]
    if not cols:
        raise Exception("El archivo no tiene columnas comerciales (MUNDO_AC, PRECIO_PROMOCIONAL, etc.).")

    actividad = _validar_pk(actividad)
    objetivo  = actividad.copy()

    if familias_permitidas:
        if CAMPO_FAMILIA not in actividad.columns:
            raise Exception(f"No existe la columna {CAMPO_FAMILIA}.")
        norm     = {_normalizar(x) for x in familias_permitidas if _normalizar(x)}
        objetivo = actividad[actividad[CAMPO_FAMILIA].apply(_normalizar).isin(norm)].copy()
        if objetivo.empty:
            raise Exception("No hay artículos de sus familias en esta actividad.")

    pks_ok  = set(objetivo[PK].astype(str).str.strip())
    archivo = archivo[[PK] + cols].drop_duplicates(subset=[PK], keep="last")
    archivo = archivo[archivo[PK].astype(str).str.strip().isin(pks_ok)]

    if archivo.empty:
        raise Exception("El archivo no tiene PK válidos para sus familias.")

    updates  = archivo.set_index(PK)
    objetivo = objetivo.set_index(PK)
    comunes  = objetivo.index.intersection(updates.index)

    if len(comunes) == 0:
        raise Exception("No hay PK coincidentes entre el archivo y la actividad.")

    for col in cols:
        if col not in objetivo.columns:
            objetivo[col] = None
        objetivo.loc[comunes, col] = updates.loc[comunes, col]

    objetivo = objetivo.reset_index()

    if familias_permitidas:
        restantes = actividad[
            ~actividad[PK].isin(set(objetivo[PK].astype(str).str.strip()))
        ].copy()
        actividad_final = pd.concat([restantes, objetivo], ignore_index=True)
    else:
        actividad_final = objetivo.copy()

    _guardar_actividad(nombre, actividad_final)
