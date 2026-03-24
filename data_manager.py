import io
import os
import unicodedata
from typing import Optional, List
import pandas as pd
from config import (
    RUTA_BD, RUTA_BASE, PK,
    CAMPO_ACTIVIDAD, CAMPO_FAMILIA,
    COLUMNAS_COMERCIALES,
)

# ─── HELPERS ──────────────────────────────────────────────
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

def _validar_pk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if PK not in df.columns:
        raise Exception(f"El archivo no tiene la columna '{PK}'.")
    df[PK] = df[PK].astype(str).str.strip()
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

def a_parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf.getvalue()

# ─── BD_ACTUALIZACION ─────────────────────────────────────
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
    return df

# ─── BASE ─────────────────────────────────────────────────
def leer_base() -> pd.DataFrame:
    base = _leer_parquet(RUTA_BASE)
    if base.empty:
        return base
    base = _agregar_cols_comerciales(base)
    base = _validar_pk(base)
    if CAMPO_ACTIVIDAD not in base.columns:
        base[CAMPO_ACTIVIDAD] = ""
    base[CAMPO_ACTIVIDAD] = base[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    return base

def _guardar_base(df: pd.DataFrame):
    df = _agregar_cols_comerciales(df)
    df = _validar_pk(df)
    if CAMPO_ACTIVIDAD not in df.columns:
        df[CAMPO_ACTIVIDAD] = ""
    df[CAMPO_ACTIVIDAD] = df[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    df.to_parquet(RUTA_BASE, index=False)

# ─── ACTIVIDADES ──────────────────────────────────────────
def obtener_actividades() -> list:
    base = leer_base()
    if base.empty:
        return []
    ac = base[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    return sorted(ac[ac != ""].unique().tolist())

def crear_actividad(nombre: str) -> pd.DataFrame:
    nombre = nombre.strip()
    if not nombre:
        raise Exception("El nombre no puede estar vacío.")
    bd = leer_bd()
    if bd.empty:
        raise Exception("No hay BD_ACTUALIZACION cargada. Súbala primero en la tab BD.")
    if nombre in obtener_actividades():
        raise Exception(f"La actividad '{nombre}' ya existe.")
    base  = leer_base()
    nueva = bd.copy()
    nueva.insert(1, CAMPO_ACTIVIDAD, nombre)
    nueva = _agregar_cols_comerciales(nueva)
    nueva = _validar_pk(nueva)
    nueva = nueva.drop_duplicates(subset=[PK], keep="last")
    if not base.empty:
        for col in base.columns:
            if col not in nueva.columns:
                nueva[col] = None
        for col in nueva.columns:
            if col not in base.columns:
                base[col] = None
        nueva = nueva[base.columns]
        base  = pd.concat([base, nueva], ignore_index=True)
    else:
        base = nueva
    _guardar_base(base)
    return base

def eliminar_actividad(nombre: str):
    base = leer_base()
    if base.empty:
        raise Exception("No existe BASE.")
    if nombre not in obtener_actividades():
        raise Exception(f"La actividad '{nombre}' no existe.")
    base = base[base[CAMPO_ACTIVIDAD].astype(str).str.strip() != nombre].copy()
    _guardar_base(base)

def regenerar_actividad(nombre: str) -> pd.DataFrame:
    nombre = nombre.strip()
    bd     = leer_bd()
    if bd.empty:
        raise Exception("No hay BD_ACTUALIZACION.")
    base   = leer_base()
    if base.empty:
        raise Exception("No existe BASE.")
    ac = base[base[CAMPO_ACTIVIDAD].astype(str).str.strip() == nombre].copy()
    if ac.empty:
        raise Exception(f"La actividad '{nombre}' no existe.")
    ac   = _validar_pk(ac)
    ac   = _agregar_cols_comerciales(ac)
    ac   = ac.drop_duplicates(subset=[PK], keep="last")
    keep = [c for c in [PK] + COLUMNAS_COMERCIALES if c in ac.columns]
    nueva = bd.copy()
    nueva.insert(1, CAMPO_ACTIVIDAD, nombre)
    nueva = nueva.merge(ac[keep], on=PK, how="left")
    nueva = _agregar_cols_comerciales(nueva)
    nueva = _validar_pk(nueva)
    base  = base[base[CAMPO_ACTIVIDAD].astype(str).str.strip() != nombre].copy()
    for col in base.columns:
        if col not in nueva.columns:
            nueva[col] = None
    for col in nueva.columns:
        if col not in base.columns:
            base[col] = None
    nueva = nueva[base.columns]
    base  = pd.concat([base, nueva], ignore_index=True)
    _guardar_base(base)
    return base

def dataset_actividad(nombre: str) -> pd.DataFrame:
    base = leer_base()
    if base.empty:
        return base
    return base[base[CAMPO_ACTIVIDAD].astype(str).str.strip() == nombre.strip()].copy()

# ─── FILTROS ──────────────────────────────────────────────
def filtrar_por_familias(df: pd.DataFrame, familias: list) -> pd.DataFrame:
    if df.empty or not familias:
        return df.iloc[0:0].copy()
    if CAMPO_FAMILIA not in df.columns:
        return df.iloc[0:0].copy()
    norm = {_normalizar(x) for x in familias if _normalizar(x)}
    return df[df[CAMPO_FAMILIA].apply(_normalizar).isin(norm)].copy()

# ─── ACTUALIZAR DESDE CSV (ADC) ───────────────────────────
def actualizar_desde_csv(
    nombre: str,
    archivo: pd.DataFrame,
    familias_permitidas: Optional[List[str]] = None
):
    nombre = nombre.strip()
    if archivo is None or archivo.empty:
        raise Exception("El archivo está vacío.")
    base = leer_base()
    if base.empty:
        raise Exception("No existe BASE.")
    if PK not in archivo.columns:
        raise Exception(f"El archivo no tiene la columna '{PK}'.")
    archivo = _validar_pk(archivo.copy())
    cols    = [c for c in COLUMNAS_COMERCIALES if c in archivo.columns]
    if not cols:
        raise Exception("El archivo no tiene columnas comerciales.")
    mask_ac   = base[CAMPO_ACTIVIDAD].astype(str).str.strip() == nombre
    actividad = base.loc[mask_ac].copy()
    if actividad.empty:
        raise Exception(f"La actividad '{nombre}' no existe.")
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
    base_final = pd.concat([
        base.loc[~mask_ac].copy(),
        actividad_final
    ], ignore_index=True)
    _guardar_base(base_final)
