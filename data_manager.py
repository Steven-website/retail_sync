import io
import os
import unicodedata
from typing import Optional, List

import pandas as pd
from config import (
    RUTA_BD,
    RUTA_MASTER,
    PK,
    CAMPO_ACTIVIDAD,
    CAMPO_FAMILIA,
    COLUMNAS_COMERCIALES,
)


# =====================================================
# HELPERS
# =====================================================
def _asegurar_columnas_comerciales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in COLUMNAS_COMERCIALES:
        if col not in df.columns:
            df[col] = None
    return df


def _leer_parquet_seguro(ruta: str) -> pd.DataFrame:
    if not os.path.exists(ruta):
        return pd.DataFrame()
    # FIX: capturar errores de lectura (archivo corrupto, permisos, etc.)
    try:
        return pd.read_parquet(ruta)
    except Exception as e:
        raise Exception(f"Error leyendo archivo parquet '{ruta}': {e}")


def _normalizar_texto(valor) -> str:
    # FIX: pd.isna() lanza error si el valor es una lista u objeto no escalar
    # Se agrega manejo seguro antes de llamar isna
    try:
        if pd.isna(valor):
            return ""
    except (TypeError, ValueError):
        pass

    txt = str(valor).strip().upper()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt


def _asegurar_pk_texto(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if PK not in df.columns:
        raise Exception(f"No existe la columna {PK}")
    df[PK] = df[PK].astype(str).str.strip()
    return df


def _normalizar_actividad(nombre: str) -> str:
    return str(nombre).strip()


# =====================================================
# BD_ACTUALIZACION
# =====================================================
def leer_bd_actualizacion() -> pd.DataFrame:
    df = _leer_parquet_seguro(RUTA_BD)

    if df.empty:
        return df

    if PK not in df.columns:
        raise Exception(f"No existe la columna {PK} en BD_ACTUALIZACION")

    df = _asegurar_pk_texto(df)
    return df


def guardar_bd_actualizacion_desde_upload(file) -> pd.DataFrame:
    # FIX: capturar error de lectura del archivo subido
    try:
        df = pd.read_parquet(file)
    except Exception as e:
        raise Exception(f"No se pudo leer el archivo parquet: {e}")

    if PK not in df.columns:
        raise Exception(f"El parquet cargado no tiene la columna {PK}")

    df = _asegurar_pk_texto(df)

    # FIX: eliminar duplicados en PK antes de guardar
    df = df.drop_duplicates(subset=[PK], keep="last")

    df.to_parquet(RUTA_BD, index=False)
    return df


# =====================================================
# MASTER
# =====================================================
def leer_master() -> pd.DataFrame:
    master = _leer_parquet_seguro(RUTA_MASTER)

    if master.empty:
        return master

    master = _asegurar_columnas_comerciales(master)

    if PK not in master.columns:
        raise Exception(f"MASTER no tiene la columna {PK}")

    master = _asegurar_pk_texto(master)

    if CAMPO_ACTIVIDAD not in master.columns:
        master[CAMPO_ACTIVIDAD] = ""

    master[CAMPO_ACTIVIDAD] = master[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    return master


def guardar_master(df: pd.DataFrame) -> None:
    df = _asegurar_columnas_comerciales(df)
    df = _asegurar_pk_texto(df)

    if CAMPO_ACTIVIDAD not in df.columns:
        df[CAMPO_ACTIVIDAD] = ""

    df[CAMPO_ACTIVIDAD] = df[CAMPO_ACTIVIDAD].fillna("").astype(str).str.strip()
    df.to_parquet(RUTA_MASTER, index=False)


# =====================================================
# ACTIVIDADES
# =====================================================
def obtener_actividades() -> list:
    master = leer_master()

    if master.empty or CAMPO_ACTIVIDAD not in master.columns:
        return []

    actividades = (
        master[CAMPO_ACTIVIDAD]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    actividades = actividades[actividades != ""].unique().tolist()
    return sorted(actividades)


def crear_actividad(nombre: str) -> pd.DataFrame:
    nombre = _normalizar_actividad(nombre)

    if not nombre:
        raise Exception("Debe escribir un nombre de actividad")

    bd = leer_bd_actualizacion()
    if bd.empty:
        raise Exception("No existe BD_ACTUALIZACION para crear la actividad")

    master = leer_master()

    actividades = obtener_actividades()
    if nombre in actividades:
        raise Exception("La actividad ya existe")

    nueva = bd.copy()
    nueva.insert(1, CAMPO_ACTIVIDAD, nombre)
    nueva = _asegurar_columnas_comerciales(nueva)
    nueva = _asegurar_pk_texto(nueva)
    nueva = nueva.drop_duplicates(subset=[PK], keep="last")

    if not master.empty:
        for col in master.columns:
            if col not in nueva.columns:
                nueva[col] = None

        for col in nueva.columns:
            if col not in master.columns:
                master[col] = None

        nueva = nueva[master.columns]
        master = pd.concat([master, nueva], ignore_index=True)
    else:
        master = nueva

    guardar_master(master)
    return master


def eliminar_actividad(nombre: str) -> pd.DataFrame:
    nombre = _normalizar_actividad(nombre)
    master = leer_master()

    if master.empty:
        raise Exception("No existe MASTER")

    if nombre not in obtener_actividades():
        raise Exception("La actividad no existe")

    mask = master[CAMPO_ACTIVIDAD].astype(str).str.strip() != nombre
    master = master.loc[mask].copy()

    guardar_master(master)
    return master


def dataset_actividad(nombre: str) -> pd.DataFrame:
    nombre = _normalizar_actividad(nombre)
    master = leer_master()

    if master.empty:
        return master

    return master[
        master[CAMPO_ACTIVIDAD].astype(str).str.strip() == nombre
    ].copy()


# =====================================================
# REGENERAR BASES
# =====================================================
def regenerar_actividad(nombre: str) -> pd.DataFrame:
    nombre = _normalizar_actividad(nombre)

    bd = leer_bd_actualizacion()
    if bd.empty:
        raise Exception("No existe BD_ACTUALIZACION")

    master = leer_master()
    if master.empty:
        raise Exception("No existe MASTER")

    base_ac = dataset_actividad(nombre)
    if base_ac.empty:
        raise Exception("La actividad no existe o no tiene registros")

    bd = _asegurar_pk_texto(bd)
    master = _asegurar_pk_texto(master)
    base_ac = _asegurar_pk_texto(base_ac)

    base_ac = _asegurar_columnas_comerciales(base_ac)
    base_ac = base_ac.drop_duplicates(subset=[PK], keep="last")

    keep_cols = [PK] + COLUMNAS_COMERCIALES
    keep_cols = [c for c in keep_cols if c in base_ac.columns]

    nuevo = bd.copy()
    nuevo.insert(1, CAMPO_ACTIVIDAD, nombre)
    nuevo = nuevo.merge(base_ac[keep_cols], on=PK, how="left")
    nuevo = _asegurar_columnas_comerciales(nuevo)
    nuevo = _asegurar_pk_texto(nuevo)

    master = master[
        master[CAMPO_ACTIVIDAD].astype(str).str.strip() != nombre
    ].copy()

    for col in master.columns:
        if col not in nuevo.columns:
            nuevo[col] = None

    for col in nuevo.columns:
        if col not in master.columns:
            master[col] = None

    nuevo = nuevo[master.columns]
    master = pd.concat([master, nuevo], ignore_index=True)

    guardar_master(master)
    return master


def regenerar_todas_las_actividades() -> pd.DataFrame:
    actividades = obtener_actividades()

    if not actividades:
        raise Exception("No hay actividades para regenerar")

    for ac in actividades:
        regenerar_actividad(ac)

    return leer_master()


# =====================================================
# FILTROS / CONSOLIDADOS
# =====================================================
def filtrar_familias(df: pd.DataFrame, familias: list) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    if CAMPO_FAMILIA not in df.columns:
        return df.iloc[0:0].copy()

    # FIX: si familias es None o vacío, retornar DataFrame vacío
    # en lugar de explotar en el set comprehension
    if not familias:
        return df.iloc[0:0].copy()

    familias_norm = {
        _normalizar_texto(x)
        for x in familias
        if _normalizar_texto(x)
    }

    if not familias_norm:
        return df.iloc[0:0].copy()

    familias_df = df[CAMPO_FAMILIA].apply(_normalizar_texto)
    return df[familias_df.isin(familias_norm)].copy()


def actualizar_actividad_desde_excel(
    nombre: str,
    base_excel: pd.DataFrame,
    familias_permitidas: Optional[List[str]] = None
) -> None:
    nombre = _normalizar_actividad(nombre)

    if base_excel is None or base_excel.empty:
        raise Exception("El archivo cargado no contiene datos")

    master = leer_master()
    if master.empty:
        raise Exception("No existe MASTER")

    if PK not in base_excel.columns:
        raise Exception(f"El archivo no contiene la columna {PK}")

    base_excel = base_excel.copy()
    base_excel = _asegurar_pk_texto(base_excel)

    cols_excel = [c for c in COLUMNAS_COMERCIALES if c in base_excel.columns]
    if not cols_excel:
        raise Exception("El Excel no contiene columnas comerciales para actualizar")

    actividad_mask = master[CAMPO_ACTIVIDAD].astype(str).str.strip() == nombre
    actividad_df = master.loc[actividad_mask].copy()

    if actividad_df.empty:
        raise Exception("La actividad no existe o no tiene registros")

    actividad_df = _asegurar_pk_texto(actividad_df)
    objetivo_df = actividad_df.copy()

    if familias_permitidas is not None:
        if CAMPO_FAMILIA not in actividad_df.columns:
            raise Exception(f"No existe columna {CAMPO_FAMILIA} en la actividad")

        familias_norm = {
            _normalizar_texto(x)
            for x in familias_permitidas
            if _normalizar_texto(x)
        }

        objetivo_mask = actividad_df[CAMPO_FAMILIA].apply(_normalizar_texto).isin(familias_norm)
        objetivo_df = actividad_df.loc[objetivo_mask].copy()

        if objetivo_df.empty:
            raise Exception("No hay filas habilitadas para sus familias en esta actividad")

    pks_objetivo = set(objetivo_df[PK].astype(str).str.strip())

    base_excel = base_excel[[PK] + cols_excel].copy()
    base_excel = base_excel.drop_duplicates(subset=[PK], keep="last")
    base_excel = base_excel[base_excel[PK].astype(str).str.strip().isin(pks_objetivo)]

    if base_excel.empty:
        raise Exception("El archivo no contiene PK válidos para actualizar en su alcance")

    updates = base_excel.set_index(PK)
    objetivo_df = objetivo_df.set_index(PK)

    pks_comunes = objetivo_df.index.intersection(updates.index)
    if len(pks_comunes) == 0:
        raise Exception("No hay coincidencias de PK entre la actividad y el archivo cargado")

    for col in cols_excel:
        if col not in objetivo_df.columns:
            objetivo_df[col] = None
        objetivo_df.loc[pks_comunes, col] = updates.loc[pks_comunes, col]

    objetivo_actualizado = objetivo_df.reset_index()

    if familias_permitidas is not None:
        restantes_actividad = actividad_df[
            ~actividad_df[PK].astype(str).str.strip().isin(
                set(objetivo_actualizado[PK].astype(str).str.strip())
            )
        ].copy()
        actividad_final = pd.concat([restantes_actividad, objetivo_actualizado], ignore_index=True)
    else:
        actividad_final = objetivo_actualizado.copy()

    master_sin_actividad = master.loc[~actividad_mask].copy()
    master_final = pd.concat([master_sin_actividad, actividad_final], ignore_index=True)

    guardar_master(master_final)


# FIX: consolidar ahora llama regenerar_actividad en lugar de solo leer datos
# Esto hace que el botón "Consolidar" en las vistas realmente actualice la base
def consolidar(nombre: str) -> pd.DataFrame:
    return regenerar_actividad(nombre)


# =====================================================
# ANALISIS
# =====================================================
def analizar_actividad(nombre: str) -> dict:
    df = dataset_actividad(nombre)

    total = len(df)

    if total == 0:
        return {
            "filas": 0,
            "familias": 0,
            "trabajadas": 0,
            "pendientes": 0,
            "avance_pct": 0.0,
        }

    cols_existentes = [c for c in COLUMNAS_COMERCIALES if c in df.columns]

    if cols_existentes:
        trabajadas = int(df[cols_existentes].notna().any(axis=1).sum())
    else:
        trabajadas = 0

    pendientes = total - trabajadas
    familias = int(df[CAMPO_FAMILIA].dropna().nunique()) if CAMPO_FAMILIA in df.columns else 0
    avance_pct = round((trabajadas / total) * 100, 2) if total > 0 else 0.0

    return {
        "filas": total,
        "familias": familias,
        "trabajadas": trabajadas,
        "pendientes": pendientes,
        "avance_pct": avance_pct,
    }


# =====================================================
# EXPORTS
# =====================================================
def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="BASE")
    output.seek(0)
    return output.getvalue()
