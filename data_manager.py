import io
import os
import unicodedata

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
    return pd.read_parquet(ruta)


def _normalizar_texto(valor) -> str:
    if pd.isna(valor):
        return ""

    txt = str(valor).strip().upper()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt


# =====================================================
# BD_ACTUALIZACION
# =====================================================
def leer_bd_actualizacion() -> pd.DataFrame:
    df = _leer_parquet_seguro(RUTA_BD)

    if df.empty:
        return df

    if PK not in df.columns:
        raise Exception(f"No existe la columna {PK} en BD_ACTUALIZACION")

    return df


def guardar_bd_actualizacion_desde_upload(file) -> pd.DataFrame:
    df = pd.read_parquet(file)

    if PK not in df.columns:
        raise Exception(f"El parquet cargado no tiene la columna {PK}")

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

    if CAMPO_ACTIVIDAD not in master.columns:
        master[CAMPO_ACTIVIDAD] = ""

    return master


def guardar_master(df: pd.DataFrame) -> None:
    df = _asegurar_columnas_comerciales(df)
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
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    return sorted([x for x in actividades if x != ""])


def crear_actividad(nombre: str) -> pd.DataFrame:
    nombre = str(nombre).strip()

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
    master = leer_master()
    if master.empty:
        raise Exception("No existe MASTER")

    mask = master[CAMPO_ACTIVIDAD].astype(str).str.strip() != str(nombre).strip()
    master = master.loc[mask].copy()

    guardar_master(master)
    return master


def dataset_actividad(nombre: str) -> pd.DataFrame:
    master = leer_master()
    if master.empty:
        return master

    return master[
        master[CAMPO_ACTIVIDAD].astype(str).str.strip() == str(nombre).strip()
    ].copy()


# =====================================================
# REGENERAR BASES
# =====================================================
def regenerar_actividad(nombre: str) -> pd.DataFrame:
    bd = leer_bd_actualizacion()
    if bd.empty:
        raise Exception("No existe BD_ACTUALIZACION")

    master = leer_master()
    if master.empty:
        raise Exception("No existe MASTER")

    base_ac = dataset_actividad(nombre)
    if base_ac.empty:
        raise Exception("La actividad no existe o no tiene registros")

    base_ac = _asegurar_columnas_comerciales(base_ac)

    keep_cols = [PK] + COLUMNAS_COMERCIALES
    keep_cols = [c for c in keep_cols if c in base_ac.columns]

    nuevo = bd.copy()
    nuevo.insert(1, CAMPO_ACTIVIDAD, nombre)
    nuevo = nuevo.merge(base_ac[keep_cols], on=PK, how="left")
    nuevo = _asegurar_columnas_comerciales(nuevo)

    master = master[
        master[CAMPO_ACTIVIDAD].astype(str).str.strip() != str(nombre).strip()
    ].copy()

    # Alinear columnas
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
    master = leer_master()

    if not actividades:
        raise Exception("No hay actividades para regenerar")

    for ac in actividades:
        master = regenerar_actividad(ac)

    return master


# =====================================================
# FILTROS / CONSOLIDADOS
# =====================================================
def filtrar_familias(df: pd.DataFrame, familias: list) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    if CAMPO_FAMILIA not in df.columns:
        return df.iloc[0:0].copy()

    familias_norm = {
        _normalizar_texto(x)
        for x in (familias or [])
        if _normalizar_texto(x)
    }
    if not familias_norm:
        return df.iloc[0:0].copy()

    familias_df = df[CAMPO_FAMILIA].apply(_normalizar_texto)
    return df[familias_df.isin(familias_norm)].copy()

 columna {PK}")

    cols_excel = [c for c in COLUMNAS_COMERCIALES if c in base_excel.columns]
    if not cols_excel:
        raise Exception("El Excel no contiene columnas comerciales para actualizar")

    actividad_mask = master[CAMPO_ACTIVIDAD].astype(str).str.strip() == str(nombre).strip()
    actividad_df = master.loc[actividad_mask].copy()
    if actividad_df.empty:
        raise Exception("La actividad no existe o no tiene registros")

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

    # Solo actualiza PK válidos del subconjunto permitido
    pks_objetivo = set(objetivo_df[PK].astype(str))
    base_excel = base_excel[[PK] + cols_excel].copy()
    base_excel = base_excel.drop_duplicates(subset=[PK], keep="last")
    base_excel = base_excel[base_excel[PK].astype(str).isin(pks_objetivo)]

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
            ~actividad_df[PK].astype(str).isin(set(objetivo_actualizado[PK].astype(str)))
        ].copy()
        actividad_final = pd.concat([restantes_actividad, objetivo_actualizado], ignore_index=True)
    else:
        actividad_final = objetivo_actualizado

    master_sin_actividad = master.loc[~actividad_mask].copy()
    master_final = pd.concat([master_sin_actividad, actividad_final], ignore_index=True)
    guardar_master(master_final)


def consolidar(nombre: str) -> pd.DataFrame:
    # En este diseño, el MASTER ya contiene el consolidado vivo de la actividad
    return dataset_actividad(nombre)


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
    familias = int(df[CAMPO_FAMILIA].nunique()) if CAMPO_FAMILIA in df.columns else 0
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


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)
    return output.getvalue()
