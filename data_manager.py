import pandas as pd
import os
from config import *


# =====================================================
# CARGAR BD ACTUALIZACION
# =====================================================
def cargar_bd_actualizacion(file):

    df = pd.read_parquet(file)

    if "PK_Articulos" not in df.columns:
        raise Exception("No existe PK_Articulos")

    df.to_parquet(RUTA_BD_ACT)

    return df


# =====================================================
# LEER BD ACT
# =====================================================
def leer_bd_act():

    if not os.path.exists(RUTA_BD_ACT):
        return pd.DataFrame()

    return pd.read_parquet(RUTA_BD_ACT)


# =====================================================
# LEER MASTER
# =====================================================
def leer_master():

    if not os.path.exists(RUTA_MASTER):
        return pd.DataFrame()

    return pd.read_parquet(RUTA_MASTER)


# =====================================================
# GUARDAR MASTER
# =====================================================
def guardar_master(df):
    df.to_parquet(RUTA_MASTER)


# =====================================================
# CREAR ACTIVIDAD
# =====================================================
def crear_actividad(nombre):

    bd = leer_bd_act()
    master = leer_master()

    if bd.empty:
        raise Exception("No existe BD_ACTUALIZACION")

    base = bd.copy()

    base["ACTIVIDAD"] = nombre

    for col in COLUMNAS_COMERCIALES:
        base[col] = ""

    master = pd.concat([master, base], ignore_index=True)

    guardar_master(master)


# =====================================================
# REGENERAR BASES
# =====================================================
def regenerar_bases(actividad):

    bd = leer_bd_act()
    master = leer_master()

    base_ac = master[master["ACTIVIDAD"] == actividad]

    # universo nuevo
    nuevo = bd.copy()
    nuevo["ACTIVIDAD"] = actividad

    # mantener comerciales
    cols_keep = ["PK_Articulos"] + COLUMNAS_COMERCIALES

    merge = base_ac[cols_keep]

    nuevo = nuevo.merge(
        merge,
        on="PK_Articulos",
        how="left"
    )

    nuevo[COLUMNAS_COMERCIALES] = nuevo[COLUMNAS_COMERCIALES].fillna("")

    master = master[master["ACTIVIDAD"] != actividad]

    master = pd.concat([master, nuevo], ignore_index=True)

    guardar_master(master)


# =====================================================
# OBTENER ACTIVIDADES
# =====================================================
def obtener_actividades():

    master = leer_master()

    if master.empty:
        return []

    return sorted(master["ACTIVIDAD"].unique())


# =====================================================
# DATASET ACTIVIDAD
# =====================================================
def dataset_actividad(actividad):

    master = leer_master()

    return master[master["ACTIVIDAD"] == actividad]


# =====================================================
# FILTRAR FAMILIAS
# =====================================================
def filtrar_familias(df, familias):

    if not familias:
        return df

    return df[df["FAMILIA"].isin(familias)]


# =====================================================
# MERGE EXCEL ADC
# =====================================================
def actualizar_desde_excel(file, actividad):

    master = leer_master()

    base = pd.read_excel(file)

    if "PK_Articulos" not in base.columns:
        raise Exception("Excel sin PK")

    base_ac = master[master["ACTIVIDAD"] == actividad]

    # SOLO columnas comerciales
    cols = ["PK_Articulos"] + COLUMNAS_COMERCIALES

    base = base[cols]

    base_ac = base_ac.drop(columns=COLUMNAS_COMERCIALES)

    base_ac = base_ac.merge(
        base,
        on="PK_Articulos",
        how="left"
    )

    base_ac[COLUMNAS_COMERCIALES] = base_ac[COLUMNAS_COMERCIALES].fillna("")

    master = master[master["ACTIVIDAD"] != actividad]

    master = pd.concat([master, base_ac], ignore_index=True)

    guardar_master(master)


# =====================================================
# CONSOLIDAR ACTIVIDAD
# =====================================================
def consolidar(actividad):

    # en este diseño el master ya es consolidado
    return dataset_actividad(actividad)


# =====================================================
# EXPORTAR EXCEL RAPIDO
# =====================================================
def exportar_excel(df):

    return df.to_excel(index=False)