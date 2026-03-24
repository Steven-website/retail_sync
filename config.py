import os

# =========================
# RUTAS BASE
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# 🔥 asegurar carpeta data
os.makedirs(DATA_DIR, exist_ok=True)

RUTA_BD = os.path.join(DATA_DIR, "BD_ACTUALIZACION.parquet")
RUTA_MASTER = os.path.join(DATA_DIR, "master.parquet")
RUTA_USERS = os.path.join(BASE_DIR, "usuarios.json")

# =========================
# CLAVES DEL MODELO
# =========================
PK = "PK_Articulos"
CAMPO_ACTIVIDAD = "ACTIVIDAD_COMERCIAL"
CAMPO_FAMILIA = "FAMILIA"

# =========================
# COLUMNAS COMERCIALES
# =========================
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

# =========================
# ROLES
# =========================
ROL_MASTER = "MASTER"
ROL_JEFE_ADC = "JEFE_ADC"
ROL_ADC = "ADC"
ROL_PRECIOS = "PRECIOS"
ROL_MARKETING = "MARKETING"

# =========================
# POLITICA DE VISIBILIDAD
# =========================
# 🔥 prioridad máxima del sistema
FILTRO_FAMILIAS_ACTIVO = True

# Roles que NO se filtran por familia
ROLES_SIN_FILTRO_FAMILIA = {
    ROL_MASTER,
}

# =========================
# PERMISOS OPERATIVOS
# =========================
ROLES_CONSOLIDAN = {
    ROL_MASTER,
    ROL_JEFE_ADC,
    ROL_PRECIOS,
    ROL_MARKETING,
}

ROLES_DESCARGA_EXCEL = {
    ROL_ADC,
    ROL_JEFE_ADC,
    ROL_PRECIOS,
    ROL_MARKETING,
}

ROLES_DESCARGA_PARQUET = {
    ROL_MASTER,
}

ROLES_SUBEN_EXCEL = {
    ROL_ADC,
}

# =========================
# SESSION STATE
# =========================
SESSION_DEFAULTS = {
    "login": False,
    "usuario": None,
    "rol": None,
    "familias": [],
}

# =========================
# PARAMETROS APP
# =========================
DEBUG_APP = True
NOMBRE_ARCHIVO_EXPORT = "BASE_ACTIVIDAD"
