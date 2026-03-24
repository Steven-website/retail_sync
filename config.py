import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

RUTA_BD    = os.path.join(DATA_DIR, "BD_ACTUALIZACION.parquet")
RUTA_BASE  = os.path.join(DATA_DIR, "BASE.parquet")
RUTA_USERS = os.path.join(BASE_DIR, "usuarios.json")

PK               = "PK_Articulos"
CAMPO_ACTIVIDAD  = "ACTIVIDAD_COMERCIAL"
CAMPO_FAMILIA    = "FAMILIA"

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

ROL_MASTER       = "MASTER"
ROL_ADC          = "ADC"
ROL_VISUALIZADOR = "VISUALIZADOR"
ROLES_DISPONIBLES = [ROL_MASTER, ROL_ADC, ROL_VISUALIZADOR]

FAMILIAS_DISPONIBLES = [
    "No_Registrado", "NAVIDAD", "ELECTRODOMESTICOS", "JUGUETERIA",
    "AUTOMOTRIZ", "FIESTA", "DEPORTE", "FERRETERIA", "CUIDADO PERSONAL",
    "BEBE", "TIENDA", "DETALLES", "LIBRERIA", "MERMA RECUPERABLE",
    "HOGAR", "JARDINERIA", "COMESTIBLE", "ZAPATERIA", "ANTIGUA COMESTIBLE",
    "RECREACION", "PERFUMERIA", "MASCOTAS", "SUMINISTROS", "MUEBLES",
    "Línea reserv. serv.", "SERVICIOS", "GENERAL",
]

SESSION_DEFAULTS = {
    "login":    False,
    "usuario":  None,
    "rol":      None,
    "familias": [],
}
