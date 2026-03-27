import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

RUTA_BD          = os.path.join(DATA_DIR, "BD_ACTUALIZACION.parquet")
RUTA_BASE        = os.path.join(DATA_DIR, "BASE.parquet")  # legado
RUTA_ACTIVIDADES = os.path.join(DATA_DIR, "actividades")
RUTA_VM          = os.path.join(DATA_DIR, "VM_MERCHANDISING.parquet")
RUTA_FILTROS_AC  = os.path.join(DATA_DIR, "filtros_ac")
RUTA_VM_AC       = os.path.join(DATA_DIR, "vm_ac")
RUTA_USERS       = os.path.join(BASE_DIR, "usuarios.json")
os.makedirs(RUTA_FILTROS_AC, exist_ok=True)
os.makedirs(RUTA_VM_AC, exist_ok=True)
os.makedirs(RUTA_ACTIVIDADES, exist_ok=True)

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

COLUMNAS_VM = [f"R{i:02d}" for i in range(1, 41)]

ROL_MASTER       = "MASTER"
ROL_ADC          = "ADC"
ROL_VISUALIZADOR = "VISUALIZADOR"
ROL_VM           = "VM"
ROLES_DISPONIBLES = [ROL_MASTER, ROL_ADC, ROL_VISUALIZADOR, ROL_VM]

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
