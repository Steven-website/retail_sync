import pandas as pd

data = {
    "PK_ARTICULO":[1,2,3,4,5],
    "DESCRIPCION":[
        "Zapato negro",
        "Zapato escolar",
        "Cuaderno rayado",
        "Lapiz HB",
        "Bulto azul"
    ],
    "CATEGORIA":[
        "Zapateria",
        "Zapateria",
        "Libreria",
        "Libreria",
        "Bultos"
    ],
    "SUBCATEGORIA":[
        "Escolar",
        "Escolar",
        "Cuadernos",
        "Lapices",
        "Escolar"
    ],
    "VENTA":[100,50,200,500,80],
    "INVENTARIO":[20,10,100,200,30],
    "ACCION":["","","","",""],
    "COMENTARIO":["","","","",""]
}

df = pd.DataFrame(data)

df.to_parquet("master.parquet")

print("Master creado")