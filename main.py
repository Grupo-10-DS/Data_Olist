from utils import Load
from resumen import dict_resume
from export import export_csv

path = "./data/e-comerce_Olist_dataset"
load = Load(path)


# E
# Se carga a un diccionario el directorio
data_dict = load.load_from_csv(path)

if __name__ == "__main__":
    pass
    # T
    # Funcion que transformará los  datos
    # transformation(data_dict)

    # dict_resume(data_dict)

    # L
    # export_csv(data_dict, "./out")

    # Subida a la base datos
