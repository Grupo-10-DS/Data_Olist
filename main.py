
from utils import Load
from resumen import dict_resume
from export import export_csv
from trasform import tranformer

path = "./data/e-comerce_Olist_dataset"
load = Load(path)

if __name__ == "__main__":
    # ETL
    # E
    # Se carga a un diccionario el directorio
    data_dict = load.load_from_csv(path)

    # T
    # Funcion que transformará los  datos
    tranformer(data_dict)

    # dict_resume(data_dict)

    # L
    # export_csv(data_dict, "./out")

    # Subida a la base datos
