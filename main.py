
from utils import Load
from resumen import dict_resume
from export import export_csv
from trasform import tranformer
from wikiframe import Extractor

data = "./data/e-comerce_Olist_dataset"
delta = "./Dataset_sinteticos"

data_extract = Extractor(data)
delta_extract = Extractor(delta)


if __name__ == "__main__":
    # ETL
    # E
    # Se carga a un diccionario el directorio
    data_dict = data_extract.extract_from_csv()
    # T
    # Funcion que transformará los  datos
    tranformer(data_dict)

    # dict_resume(data_dict)

    # L
    export_csv(data_dict, "./out")


    

