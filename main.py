from utils import Load
from resumen import dict_resume

path = './data/e-comerce_Olist_dataset'
load = Load(path)

data_dict = load.load_from_csv(path)

if __name__ == '__main__':

    dict_resume(data_dict)

    