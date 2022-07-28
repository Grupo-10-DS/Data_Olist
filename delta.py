from wikiframe import Extractor

from validators import find_date, col_lower, col_upp, dupli_id, date_order_val
from load import load_csv

general_transform = [find_date, col_lower, col_upp, dupli_id, date_order_val]

if __name__ == "__main__":

    extract = Extractor("Dataset_sinteticos")

    # extrac and Transform Delta
    delta_dict = extract.extract_from_csv(func=general_transform, verbose=False)

    # Export
    load_csv(delta_dict, "Dataset_delta")
