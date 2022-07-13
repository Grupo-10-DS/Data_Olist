from email import header
import pandas
from utils import Say

say = Say()


def export_csv(dict: dict, path):
    for name in dict.keys():
        dict[name].to_csv(f"{path}/{name}.csv", sep=",", index=False, header=False)

    return say.cow_says_good(
        f"{len(dict.keys())} datasets exportados correctamente en {path} "
    )
