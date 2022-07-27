from wikiframe import Say


def load_csv(dict: dict, path):
    for name in dict.keys():
        dict[name].to_csv(f"{path}/{name}.csv", sep=",", index=False, header=False)

    return Say(f"{len(dict.keys())} datasets exportados correctamente en {path} ").cow_says_good()
