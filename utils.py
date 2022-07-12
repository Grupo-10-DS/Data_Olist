import os

import pandas as pd
import csv

# ----- Validadores------
def val_date(col) -> bool:
    key = ["date", "times", "order_approved_at", ""]
    for name in key:
        return name in col


def find_date(df):
    cols = list(df.columns)

    for col in cols:
        if val_date(col):
            df[f"{col}"] = pd.to_datetime(df[f"{col}"], infer_datetime_format=True)
    return df


def find_date_dict(dict):
    for name in dict.keys():
        dict[name] = find_date(dict[name])

    return dict


class Labels:
    def __init__(self, path):
        self.path = path

        # self.encoding = encoding

    def get_list_dir(self, path):
        self.__labels = os.listdir(path)
        return self.__labels

    def get_labels(self, path):
        labels = self.get_list_dir(path)
        labels_clean = []

        for label in labels:
            if label == "Localidades.csv":
                continue
            labels_clean.append(label[:-4])

        return labels_clean

    def get_delimiter(self, path, bytes=4096):
        sniffer = csv.Sniffer()
        data = open(path, "r").read(bytes)
        delimiter = sniffer.sniff(data).delimiter
        return delimiter


class Load:
    def __init__(self, path) -> None:
        self.path = path

    def load_from_csv(self, path):
        labels = Labels(path=path).get_list_dir(path)
        data_dict = {}

        for name in labels:

            full_path = os.path.join(path, name)

            data_dict[name[:-4]] = pd.read_csv(full_path)

        # ----to datetime-----
        data_dict = find_date_dict(data_dict)

        return data_dict


class Say:

    # -----------------------------------------------
    def cow_says_good(self, str):
        """
        Aquí va un string , y la vaquita lo dirá :v
        """
        lenght = len(str)
        print(" _" + lenght * "_" + "_ ")
        print("< " + str + " > ")
        print(" -" + lenght * "-" + "- ")
        print("        \   ^__^ ")
        print("         \  (oo)\_______ ")
        print("            (__)\  good )\/\ ")
        print("                ||----w | ")
        print("                ||     || ")

    def cow_says_error(self, str):
        lenght = len(str)
        print(" _" + lenght * "_" + "_ ")
        print("< " + str + " > ")
        print(" -" + lenght * "-" + "- ")
        print("        \   ^__^ ")
        print("         \  (oo)\_______ ")
        print("            (__)\  error )\/\ ")
        print("                ||----w | ")
        print("                ||     || ")

    def bart_says(self, str):
        print("⌈" + len(str) * "¯" + "⌉")
        print("⁞" + str + "⁞")
        print("⌊" + len(str) * "_" + "⌋")
        print("   |/")
        print("|\/\/\/|")
        print("|      |")
        print("|      |")
        print("| (o)(o)")
        print("C      _)")
        print("| ,___|")
        print("|   /")
        print("/____|")
        print("/     |")


# --------------------------------------------
