from wikiframe import Extractor, Say
import pandas as pd
import numpy as np


#------------------------validator----------------------------------
def val_date(col) -> bool:
    key = ["date", "times", "order_approved_at"]
    for name in key:
        return name in col

def val_up_col(col):
    return 'state' in col


def find_date(df):
    cols = list(df.columns)
    for col in cols:
        if val_date(col):
            df[f"{col}"] = pd.to_datetime(df[f"{col}"], infer_datetime_format=True)
    return df

def col_lower(df):
    for name in df.dtypes.to_dict().keys():
        if df.dtypes.to_dict()[name] == np.dtype(object):
            df[name] = df[name].str.lower()
    return df

def col_upp(df):
    cols = list(df.columns)
    for col in cols:
        if val_up_col(col):
            df[col] = df[col].str.upper()
    return df

def dupli_id(df):
    if 'order_id' in df:
        df.drop_duplicates(subset='order_id',keep='first', inplace = True)
    return df

def date_order_val(df):
    if 'order_delivered_customer_date' in df:
        df['diff'] = df['order_delivered_customer_date'] - df['order_delivered_carrier_date'] 
        index = df['diff'][df['diff'] < pd.Timedelta(0)].index
        df.drop(index = index, axis = 0, inplace = True)
        if list(index) != []:
            Say(f'drop {index.values}').cow_says_error()
        df.drop('diff', axis=1, inplace=True)
    return df

#-----------------------------------Carga-----------------
def load_csv(dict: dict, path):
    for name in dict.keys():
        dict[name].to_csv(f"{path}/{name}.csv", sep=",", index=False, header=False)

    return Say(f"{len(dict.keys())} datasets exportados correctamente en {path} ").cow_says_good()

    

general_transform = [find_date,col_lower,col_upp,dupli_id,date_order_val]

if __name__ == '__main__':

    extract = Extractor('Dataset_sinteticos')

    #extrac and Transform Delta
    delta_dict = extract.extract_from_csv(func=general_transform,verbose=False)

    #Export
    load_csv(delta_dict,'Dataset_delta')