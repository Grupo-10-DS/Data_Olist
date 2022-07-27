import pandas as pd
import numpy as np
from wikiframe import Say


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

def sum_null(df):
    null_count = df.isnull().sum().sum()
    percent = null_count/df.shape[0]
    return percent


def scan_null(df, label):
    null_list = []
    for name in df.isnull().sum().to_dict().keys():
        if df.isnull().sum().to_dict().keys().name == 0 :
            null_list.append(name)
            return True
    print(f'Tabla {label}: col -> {len(null_list)} nulas, valores nulos -> {sum_null(df)} % ')
    return False


def find_date_dict(dict):
    for name in dict.keys():
        dict[name] = find_date(dict[name])

    return dict

def dict_lower(dict):
    for name in dict.keys():
        dict[name] = col_lower(dict[name])

    return dict

def dict_upper(dict):
    for name in dict.keys():
        dict[name] = col_upp(dict[name])

    return dict

def dict_dupli_id(dict):
    #Mantenimento
    """ for name in dict.keys():
        dict[name] = dupli_id(dict[name]) """
    dict['olist_order_reviews_dataset'] = dupli_id(dict['olist_order_reviews_dataset'])
    return dict

def dict_scan_null(dict):
    bool_list = {}
    for name in dict.keys():
        if scan_null(dict[name],name) == True:
            bool_list[name] = True
        else:
            bool_list[name] = False

    print(pd.Series(bool_list))
    
    pase = input('Desea continuar? (y/n): ')

    return pase






            
    

    