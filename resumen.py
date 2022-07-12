import pandas as pd

def dataset_resume(df, label):
    parameters = {
        "Overview": df.tail(),
        "Shape": df.shape,
        "Type of data": df.dtypes,
        "Null Values": df.isnull().sum(),
    }
    print("")
    print("")
    print("=" * 20, f"Review {label}", "=" * 20)
    for name, func in parameters.items():
        print("-" * 20, name, "-" * 20)
        print(func)

def dict_resume(dict):
    for name in dict.keys():
        dataset_resume(dict[name],name)