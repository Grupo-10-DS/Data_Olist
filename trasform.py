import pandas as pd
from time import sleep
from utils import Say

say = Say()


def metodo_de_pago(dict):
    name = 'olist_order_payments_dataset'
    metodo_de_pago = dict[name]

    payment_type = metodo_de_pago["payment_type"]
    tipos_pago = {j: i + 1 for i, j in enumerate(payment_type.unique())}
    metodo_de_pago["payment_type"] = metodo_de_pago["payment_type"].apply(
        lambda x: tipos_pago[x]
    )
    metodo_de_pago.rename(columns={"payment_type": "payment_type_id"}, inplace=True)
    print(f'{name}.csv transformado a payment.csv✅')
    sleep(0.2)
    return metodo_de_pago

def payment_type(dict):
    name = 'olist_order_payments_dataset'
    metodo_de_pago = dict[name]
    payment_type = metodo_de_pago['payment_type_id']
    tipos_pago = {j:i+1 for i,j in enumerate(payment_type.unique())}
    payment_type_df = pd.DataFrame(
    {
        'payment_type_id': tipos_pago.values(),
        'payment_type': tipos_pago.keys()
    })
    print(f'{name}.csv transformado a payment_type✅')
    sleep(0.2)
    return payment_type_df



def ordenes(dict):
    name = 'olist_orders_dataset'
    ordenes = dict[name]
    estado_orden = ordenes["order_status"].unique()
    estado_orden = {j: i + 1 for i, j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {"order_status_id": estado_orden.values(), "order_status": estado_orden.keys()}
    )
    ordenes["order_status"] = ordenes["order_status"].apply(lambda x: estado_orden[x])
    ordenes.rename(columns={"order_status": "order_status_id"}, inplace=True)
    print(f'{name}.csv transformado a order.csv✅')
    sleep(0.2)
    return ordenes


def order_status(dict):
    name = "olist_orders_dataset"
    ordenes = dict[name]
    estado_orden = ordenes["order_status_id"].unique()
    estado_orden = {j: i + 1 for i, j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {"order_status_id": estado_orden.values(), "order_status": estado_orden.keys()}
    )
    print(f'{name} transformado a order_status.csv✅')
    sleep(0.2)
    return order_status_df


def productos(dict):
    name = "olist_products_dataset"
    productos = dict[name]
    productos_name = productos["product_category_name"].unique()
    productos_name = {j: i + 1 for i, j in enumerate(productos_name)}
    product_category_name_df = pd.DataFrame(
        {
            "product_category_name_id": productos_name.values(),
            "category_name": productos_name.keys(),
        }
    )

    productos["product_category_name"] = productos["product_category_name"].apply(
        lambda x: productos_name[x]
    )
    productos.rename(
        columns={"product_category_name": "product_category_name_id"}, inplace=True
    )
    print(f'{name} transformado a product.csv✅')
    sleep(0.2)
    return productos


def product_category_name(dict):
    name = "olist_products_dataset"
    productos = dict[name]
    productos_name = productos["product_category_name_id"].unique()
    productos_name = {j: i + 1 for i, j in enumerate(productos_name)}
    product_category_name_df = pd.DataFrame(
        {
            "product_category_name_id": productos_name.values(),
            "category_name": productos_name.keys(),
        }
    )
    print(f'{name} transformado a product_category_name.csv✅')
    sleep(0.2)
    return product_category_name_df


def tranformer(dict):
    
    pd.read_csv("./Data_subir/geolocalition_etl.csv").to_csv(
        "./Dataset_etl/item.csv", index=False
    )

    pd.read_csv("./Data_subir/customer_etl_solo_con_IdGeoloc.csv").to_csv(
        "./Dataset_etl/customer.csv", index=False
    )
    pd.read_csv("./Data_subir/seller_etl_solo_con_IdGeoloc.csv").to_csv(
        "./Dataset_etl/seller.csv", index=False
    )

    pd.read_csv('./Data_subir/product_category_name.csv').to_csv('./Dataset_etl/product_category_name.csv')


    dict["product_category_name_translation"].to_csv(
        "./Dataset_etl/product_info.csv", index=False
    )

    dict["olist_order_reviews_dataset"].to_csv("./Dataset_etl/reviews.csv", index=False)

    dict["olist_geolocation_dataset"].to_csv(
        "./Dataset_etl/geolocalizacion_etl.csv", index=False
    )   

    productos(dict).to_csv("./Dataset_etl/product.csv", index=False)

    ordenes(dict).to_csv("./Dataset_etl/order.csv", index=False)

    order_status(dict).to_csv("./Dataset_etl/order_status.csv", index=False)

    metodo_de_pago(dict).to_csv("./Dataset_etl/payment.csv", index=False)

    payment_type(dict).to_csv("./Dataset_etl/payment_type.csv", index=False)

    product_category_name(dict).to_csv("./Dataset_etl/product_category_name.csv", index=False)   
    
    return say.cow_says_good("exportado")
