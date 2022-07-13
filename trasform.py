import pandas as pd
from utils import Say

say = Say()


def metodo_de_pago(dict):
    metodo_de_pago = dict["olist_order_payments_dataset"]

    payment_type = metodo_de_pago["payment_type"]
    tipos_pago = {j: i + 1 for i, j in enumerate(payment_type.unique())}
    metodo_de_pago["payment_type"] = metodo_de_pago["payment_type"].apply(
        lambda x: tipos_pago[x]
    )
    metodo_de_pago.rename(columns={"payment_type": "payment_type_id"}, inplace=True)

    return metodo_de_pago


def ordenes(dict):
    ordenes = dict["olist_orders_dataset"]

    estado_orden = ordenes["order_status"].unique()
    estado_orden = {j: i + 1 for i, j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {"order_status_id": estado_orden.values(), "order_status": estado_orden.keys()}
    )

    ordenes["order_status"] = ordenes["order_status"].apply(lambda x: estado_orden[x])
    ordenes.rename(columns={"order_status": "order_status_id"}, inplace=True)

    return ordenes


def order_status(dict):
    ordenes = dict["olist_orders_dataset"]

    estado_orden = ordenes["order_status"].unique()
    estado_orden = {j: i + 1 for i, j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {"order_status_id": estado_orden.values(), "order_status": estado_orden.keys()}
    )
    return order_status_df


def productos(dict):
    productos = dict["olist_products_dataset"]
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

    return productos


def product_category_name(dict):
    productos = dict["olist_products_dataset"]
    productos_name = productos["product_category_name"].unique()
    productos_name = {j: i + 1 for i, j in enumerate(productos_name)}
    product_category_name_df = pd.DataFrame(
        {
            "product_category_name_id": productos_name.values(),
            "category_name": productos_name.keys(),
        }
    )
    return product_category_name_df


def tranformer(dict):
    dict["product_category_name_translation"].to_csv(
        "./Dataset_etl/product_info.csv", index=False
    )

    dict["olist_order_reviews_dataset"].to_csv("./Dataset_etl/reviews.csv", index=False)

    pd.read_csv("./Dataset_aux/olist_geolocation_dataset.csv").to_csv(
        "./Dataset_etl/item.csv", index=False
    )

    dict["olist_geolocation_dataset"].to_csv(
        "./Dataset_etl/geolocalizacion_etl.csv", index=False
    )
    pd.read_csv("./Dataset_aux/clientes_etl_solo_con_IdGeoloc.csv").to_csv(
        "./Dataset_etl/customer.csv", index=False
    )
    pd.read_csv("./Dataset_aux/vendedores_etl_solo_con_IdGeoloc.csv").to_csv(
        "./Dataset_etl/seller.csv", index=False
    )
    ordenes(dict).to_csv("./Dataset_etl/order.csv", index=False)

    product_category_name(dict).to_csv(
        "./Dataset_etl/product_category_name.csv", index=False
    )
    metodo_de_pago(dict).to_csv("./Dataset_etl/payment.csv", index=False)
    pd.read_csv("./Dataset_aux/payment_type.csv").to_csv(
        "./Dataset_etl/payment_type.csv", index=False
    )
    productos(dict).to_csv("./Dataset_etl/product.csv", index=False)
    pd.read_csv("./Dataset_aux/order_status.csv").to_csv(
        "./Dataset_etl/order_status.csv", index=False
    )

    return say.cow_says_good("exportado")
