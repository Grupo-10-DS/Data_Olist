import pandas as pd
from hermetrics.levenshtein  import Levenshtein 

def geolocalizacion(dict):
    geoloc_filtrado = dict['olist_geolocation_dataset'].groupby('geolocation_zip_code_prefix').mean().reset_index()

    #Agregaciones:
    geoloc_filtrado['IdGeolocalizacion'] = geoloc_filtrado.index
    geoloc_filtrado['Ciudad'] = geoloc_filtrado.apply(lambda r: geolocalizacion[geolocalizacion.geolocation_zip_code_prefix==r.geolocation_zip_code_prefix].geolocation_city.values[0], axis =1)
    geoloc_filtrado['Estado'] = geoloc_filtrado.apply(lambda r: geolocalizacion[geolocalizacion.geolocation_zip_code_prefix==r.geolocation_zip_code_prefix].geolocation_state.values[0], axis =1)

    return geoloc_filtrado

def costumers(dict):
    clientes = dict['olist_customers_dataset']
    geoloc_unique_code = geolocalizacion(dict).zip_code_prefix.unique()
    clientes_unique_code = clientes.customer_zip_code_prefix.unique()
    zip_codes_dif = [item for item in clientes_unique_code if item not in geoloc_unique_code]

    geoloc_unique_code = geolocalizacion.geolocation_zip_code_prefix.unique()
    clientes_unique_code = clientes.customer_zip_code_prefix.unique()
    zip_codes_missing = []
    for item in clientes_unique_code:
        if item not in geoloc_unique_code:
            try:
                city = clientes.loc[clientes['customer_zip_code_prefix'] == item].customer_city.values[0]
                zip_code = geolocalizacion[geolocalizacion.geolocation_city==city].geolocation_zip_code_prefix.mode()[0]
                clientes['customer_zip_code_prefix'] = clientes['customer_zip_code_prefix'].replace(item, zip_code)
            except:
                zip_codes_missing.append(item)

    ciudades_eval = []
    for item in zip_codes_missing:
        ciudades_eval.append(clientes.loc[clientes['customer_zip_code_prefix'] == item].customer_city.values[0])
    #ciudades de geolocalizacion
    ciudades_geoloc = geolocalizacion(dict).Ciudad.unique()

    lev = Levenshtein ()

    resultado_favorable_lev = {}
    resultado_desfavorable_lev = {}

    for x in ciudades_eval:
        puntuacion = 0
        i=-1
        for y in ciudades_geoloc:
            puntos = lev.similarity(x, y)
            i+=1
            if puntos > puntuacion:
                puntuacion = puntos
                indice = i
        if puntuacion>=0.7:
            resultado_favorable_lev[x] = [ciudades_geoloc[indice], puntuacion]
        else:
            resultado_desfavorable_lev[x] = [ciudades_geoloc[indice],puntuacion]

    indexes = []
    for item in zip_codes_missing:
        ind = clientes[clientes.customer_zip_code_prefix==item].index.values
        for i in ind:
            indexes.append(i)

    
    clientes.drop(index = indexes, axis=0, inplace=True)
    
    clientes['Id_Geolocalizacion'] = clientes.apply(lambda r: geolocalizacion(dict)[geolocalizacion(dict).zip_code_prefix==r.customer_zip_code_prefix].Id_Geolocalizacion.values[0], axis = 1)

    clientes_etl_2 = clientes.drop(['customer_zip_code_prefix','customer_city','customer_state'],axis=1)

    return clientes_etl_2


def seller(dict):
    vendedores = dict['olist_sellers_dataset']
    geoloc_unique_code = geolocalizacion(dict).zip_code_prefix.unique()
    vendedores_unique_code = vendedores.seller_zip_code_prefix.unique()
    zip_codes_dif = [item for item in vendedores_unique_code if item not in geoloc_unique_code]

    geoloc_unique_code = geolocalizacion.geolocation_zip_code_prefix.unique()
    vendedores_unique_code = vendedores.seller_zip_code_prefix.unique()
    zip_codes_missing = []
    for item in vendedores_unique_code:
        if item not in geoloc_unique_code:
            try:
                city = vendedores.loc[vendedores['seller_zip_code_prefix'] == item].seller_city.values[0]
                zip_code = geolocalizacion[geolocalizacion.geolocation_city==city].geolocation_zip_code_prefix.mode()[0]
                vendedores['seller_zip_code_prefix'] = vendedores['seller_zip_code_prefix'].replace(item, zip_code)
            except:
                zip_codes_missing.append(item)
    zip_codes_missing

    vendedores['Id_Geolocalizacion'] = vendedores.apply(lambda r: geolocalizacion(dict)[geolocalizacion(dict).zip_code_prefix==r.seller_zip_code_prefix].Id_Geolocalizacion.values[0], axis = 1)

    vendedores_etl_2 = vendedores.drop(['seller_zip_code_prefix','seller_city','seller_state'],axis=1)

    return vendedores_etl_2

def metodo_de_pago(dict):
    metodo_de_pago = dict['olist_order_payments_dataset']

    payment_type = metodo_de_pago['payment_type']
    tipos_pago = {j:i+1 for i,j in enumerate(payment_type.unique())}
    payment_type_df = pd.DataFrame(
        {
            'payment_type_id': tipos_pago.values(),
            'payment_type': tipos_pago.keys()
        })

    metodo_de_pago['payment_type'] = metodo_de_pago['payment_type'].apply(lambda x: tipos_pago[x])
    metodo_de_pago.rename(columns={'payment_type': 'payment_type_id'}, inplace=True)

    return metodo_de_pago

def payment_type(dict):
    metodo_de_pago = dict['olist_order_payments_dataset']

    payment_type = metodo_de_pago['payment_type']
    tipos_pago = {j:i+1 for i,j in enumerate(payment_type.unique())}
    payment_type_df = pd.DataFrame(
        {
            'payment_type_id': tipos_pago.values(),
            'payment_type': tipos_pago.keys()
        })
    return payment_type_df

def ordenes(dict):
    ordenes = dict['olist_orders_dataset']

    estado_orden = ordenes['order_status'].unique()
    estado_orden = {j:i+1 for i,j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {
            'order_status_id': estado_orden.values(),
            'order_status': estado_orden.keys()
        })

    ordenes['order_status'] = ordenes['order_status'].apply(lambda x: estado_orden[x])
    ordenes.rename(columns={'order_status': 'order_status_id'}, inplace=True)

    return ordenes

def order_status(dict):
    ordenes = dict['olist_orders_dataset']

    estado_orden = ordenes['order_status'].unique()
    estado_orden = {j:i+1 for i,j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {
            'order_status_id': estado_orden.values(),
            'order_status': estado_orden.keys()
        })
    return order_status_df


def productos(dict):
    productos_name = dict['product_category_name'].unique()
    productos_name = {j:i+1 for i,j in enumerate(productos_name)}
    product_category_name_df = pd.DataFrame(
        {
            'product_category_name_id': productos_name.values(),
            'category_name': productos_name.keys()
        })

    productos['product_category_name'] = productos['product_category_name'].apply(lambda x: productos_name[x])
    productos.rename(columns={'product_category_name': 'product_category_name_id'}, inplace=True)   

    return productos

def product_category_name(dict):
    productos_name = dict['product_category_name'].unique()
    productos_name = {j:i+1 for i,j in enumerate(productos_name)}
    product_category_name_df = pd.DataFrame(
    {
        'product_category_name_id': productos_name.values(),
        'category_name': productos_name.keys()
    })
    return product_category_name_df

def exporter(dict):
    geolocalizacion(dict).to_csv('./Dataset_etl/geolocalizacion_etl.csv', index =False)
    costumers(dict).to_csv("./Dataset_etl/clientes_etl_solo_con_IdGeoloc.csv", index=False)
    seller(dict).to_csv('./Dataset_etl/vendedores_etl_solo_con_IdGeoloc.csv', index = False)
    ordenes(dict).to_csv('./Dataset_etl/ordenes_corregido.csv', index=False)
    product_category_name(dict).to_csv('./Dataset_etl/product_category_name.csv', index=False)
    metodo_de_pago(dict).to_csv('./Dataset_etl/metodo_de_pago_corregido.csv', index=False)
    payment_type(dict).to_csv('./Dataset_etl/payment_type.csv', index=False)
    productos(dict).to_csv('./Dataset_etl/productos_corregido.csv', index=False)
    order_status(dict).to_csv('./Dataset_etl/order_status.csv', index=False)

    return print('exportado')
