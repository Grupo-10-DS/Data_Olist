import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import os  
from utils import Load

#Importamos la data
path = "./data/e-comerce_Olist_dataset"
load = Load(path)

# Cargamos todos los csv a un diccionario que alojara los df
data_dict = load.load_from_csv(path)

# traemos aqui los csv
clientes = data_dict['olist_customers_dataset']
geolocalizacion = data_dict['olist_geolocation_dataset']
items = data_dict['olist_order_items_dataset']
metodo_de_pago = data_dict['olist_order_payments_dataset']
reviews = data_dict['olist_order_reviews_dataset']
ordenes =data_dict['olist_orders_dataset']
productos = data_dict['olist_products_dataset']
vendedores = data_dict['olist_sellers_dataset']
productos_info = data_dict['product_category_name_translation']

# Creamos las carpetas para guardar nuestros archivos etl y auxiliar que usaremos mas adelante
path_etl = "Dataset_etl"
path_aux = "Dataset_aux"
os.makedirs(path_etl, exist_ok=True)  
os.makedirs(path_aux, exist_ok=True)  

## ETL Gelolocalizacion

def etl_geolocalizacion(geolocalizacion):
    """
    Esta funcion le realiza ETL a la tabla geolocalizacion, normalizando latitudes y longitudes tomando como parametro
    que la mayor parte de brasil se halla dentro del hemisferio sur, entre los paralelos 5.5 de latitud N, y los -34 de latitud S; y entre los meridianos que señalan los -32 y los -74 de longitud.
    En un segundo paso crea un nuevo dataframe con zip codes unicos y se les asigna un ID.
    retorna el dataframe de geolocalizacion al que solo se le realiza el ETL, y el df filtrado para ser utilizados luego en el proceso de etl de vendedores y clientes
    
    """
    # Seleccionamos los outliers. 
    out_latylon = geolocalizacion [(geolocalizacion.geolocation_lat > 5.5)| (geolocalizacion.geolocation_lat < -34)|(geolocalizacion.geolocation_lng > -32 )| (geolocalizacion.geolocation_lng  < -74)]

    # generamos un user random
    import string, random
    user = ''.join(random.choice(string.ascii_lowercase) for i in range(6))

    #Usamos Geopy
    from geopy.geocoders import Nominatim
    geolocator = Nominatim(user_agent= user)
    location_city = out_latylon.geolocation_city.values
    index = out_latylon.index
    for i in range(0,len(location_city)):
        location = geolocator.geocode(location_city[i]+', Brasil')
        try:
            geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lng'] = location.longitude
            geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lat'] = location.latitude
        except:
            try:
                location = geolocator.geocode(location_city[i])
                geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lng'] = location.longitude
                geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lat'] = location.latitude
            except:
                geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lat'] = geolocalizacion[geolocalizacion.geolocation_city==location_city[i]].geolocation_lat.mean()
                geolocalizacion.loc[geolocalizacion.index==index[i], 'geolocation_lng'] = geolocalizacion[geolocalizacion.geolocation_city==location_city[i]].geolocation_lng.mean()

    # ahora no es realmente necesario exportar el corregido.. pero si quisieramos podemos ejecutar el codigo:
    geolocalizacion.to_csv('data\e-comerce_Olist_dataset\olist_geolocation_dataset_coregido.csv', index_label=False)

    # Agrupamos por geolocation_zip_code_prefix
    geoloc_filtrado = geolocalizacion.groupby('geolocation_zip_code_prefix').mean().reset_index()
    # agregamos columna de Id igual a la de index
    geoloc_filtrado['IdGeolocalizacion'] = geoloc_filtrado.index

    # Agregamos Ciudad y Estado
    geoloc_filtrado['Ciudad'] = geoloc_filtrado.apply(lambda r: geolocalizacion[geolocalizacion.geolocation_zip_code_prefix==r.geolocation_zip_code_prefix].geolocation_city.values[0], axis =1)
    geoloc_filtrado['Estado'] = geoloc_filtrado.apply(lambda r: geolocalizacion[geolocalizacion.geolocation_zip_code_prefix==r.geolocation_zip_code_prefix].geolocation_state.values[0], axis =1)

    # renombramos y reordenamos las columnas
    geoloc_filtrado.rename(columns={'geolocation_zip_code_prefix': 'zip_code_prefix', 
                                    'geolocation_lat': 'Latitud',
                                    'geolocation_lng': 'Longitud',
                                    'IdGeolocalizacion':'Id_Geolocalizacion'}, inplace=True)
    geoloc_filtrado = geoloc_filtrado.reindex(columns=['Id_Geolocalizacion', 'zip_code_prefix', 'Latitud', 
                                    'Longitud','Ciudad','Estado'])


    # En la carpeta Dataset_etl guardamos el archivos de geolocalizacion luego del proceso de ETL
    geoloc_filtrado.to_csv("{}/geolocalizacion_etl.csv".format(path_etl), index=False)

    #Retornamos el df de geolocalizacion, de la primer parte del etl, porque es el que necesitamos para el etl de clientes y vendedores
    return geolocalizacion, geoloc_filtrado

geolocalizacion,  geoloc_filtrado = etl_geolocalizacion(geolocalizacion)

### ETL a clientes

def clientes_etl(clientes):
    """
    Funcion que realiza el ETL a la tabla clientes. 
        Se normalizan los codigos postales que no coinciden con los de la tabla geolocalizacion
        Para los zip codes de clientes que no se encuentran en geolocalizacion pero si se reconoce su ciudad
        se reemplazara el zip_code por la moda del zip_code correspondiente a dicha ciudad en geolocalizacion.
        Como resultado guardara dos archivos 
            clientes_etl.csv con el resultado del etl 
            clientes_aux.csv con los datos que no logro resolver
    """
    # usamos el df de geolocalizacion porque no tiene filtrados los zip_codes y por ende hay mas datos para calcular el modo
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
    
    # buscamos los index de los codigos que no logramos resolver para luego guardarlos en una tabla auxiliar
    indexes = []
    for item in zip_codes_missing:
        ind = clientes[clientes.customer_zip_code_prefix==item].index.values
        for i in ind:
            indexes.append(i)

    cientes_aux = clientes.iloc[indexes]
    clientes.drop(index = indexes, axis=0, inplace=True)

    # Ahora que eliminamos estos valores problema aplicamos el id_Geolocalizacion a la tabla clientes
    clientes['Id_Geolocalizacion'] = clientes.apply(lambda r: geoloc_filtrado[geoloc_filtrado.zip_code_prefix==r.customer_zip_code_prefix].Id_Geolocalizacion.values[0], axis = 1)
    clientes = clientes.drop(['customer_zip_code_prefix','customer_city','customer_state'],axis=1)

    # guardamos clientes_aux en un csv
    cientes_aux.to_csv("{}/cientes_aux.csv".format(path_aux), index=False)

    # Guardamos las etl
    clientes.to_csv("{}/clientes_etl.csv".format(path_etl), index=False)

clientes_etl(clientes)

### ETL Vendedores

def vendedores_etl(vendedores):
    """
    Funcion que realiza el ETL a la tabla vendedores. 
        Se normalizan los codigos postales que no coinciden con los de la tabla geolocalizacion
        Para los zip codes de vendedores que no se encuentran en geolocalizacion pero si se reconoce su ciudad
        se reemplazara el zip_code por la moda del zip_code correspondiente a dicha ciudad en geolocalizacion.
        Como resultado guardara un archivo vendedores_etl.csv con el resultado del etl 
    """

    # Hay 7 codigos postales de vendedores que no coinciden con los codigos de la tabla geolocalizacion.
    # Para cada ciudad en la tabla de geolocalizacion hay mas de un zip-code... 
    # lo que vamos a hacer es cambiar los de los vendedores por el modo de los de geolocalizacion  
    # usamos el df de geolocalizacion porque no tiene filtrados los zip_codes y por ende hay mas datos para calcular el modo
 
    geoloc_unique_code = geolocalizacion.geolocation_zip_code_prefix.unique()
    vendedores_unique_code = vendedores.seller_zip_code_prefix.unique()
    for item in vendedores_unique_code:
        if item not in geoloc_unique_code:
            city = vendedores.loc[vendedores['seller_zip_code_prefix'] == item].seller_city.values[0]
            zip_code = geolocalizacion[geolocalizacion.geolocation_city==city].geolocation_zip_code_prefix.mode()[0]
            vendedores['seller_zip_code_prefix'] = vendedores['seller_zip_code_prefix'].replace(item, zip_code)
    
    #Logramos arreglar todos :) Ahora aplicamos el id_Geolocalizacion a la tabla vendedores

    vendedores['Id_Geolocalizacion'] = vendedores.apply(lambda r: geoloc_filtrado[geoloc_filtrado.zip_code_prefix==r.seller_zip_code_prefix].Id_Geolocalizacion.values[0], axis = 1)
    vendedores = vendedores.drop(['seller_zip_code_prefix','seller_city','seller_state'],axis=1)

    
    # Guardamos las etl
    vendedores.to_csv("{}/vendedores_etl.csv".format(path_etl), index=False)

vendedores_etl(vendedores)

## Metodo de pago // tipo de pago

def metodo_tipo_pago(metodo_de_pago):
    """
    Se agregan los payment_type_id y se crean las tablas normalizadas payment_type y metodo_de_pago_corregido a partir de la tabla metodo_de_pago
    """
    payment_type = metodo_de_pago['payment_type']
    tipos_pago = {j:i+1 for i,j in enumerate(payment_type.unique())}
    payment_type_df = pd.DataFrame(
        {
            'payment_type_id': tipos_pago.values(),
            'payment_type': tipos_pago.keys()
        })
    metodo_de_pago['payment_type'] = metodo_de_pago['payment_type'].apply(lambda x: tipos_pago[x])
    metodo_de_pago.rename(columns={'payment_type': 'payment_type_id'}, inplace=True)

    #exportamos a csv 
    payment_type_df.to_csv('{}/payment_type.csv'.format(path_etl), index=False)
    metodo_de_pago.to_csv('{}/metodo_de_pago_corregido.csv'.format(path_etl), index=False)

metodo_tipo_pago(metodo_de_pago)

## Ordenes // estado de la orden

def ordenes_estado_orden (ordenes):
    """
    Se agregan los order_status_id y se crean las tablas normalizadas order_status y ordenes_corregido a partir de la tabla ordenes
    """
    estado_orden = ordenes['order_status'].unique()
    estado_orden = {j:i+1 for i,j in enumerate(estado_orden)}
    order_status_df = pd.DataFrame(
        {
            'order_status_id': estado_orden.values(),
            'order_status': estado_orden.keys()
        })
    ordenes['order_status'] = ordenes['order_status'].apply(lambda x: estado_orden[x])
    ordenes.rename(columns={'order_status': 'order_status_id'}, inplace=True)

    # Exportamos los csv
    ordenes.to_csv('./Dataset_etl/ordenes_corregido.csv', index=False)
    order_status_df.to_csv('./Dataset_etl/order_status.csv', index=False)

ordenes_estado_orden(ordenes)

## Productos // tipo de producto

def productos_tipo_producto(productos):
    """
    Se agregan product_category_name_id y se crean las tablas normalizadas product_category_name y productos_corregido a partir de la tabla productos
    """

    productos_name = productos['product_category_name'].unique()
    productos_name = {j:i+1 for i,j in enumerate(productos_name)}

    product_category_name_df = pd.DataFrame(
        {
            'product_category_name_id': productos_name.values(),
            'category_name': productos_name.keys()
        })
    productos['product_category_name'] = productos['product_category_name'].apply(lambda x: productos_name[x])
    productos.rename(columns={'product_category_name': 'product_category_name_id'}, inplace=True)
    
    # Exportamos a csv
    productos.to_csv('./Dataset_etl/productos_corregido.csv', index=False)
    product_category_name_df.to_csv('./Dataset_etl/product_category_name.csv', index=False)

productos_tipo_producto(productos)