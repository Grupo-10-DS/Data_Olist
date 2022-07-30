import pandas as pd
from google.cloud import storage
import mysql.connector
from mysql.connector import Error


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    return [blob.name.replace("deltas_por_cargar/", "") for blob in blobs][1:]


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name, password=user_password, user=user_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name, user=user_name, passwd=user_password, database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another with a new name."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def prepare_to_sql(df):
    df = list(df.itertuples(index=False, name=None))
    df_string = ",".join(["(" + ",".join([str(i) for i in df_d]) + ")" for df_d in df])
    return df_string


def run():

    from datetime import datetime

    BUCKET_NAME = "data_olist_csv"
    MYSQL_USR = "root"
    MYSQL_PWD = ""
    MYSQL_HOST = "34.135.162.156"
    MYSQL_DB_NAME = "Olist"

    item_delta = pd.read_csv("gs://data_olist_csv/deltas_por_cargar/item_delta.csv")
    order_delta = pd.read_csv("gs://data_olist_csv/deltas_por_cargar/order_delta.csv")
    payment_delta = pd.read_csv(
        "gs://data_olist_csv/deltas_por_cargar/payment_delta.csv"
    )
    review_delta = pd.read_csv("gs://data_olist_csv/deltas_por_cargar/review_delta.csv")

    list_blobs = list_blobs_with_prefix(BUCKET_NAME, prefix="deltas_por_cargar/")

    item_string = prepare_to_sql(item_delta)
    order_string = prepare_to_sql(order_delta)
    payment_string = prepare_to_sql(payment_delta)
    review_string = prepare_to_sql(review_delta)

    insert_item = f"""INSERT INTO item (order_id, order_item_id,
                    product_id, seller_id, shipping_limit_date,
                    price, freight_value) VALUES {item_string};"""

    insert_payment = f"""INSERT INTO payment (order_id, payment_sequential,
                    payment_type_id, payment_installments, payment_value)
                    VALUES{payment_string};"""

    insert_review = f"""INSERT INTO review (review_id, order_id,review_score,
                    review_comment_title,review_comment_message,
                    review_creation_date,review_answer_timestamp)
                    VALUES {review_string};"""

    insert_order = f"""INSERT INTO `order` (order_id,customer_id,order_status_id,
                    order_purchase_timestamp,order_approved_at,
                    order_delivered_carrier_date,order_delivered_customer_date,
                    order_estimated_delivery_date) VALUES {order_string};"""

    db_connection = create_db_connection(
        host_name=MYSQL_HOST,
        user_name=MYSQL_USR,
        user_password=MYSQL_PWD,
        db_name=MYSQL_DB_NAME,
    )

    execute_query(db_connection, insert_item)
    execute_query(db_connection, insert_order)
    execute_query(db_connection, insert_payment)
    execute_query(db_connection, insert_review)

    for blob in list_blobs:
        move_blob(
            BUCKET_NAME,
            f"deltas_por_cargar/{blob}",
            BUCKET_NAME,
            f"deltas_cargados/{blob[:-4]} {datetime.now()}.csv",
        )


if __name__ == "__main__":
    run()
