import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Load clean data into postgres database
def task_data_upload(data):
    print(data.head())

    data = data.to_csv(index=None, header=None)

    postgres_sql_upload = PostgresHook(postgres_conn_id="postgres_connection")
    postgres_sql_upload.bulk_load("twitter_etl_table", data)

    return True


## perform data cleaning and transformation
def transform_data(df):
    print(df.info())
    ### Transformation happens here

    # load transformed data into database
    task_data_upload(df)
