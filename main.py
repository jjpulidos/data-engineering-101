import requests
import os

import sqlalchemy.exc
from sqlalchemy import event, create_engine
import xmltodict
from tqdm import tqdm
import re
import logging
import sqlite3
import pandas as pd
import dask.dataframe as ddf
import zipfile


#logging.basicConfig(level=logging.INFO)
datalake_folder = 'datalake'
path_to_db = "sqlite:///db/test.db"
data_url = 'https://s3.amazonaws.com/tripdata/'
raw_folder = 'raw'
unzip_folder = 'unzip'
new_columns= ["tripduration", "starttime", "stoptime", "start station id", "start station name",
              "start station latitude", "start station longitude", "end station id", "end station name",
              "end station latitude", "end station longitude", "bikeid", "usertype", "birth year", "gender"]

new_schema= {
    'started_at': "starttime",
    'ended_at': "stoptime",
    'start_station_name': "start station name",
    'start_station_id': "start station id",
    'end_station_name': "end station name",
    'end_station_id': 'end station id',
    'start_lat': "start station latitude",
    'start_lng': "start station longitude",
    'end_lat': "end station latitude",
    'end_lng': "end station longitude",
    'member_casual': 'usertype'
}

engine = create_engine(path_to_db)


@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()


def get_filenames_already_processed() -> list:
    conn = sqlite3.connect('db/test.db')
    cursor = conn.cursor()
    try:
        exec = cursor.execute('SELECT DISTINCT file_name from data')
        result = [row[0] for row in exec]
        cursor.close()
    except sqlite3.OperationalError:
        return []
    return result


def standardize_schema(df: ddf.DataFrame, file_name: str) -> ddf.DataFrame:
    if 'Trip Duration' in df.columns:
        df = df.rename(columns=dict(zip(df.columns, new_columns)))

    if 'ride_id' not in df.columns:
        df['ride_id'] = None
        df['rideable_type'] = None
    else:

        df = df.rename(columns=new_schema)
        df['starttime'] = ddf.to_datetime(df['starttime'], unit='ns')
        df['stoptime'] = ddf.to_datetime(df['stoptime'], unit='ns')
        df['tripduration'] = (df['stoptime'] - df['starttime']).dt.seconds
        df["bikeid"] = None
        df["birth year"] = None
        df["gender"] = None

    df['starttime'] = ddf.to_datetime(df['starttime'], unit='ns')
    df['file_name'] = file_name
    df['week_day'] = df['starttime'].dt.dayofweek
    return df


# TODO parallelize download URL and research possible ways to be able to consume parallel tasks from tqdm
def download_url(url, save_path, chunk_size=1024*1024):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


# TODO check if downloaded zip file is corrupted
def get_data(window_time: int = None) -> list:

    if not os.path.isdir(datalake_folder):
        os.makedirs(datalake_folder)

    website_plain_text = requests.get(data_url).text
    dictionary_data = xmltodict.parse(website_plain_text)
    file_names = [i['Key'] for i in dictionary_data['ListBucketResult']['Contents'] if '.html' not in i['Key'] and not re.search("[0-9]+-[0-9]+", i['Key'])]
    file_names = file_names[0:window_time if window_time is not None else len(file_names)]

    # To avoid duplicate dates in file_name
    #dates = [re.search("[0-9]+", i).group() for i in file_names]
    #print(set([x for x in dates if dates.count(x) > 1]))

    for file_name in (pbar := tqdm(file_names, desc=f'Downloading Files...')):
        if not os.path.isfile(f'{datalake_folder}/{raw_folder}/{file_name}'):
            pbar.set_description(f"Downloading {file_name}")
            download_url(f'{data_url}{file_name}', f"{datalake_folder}/{raw_folder}/{file_name}")
        else:
            logging.info(f'file {file_name} already exists')

    return file_names


def unzip_data(file_names: list) -> list:

    if not os.path.isdir(f'{datalake_folder}/{unzip_folder}'):
        os.makedirs(f'{datalake_folder}/{unzip_folder}')

    unzipped_filenames = []
    for file_name in (pbar := tqdm(file_names, desc=f'Unzipping Files...')):
        with zipfile.ZipFile(f'{datalake_folder}/{raw_folder}/{file_name}', 'r') as zip_ref:
            unzipped_filename = [i for i in zip_ref.namelist() if '__MACOSX/' not in i][0]
            unzipped_filenames.append(unzipped_filename)
            if not os.path.isfile(f'{datalake_folder}/{unzip_folder}/{unzipped_filename}'):
                pbar.set_description(f"Unzipping {file_name}")
                zip_ref.extractall(f'{datalake_folder}/{unzip_folder}/')
            else:
                logging.info(f'file {unzipped_filename} already unzipped')

    if os.path.isdir(f'{datalake_folder}/{unzip_folder}/__MACOSX/'):
        os.rmdir(f'{datalake_folder}/{unzip_folder}/__MACOSX/')

    return unzipped_filenames


def write_db(file_names: list):

    file_names_already_processed = get_filenames_already_processed()

    for file_name in (pbar := tqdm(file_names, desc=f'Storing Data in Database...')):
        pbar.set_description(f"Storing Data from {file_name} in Database")

        if file_name not in file_names_already_processed:
            df = ddf.read_csv(f'{datalake_folder}/{unzip_folder}/{file_name}', dtype={'end station id': 'float64',
                                                                                  'birth year': 'object',
                                                                                  'Birth Year': 'float64',
                                                                                  'start station id': 'float64',
                                                                                  'end_station_id': 'object',
                                                                                  'start_station_id': 'object',
                                                                                  })

            df = standardize_schema(df, file_name)
            df.to_sql('data', path_to_db, if_exists="append", index=False)

    #concatDF = ddf.concat(dfs).compute()
    #print(concatDF.head())
    # ~~ Dask query using index_col ~~ #
    #daskDF = ddf.read_sql_table('contacts',  path_to_db, index_col='contact_id', npartitions=4)
   # daskDF = daskDF.repartition(npartitions=1)
   #  newDF = ddf.from_pandas(pd.DataFrame({'first_name': ['Test'], 'last_name': ['test'], 'email': ['me2@mail.com'], 'phone': [305]}), npartitions=4)
    #newDF = newDF.repartition(npartitions=1)
    # concatDF = ddf.concat([daskDF, newDF]).compute()


def main():

    file_names = get_data(5)
    write_db(unzip_data(file_names))

main()