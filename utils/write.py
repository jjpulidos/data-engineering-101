import os
import sqlite3
from tqdm import tqdm
import dask.dataframe as ddf
from transform import standardize_schema


path_to_db = os.getenv('path_to_db')
datalake_folder = os.getenv("datalake_folder")
unzip_folder = os.getenv("unzip_folder")
tmp_file = os.getenv("tmp_file")


def get_filenames_already_processed() -> list:
    conn = sqlite3.connect(path_to_db.replace('sqlite:///', ''))
    cursor = conn.cursor()
    try:
        exec = cursor.execute('SELECT DISTINCT file_name from data')
        result = [row[0] for row in exec]
        cursor.close()
    except sqlite3.OperationalError:
        return []
    return result


def write_db():

    file_names = [line[:-1] for line in open(tmp_file, 'r').readlines()]
    os.remove(tmp_file)

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


def main():
    write_db()


if __name__ == '__main__':
    main()