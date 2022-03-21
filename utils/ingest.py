import os, sys
import requests
import xmltodict
import logging
import re
from tqdm import tqdm
import zipfile


datalake_folder = os.getenv("datalake_folder")
unzip_folder = os.getenv("unzip_folder")
raw_folder = os.getenv("raw_folder")
data_url = os.getenv("data_url")
tmp_file = os.getenv("tmp_file")


def save_temporal_file(unzipped_filenames:list) -> None:
    output_file = open(tmp_file, 'w')

    for file_name in unzipped_filenames:
        output_file.write(file_name + '\n')

    output_file.close()


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
        os.makedirs(f'{datalake_folder}/{raw_folder}')

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


def main():
    number_of_files = int(sys.argv[1])

    zipped_filenames = get_data(number_of_files)
    unzipped_filenames = unzip_data(zipped_filenames)
    save_temporal_file(unzipped_filenames)


if __name__ == '__main__':
    main()
