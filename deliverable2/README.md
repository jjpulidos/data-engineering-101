#Readme


https://github.com/ets/tap-spreadsheets-anywhere

.meltano/extractors/tap-spreadsheets-anywhere/venv/lib/python3.8/site-packages/tap-spreadsheets-anywhere/format_handler.py 

function get_streamreader after line 22

    if uri[-3:] == 'zip' and uri[:2] == 's3':
        s3_components = uri[5:].split('/')
        f = zipfile.ZipFile(io.BytesIO(streamreader.read().encode('utf8','surrogateescape'))).read(s3_components[1].replace('.zip', ''))
        streamreader = io.TextIOWrapper(io.BufferedReader(io.BytesIO(f)), encoding='utf-8')
