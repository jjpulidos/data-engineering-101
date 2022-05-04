import json
import dask.dataframe as ddf


new_columns=["tripduration", "starttime", "stoptime", "start station id", "start station name", "start station latitude", "start station longitude", "end station id", "end station name", "end station latitude", "end station longitude", "bikeid", "usertype", "birth year", "gender"]
new_schema = json.load(open('utils/new_schema.json'))


# @event.listens_for(engine, 'before_cursor_execute')
# def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#     if executemany:
#         cursor.fast_executemany = True
#         cursor.commit()


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



    #concatDF = ddf.concat(dfs).compute()
    #print(concatDF.head())
    # ~~ Dask query using index_col ~~ #
    #daskDF = ddf.read_sql_table('contacts',  path_to_db, index_col='contact_id', npartitions=4)
   # daskDF = daskDF.repartition(npartitions=1)
   #  newDF = ddf.from_pandas(pd.DataFrame({'first_name': ['Test'], 'last_name': ['test'], 'email': ['me2@mail.com'], 'phone': [305]}), npartitions=4)
    #newDF = newDF.repartition(npartitions=1)
    # concatDF = ddf.concat([daskDF, newDF]).compute()

