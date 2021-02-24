#!/usr/bin/env python
"""
Script to set up the tests of the Ibis OmniSciDB backend.

It downloads the data from the Ibis repository, and loads
it into the local OmniSciDB instance.
"""
import os
import pathlib
import sys
import urllib.request
import zipfile

import pandas
import pyomnisci

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def download(repo_url, directory):
    """Download and extract the data to use for the tests."""
    # download the master branch
    url = repo_url + '/archive/master.zip'
    # download the zip next to the target directory with the same name
    path = directory / 'master.zip'

    if not path.exists():
        print(f'Downloading {url} to {path}...', file=sys.stderr)
        path.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, path)
    else:
        print(f'Skipping download: {path} already exists')

    print(f'Extracting archive to {directory}')

    # extract all files
    with zipfile.ZipFile(str(path), 'r') as f:
        f.extractall(str(directory))


def read_tables(names, data_directory):
    """Yield the data tables as pandas dataframes."""
    for name in names:
        path = data_directory / 'testing-data-master' / '{}.csv'.format(name)

        params = {}

        if name == 'geo':
            params['quotechar'] = '"'

        df = pandas.read_csv(str(path), index_col=None, header=0, **params)

        if name == 'functional_alltypes':
            df['bool_col'] = df['bool_col'].astype(bool)
            # string_col is actually dt.int64
            df['string_col'] = df['string_col'].astype(str)
            df['date_string_col'] = df['date_string_col'].astype(str)
            # timestamp_col has object dtype
            df['timestamp_col'] = pandas.to_datetime(df['timestamp_col'])

        yield name, df


def main(repo_url, schema, tables, data_directory, **params):
    """Create the schema and fetch and load the data into it."""
    data_directory = pathlib.Path(data_directory)
    reserved_words = ['table', 'year', 'month']

    download(repo_url, data_directory)

    # connection
    print('Initializing OmniSci...', file=sys.stderr)
    default_db = 'omnisci'
    database = params["database"]

    if database != default_db:
        conn = pyomnisci.connect(
            host=params['host'],
            user=params['user'],
            password=params['password'],
            port=params['port'],
            dbname=default_db,
            protocol=params['protocol'],
        )
        stmt = "DROP DATABASE IF EXISTS {}".format(database)
        try:
            conn.execute(stmt)
        except Exception:
            print(f'OmniSci DDL statement {stmt} failed', file=sys.stderr)

        stmt = 'CREATE DATABASE {}'.format(database)
        try:
            conn.execute(stmt)
        except Exception:
            print(f'OmniSci DDL statement {stmt} failed', file=sys.stderr)
        conn.close()

    conn = pyomnisci.connect(
        host=params['host'],
        user=params['user'],
        password=params['password'],
        port=params['port'],
        dbname=database,
        protocol=params['protocol'],
    )

    # create tables
    for stmt in filter(None, map(str.strip, open(schema).read().split(';'))):
        try:
            conn.execute(stmt)
        except Exception:
            print(f'OmniSci DDL statement \n{stmt}\n failed', file=sys.stderr)

    # import data
    for table, df in read_tables(tables, data_directory):
        if table == 'batting':
            # float nan problem
            cols = df.select_dtypes([float]).columns
            df[cols] = df[cols].fillna(0).astype(int)

            # string None driver problem
            cols = df.select_dtypes([object]).columns
            df[cols] = df[cols].fillna('')
        elif table == 'awards_players':
            # string None driver problem
            cols = df.select_dtypes([object]).columns
            df[cols] = df[cols].fillna('')

        # rename fields
        for df_col in df.columns:
            if ' ' in df_col or ':' in df_col:
                column = df_col.replace(' ', '_').replace(':', '_')
            elif df_col in reserved_words:
                column = '{}_'.format(df_col)
            else:
                continue
            df.rename(columns={df_col: column}, inplace=True)

        load_method = 'rows' if table == 'geo' else 'columnar'
        conn.load_table(table, df, method=load_method)

    conn.close()


if __name__ == '__main__':
    schema = os.path.join(BASE_PATH, 'schema.sql')
    tables = [
        'functional_alltypes',
        'diamonds',
        'batting',
        'awards_players',
        'geo',
    ]
    data_directory = '/tmp/'
    main(
        repo_url='https://github.com/ibis-project/testing-data',
        schema=schema,
        tables=tables,
        data_directory=data_directory,
        host='localhost',
        port=6274,
        user='admin',
        password='HyperInteractive',
        database='ibis_testing',
        protocol='binary',
    )
