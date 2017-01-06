#%%
import glob
from os.path import basename
import pandas as pd
import numpy as np
from postal.expand import expand_address
from postal.parser import parse_address
import logging
import zipcode
import dask.dataframe as dd
import multiprocessing as mp
import os

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def get_zipcode_obj(zipcode_str):
    if zipcode_str and zipcode_str.isdigit():
        zipcode_obj = zipcode.isequal(zipcode_str)
        if zipcode_obj: return zipcode_obj
        else: return None
    else: return None

def get_state(row):
    zipcode_obj = get_zipcode_obj(row['ZIP'])
    if zipcode_obj:
        return zipcode_obj.state
    else: return row['STATE']

def get_city(row):
    zipcode_obj = get_zipcode_obj(row['ZIP'])
    if zipcode_obj:
        return zipcode_obj.city
    else: return row['CITY']

#%%

acris_datasets = {}
   #acris_datasets[basename(file).split('.')[0]] = pd.read_csv(file, nrows=1000)

def clean_acris_csv(filename):
    curr_row_progress = 0
    logger.debug('Starting to process %s', filename)
    t = pd.read_csv(filename, chunksize=chunksize, iterator=True, dtype=object, converters={'ZIP': str}, 
    header=None, names=['DOCUMENT ID','RECORD TYPE','PARTY TYPE','NAME','ADDRESS 1','ADDRESS 2','COUNTRY','CITY','STATE','ZIP','GOOD THROUGH DATE'])
    #t = dd.read_csv('./data/parsed_sample/real_property_parties_out.csv', blocksize=chunksize, dtype=object, converters={'ZIP': str})

    for chunk in t:
        address_extract = chunk.loc[:, ['ADDRESS 1', 'ADDRESS 2', 'CITY', 'STATE', 'COUNTRY', 'ZIP']]
        
        address_extract['STATE'] = address_extract.apply(get_state, axis=1)
        address_extract['CITY'] = address_extract.apply(get_city, axis=1)

        sample = address_extract.apply(lambda x: ', '.join(x.dropna().astype(str).values), axis=1)

        chunk['parsed_address'] = sample.apply(lambda x: expand_address(x, languages=['en'], delete_final_periods=True, delete_acronym_periods=True)[0])
        chunk['STATE'] = address_extract['STATE']
        chunk['CITY'] = address_extract['CITY']

        chunk.to_csv(out_dir + os.path.basename(filename) + '.csv', index=False, header=False, mode='a', chunksize=chunksize)

        curr_row_progress += chunksize
        logger.debug('%s: Wrote out %s rows', filename, curr_row_progress)

pool = mp.Pool(processes = (mp.cpu_count() - 1))
if __name__ == '__main__':
    chunksize = 50000
    out_csv = './cleaned_addresses1.csv'
    out_dir = './data/parsed_addresses/'
    to_be_processed = []
    for file in glob.glob('./data/parsed_sample/split_files/*'):
        to_be_processed.append(file)
    pool = mp.Pool(processes = (mp.cpu_count() - 1))
    pool.map(clean_acris_csv, to_be_processed)
    pool.close()
    pool.join()