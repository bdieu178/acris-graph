#%%
import glob
from os.path import basename
import pandas as pd
import numpy as np
from postal.expand import expand_address
from postal.parser import parse_address
import logging
# import zipcode
# from uszipcode import ZipcodeSearchEngine
# from pyzipcode import Pyzipcode as pz
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

#Useless now.
def get_zipcode_obj(zipcode_str):
    if zipcode_str and zipcode_str.isdigit():
        zipcode_obj = pz.get(zipcode_str, "US")
        if zipcode_obj: return zipcode_obj
        else: return None
    else: return None
    
#Useless now.
def get_state(row):
    zipcode_obj = get_zipcode_obj(row['ZIP'])
    if zipcode_obj:
        return zipcode_obj.state
    else: return row['STATE']

#Useless now.
def get_city(row):
    zipcode_obj = get_zipcode_obj(row['ZIP'])
    if zipcode_obj:
        return zipcode_obj.city
    else: return row['CITY']

#%%

acris_datasets = {}
   #acris_datasets[basename(file).split('.')[0]] = pd.read_csv(file, nrows=1000)

def clean_acris_csv(filename):
        # global search
        # search = ZipcodeSearchEngine()
        zipcodes = pd.read_csv('./data/zip_code_database.csv', encoding='latin-1', index_col='zip')

        curr_row_progress = 0
        logger.debug('Starting to process %s', filename)
        t = pd.read_csv(filename, chunksize=chunksize, iterator=True, dtype=object, converters={'ZIP': str}, 
        header=None, names=['DOCUMENT ID','RECORD TYPE','PARTY TYPE','NAME','ADDRESS 1','ADDRESS 2','COUNTRY','CITY','STATE','ZIP','GOOD THROUGH DATE'])
        #t = dd.read_csv('./data/parsed_sample/real_property_parties_out.csv', blocksize=chunksize, dtype=object, converters={'ZIP': str})

        for chunk in t:
            address_extract = chunk.loc[:, ['ADDRESS 1', 'ADDRESS 2', 'CITY', 'STATE', 'COUNTRY', 'ZIP']]
            
            address_extract = pd.merge(address_extract, zipcodes.loc[:,['state','primary_city']], how='left', left_on='ZIP', right_index=True, sort=True, suffixes=('_x', '_y'), copy=False)
            
            #If zip code lookup fails, use default
            address_extract['primary_city'].fillna(value=address_extract['CITY'], inplace=True)
            address_extract['state'].fillna(value=address_extract['STATE'], inplace=True)

            address_extract.drop(['STATE', 'CITY'], axis = 1, inplace = True, errors = 'ignore')
            address_extract = address_extract.reindex_axis(['ADDRESS 1', 'ADDRESS 2', 'primary_city', 'state', 'COUNTRY', 'ZIP'], axis=1)

            #address_extract['STATE'] = address_extract.apply(get_state, axis=1)
            #address_extract['CITY'] = address_extract.apply(get_city, axis=1)

            sample = address_extract.apply(lambda x: ', '.join(x.dropna().astype(str).values), axis=1)

            chunk['parsed_address'] = sample.apply(lambda x: expand_address(x, languages=['en'], delete_final_periods=True, delete_acronym_periods=True)[0])
            chunk['STATE'] = address_extract['state']
            chunk['CITY'] = address_extract['primary_city']

            chunk.to_csv(out_dir + os.path.basename(filename) + '.csv', index=False, header=False, mode='a', chunksize=chunksize)

            curr_row_progress += chunksize
            logger.debug('%s: Wrote out %s rows', filename, curr_row_progress)

pool = mp.Pool(processes = (mp.cpu_count() - 3))
if __name__ == '__main__':
    
    chunksize = 100000
    out_csv = './cleaned_addresses1.csv'
    out_dir = './data/parsed_addresses/'
    to_be_processed = []
    for file in glob.glob('./data/parsed_sample/split_files/*'):
        to_be_processed.append(file)
    pool = mp.Pool(processes = (mp.cpu_count() - 1))
    pool.map(clean_acris_csv, to_be_processed)
    pool.close()
    pool.join()