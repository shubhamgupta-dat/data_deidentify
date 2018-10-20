"""
@author: shubhamgupta-dat
"""


### Import Libraries ###

import argparse
import gocept.pseudonymize as encode
import yaml
import json
import os.path as path_check
import getpass
import numpy as np
import dask.dataframe as ddf
import dask.diagnostics.progress as dask_progress#

## =========================== ##
##    ADD UTILITY FUNCTIONS    ##
## =========================== ##

def _update_input_config(input_config,secret_key):
    """
    Function updates the input configuration dictionary used for deidentification
    of data. This configuration is used later for dask operation.

    Parameters
    ----------------
    input_config: dictionary of configuration
    secret_key: string to be used as default secret_key key unless provided
    """

    for key in input_config.keys():
        if input_config[key].get('arguments') is None:
            input_config[key]['arguments'] = {'secret':secret_key}
        elif input_config[key]['arguments'].get('secret') is None:
            input_config[key]['arguments']['secret'] = secret_key

def _encode_dataframe(dataframe,column_config,**kwargs):
    """
    Encode the pandas.Dataframe or dask.dataframe with deidentification
    opertions.

    Parameters
    ----------------
    dataframe: dataframe that would be deidentified
    column_config: dictionary configuration provided for each column in
                   dataframe

    returns: encoded dataframe
    """

    if column_config is None:
        print ("Column configuration is not provided. No changes were made.")
        return dataframe

    dict_encoder = {
    'identifier': encode.bic,
    'datestring': encode.datestring,
    'decimal': encode.decimal,
    'email':encode.email,
    'bank_account':encode.iban,
    'integer':encode.integer,
    'license':encode.license_tag,
    'month':encode.month,
    'name':encode.name,
    'phone':encode.phone,
    'address':encode.address,
    'string':encode.string,
    'text':encode.text,
    'time':encode.time,
    'year':encode.year
    }

    for column in column_config.keys():
        try:
            encoder = dict_encoder.get(column_config.get(column).get('type'))
            dataframe[column] = dataframe[column].apply(lambda x: \
                    encoder(str(x),**column_config.get(column).get('arguments')))
        except:
            print("Issue with Column: {0}, check configuration.".format(column))
            print("+"*20)

    return dataframe

## =========================== ##
##      MAIN FUNCTIONALITY     ##
## =========================== ##

### Defining Argument Parser ###

args = argparse.ArgumentParser(usage = 'This utility is to deidentify data \
                             using Pseudonymization')

args.add_argument('input',help ='locaiton of the file or folder to deidetify' )
args.add_argument('config',help ='locaiton of the yml configuration')
args.add_argument('--delimiter',help = 'delimiter to be used while reading the\
                 file',defaul = ',')
args.add_argument('header_config',help = 'location of the json configuration\
                  to be used while reading header from dask framework')
args.add_argument('output',help = 'location of the folder to be used to save \
                 output')

### Collect Arguments ###
arguments = args.parge_args()
delimiter = str(arguments.delimiter)
input_location = str(arguments.input)
header_config_file  = str(arguments.header_config)
input_config_file = str(arguments.config)
output_location = str(arguments.output)

### Check Address Validity ###

if not ((path_check.isfile(input_location)) |
        (path_check.isdire(input_location))):
        print("Input location is not found. "
             "Exiting from existing program.")
        exit()
elif not path_check.isfile(input_config_file):
    print ("Input Configuration File not found. "
           "Exiting from existing program.")
    exit()
elif not path_check.isfile(header_config_file):
    print ("Header Configuration File not found. "
           "Exiting from existing program.")
    exit()
elif not path_check.isdir(output_location):
    print ("Output folder location not found. "
           "Exiting from existing program.")
    exit()

secret_key = getpass.getpass("Please provide default Encryption Key >>>")

### Read Configuration ###
try:
    with open(header_config_file,'r') as header_file:
        header_config = json.load(header_file)
except:
    print("Check Header Json")
    exit()

try:
    with open(input_config,'r') as input_file:
        input_config = yaml.load(input_file)
except:
    print("Check Configuration YML")
    exit()

### Perform Dask Operations ###

if path_check.isdir(input_file):
    input_file = input_file+'/*'

_update_input_config(input_config,secret_key)

dpbar = dask_progress.ProgressBar()
dpbar.register()

ddf_input = ddf.read_csv(input_file,delimiter,dtype = np.object,
                        **header_config)

ddf_input = _encode_dataframe(ddf_input,input_config)
ddf_input.to_csv(output_location+'/output_*.csv')

print("Data deidentified and saved to the location: {0}"\
      .format(output_location+'/output_*.csv'))
