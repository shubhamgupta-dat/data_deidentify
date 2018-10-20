# data_deidentify
De-identification of Data using scalable operations on Dask.


### USAGE:
> usage: This utility is to deidentify data using Pseudonymization
>
> positional arguments:
>  input                 locaiton of the file or folder to deidetify
>  config                locaiton of the yml configuration
>  header_config         location of the json configuration to be used while
>                        reading header from dask framework
>  output                location of the folder to be used to save output>
> 
> optional arguments:
>  -h, --help            show this help message and exit
>  --delimiter DELIMITER
>                        delimiter to be used while reading the file

Reference: 
1. [Pseudonymization](https://en.wikipedia.org/wiki/Pseudonymization)
2. [Dask](dask.pydata.org/)
3. [Gocept's Pseudonymization](https://pypi.org/project/gocept.pseudonymize/)
