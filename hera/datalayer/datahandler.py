import pandas
import dask.dataframe
import xarray
import json
from json import JSONDecodeError
import geopandas

def getHandler(type):
    return globals()['DataHandler_%s' % type]


class DataHandler_string(object):

    @staticmethod
    def getData(resource):
        return resource

class DataHandler_time(object):

    @staticmethod
    def getData(resource):
        return pandas.Timestamp(resource)

class DataHandler_HDF(object):

    @staticmethod
    def getData(resource, usePandas=False):
        df = dask.dataframe.read_hdf(resource['path'], resource['key'], sorted_index=True)

        if usePandas:
            df = df.compute()

        return df


class DataHandler_dict(object):

    @staticmethod
    def getData(resource, usePandas=True):
        df = pandas.DataFrame(resource)
        if not usePandas:
            df = dask.dataframe.from_pandas(df, npartitions=1)

        return df


class DataHandler_netcdf_xarray(object):

    @staticmethod
    def getData(resource):
        df = xarray.open_mfdataset(resource, combine='by_coords')

        return df

class DataHandler_JSON_dict(object):

    @staticmethod
    def getData(resource):
        try:
            df = json.loads(resource)
        except JSONDecodeError:
            with open(resource, 'r') as myFile:
                df = json.read(myFile)

        return df


class DataHandler_JSON_pandas(object):

    @staticmethod
    def getData(resource, usePandas=True):
        if usePandas:
            df = pandas.read_json(resource)
        else:
            df = dask.dataframe.read_json(resource)

        return df

class DataHandler_geopandas(object):
    @staticmethod
    def getData(resource):
        df = geopandas.read_file(resource)

        return df

class DataHandler_parquet(object):

    @staticmethod
    def getData(resource, usePandas=False):
        df = dask.dataframe.read_parquet(resource)

        if usePandas:
            df = df.compute()

        return df