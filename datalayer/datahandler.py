import pandas
import dask.dataframe
import xarray

def getHandler(type):
    return globals()['DataHandler_%s' % type]

class DataHandler_HDF(object):

    @staticmethod
    def getData(resource):
        df = dask.dataframe.read_hdf(resource['path'], resource['key'], sorted_index=True)

        # if usePandas is True:
        #     df = df.compute()

        return df

class DataHandler_JSONpandas(object):

    @staticmethod
    def getData(resource):
        return pandas.DataFrame(resource)

class DataHandler_netcdf(object):

    @staticmethod
    def getData(resource):
        return xarray.open_dataset(resource).to_dataframe()

class DataHnadler_JSON(object):

    @staticmethod
    def getData(resource):
        return pandas.read_json(resource)

class DataHandler_parquet(object):

    @staticmethod
    def getData(resource):
        return pandas.read_parquet(resource)