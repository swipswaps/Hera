import pandas
import dask.dataframe
import xarray


def getHandler(type):
    return globals()['DataHandler_%s' % type]


class DataHandler_string(object):

    @staticmethod
    def getData(resource):
        return resource


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


class DataHandler_netcdf(object):

    @staticmethod
    def getData(resource, usePandas=False):
        df = xarray.open_dataset(resource).to_dask_dataframe()

        if usePandas:
            df = df.compute()

        return df


class DataHnadler_JSON(object):

    @staticmethod
    def getData(resource, usePandas=True):
        df = dask.dataframe.read_json(resource)

        if usePandas:
            df = df.compute()

        return df


class DataHandler_parquet(object):

    @staticmethod
    def getData(resource, usePandas=False):
        df = dask.dataframe.read_parquet(resource)

        if usePandas:
            df = df.compute()

        return df