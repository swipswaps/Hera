import pandas
import dask.dataframe
import xarray
import json
from json import JSONDecodeError
import geopandas
import matplotlib.image as mpimg


def getHandler(type):
    return globals()['DataHandler_%s' % type]


class DataHandler_string(object):
    """
        The resource is a string.
    """

    @staticmethod
    def getData(resource):
        """
            The data in the record is a string.

        :param resource: the resource of the record
        :return:
            string
        """
        return resource


class DataHandler_time(object):
    """
        The resource is a timestamp.
    """

    @staticmethod
    def getData(resource):
        """
            The data in the record is a timestamp.


        :param resource: boolean the resource of the record
        :return:
            pandas.Timestamp
        """
        return pandas.Timestamp(resource)


class DataHandler_HDF(object):
    """
        Loads a single key from HDF file or files.

        Returns a pandas or a dask dataframe.

        The structure of the resource is a dictionary with the keys:
         -  path: the path to the HDF file (can be a pattern to represent a list of files).
         -  key : a single key.
    """

    @staticmethod
    def getData(resource, usePandas=False):
        """
            Loads a key from a HDF file or files.

        :param resource: The directory of the parquet file.
        :param usePandas: if True, compute the dask.
        :return:
                dask dataframe or pandas.dataframe (if usePandas is true).
        """
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
        """
            Loads a parquet file using the resource.

        :param resource: The directory of the parquet file.
        :param usePandas: if True, compute the dask.
        :return:
                dask dataframe or pandas.dataframe (if usePandas is true).
        """
        df = dask.dataframe.read_parquet(resource)

        if usePandas:
            df = df.compute()

        return df


class DataHandler_image(object):

    @staticmethod
    def getData(resource):
        img = mpimg.imread(resource)

        return img