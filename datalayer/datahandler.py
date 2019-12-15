import pandas
import dask.dataframe

def getHandler(type):
    return globals()['DataHandler_%s' % type]

class DataHandler_HDF(object):

    @staticmethod
    def getData(resource):
        df = dask.dataframe.read_hdf(resource['path'], resource['key'], sorted_index=True)

        # if usePandas is True:
        #     df = df.compute()

        return df

