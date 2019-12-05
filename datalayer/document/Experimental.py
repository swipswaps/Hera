import pydoc
from .abstractDocument import AbstractDocument
import pandas
import dask.dataframe
import os

class AbstractExperimentalDocument(AbstractDocument):
    @staticmethod
    def getDocument(documentJSON):
        docType = documentJSON['fileFormat']

        return pydoc.locate("pyhera.datalayer.document.Experimental.ExperimentalDocument_%s" % docType)(documentJSON)

    def __init__(self, data):
        super().__init__(data=data)

    @property
    def fileFormat(self):
        return self.data['fileFormat']

    @property
    def properties(self):
        return self.data['properties']

    def getPandasByKey(self, key):
        """

        :return: pandas of the relevant key
        """
        return pandas.DataFrame(self.data['desc'][key])

    def getDescKeys(self):
        return list(self.data['desc'].keys())



class ExperimentalDocument_HDF(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)

    def getData(self, station, instrument, height, startTime = None, endTime = None, usePandas = None):
        """
        This method loads the raw data that corresponds to the requirements (campaign, station, instrument.. )

        :param station:     The name of the station.
        :param instrument:  The name of the instrument.
        :param height:      The station height.
        :param startTime:   Datetime of the begin.
        :param endTime:     Datetime of the end.
        :param usePandas:   A flag of whether or not to use pandas.

        :return: A dask/pandas dataframe depends on the usePandas flag (default is dask).
        """
        if type(startTime) is str:
            startTime = pandas.to_datetime(startTime, dayfirst=True)
        if type(endTime) is str:
            endTime = pandas.to_datetime(endTime, dayfirst=True)

        if usePandas is None:
            if 'usePandas' in self.properties.keys():
                usePandas = self.properties['usePandas']
            else:
                usePandas = False

        fileNameFormat = self.properties['fileNameFormat']
        fileKeyFormat = self.properties['fileKeyFormat']
        fileDictFormat = {'station': station, 'instrument': instrument, 'height': height,
                          'resource': self.resource}

        if self.properties['filesSearchType'] == 'pattern' or startTime is None or endTime is None:
            df = self._getDataByPattern(fileNameFormat=fileNameFormat,
                                        fileKeyFormat=fileKeyFormat,
                                        fileDictFormat=fileDictFormat,
                                        startTime=startTime, endTime=endTime)
        else:
            df = self._getDataByDateRange(fileNameFormat=fileNameFormat,
                                          fileKeyFormat=fileKeyFormat,
                                          fileDictFormat=fileDictFormat,
                                          startTime=startTime, endTime=endTime)

        if usePandas is True:
            df = df.compute()

        return df

    def _getDataByPattern(self, fileNameFormat, fileKeyFormat, fileDictFormat, startTime, endTime):
        filesPattern = '%s%s' % (os.path.join(*['*' if '%' in frmtStr else frmtStr for frmtStr in fileNameFormat]).format(**fileDictFormat), '.hdf')
        try:
            df = dask.dataframe.read_hdf(pattern = str(filesPattern),
                                         key = '/'.join(['*' if '%' in frmtStr else frmtStr for frmtStr in fileKeyFormat]).format(**fileDictFormat),
                                         sorted_index = True)
        except ValueError:
            raise ValueError("An error occurred while reading the files by the pattern: %s.\n"
                             "This is probably because duplicate in the time index. Please check the data and try again." % filesPattern)

        return df[startTime : endTime]

    def _getDataByDateRange(self, fileNameFormat, fileKeyFormat,fileDictFormat, startTime, endTime):
        allDatesList = pandas.date_range(startTime - pandas.Timedelta(days = 1), endTime + pandas.Timedelta(days = 1))
        filesList = []

        for date in allDatesList:
            currentFileName = os.path.join(*[date.strftime(frmtStr).format(**fileDictFormat) for frmtStr in fileNameFormat])

            if os.path.isfile(currentFileName):
                filesList.append(currentFileName)

        if len(filesList) < 1:
            raise IOError("There are no files in the desired date range.")

        dfs = [dask.dataframe.read_hdf(pattern = fileName,
                                       key = '/'.join(['*' if '%' in frmtStr else frmtStr for frmtStr in fileKeyFormat]).format(**fileDictFormat),
                                       sorted_index = True
                                       ) for fileName in filesList]
        try:
            df = dask.dataframe.concat(dfs = dfs)
        except ValueError:
            filesPattern = '%s%s' % (os.path.join(*['*' if '%' in frmtStr else frmtStr for frmtStr in fileNameFormat]).format(**fileDictFormat), '.hdf')

            raise ValueError("An error occurred while reading the files by the pattern: %s.\n"
                             "This is probably because duplicate in the time index. Please check the data and try again." % filesPattern)

        return df[startTime : endTime]


class ExperimentalDocument_Parquet(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)
