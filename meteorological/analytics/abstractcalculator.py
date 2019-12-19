import pandas
import dask.dataframe.core
from ..inmemoryavgdata import InMemoryAvgData
from pyhera import datalayer

class AbstractCalculator(object):
    _RawData = None
    _Metadata = None
    _DataType = None
    _TemporaryData = None
    _Identifier = None
    _CalculatedParams = None
    _InMemoryAvgRef = None
    _Karman = 0.4
    _saveProperties = {'fileFormat':None}

    def __init__(self, rawData, metadata, identifier):
        if type(rawData) == pandas.DataFrame:
            self._DataType = 'pandas'
        elif type(rawData) == dask.dataframe.core.DataFrame:
            self._DataType = 'dask'
        else:
            raise ValueError("'rawData' type must be 'pandas.DataFrame' or 'dask.dataframe.core.DataFrame'.\nGot '%s'." % type(rawData))

        self._RawData = rawData
        self._Metadata = metadata
        self._TemporaryData = pandas.DataFrame()
        self._Identifier = identifier
        self._CalculatedParams = []
        self._joinmethod = "left"

    @property
    def JoinMethod(self):
        return self._joinmethod

    @property
    def RawData(self):
        return self._RawData

    @property
    def Metadata(self):
        return self._Metadata

    @property
    def TemporaryData(self):
        return self._TemporaryData

    @property
    def Identifier(self):
        return self._Identifier

    @property
    def SamplingWindow(self):
        return self._Identifier['samplingWindow']

    @property
    def Karman(self):
        return self._Karman

    def set_saveProperties(self, fileFormat, **kwargs):
        self._saveProperties['fileFormat'] = fileFormat
        self._saveProperties.update(kwargs)

    def compute(self, mode=None):
        params = list(self.TemporaryData.columns)
        query = dict(projectName=self.Identifier['projectName'],
                     station=self.Identifier['station'],
                     instrument=self.Identifier['instrument'],
                     height=self.Identifier['height'],
                     start=self.Identifier['start'],
                     end=self.Identifier['end']
                     )
        docExist = datalayer.Analysis.getDocuments(params__all=params,start__lt=self.Identifier['end'],end__gt=self.Identifier['start'],**query)
        if docExist:
            print('exist')
        else:
            self._joinmethod = "left"
            if self._TemporaryData.columns.empty:
                raise ValueError("Parameters have not been calculated yet.")

            if self._DataType == 'dask':
                try:
                    df = self._TemporaryData[self._CalculatedParams].compute()
                except ValueError as valueError:
                    errorMessage = """A value error occurred while computing the data.
                                        This is probably because one of the problems bellow:
                                        1.The time index of the data is not divisible in the sampling window.
                                          Please ensure that the sampling window and the time index are matching and try again.
                                        2.Data is missing. Try using the keyword argument \'isMissingData\' (isMissingData=True).

                                        The error that was raised: %s""" % valueError

                    raise ValueError(errorMessage)
            else:
                df = self._TemporaryData[self._CalculatedParams]

            if self._InMemoryAvgRef is None:
                self._InMemoryAvgRef = InMemoryAvgData(df, turbulenceCalculator=self)
            else:
                self._InMemoryAvgRef = InMemoryAvgData(pandas.concat([df, self._InMemoryAvgRef], axis=1),
                                                       turbulenceCalculator=self)

            if self._saveProperties['fileFormat'] is None:
                raise AttributeError('No save properties are set. Please use set_saveProperties function')
            else:
                doc = {}
                doc['projectName'] = query.pop('projectName')
                doc['fileFormat'] = self._saveProperties['fileFormat']
                doc['desc'] = query
                doc['desc']['start']: self.Identifier['start']
                doc['desc']['end']: self.Identifier['end']
                doc['desc']['params'] = params
                doc.update(getSaveData(data=df, **self._saveProperties))
                datalayer.Analysis.addDocument(**doc)

        self._CalculatedParams = []

        return self._InMemoryAvgRef


def getSaveData(fileFormat, **kwargs):
    return getattr(globals()['SaveDataHandler'], 'getSaveData_%s' % fileFormat)(**kwargs)


class SaveDataHandler(object):

    @staticmethod
    def getSaveData_HDF(data, path, key):
        data.to_HDF(path, key)
        return dict(resource=dict(path=path,
                                  key=key
                                  )
                    )

    @staticmethod
    def getSaveData_dict(data):
        return dict(resource=data.to_dict('split'))

    @staticmethod
    def getSaveData_JSON(data, path=None):
        if path is None:
            return dict(resource=data.to_json())
        else:
            data.to_json(path)
            return dict(resource=path)

    @staticmethod
    def getSaveData_parquet(data, path):
        data.to_parquet(path)
        return dict(resource=path)

# class CalculatedField(object):
#
#     def WindDirection(self):
#         pass
#
#     def WindVelocity(self|):
#         pass
