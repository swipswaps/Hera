import dask.dataframe
from .document.metadataDocument import Metadata,GIS,Measurements,Simulation,Analysis,Project
from mongoengine import ValidationError, MultipleObjectsReturned, DoesNotExist
import pandas
import json

class AbstractCollection(object):
    _metadataCol = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, ctype=None):
        self._type = ctype
        self._metadataCol = Metadata if self.type is None else globals()['%s' % self.type]

    def getDocumentsAsDict(self, projectName, **kwargs):
        dictList = QueryResult(self.getDocuments(projectName=projectName, **kwargs)).asDict()
        ret = dict(documents=dictList)
        return ret

    def getUnique(self, projectName, **kwargs):
        params = {}
        for key, value in kwargs.items():
            params['desc__%s' % key] = value
        return self._metadataCol.objects.get(projectName=projectName, **params)

    def getDocuments(self, projectName, **kwargs):
        # if self.type is not None:
        #     kwargs['type'] = self.type
        params = {}
        for key, value in kwargs.items():
            params['desc__%s' % key] = value
        return self._metadataCol.objects(projectName=projectName, **params)

    def addDocument(self, **kwargs):
        # if self.type is not None:
        #     kwargs['type'] = self.type
        try:
            self._metadataCol(**kwargs).save()
        except ValidationError:
            raise ValidationError("Not all of the required fields are delivered.\nOr the field type is not proper.")

    def addDocumentFromJSON(self, json_data):
        self._metadataCol.from_json(json_data).save()

    def deleteDocuments(self, projectName, **kwargs):
        QueryResult(self.getDocuments(projectName=projectName, **kwargs)).delete()


class Record_Collection(AbstractCollection):
    def __init__(self, ctype=None):
        super().__init__(ctype)

    def getData(self, projectName, usePandas=None, **kwargs):
        """
        Returns the data by the given parameters.

        :param projectName: The name of the project.
        :param usePandas: Return the data as pandas if True, dask if False. Default is None, and returns the data depends on the data format.
        :param kwargs: Other properties of the data.
        :return: pandas/dask dataframe.
        """
        queryResult = QueryResult(self.getDocuments(projectName=projectName, **kwargs))
        if usePandas is None:
            return queryResult.getData()
        else:
            return queryResult.getData(usePandas=usePandas)


class GIS_Collection(Record_Collection):
    def __init__(self):
        super().__init__(ctype='GIS')


class Measurements_Collection(Record_Collection):

    def __init__(self):
        super().__init__(ctype='Measurements')


class Simulation_Collection(Record_Collection):

    def __init__(self):
        super().__init__(ctype='Simulation')


class Analysis_Collection(Record_Collection):

    def __init__(self):
        super().__init__(ctype='Analysis')


class Project_Collection(AbstractCollection):
    def __init__(self):
        super().__init__(ctype='Project')

    def namesList(self):
        """
        Returns the list of the names of the existing projects.

        :return:  list
        """
        queryResult = QueryResult(self._metadataCol.objects())
        return queryResult.projectName()

    def __getitem__(self, projectName):
        try:
            return self.getUnique(projectName=projectName)
        except MultipleObjectsReturned:
            raise MultipleObjectsReturned('There are multiple documents for this project.\n'
                                          'You should have only one document per project in the Project collection.')
        except DoesNotExist:
            raise DoesNotExist('There is no document for this project.')

    def __contains__(self, projectName):
        return projectName in self.namesList()


class QueryResult(object):
    _docList = None

    def __init__(self, docList):
        self._docList = docList

    def getData(self, **kwargs):
        dataList = [doc.getData(**kwargs) for doc in self._docList]
        if len(dataList)==0:
            if 'usePandas' in kwargs:
                if kwargs['usePandas']:
                    return pandas.concat(dataList)
                else:
                    return dask.dataframe.concat(dataList)
        else:
            raise FileNotFoundError('There is no data for those parameters')
        return dataList[0]

    def projectName(self):
        namesList = [doc.projectName for doc in self._docList]
        return namesList

    def delete(self):
        for doc in self._docList:
            doc.delete()

    def asDict(self):
        jsonList = [doc.asDict() for doc in self._docList]
        return jsonList
