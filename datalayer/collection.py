import dask.dataframe
from .document.metadataDocument import Metadata,GISMetadata,ExperimentalMetadata,NumericalMetadata,AnalysisMetadata,ProjectMetadata
from mongoengine import ValidationError, MultipleObjectsReturned, DoesNotExist
import pandas

class AbstractCollection(object):
    _metadataCol = None
    _datatypeCol = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, ctype=None):
        self._type = ctype
        self._metadataCol = Metadata if self.type is None else globals()['%sMetadata' % self.type]

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

    def deleteDocuments(self, projectName, **kwargs):
        QueryResult(self.getDocuments(projectName=projectName, **kwargs)).delete()


class Data_Collection(AbstractCollection):
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
        return queryResult.getData(usePandas)


class GIS_Collection(Data_Collection):
    def __init__(self):
        super().__init__(ctype='GIS')


class Experimental_Collection(Data_Collection):

    def __init__(self):
        super().__init__(ctype='Experimental')


class Numerical_Collection(Data_Collection):

    def __init__(self):
        super().__init__(ctype='Numerical')


class Analysis_Collection(Data_Collection):

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

    def getData(self, usePandas):
        dataList = []
        for doc in self._docList:
            dataList.append(doc.getData(usePandas))
        try:
            if usePandas:
                return pandas.concat(dataList)
            else:
                return dask.dataframe.concat(dataList)
        except ValueError:
            raise FileNotFoundError('There is no data for those parameters')
            #return pandas.DataFrame()

    def projectName(self):
        namesList = []
        for doc in self._docList:
            namesList.append(doc.projectName)
        return namesList

    def delete(self):
        for doc in self._docList:
            doc.delete()