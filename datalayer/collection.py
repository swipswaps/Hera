import pymongo
import pydoc
import pandas
import dask.dataframe
from .document.metadataDocument import Metadata,GISMetadata,ExperimentalMetadata,NumericalMetadata,AnalysisMetadata,ProjectMetadata
from mongoengine import ValidationError

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

    def getDocuments(self, **kwargs):
        # if self.type is not None:
        #     kwargs['type'] = self.type
        return self._metadataCol.objects(**kwargs)

    def addDocument(self, **kwargs):
        # if self.type is not None:
        #     kwargs['type'] = self.type
        try:
            self._metadataCol(**kwargs).save()
        except ValidationError:
            raise ValidationError("Not all of the required fields are delivered!")


class Data_Collection(AbstractCollection):
    def __init__(self, ctype=None):
        super().__init__(ctype)

    def getData(self, **kwargs):
        queryResult = QueryResult(self.getDocuments(**kwargs))
        return queryResult.getData()


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
        queryResult = QueryResult(self.getDocuments())
        return queryResult.projectName()

    def getProject(self, projectName):
        return self.getDocuments(projectName=projectName)[0]


class QueryResult(object):
    _docList = None

    def __init__(self, docList):
        self._docList = docList

    def getData(self):
        dataList = []
        for doc in self._docList:
            dataList.append(doc.getData())
        try:
            return dask.dataframe.concat(dataList)
        except ValueError:
            return pandas.DataFrame()

    def projectName(self):
        namesList = []
        for doc in self._docList:
            namesList.append(doc.projectName)
        return namesList
