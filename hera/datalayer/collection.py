# from .document.metadataDocument import Metadata,Measurements,Simulations,Analysis,Projects
from . import getDBObject
from mongoengine import ValidationError, MultipleObjectsReturned, DoesNotExist

class AbstractCollection(object):
    _metadataCol = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, ctype=None, user=None):
        self._type = ctype
        self._metadataCol = getDBObject('Metadata', user) if self.type is None else getDBObject(ctype, user)

    def getDocumentsAsDict(self, projectName, **kwargs):
        dictList = QueryResult(self.getDocuments(projectName=projectName, **kwargs)).asDict()
        ret = dict(documents=dictList)
        return ret

    def getDocuments(self, projectName, resource=None, dataFormat=None, type=None, **desc):
        """
        Get the documents satisfies the given query.
        If projectName is None search over all projects.

        :param projectName:
        :param resource:
        :param dataFormat:
        :param type:
        :param desc:
        :return:
        """
        query = {}
        if resource is not None:
            query['resource'] = resource
        if dataFormat is not None:
            query['dataFormat'] = dataFormat
        if type is not None:
            query['type'] = type
        if projectName is not None:
            query['projectName'] = projectName
        for key, value in desc.items():
                query['desc__%s' % key] = value
        return self._metadataCol.objects(**query)

    def getAllDocuments(self):
        return self._metadataCol.objects()

    def _getAllValueByKey(self, key, **query):
        return [doc[key] for doc in self.getAllDocuments(**query)]

    def getProjectList(self):
        return self._getAllValueByKey('projectName')

    def getDocumentByID(self, id):
        return self._metadataCol.objects.get(id=id)

    def addDocument(self, **kwargs):
        if 'desc__type' in kwargs or 'type' in kwargs['desc']:
            raise KeyError("'type' key can't be in the desc")
        try:
            self._metadataCol(**kwargs).save()
        except ValidationError:
            raise ValidationError("Not all of the required fields are delivered.\nOr the field type is not proper.")

    def addDocumentFromJSON(self, json_data):
        self._metadataCol.from_json(json_data).save()

    def deleteDocuments(self, projectName, **kwargs):
        QueryResult(self.getDocuments(projectName=projectName, **kwargs)).delete()

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


class Measurements_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Measurements', user=user)

    def meta(self):
        return self._metadataCol


class Simulations_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Simulations', user=user)


class Analysis_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Analysis', user=user)


# class Projects_Collection(AbstractCollection):
#     def __init__(self, user=None):
#         super().__init__(ctype='Projects', user=user)
#
#     def namesList(self):
#         """
#         Returns the list of the names of the existing projects.
#
#         :return:  list
#         """
#         queryResult = QueryResult(self._metadataCol.objects())
#         return queryResult.projectName()
#
#     def __getitem__(self, projectName):
#         try:
#             return self.getUnique(projectName=projectName)
#         except MultipleObjectsReturned:
#             raise MultipleObjectsReturned('There are multiple documents for this project.\n'
#                                           'You should have only one document per project in the Project collection.')
#         except DoesNotExist:
#             raise DoesNotExist('There is no document for this project.')
#
#     def __contains__(self, projectName):
#         return projectName in self.namesList()


class QueryResult(object):
    _docList = None

    def __init__(self, docList):
        self._docList = docList

    def getData(self, **kwargs):
        return [doc.getData(**kwargs) for doc in self._docList]

    def delete(self):
        for doc in self._docList:
            doc.delete()

    def asDict(self):
        return [doc.asDict() for doc in self._docList]
