# from .document.metadataDocument import Metadata,Measurements,Simulations,Analysis,Projects
from . import getDBObject
from mongoengine import ValidationError, MultipleObjectsReturned, DoesNotExist
import warnings


class AbstractCollection(object):
    _metadataCol = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, ctype=None, user=None):
        self._type = ctype
        self._metadataCol = getDBObject('Metadata', user) if self.type is None else getDBObject(ctype, user)

    def getDocumentsAsDict(self, projectName, with_id=False, **kwargs):
        dictList = [doc.asDict(with_id=with_id) for doc in self.getDocuments(projectName=projectName, **kwargs)]
        return dict(documents=dictList)

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
        if 'desc__type' in kwargs or ('desc' in kwargs and 'type' in kwargs['desc']):
            raise KeyError("'type' key can't be in the desc")
        try:
            self._metadataCol(**kwargs).save()
        except ValidationError:
            raise ValidationError("Not all of the required fields are delivered "
                                  "or one of the fields type is not proper.")

    def addDocumentFromJSON(self, json_data):
        self._metadataCol.from_json(json_data).save()

    def deleteDocuments(self, projectName, **kwargs):
        for doc in self.getDocuments(projectName=projectName, **kwargs):
            doc.delete()

    def deleteDocumentByID(self, id):
        self.getDocumentByID(id=id).delete()

    def getData(self, projectName, usePandas=None, **kwargs):
        """
        Returns the data by the given parameters.

        :param projectName: The name of the project.
        :param usePandas: Return the data as pandas if True, dask if False. Default is None, and returns the data depends on the data format.
        :param kwargs: Other properties of the data.
        :return: pandas/dask dataframe.
        """
        warnings.warn('getData is going to be deprecated in the next version.'
                      'use getDocuments to get documents and use getData of the document object' ,DeprecationWarning)
        docList = self.getDocuments(projectName=projectName, **kwargs)
        if usePandas is None:
            return [doc.getData() for doc in docList]
        else:
            return [doc.getData(usePandas=usePandas) for doc in docList]


class Measurements_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Measurements', user=user)
    #
    # def meta(self):
    #     return self._metadataCol


class Simulations_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Simulations', user=user)


class Analysis_Collection(AbstractCollection):

    def __init__(self, user=None):
        super().__init__(ctype='Analysis', user=user)
