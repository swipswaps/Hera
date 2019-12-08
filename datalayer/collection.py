import pymongo
import pydoc
from .mongoEngineInitialize import Metadata,GISMetadata,ExperimentalMetadata,NumericalMetadata,AnalysisMetadata
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

        if self.type is None:
            self._metadataCol = Metadata
        else:
            self._metadataCol = globals()['%sMetadata' % self.type]


    def getDocuments(self, **kwargs):
        if self.type is not None:
            kwargs['type'] = self.type
        docList = []
        for doc in self._metadataCol.objects(**kwargs):
            docList.append(pydoc.locate('pyhera.datalayer.document.{type}.Abstract{type}Document'.format(type=self.type)).getDocuments(doc))
        return self._metadataCol.objects(**kwargs)

    def addDocument(self, **kwargs):
        if self.type is not None:
            kwargs['type'] = self.type
        try:
            self._metadataCol(**kwargs).save()
        except ValidationError:
            raise ValidationError("Not all of the required fields are delivered!")

    def _getMetadataCollection(self):
        """

        :return: pymongo 'Metadata' collection object
        """
        mongoClient = pymongo.MongoClient("mongodb://{username}:{password}@{dbIP}:27017/".format(**self._config))
        mongoDataBase = mongoClient[self._config['dbName']]
        mongoCollection = mongoDataBase['Metadata']
        return mongoCollection

    def getDocumentsByQuery(self, query):
        """

        :return: List of documents
        """
        mongoCollection = self._getMetadataCollection()

        query.setdefault('type', self.type)

        dataList = mongoCollection.find(query)

        documentList = []
        for data in dataList:
            documentList.append(pydoc.locate('pyhera.datalayer.document.{type}.Abstract{type}Document'.format(type=self.type)).getDocument(data))

        return documentList

    def addDocument2(self, data):
        """
        Adding a document to the Metadata collection

        :param data: The metadata to add
        :return:
        """
        mongoCollection = self._getMetadataCollection()
        mongoCollection.insert_one(data)


class GIS_Collection(AbstractCollection):
    def __init__(self):
        super().__init__(ctype='GIS')


class Experimental_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(ctype='Experimental')


class Numerical_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(ctype='Numerical')


class Analysis_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(ctype='Analysis')


