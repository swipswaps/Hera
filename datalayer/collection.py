from mongoengine import *
import os
import json
import pymongo
import pydoc

class AbstractCollection(object):
    _metadataCol = None
    _datatypeCol = None
    _config = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, type=None):

        configFile = os.path.join(os.environ.get('HOME'), '.pyhera', 'config.json')
        if os.path.isfile(configFile):
            with open(configFile, 'r') as jsonFile:
                mongoConfig = json.load(jsonFile)
        else:
            configData = dict(username='{username}',
                              password='{password}',
                              dbIP='{databaseIP}',
                              dbName='{databaseName}'
                              )

            if not os.path.isdir(os.path.dirname(configFile)):
                os.makedirs(os.path.dirname(configFile))

            with open(configFile, 'w') as jsonFile:
                json.dump(configData, jsonFile, indent=4, sort_keys=True)

            errorMessage = "The config file doesn't exist in the default directory.\n" \
                           "A default config data file named '{}' was created. Please fill it and try again.".format(
                configFile)

            raise IOError(errorMessage)

        connect(alias='%s-alias' % mongoConfig['dbName'],
                db=mongoConfig['dbName'],
                username=mongoConfig['username'],
                password=mongoConfig['password'],
                authentication_source='admin'
                )

        class Metadata(DynamicDocument):
            name = StringField(required=True)
            type = StringField(required=True)
            resource = StringField(required=True)
            meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
                    'allow_inheritance': True}

        class GISMetadata(Metadata):
            pass

        class ExperimentalMetadata(Metadata):
            fileFormat = StringField(required=True)

        class NumericalMetadata(Metadata):
            pass

        class AnalysisMetadata(Metadata):
            pass

        if self.type is None:
            self._metadataCol = Metadata
        else:
            self._metadataCol = getattr('%sMetadata' % self.type)
        self._config = mongoConfig

    def getDocuments(self, **kwargs):
        if self.type is not None:
            kwargs['type'] = self.type
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
        super().__init__(type='GIS')


class Experimental_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(type='Experimental')


class Numerical_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(type='Numerical')


class Analysis_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(type='Analysis')

