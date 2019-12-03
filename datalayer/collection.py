import os
import json
import pymongo
import pydoc

class AbstractCollection(object):
    _config = None
    _type = None

    @property
    def type(self):
        return self._type

    def __init__(self, doctype):
        configFile = os.path.join(os.environ.get('HOME'), '.pyhera', 'config.json')
        if os.path.isfile(configFile):
            with open(configFile, 'r') as jsonFile:
                self._config = json.load(jsonFile)
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

        self._type = doctype

    def _getMetadataCollection(self):
        """

        :return: pymongo 'metadata' collection object
        """
        mongoClient = pymongo.MongoClient("mongodb://{username}:{password}@{dbIP}:27017/".format(**self._config))
        mongoDataBase = mongoClient[self._config['dbName']]
        mongoCollection = mongoDataBase['metadata']
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

    def addDocument(self, data):
        """
        Adding a document to the metadata collection

        :param data: The metadata to add
        :return:
        """
        mongoCollection = self._getMetadataCollection()
        mongoCollection.insert_one(data)


class GIS_Collection(AbstractCollection):
    def __init__(self):
        super().__init__(doctype='GIS')


class Experimental_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(doctype='Experimental')


class Numerical_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(doctype='Numerical')


class Analysis_Collection(AbstractCollection):

    def __init__(self):
        super().__init__(doctype='Analysis')

