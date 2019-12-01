import os
import json
import pymongo
from .document import Experimental_Document

class AbstractCollection(object):
    _config = None
    _type = None

    def __init__(self, type):
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

        self._type = type

    def _getMetadataCollection(self):
        mongoClient = pymongo.MongoClient("mongodb://{username}:{password}@{dbIP}:27017/".format(self._config))
        mongoDataBase = mongoClient[self._metaData['dbName']]
        mongoCollection = mongoDataBase['metadata']
        return mongoCollection

    def getDocuments(self, query):
        """

        :return: Metadata json
        """
        mongoCollection = self._getMetadataCollection()

        metadataList = mongoCollection.find(query)

        documentList = []
        for metadata in metadataList:
            documentList.append(getattr('%s_Document'%self._type)(metadata))

        return documentList


class GIS_Collection(AbstractCollection):

    def __init__(self):
        super.__init__(type='GIS')


class Experimental_Collection(AbstractCollection):

    def __init__(self):
        super.__init__(type='Experimental')


class Numerical_Collection(AbstractCollection):

    def __init__(self):
        super.__init__(type='Numerical')


class Analysis_Collection(AbstractCollection):

    def __init__(self):
        super.__init__(type='Analysis')

