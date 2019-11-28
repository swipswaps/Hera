import pandas
import os
import json
import pymongo

class DataLayer(object):
    _type = None
    _project = None
    _resource = None
    _config = None
    _metadata = None

    def __init__(self, type, project):
        self._type = type
        self._project = project

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
                json.dump(configData, jsonFile, indent = 4, sort_keys = True)

            errorMessage = "The config file doesn't exist in the default directory.\n" \
                           "A default config data file named '{}' was created. Please fill it and try again.".format(configFile)

            raise IOError(errorMessage)

        self._metadata = self._getMetaData()

    def _getMetaData(self):
        """

        :return: Metadata json
        """
        mongoClient = pymongo.MongoClient("mongodb://{username}:{password}@{dbIP}:27017/".format(self._config))
        mongoDataBase = mongoClient[self._metaData['dbName']]
        mongoCollection = mongoDataBase['metadata']

        queryDict = dict(type=self._type,
                         project=self._project
                         )

        metadata = mongoCollection.find_one(queryDict)

        return metadata

class DataLayer_LongCampaign(DataLayer):
    def __init__(self, project, resource):
        super.__init__('LongCampaign', project, resource)

    def getStations(self):
        """

        :return: Stations pandas
        """
        return pandas.DataFrame(self._metadata['stations'])

    def getDevices(self):
        """

        :return: Devices pandas
        """
        return pandas.DataFrame(self._metadata['devices'])

    def getData(self, start, end):
        pass


