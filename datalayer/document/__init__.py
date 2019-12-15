from mongoengine import *
import os
import json


def connectToDatabase(dbName = None):
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

    dbName = mongoConfig['dbName'] if dbName is None else dbName

    ## build the connection to the db.
    connect(alias='%s-alias' % mongoConfig['dbName'],
            db=dbName,
            username=mongoConfig['username'],
            password=mongoConfig['password'],
            authentication_source='admin'
            )

    return mongoConfig