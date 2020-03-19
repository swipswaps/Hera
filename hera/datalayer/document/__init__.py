from mongoengine import *
import os
import json
import getpass
from .metadataDocument import MetadataFrame, RecordFrame, MeasurementsFrame, AnalysisFrame, SimulationsFrame, ProjectsFrame

dbObjects = {}


def getMongoConfigFromJson(user=None):
    """
    Get the mongoConfig of a user from .pyhera/config.json

    :param user:
    :return:
    """
    configFile = os.path.join(os.environ.get('HOME'), '.pyhera', 'config.json')
    if os.path.isfile(configFile):
        with open(configFile, 'r') as jsonFile:
            mongoConfig = json.load(jsonFile)
            mongoConfig = mongoConfig[getpass.getuser()] if user is None else mongoConfig[user]
    else:
        configData = {getpass.getuser(): dict(username='{username}',
                                              password='{password}',
                                              dbIP='{databaseIP}',
                                              dbName='{databaseName}'
                                              ),
                      'public': dict(username='{username}',
                                     password='{password}',
                                     dbIP='{databaseIP}',
                                     dbName='public'
                                     )
                      }

        if not os.path.isdir(os.path.dirname(configFile)):
            os.makedirs(os.path.dirname(configFile))

        with open(configFile, 'w') as jsonFile:
            json.dump(configData, jsonFile, indent=4, sort_keys=True)

        errorMessage = "The config file doesn't exist in the default directory.\n" \
                       "A default config data file named '{}' was created. Please fill it and try again.".format(
            configFile)

        raise IOError(errorMessage)
    return mongoConfig
    ## build the connection to the db.


def connectToDatabase(mongoConfig):
    """
    Creates a connection to the database according to the mongoConfig.

    :param mongoConfig:
    :return:
    """
    connect(alias='%s-alias' % mongoConfig['dbName'],
            host=mongoConfig['dbIP'],
            db=mongoConfig['dbName'],
            username=mongoConfig['username'],
            password=mongoConfig['password'],
            authentication_source='admin'
            )


def createDBConnection(user, mongoConfig):
    """
    Creates a connection to the database.
    Creates mongoengine objects and saving them to a global dictionary dbObjects.

    :param user:
    :param mongoConfig
    :return:
    """
    dbDict = {}

    connectToDatabase(mongoConfig=mongoConfig)

    dbName = mongoConfig['dbName']

    new_Metadata = type('Metadata', (DynamicDocument, MetadataFrame), {'meta': {'db_alias': '%s-alias' % dbName,
                                                                                'allow_inheritance': True
                                                                                }
                                                                       }
                        )
    dbDict['Metadata'] = new_Metadata

    new_Record = type('Record', (RecordFrame, new_Metadata), {'meta': {'db_alias': '%s-alias'% dbName,
                                                                       'allow_inheritance': True,
                                                                       'auto_create_indexes': True,
                                                                       'indexes': [('geometery', '2dsphere')]}
                                                              }
                      )
    dbDict['Record'] = new_Record

    new_Measurements = type('Measurements', (MeasurementsFrame, new_Record), {})
    dbDict['Measurements'] = new_Measurements

    new_Analysis = type('Analysis', (AnalysisFrame, new_Record), {})
    dbDict['Analysis'] = new_Analysis

    new_Simulations = type('Simulations', (SimulationsFrame, new_Record), {})
    dbDict['Simulations'] = new_Simulations

    new_Projects = type('Projects', (ProjectsFrame ,new_Metadata), {})
    dbDict['Projects'] = new_Projects

    dbObjects[user] = dbDict

    return user, dbDict


# ---------------------default connections--------------------------
for user in [getpass.getuser(), 'public']:
    createDBConnection(user=user,
                       mongoConfig=getMongoConfigFromJson(user=user)
                       )
# -------------------------------------------------------------------


def getDBObject(objectName, user=None):
    """
    Returns the mongoengine object(objectName) of a given user from the global dictionary dbObjects.

    :param objectName:
    :param user:
    :return:
    """
    user = getpass.getuser() if user is None else user
    return dbObjects[user][objectName]

