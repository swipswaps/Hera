from .collection import AbstractCollection
from .collection import Cache_Collection,Measurements_Collection,Simulations_Collection
import pandas
import numpy
import getpass
from functools import partial
from itertools import product
from . import getDBNamesFromJSON


def getProjectList(user=None):
    """
        Return the list with the names of the existing projects .

    :param user: str
        The name of the database.

    :return:
        list.
    """
    return list(set(AbstractCollection(user=user).getProjectList()))



class ProjectMultiDB:
    """
        Provides a simple interface to the data of a specific project.

        The class has all the following functions for the measurements, simulations and Cache:

        -  getMeasurementsDocumentsAsDict"
        -  getMeasurementsDocuments
        -  addMeasurementsDocument
        -  deleteMeasurementsDocuments


        -  getSimulationsDocumentsAsDict"
        -  getSimulationsDocuments
        -  addSimulationsDocument
        -  deleteSimulationsDocuments

        -  getCacheDocumentsAsDict"
        -  getCacheDocuments
        -  addCacheDocument
        -  deleteCacheDocuments

    """
    _projectName = None


    _all = None
    _measurements = None
    _cache     = None
    _simulations  = None
    _users = None
    _useAll = None

    @property
    def measurements(self):
        """
            Access the measurement type documents.

        :return:
            hera.datalayer.collection.Measurements_Collection
        """
        return self._measurements

    @property
    def cache(self):
        """
            Access the Cache type documents.

        :return:
            hera.datalayer.collection.Cache_Collection

        """
        return self._cache

    @property
    def simulations(self):
        """
            Access the simulation type documents.

        :return:
            hera.datalayer.collection.Simulation_Collection

        """
        return self._simulations

    @property
    def users(self):
        return self._users

    @property
    def all(self):
        return self._all

    @users.setter
    def users(self, newUsers):
        self._users = newUsers
        self._measurements  = [Measurements_Collection(user=user) for user in newUsers]
        self._cache         = [Cache_Collection(user=user) for user in newUsers]
        self._simulations   = [Simulations_Collection(user=user) for user in newUsers]
        self._all           = [AbstractCollection(user=user) for user in newUsers]

    @property
    def useAll(self):
        return self._useAll

    @useAll.setter
    def useAll(self,newUseAll):
        self._useAll=newUseAll


    def getProjectName(self,user=None):

        if user is None:
            projectName = self._projectName
        if isinstance(self._projectName,str):
            projectName = self._projectName
        else:
            projectName = self._projectName[user]

        return  projectName


    def __init__(self, projectName,users=None, useAll=False):
        """
            Initialize the project.

        :param projectName: str, dict .
                The name of the project.
                if dict, the project name depends on the database.

        :param user: str
                the name of the database to use. If None, use the default database (the name of the current user).

        """
        self._projectName = projectName
        self._users = numpy.atleast_1d(users)
        self._useAll = useAll
        self._measurements  = dict([(user,Measurements_Collection(user=user)) for user in self._users])
        self._cache         = dict([(user,Cache_Collection(user=user)) for user in self._users])
        self._simulations   = dict([(user,Simulations_Collection(user=user)) for user in self._users])
        self._all           = dict([(user,AbstractCollection(user=user)) for user in self._users])

    def getMetadata(self):
        """
        Returns a pandas dataframe which contains all the description of all ot the documents in the current project.

        :return: pandas
        """
        descList = []
        for userName,allDB in self._all.item():
            projectName = self.getProjectName(userName)
            descList = [doc.desc for doc in self._all.getDocuments(projectName=projectName)]

        return pandas.DataFrame(descList)

    def _getSomeTypeDocumentsAsDict(self, searchtype, with_id, **kwargs):
        returnData = []
        for userName, searched in searchtype.items():
            projectName = self.getProjectName(userName)
            data = searched.getDocumentsAsDict(projectName=projectName, with_id=with_id, **kwargs)
            if len(data["documents"]) != 0:
                if self._useAll:
                    returnData.append(data)
                else:
                    returnData = data
                    break
        return returnData

    def _getSomeTypeDocuments(self, searchtype, resource, dataFormat, type, **desc):
        returnData = []
        for userName, searched in searchtype.items():
            projectName = self.getProjectName(userName)
            data = searched.getDocuments(projectName=projectName, resource=resource, dataFormat=dataFormat,type=type, **desc)
            if len(data) != 0:
                if self._useAll:
                    returnData.append(data)
                else:
                    returnData = data
                    break
        return returnData

    def _addSomeTypeDocuments(self, searchtype, resource, dataFormat, type, users=None, **desc):
        if users is None:
            userName = self._users[0]
            projectName = self.getProjectName(userName)
            searchtype[userName].addDocument(projectName=projectName, resource=resource, dataFormat=dataFormat, type=type, **desc)
        else:
            for user in numpy.atleast_1d(users):
                projectName = self.getProjectName(user)
                searchtype[user].addDocument(projectName=projectName, resource=resource, dataFormat=dataFormat, type=type, **desc)

    def _deleteSomeTypeDocuments(self, searchtype, users=None, **kwargs):
        if users is None:
            userName = self._users[0]
            projectName = self.getProjectName(userName)
            searchtype[userName ].deleteDocuments(projectName=projectName, **kwargs)
        else:
            for user in numpy.atleast_1d(users):
                projectName = self.getProjectName(user)
                searchtype[user].deleteDocuments(projectName=projectName, **kwargs)

    def getMeasurementsDocumentsAsDict(self, with_id=False, **kwargs):
        return self._getSomeTypeDocumentsAsDict(searchtype=self._measurements, with_id=with_id, **kwargs)

    def getMeasurementsDocuments(self, resource=None, dataFormat=None, type=None, **desc):
        return self._getSomeTypeDocuments(searchtype=self._measurements, resource=resource, dataFormat=dataFormat, type=type, **desc)

    def addMeasurementsDocument(self, resource="", dataFormat="string", type="", desc={}, users=None):
        return self._addSomeTypeDocuments(searchtype=self._measurements, resource=resource, dataFormat=dataFormat, type=type, desc=desc, users=users)

    def deleteMeasurementsDocuments(self, users=None, **kwargs):
        return self._deleteSomeTypeDocuments(searchType=self._measurements, users=users, **kwargs)

    def getSimulationsDocumentsAsDict(self, with_id=False, **kwargs):
        return self._getSomeTypeDocumentsAsDict(searchtype=self._simulations, with_id=with_id, **kwargs)

    def getSimulationsDocuments(self, resource=None, dataFormat=None, type=None, **desc):
        return self._getSomeTypeDocuments(searchtype=self._simulations, resource=resource, dataFormat=dataFormat, type=type, **desc)

    def addSimulationsDocument(self, resource="", dataFormat="string", type="", desc={}, users=None):
        return self._addSomeTypeDocuments(searchtype=self._simulations, resource=resource, dataFormat=dataFormat, type=type, desc=desc, users=users)

    def deleteSimulationsDocuments(self, users=None, **kwargs):
        return self._deleteSomeTypeDocuments(searchtype=self._simulations, users=users, **kwargs)

    def getCacheDocumentsAsDict(self,  with_id=False, **kwargs):
        return self._getSomeTypeDocumentsAsDict(searchtype=self._cache, with_id=with_id, **kwargs)

    def getCacheDocuments(self, resource=None, dataFormat=None, type=None, **desc):
        return self._getSomeTypeDocuments(searchtype=self._cache, resource=resource, dataFormat=dataFormat, type=type, **desc)

    def addCacheDocument(self, resource="", dataFormat="string", type="", desc={}, users=None):
        return self._addSomeTypeDocuments(searchtype=self._cache, resource=resource, dataFormat=dataFormat, type=type, desc=desc, users=users)

    def deleteCacheDocuments(self, users=None, **kwargs):
        return self._deleteSomeTypeDocuments(searchtype=self._cache, users=users, **kwargs)


class ProjectMultiDBPublic(ProjectMultiDB):
    """
        A multi-project db, but adds the Public (or public) to the search db list.

        The class accepts the default public project name.

    """
    def __init__(self, projectName,publicProjectName, users=None, useAll=False):
        """
            Initializes the search list of the DB.

            The class is initiated with the default project name for the public DB
            and the list of DB's and project names to look for.

            The public is initiated as the first DB to look in.

        Parameters:
        -----------

         projectName: str, dict
            The project name (if str).
            if dict, the map of project name for a DB.

         publicProjectName: str
                The project name in the public DB.
         users: str, list of str
                The name of the DB to look in (except for public).
                Can be a str or a list.

         useAll: bool
                If true, return a union of all the results from all the DB.

        """

        projectNamesDict = dict()
        dbListNames = []
        if ('public' in getDBNamesFromJSON()):
            dbListNames = ['public']
            projectNamesDict['public'] = publicProjectName
        if ('Public' in getDBNamesFromJSON()):
            dbListNames = ['Public']
            projectNamesDict['Public'] = publicProjectName

        if isinstance(projectName,str):
            for user in numpy.atleast_1d(users):
                projectNamesDict[user] = projectName

        elif isinstance(projectName,dict):
                projectNamesDict.update(projectName)

        if users is None:
            usersList = dbListNames + [getpass.getuser()]
        else:
            usersList = dbListNames + list(numpy.atleast_1d(users))

        super.__init__(projectName,users=usersList, useAll=useAll)