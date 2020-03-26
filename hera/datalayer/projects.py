from .collection import Measurements_Collection
import pandas


def getProjectList():
    return list(set(Measurements_Collection().getProjectList()))


class Projects(object):
    _projectName = None

    def __init__(self, projectName):
        self._projectName = projectName

    def getMetadata(self):
        descList = [doc.desc for doc in Measurements_Collection().getDocuments(projectName=self._projectName)]
        return pandas.DataFrame(descList)