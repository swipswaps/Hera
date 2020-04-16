from .collection import AbstractCollection
import pandas


def getProjectList():
    return list(set(AbstractCollection().getProjectList()))


class Projects(object):
    _projectName = None

    def __init__(self, projectName):
        self._projectName = projectName

    def getMetadata(self):
        """
        Returns a pandas dataframe which contains all the description of all ot the documents in the current project.

        :return: pandas
        """
        descList = [doc.desc for doc in AbstractCollection().getDocuments(projectName=self._projectName)]
        return pandas.DataFrame(descList)