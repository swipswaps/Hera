from .DataLayer import DataLayerLSM
from ...datalayer import Simulation
from ..templates import LSMTemplate


datalayer = DataLayerLSM()

def getTemplates(**kwargs):
    """

    :param kwargs:
    :return:
    """
    docList = Simulation.getDocuments(projectName='LSM', **kwargs)
    return [LSMTemplate(doc) for doc in docList]