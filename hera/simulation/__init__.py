from ..datalayer import Simulation
from .templates import Template


def abstract_getTemplates(projectName, **kwargs):
    docList = Simulation.getDocuments(projectName=projectName, **kwargs)
    return [Template(doc) for doc in docList]