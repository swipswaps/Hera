from .DataLayer import DataLayerLSM
from .. import abstract_getTemplates


datalayer = DataLayerLSM()


def getTemplates(**kwargs):
    return abstract_getTemplates('LSM', **kwargs)