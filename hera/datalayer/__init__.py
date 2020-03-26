from .document import getDBObject, createDBConnection
from .collection import AbstractCollection,Measurements_Collection,Simulations_Collection,Analysis_Collection
from .projects import getProjectList
from .projects import Projects


Measurements = Measurements_Collection()
Simulations = Simulations_Collection()
Analysis = Analysis_Collection()

All = AbstractCollection()
