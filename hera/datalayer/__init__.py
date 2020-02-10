from .collection import AbstractCollection,Measurements_Collection,Simulations_Collection,Analysis_Collection,Projects_Collection

Measurements = Measurements_Collection()
Simulations = Simulations_Collection()
Analysis = Analysis_Collection()
Projects = Projects_Collection()

All = AbstractCollection()