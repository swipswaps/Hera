from .collection import AbstractCollection,GIS_Collection,Measurements_Collection,Simulation_Collection,Analysis_Collection,Project_Collection

GIS = GIS_Collection()
Measurements = Measurements_Collection()
Simulation = Simulation_Collection()
Analysis = Analysis_Collection()
Project = Project_Collection()

All = AbstractCollection()