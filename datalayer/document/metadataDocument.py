from mongoengine import *
import json
from ..document import connectToDatabase
import pydoc
import pandas

mongoConfig = connectToDatabase()

class Metadata(DynamicDocument):
    projectName = StringField(required=True)
    desc = DictField(required=True)
    meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
            'allow_inheritance': True}

    @property
    def data(self):
        return json.loads(self.to_json())

# ---------------- Data Documents ------------------------

class DataMetadata(Metadata):
    # type = StringField(required=True)
    resource = StringField(required=True)
    fileFormat = StringField(required=True)
    meta = {'allow_inheritance': True}

    def getData(self):
        return pydoc.locate('pyhera.datalayer.datahandler.DataHandler_%s' % self.fileFormat).getData(self.resource)

class GISMetadata(DataMetadata):
    pass

class ExperimentalMetadata(DataMetadata):
    pass

class NumericalMetadata(DataMetadata):
    pass

class AnalysisMetadata(DataMetadata):
    pass

# ---------------- Project Documents ---------------------

class ProjectMetadata(Metadata):
    def getPandas(self, key):
        return pandas.DataFrame(self.desc[key])

    def getDescKeys(self):
        return list(self.desc.keys())