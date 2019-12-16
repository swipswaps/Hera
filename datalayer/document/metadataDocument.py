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
        return pydoc.locate('pyhera.datalayer.datahandler').getHandler(self.fileFormat).getData(self.resource)

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

    def get(self, key, defaultValue=None):
        if key in self.desc.keys():
            return self._getData(key)
        else:
            return defaultValue

    def __getitem__(self, key):
        fileFormat = self.desc[key]['fileFormat']
        data = self.desc[key]['data']
        return pydoc.locate('pyhera.datalayer.datahandler').getHandler(fileFormat).getData(data)

    def keys(self):
        return list(self.desc.keys())

    def values(self):
        values = []
        for key in self.keys():
            values.append(self[key])
        return values

    def __iter__(self):
        return iter(self.keys())

    def items(self):
        items = []
        for key in self.keys():
            items.append((key, self[key]))
        return items

    def __contains__(self, item):
        return item in self.keys()
