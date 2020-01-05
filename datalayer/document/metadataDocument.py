from mongoengine import *
import json
from ..document import connectToDatabase
import pydoc

mongoConfig = connectToDatabase()

class Metadata(DynamicDocument):
    projectName = StringField(required=True)
    desc = DictField(required=True)
    meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
            'allow_inheritance': True}

    def asDict(self):
        docDict = json.loads(self.to_json())
        docDict.pop('_id')
        # docDict.pop('_cls')
        return docDict

# ---------------- Data Documents ------------------------

class RecordMetadata(Metadata):
    # type = StringField(required=True)
    resource = DynamicField(required=True)
    fileFormat = StringField(required=True)
    meta = {'allow_inheritance': True}

    def getData(self, usePandas=None):
        if usePandas is None:
            return pydoc.locate('pyhera.datalayer.datahandler').getHandler(self.fileFormat).getData(self.resource)
        else:
            return pydoc.locate('pyhera.datalayer.datahandler').getHandler(self.fileFormat).getData(self.resource, usePandas)

class GISMetadata(RecordMetadata):
    pass

class MeasurementsMetadata(RecordMetadata):
    pass

class NumericalMetadata(RecordMetadata):
    pass

class AnalysisMetadata(RecordMetadata):
    pass

# ---------------- Project Documents ---------------------

class ProjectMetadata(Metadata):

    @property
    def info(self):
        return self.metadata(self)

    class metadata(object):
        _project = None

        def __init__(self, project):
            if self._project is None:
                self._project = project

        def get(self, key, defaultValue=None):
            if key in self.keys():
                return self[key]
            else:
                return defaultValue

        def __getitem__(self, key):
            fileFormat = self._project.desc[key]['fileFormat']
            data = self._project.desc[key]['data']
            return pydoc.locate('pyhera.datalayer.datahandler').getHandler(fileFormat).getData(data)

        def keys(self):
            return list(self._project.desc.keys())

        def values(self):
            return [self[key] for key in self.keys()]

        def __iter__(self):
            return iter(self.keys())

        def items(self):
            return [(key, self[key]) for key in self.keys()]

        def __contains__(self, item):
            return item in self.keys()