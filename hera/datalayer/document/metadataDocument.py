from mongoengine import *
import json
from ..datahandler import getHandler

# mongoConfig = connectToDatabase()

class MetadataFrame(object):
    projectName = StringField(required=True)
    desc = DictField(required=True)
    # meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
    #         'allow_inheritance': True}

    def asDict(self):
        docDict = json.loads(self.to_json())
        docDict.pop('_id')
        # docDict.pop('_cls')
        return docDict

# ---------------- Data Documents ------------------------

class RecordFrame(object):
    type = StringField(required=True)
    resource = DynamicField(required=True)
    dataFormat = StringField(required=True)
    # meta = {'allow_inheritance': True,
    #         'auto_create_index': True,
    #         'indexes': [('geometry','2dsphere')]}

    def getData(self, **kwargs):
            return getHandler(self.dataFormat).getData(self.resource, **kwargs)

class MeasurementsFrame(object):
    pass

class SimulationsFrame(object):
    pass

class AnalysisFrame(object):
    pass



# ---------------- Project Documents ---------------------

class ProjectsFrame(object):

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
            dataFormat = self._project.desc[key]['dataFormat']
            data = self._project.desc[key]['data']
            return getHandler(dataFormat).getData(data)

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