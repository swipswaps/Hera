from mongoengine import *
import json
from ..datahandler import getHandler


class MetadataFrame(object):
    projectName = StringField(required=True)
    desc = DictField(required=True)
    type = StringField(required=True)
    resource = DynamicField(required=True)
    dataFormat = StringField(required=True)

    def asDict(self):
        docDict = json.loads(self.to_json())
        docDict.pop('_id')
        # docDict.pop('_cls')
        return docDict

    def getData(self, **kwargs):
            return getHandler(self.dataFormat).getData(self.resource, **kwargs)
