from mongoengine import *
import json
from ..document import connectToDatabase

mongoConfig = connectToDatabase()

class Metadata(DynamicDocument):
    name = StringField(required=True)
    type = StringField(required=True)
    resource = StringField(required=True)

    meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
            'allow_inheritance': True}

    @property
    def data(self):
        return json.loads(self.to_json())


class GISMetadata(Metadata):
    pass

class ExperimentalMetadata(Metadata):
    fileFormat = StringField(required=True)

class NumericalMetadata(Metadata):
    pass

class AnalysisMetadata(Metadata):
    pass



