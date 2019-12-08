

class Metadata(DynamicDocument):
    name = StringField(required=True)
    type = StringField(required=True)
    resource = StringField(required=True)

    meta = {'db_alias': '%s-alias' % mongoConfig['dbName'],
            'allow_inheritance': True}

