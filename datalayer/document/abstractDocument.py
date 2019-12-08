import json

class AbstractDocument(object):
    _doc = None

    def __init__(self, doc):
        self._doc = doc

    @property
    def data(self):
        return json.loads(self._doc.to_json())

    @property
    def type(self):
        return self._doc.type

    @property
    def name(self):
        return self._doc.name

    @property
    def resource(self):
        return self._doc.resource

    @property
    def id(self):
        return self._doc.id

    def save(self):
        self._doc.save()