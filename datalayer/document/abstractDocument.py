
class AbstractDocument(object):
    _data = None

    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def type(self):
        return self.data['type']

    @property
    def name(self):
        return self.data['name']

    @property
    def resource(self):
        return self.data['resource']

    @property
    def id(self):
        return self.data['_id']