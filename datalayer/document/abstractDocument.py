import pandas

class AbstractDocument(object):
    _data = None

    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def type(self):
        return self._data['type']

    @property
    def name(self):
        return self._data['name']
