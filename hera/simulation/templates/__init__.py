

class Template():
    _document = None

    def __init__(self, document):
        self._document = document
        pass

    @property
    def params(self):
        return self._document['desc']['params']

    @property
    def version(self):
        return self._document['desc']['version']

    def run(self, saveDir, toDB=False):
        pass