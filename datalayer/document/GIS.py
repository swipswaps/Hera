from .abstractDocument import AbstractDocument

class AbstractGISDocument(AbstractDocument):

    @staticmethod
    def getDocument(cls,documentJSON):
        return GISDocument(documentJSON)

    def __init__(self, data):
        super().__init__(data=data)


class GISDocument(AbstractGISDocument):
    def __init__(self,data):
        super().__init__(data=data)