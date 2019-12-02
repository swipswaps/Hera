from .abstractDocument import AbstractDocument

class AbstractNumericalDocument(AbstractDocument):

    @staticmethod
    def getDocument(cls,documentJSON):
        return NumericalDocument(documentJSON)

    def __init__(self,data):
        super().__init__(data=data)

class NumericalDocument(AbstractNumericalDocument):
    def __init__(self,data):
        super().__init__(data=data)