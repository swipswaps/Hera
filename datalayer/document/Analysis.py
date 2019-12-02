from .abstractDocument import AbstractDocument

class AbstractAnalysisDocument(AbstractDocument):

    @staticmethod
    def getDocument(cls,documentJSON):
        return AnalysisDocument(documentJSON)

    def __init__(self,data):
        super().__init__(data=data)

class AnalysisDocument(AbstractAnalysisDocument):
    def __init__(self,data):
        super().__init__(data=data)