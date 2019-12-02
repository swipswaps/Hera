import pydoc
from .abstractDocument import AbstractDocument
import pandas

class AbstractExperimentalDocument(AbstractDocument):
    @staticmethod
    def getDocument(cls, documentJSON):
        docType = documentJSON['fileFormat']

        return pydoc.locate("ExperimentalDocument_%s" % docType)(documentJSON)

    def __init__(self, data):
        super().__init__(data=data)

    def getPandasByKey(self, key):
        """

        :return: pandas of the relevant key

        TODO:
            - Change to a general desc.

        """
        return pandas.DataFrame(self.data['desc'][key])

    def getDescKeys(self):
        return self.data['desc'].keys()

class ExperimentalDocument_HDF(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)




class ExperimentalDocument_Parquet(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)
