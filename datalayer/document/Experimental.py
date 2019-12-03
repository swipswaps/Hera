import pydoc
from .abstractDocument import AbstractDocument
import pandas

class AbstractExperimentalDocument(AbstractDocument):
    @staticmethod
    def getDocument(documentJSON):
        docType = documentJSON['fileFormat']

        return pydoc.locate("pyhera.datalayer.document.Experimental.ExperimentalDocument_%s" % docType)(documentJSON)

    def __init__(self, data):
        super().__init__(data=data)

    def getPandasByKey(self, key):
        """

        :return: pandas of the relevant key
        """
        return pandas.DataFrame(self.data['desc'][key])

    def getDescKeys(self):
        return list(self.data['desc'].keys())


class ExperimentalDocument_HDF(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)


class ExperimentalDocument_Parquet(AbstractExperimentalDocument):
    def __init__(self, data):
        super().__init__(data=data)
