import pandas
import pymongo

class AbstractDocument(object):
    _metadata = None

    def __init__(self, metadata):
        self._metadata = metadata

    @property
    def type(self):
        return self._metadata['type']

    @property
    def project(self):
        return self._metadata['project']

class GIS_Document(AbstractDocument):
    def __init__(self, metadata):
        super.__init__(metadata=metadata)



class Experimental_Document(AbstractDocument):
    def __init__(self, metadata):
        super.__init__(metadata=metadata)
    
    def getStations(self):
        """

        :return: Stations pandas
        """
        return pandas.DataFrame(self._metadata['stations'])

    def getDevices(self):
        """

        :return: Devices pandas
        """
        return pandas.DataFrame(self._metadata['devices'])


class Numerical_Document(AbstractDocument):
    def __init__(self, metadata):
        super.__init__(metadata=metadata)

class Analysis_Document(AbstractDocument):
    def __init__(self, metadata):
        super.__init__(metadata=metadata)
