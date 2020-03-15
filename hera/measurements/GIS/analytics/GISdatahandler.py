from ..datalayer.datalayer import GIS_datalayer

class GIS_datahandler():

    _datalayer = None
    _projectName = None

    def __init__(self, projectName):

        self._projectName = projectName
        self._datalayer = GIS_datalayer(self._projectName)

    def getGISData(self, points=None, CutName=None, **kwargs):

        if points==None and CutName==None:
            check = self._datalayer.check_data(**kwargs)
        elif points==None and CutName!=None:
            check = self._datalayer.check_data(CutName=CutName, **kwargs)
        elif CutName==None and points!=None:
            check = self._datalayer.check_data(points=points, **kwargs)
        else:
            check = self._datalayer.check_data(points=points, CutName=CutName, **kwargs)

        if check:
            print(kwargs)
            data = self._datalayer.getData(**kwargs)
        else:
            if points == None or CutName == None:
                raise KeyError("Could not find data. Please insert points and CutName for making new data.")
            else:
                self._datalayer.makeData(points=points, CutName=CutName, additional_data=kwargs)
                data = self._datalayer.getData(points=points, CutName=CutName)

        return data

    def getPoly(self):
        return self._datalayer.getFilesPolygonList()
