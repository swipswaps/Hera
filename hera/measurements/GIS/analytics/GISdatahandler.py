from ..datalayer.datalayer import GIS_datalayer

class GIS_datahandler():

    _datalayer = None
    _FilesDirectory = None

    def __init__(self, projectName, FilesDirectory):

        self._FilesDirectory = FilesDirectory
        self._projectName = projectName
        self._datalayer = GIS_datalayer(self._projectName, self._FilesDirectory)

    def getGISData(self, points=None, CutName=None, mode="Contour", GeometryMode="contains", geometry=None, **kwargs):

        if geometry is not None:
            containPoints = []
            points = self._datalayer.getFilesPointList(**kwargs)
            polygons = self._datalayer.getFilesPolygonList(**kwargs)
            for i in range(len(points)):
                if GeometryMode == "contains":
                    if polygons[i].contains(geometry):
                        containPoints.append(points[i])
                elif GeometryMode == "intersects":
                    if polygons[i].intersects(geometry):
                        containPoints.append(points[i])
                else:
                    raise KeyError("GeometryMode incorrectly called. Choose 'contains' or 'intersects'.")
            if 1 == len(containPoints):
                data = self._datalayer.getData(points=containPoints[0], **kwargs)
            else:
                data = []
                for p in containPoints:
                    data.append(self._datalayer.getData(points=p, **kwargs))

        else:
            if points==None and CutName==None:
                check = self._datalayer.check_data(mode=mode, **kwargs)
            elif points==None and CutName!=None:
                check = self._datalayer.check_data(CutName=CutName, mode=mode, **kwargs)
            elif CutName==None and points!=None:
                check = self._datalayer.check_data(points=points, mode=mode, **kwargs)
            else:
                check = self._datalayer.check_data(points=points, CutName=CutName, mode=mode, **kwargs)

            if check:
                data = self._datalayer.getData(mode=mode, **kwargs)
            else:
                if points == None or CutName == None:
                    raise KeyError("Could not find data. Please insert points and CutName for making new data.")
                else:
                    self._datalayer.makeData(points=points, CutName=CutName, mode=mode, additional_data=kwargs)
                    data = self._datalayer.getData(points=points, CutName=CutName, mode=mode)

        return data

    def addGeometry(self, geometry, name, geometry_type="Polygon"):

        check = self._datalayer.check_data(name=name)
        if check:
            raise KeyError("Name is already used.")
        else:
            if geometry_type not in ["Polygon", "Point"]:
                raise KeyError("polygon_type incorrectly called. Choose 'Polygon' or 'Point'.")
            self._datalayer.addGeometry(geometry=geometry, name=name, geometry_type=geometry_type)

    def getGeometry(self, name):

        return self._datalayer.getGeometry(name)