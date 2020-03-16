from ..datalayer.datalayer import GIS_datalayer
from shapely import geometry
import matplotlib.pyplot as plt

class GIS_datahandler():

    _datalayer = None
    _FilesDirectory = None

    def __init__(self, projectName, FilesDirectory):

        self._FilesDirectory = FilesDirectory
        self._projectName = projectName
        self._datalayer = GIS_datalayer(self._projectName, self._FilesDirectory)

    def getGISData(self, points=None, CutName=None, mode="Contour", GeometryMode="contains", Geometry=None, **kwargs):

        if Geometry is not None:
            if type(Geometry)==str:
                try:
                    Geometry = self._datalayer.getGeometry(Geometry)
                except IndexError:
                    raise IndexError("Geometry isn't defined.")
            containPoints = []
            points = self._datalayer.getFilesPointList(**kwargs)
            polygons = self._datalayer.getFilesPolygonList(**kwargs)
            for i in range(len(points)):
                if GeometryMode == "contains":
                    if polygons[i].contains(Geometry):
                        containPoints.append(points[i])
                elif GeometryMode == "intersects":
                    if polygons[i].intersects(Geometry):
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

    def addGeometry(self, Geometry, name):

        check = self._datalayer.check_data(name=name)
        KeyErrorText = "Geometry must be given as one of the following structurs.\n" \
                        "Shapely polygon or point, point coordinates ([x,y]), list of point coordinates ([[x1,y1],[x2,y2],...]),\n" \
                        "list of x coordinates and y coordinates ([[x1,x2,...],[y1,y2,...]])"
        if check:
            raise KeyError("Name is already used.")
        else:
            if type(Geometry)==geometry.polygon.Polygon:
                geopoints = list(zip(*Geometry.exterior.coords.xy))
                geometry_type = "Polygon"
            elif type(Geometry)==geometry.point.Point:
                geopoints = list(Geometry.coords)
                geometry_type = "Point"
            elif type(Geometry)==list:
                if type(Geometry[0])==list:
                    if len(Geometry)>=3:
                        for geo in Geometry:
                            if len(geo)!=2:
                                raise KeyError(KeyErrorText)
                        geometry_type = "Polygon"
                        geopoints = Geometry
                    elif len(Geometry)==2:
                        if len(Geometry[0])==len(Geometry[1])>=3:
                            geometry_type = "Polygon"
                            geopoints=[]
                            for i in range(len(Geometry[0])):
                                geopoints.append([Geometry[0][i], Geometry[1][i]])
                        else:
                            raise KeyError(KeyErrorText)
                    else:
                        raise KeyError(KeyErrorText)
                else:
                    if len(Geometry)!=2:
                        raise KeyError(KeyErrorText)
                    geometry_type = "Point"
                    geopoints = [Geometry]
            else:
                raise KeyError(KeyErrorText)
            self._datalayer.addGeometry(geometry=geopoints, name=name, geometry_type=geometry_type)

    def getGeometry(self, name):

        return self._datalayer.getGeometry(name)

    def getGeometryPoints(self, name):

        return self._datalayer.getGeometryPoints(name)

    def plotGeometry(self, names, color="black", marker="*", ax=None):

        if ax is None:
            fig, ax = plt.subplots(1,1)
        else:
            plt.sca(ax)
        if type(names) == list:
            for name in names:
                self.plotSingleGeometry(name, color, marker, ax)
        else:
            self.plotSingleGeometry(names, color, marker, ax)

        return ax

    def plotSingleGeometry(self, name, color, marker, ax):
        geo, geometry_type = self._datalayer.getGeometryPoints(name)
        if geometry_type == "Point":
            plt.scatter(*geo[0], color=color, marker=marker)
        elif geometry_type == "Polygon":
            geo = self._datalayer.getGeometry(name)
            x, y = geo.exterior.xy
            ax = plt.plot(x, y, color=color)
        return ax