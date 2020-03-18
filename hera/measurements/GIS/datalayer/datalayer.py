from .... import datalayer
import pandas
from shapely import geometry
import os

class GIS_datalayer():

    _Measurments = None
    _projectName = None
    _FilesDirectory = None

    def __init__(self, projectName, FilesDirectory):

        self._FilesDirectory = FilesDirectory
        self._projectName = projectName
        self._Measurments = datalayer.Measurements

        os.system("mkdir -p %s" % self._FilesDirectory)

    def getData(self, **kwargs):

        data = self._Measurments.getData(projectName=self._projectName, **kwargs)

        return data

    def makeData(self, points=None, CutName=None, mode="Contour", additional_data=None):

        fullfilesdirect = {"Contour": "RELIEF/CONTOUR.shp", "Buildings": "BUILDINGS/BLDG.shp",
                           "Roads": "TRANSPORTATION/MAIN_ROAD.shp"}

        if additional_data is not None:
            additional_data["CutName"] = CutName
            additional_data["points"] = points
            additional_data["mode"] = mode
        else:
            additional_data = {"CutName": CutName, "points": points, "mode": mode}

        documents = self._Measurments.getDocuments(projectName=self._projectName, points=points, mode=mode)
        if len(documents) == 0:

            FileName = "%s//%s%s-%s.shp" % (self._FilesDirectory, self._projectName, CutName, mode)

            os.system("ogr2ogr -clipsrc %s %s %s %s %s /mnt/public/New-MAPI-data/BNTL_MALE_ARZI/BNTL_MALE_ARZI/%s" % (*points, FileName, fullfilesdirect[mode]))
            datalayer.Measurements.addDocument(projectName=self._projectName, desc=dict(**additional_data), type="GIS",
                                               resource = FileName, dataFormat = "geopandas")
        else:
            resource = documents[0].asDict()["resource"]
            datalayer.Measurements.addDocument(projectName=self._projectName, desc=dict(**additional_data), type="GIS",
                                               resource = resource, dataFormat = "geopandas")

    def check_data(self, **kwargs):

        check = self._Measurments.getDocuments(projectName=self._projectName, **kwargs)

        if 0 == len(check):
            result = False
        else:
            result = True

        return result

    def getFilesPointList(self, **kwargs):

        documents = self._Measurments.getDocuments(projectName=self._projectName, **kwargs)
        points = []
        for document in documents:
            data = document.asDict()
            if "points" in data["desc"].keys():
                if data["desc"]["points"] not in points:
                    points.append(data["desc"]["points"])
        return points

    def getFilesPolygonList(self, **kwargs):

        points = self.getFilesPointList(**kwargs)
        polygons = []
        for coordinates in points:
            polygons.append(geometry.Polygon([[coordinates[0], coordinates[1]], [coordinates[2], coordinates[1]],
                                              [coordinates[2], coordinates[3]], [coordinates[0], coordinates[3]]]))
        return polygons

    def getGeometry(self, name):

        geo, geometry_type = self.getGeometryPoints(name)
        if geometry_type == "Polygon":
            geo = geometry.Polygon(geo)
        elif geometry_type == "Point":
            geo = geometry.Point(geo[0])
        return geo

    def getGeometryPoints(self, name):

        document = self._Measurments.getDocuments(projectName=self._projectName, name=name)
        geo = document[0].asDict()["desc"]["geometry"]
        geometry_type = document[0].asDict()["desc"]["geometry_type"]

        return geo, geometry_type

    def addGeometry(self, geometry, name, geometry_type):

        datalayer.Measurements.addDocument(projectName=self._projectName, desc=dict(geometry=geometry, geometry_type=geometry_type, name=name), type="GeometryShape",
                                           resource="/mnt/public/New-MAPI-data/BNTL_MALE_ARZI/BNTL_MALE_ARZI/RELIEF/CONTOUR.shp", dataFormat="geopandas")

