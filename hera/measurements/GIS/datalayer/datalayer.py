from .... import datalayer
import pandas
import os

class GIS_datalayer():

    _Measurments = None
    _projectName = None
    _FilesDirectory = None

    def __init__(self, projectName, FilesDirectory):

        self._projectName = projectName
        self._Measurments = datalayer.Measurements
        self._FilesDirectory = FilesDirectory
        os.system("mkdir -p %s" % DESTDIR)

    def getData(self, **kwargs):

        data = self._Measurments.getData(projectName=self._projectName, **kwargs)

        return data

    def makeData(self, points=None, CutName=None, additional_data=None):

        if additional_data is not None:
            additional_data["CutName"] = CutName
            additional_data["points"] = points
        else:
            additional_data = {"CutName": CutName, "points": points}

        documents = self._Measurments.getDocuments(projectName=self._projectName, points=points)
        if len(documents) == 0:

            FileName = "%s//%s%s-CONTOUR.shp" % (self._FilesDirectory, self._projectName, CutName)

            os.system("ogr2ogr -clipsrc %s %s %s %s %s /mnt/public/New-MAPI-data/BNTL_MALE_ARZI/BNTL_MALE_ARZI/RELIEF/CONTOUR.shp" % (*points, FileName))
            datalayer.Measurements.addDocument(projectName=self._projectName, desc=dict(**additional_data), type="GIS",
                                               resource = FileName, dataFormat = "geopandas")
        else:
            resource = documents[0].asDict()["resource"]
            print(resource)
            datalayer.Measurements.addDocument(projectName=self._projectName, desc=dict(**additional_data), type="GIS",
                                               resource = resource, dataFormat = "geopandas")

    def check_data(self, **kwargs):

        check = self._Measurments.getDocuments(projectName=self._projectName, **kwargs)

        if 0 == len(check):
            result = False
        else:
            result = True

        return result

    def getFilesPolygonList(self, **kwargs):

        documents = self._Measurments.getDocuments(projectName=self._projectName, **kwargs)
        polygons = []
        for document in documents:
            data = document.asDict()
            print(data)
            if "points" in data["desc"].keys():
                if data["desc"]["points"] not in polygons:
                    polygons.append(data["desc"]["points"])

        return polygons

