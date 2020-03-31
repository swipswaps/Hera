from ..datalayer.datalayer import GIS_datalayer
from hera import datalayer
import matplotlib.pyplot as plt
import cv2 as cv


class Plotting():

    _Measurments = None
    _projectName = None
    _FilesDirectory = None

    def __init__(self, projectName, FilesDirectory):

        self._FilesDirectory = FilesDirectory
        self._projectName = projectName
        self._Measurments = datalayer.Measurements

    def plotImageLocationFromDocument(self, doc, ax=None):
        """
        Plots an image from a document

        :param doc: The document with the image metadata
        :param ax: The ax to plot to
        :return: ax
        """
        if ax is None:
            fig, ax = plt.subplots()

        path = doc.resource
        extents = [doc.desc['left'], doc.desc['right'], doc.desc['bottom'], doc.desc['top']]
        image = cv.imread(path)

        ax = plt.imshow(image, extent=extents)

        return ax

    def plotImageLocation(self, locationName, ax=None, **query):
        """
        Plots an image of the location called [locationName]

        :param projectName: The projectName
        :param locationName: The location name
        :param query: Some more specific details to query on
        :return:
        """
        doc =  self._Measurments.getDocuments(projectName=self._projectName,
                                                  dataFormat='image',
                                                  type='GIS',
                                                  locationName=locationName,
                                                  **query
                                                  )

        if len(doc) > 1:
            raise ValueError('More than 1 documents fills those requirements')

        doc = doc[0]

        return self.plotImageLocationFromDocument(doc=doc, ax=ax)


    def plotGeometry(self, names, color="black", marker="*", ax=None):
        """
        Plots saved geometry shapes.

        Parameters:
            names: The name/s of the shape/s (string or list of strings) \n
            color: The color of the shape (string) n\
            marker: The marker type for points. (string) \n
            ax: The ax of the plot. \n
            return: ax
        """

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

    def plotSingleGeometry(self, name, color, marker, ax=None):

        if ax is None:
            fig, ax = plt.subplots(1,1)
        else:
            plt.sca(ax)

        d = GIS_datalayer(projectName = self._projectName, FilesDirectory=self._FilesDirectory)

        geo, geometry_type = d.getGeometryPoints(name)
        if geometry_type == "Point":
            plt.scatter(*geo[0], color=color, marker=marker)
        elif geometry_type == "Polygon":
            geo = d.getGeometry(name)
            x, y = geo.exterior.xy
            ax = plt.plot(x, y, color=color)
        return ax

