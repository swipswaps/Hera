import geopandas
import shapely

class dataManipulations():

    def PolygonDataFrameIntersection(self, dataframe, polygon):
        """
        Creates a new dataframe based on the intersection of a dataframe and a polygon.
        Parameters:
        ----------
        dataframe: A geopandas dataframe.
        polygon: A shapely polygon

        Returns: A new geopandas dataframe

        """

        newlines = []
        for line in dataframe["geometry"]:
            newline = polygon.intersection(line)
            newlines.append(newline)
        dataframe["geometry"] = newlines
        dataframe = dataframe[~dataframe["geometry"].is_empty]

        return dataframe