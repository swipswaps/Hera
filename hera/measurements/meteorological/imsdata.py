import pandas
import glob
import numpy
import dask.dataframe




class data_loader():
    """

    This class handles loading IMS data into the database

    """
    def __init__(self,np_factor=1000000):
        self._np_factor=np_factor
        self._HebRenameDict={"שם תחנה":'Station_name',
                             "תאריך":"Date",
                             "שעה- LST":"Time_(LST)",
                             "טמפרטורה(C°)":"Temperature_(°C)",
                             "טמפרטורת מקסימום(C°)":"Maximum_Temperature_(°C)",
                             "טמפרטורת מינימום(C°)":"Minimum_Temperature_(°C)",
                             "טמפרטורה ליד הקרקע(C°)":"Ground_Temperature_(°C)",
                             "לחות יחסית(%)":"Relative_humidity_(%)",
                             "לחץ בגובה התחנה(hPa)":"Pressure_at_station_height_(hPa)",
                             "קרינה גלובלית(W/m²)":"Global_radiation_(W/m²)",
                             "קרינה ישירה(W/m²)":"Direct Radiation_(W/m²)",
                             "קרינה מפוזרת(W/m²)":"scattered radiation_(W/m²)",
                             'כמות גשם(מ"מ)':"Rain_(mm)",
                             "מהירות הרוח(m/s)":"wind_speed_(m/s)",
                             "כיוון הרוח(מעלות)":"wind_direction_(deg)",
                             "סטיית התקן של כיוון הרוח(מעלות)":"wind_direction_std_(deg)",
                             "מהירות המשב העליון(m/s)":"upper_gust_(m/s)",
                             "כיוון המשב העליון(מעלות)":"upper_gust_direction_(deg)",
                             'מהירות רוח דקתית מקסימלית(m/s)':"maximum_wind_1minute(m/s)",
                             "מהירות רוח 10 דקתית מקסימלית(m/s)":"maximum_wind_10minute(m/s)",
                             "זמן סיום 10 הדקות המקסימליות()":"maximum_wind_10minute_time"

                            }



    def LoadData(self,url,M):
        """

        :param url: the path to data
        :param M: json/dict with additional metadata
        :return:
        """
        pass

    def _getFromDir(self,path):
        """

        this function converts json/csv data into dask

        :param path: the path to data (json/csv)
        :return: tmpdata: dask pandas
        """

        # path = r'C:\DRO\DCL_rawdata_files'  # use your path
        all_files = glob.glob(path + "/*.csv")

        L = []

        for filename in all_files:
            df = pandas.read_csv(filename,encoding='iso8859_8', index_col=None, header=0)
            L.append(df)

        tmppandas = pandas.concat(L, axis=0, ignore_index=True)
        npfactor=10000000
        npartitions=len(tmppandas)//self._np_factor+1
        tmpdata = dask.dataframe.from_pandas(tmppandas, npartitions=npartitions)
        return tmpdata


    def _CreateMD(self,daskdata,stn_mame,M):
        pass

    def _getFromWeb(self):
        """
        This function (to be implemented) load data from web and converts it to pandas
        :return:
        """
        pass


class datagetter():
    """

    This class handles the IMS data reading

    """

    pass