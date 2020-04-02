import pandas
import glob
import numpy
import dask.dataframe as dd
from ... import datalayer
import os
import shutil



class DataLoader(object):

    ## to do: add None for all properties from init
    """

    This class handles loading IMS data into the database

    """
    def __init__(self, np_size=None):
        """
            Initializes the object.
            sets the map fot the hebrew fields.



        :param np_size: the number of partitions to create.
                          if None, take 1000000.
        """
        self._np_size=np_size if np_size is not None else "100Mb"
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
        self._hebStnRenameDict={'בית דגן                                           ':"Bet_Dagan"

                               }
        self._removelist = ['BET DAGAN RAD', 'SEDE BOQER UNI', 'BEER SHEVA UNI']



    def _process_HebName(self, Station):
        HebName = Station.Stn_name_Heb.item()
        return HebName

    def _process_ITM_E(self, Station):
        ITM_E = Station.ITM_E.item()
        return ITM_E

    def _process_ITM_N(self, Station):
        ITM_N = Station.ITM_N.item()
        return ITM_N

    def _process_LAT_deg(self, Station):
        LAT_deg = float(Station.Lat_deg.item()[:-1])
        return LAT_deg

    def _process_LON_deg(self, Station):
        LON_deg = float(Station.Lon_deg.item()[:-1])
        return LON_deg

    def _process_MASL(self, Station):
        MASL = float(Station.MASL.item().replace("~", "")) if not Station.MASL.size == 0 else None
        return MASL

    def _process_Station_Open_date(self, Station):
        Station_Open_date = pandas.to_datetime(Station.Open_Date.item())
        return Station_Open_date

    def _process_Rain_instrument(self, Station):
        Rain_instrument = True if "גשם" in Station.vars.item() else False
        return Rain_instrument

    def _process_Temperature_instrument(self, Station):
        Temperature_instrument = True if "טמפ'" in Station.vars.item() else False
        return Temperature_instrument

    def _process_Wind_instrument(self, Station):
        Wind_instrument = True if "רוח" in Station.vars.item() else False
        return Wind_instrument

    def _process_Humidity_instrument(self, Station):
        Humidity_instrument = True if "לחות" in Station.vars.item() else False
        return Humidity_instrument

    def _process_Pressure_instrument(self, Station):
        Pressure_instrument = True if "לחץ" in Station.vars.item() else False
        return Pressure_instrument

    def _process_Radiation_instrument(self, Station):
        Radiation_instrument = True if "קרינה" in Station.vars.item() else False
        return Radiation_instrument

    def _process_Screen_Model(self,Station):
        Screen_Model=Station.Screen_Model.item()
        return Screen_Model

    def _process_InstLoc_AnemometeLoc(self,Station):
        InstLoc_AnemometeLoc=Station.Instruments_loc_and_Anemometer_loc.item()
        return InstLoc_AnemometeLoc

    def _process_Anemometer_h(self,Station):
        Anemometer_h=Station.Anemometer_height_m.item()
        return Anemometer_h

    def _process_comments(self,Station):
        comments=Station.comments.item()
        return comments






    def loadDirectory(self,path, metadata=None,metadatafile=None,ParquetOutDir=None,**desc):
        """
        This function loads data from directory to the database

        :param path: the path to the 'raw' files
        :param metadata: metadata dict from user
        :param metadatafile: path to metadata file which will be processed
        :param ParquetOutDir: the directory in which the parquet files will be saved
        :param desc:
        :return:
        """


        fileformat='json'
        all_files = glob.glob(os.path.join(path,"*"+fileformat))

        L = []

        for filename in all_files:
            df = pandas.read_json(filename)
            L.append(df)

        tmppandas = pandas.concat(L, axis=0, ignore_index=True)

        ##################################################
        ##'temp solution: removing stations with issues'##
        ##################################################

        removelist=['BET DAGAN RAD','SEDE BOQER UNI','BEER SHEVA UNI']
        stations = [x for x in tmppandas['stn_name'].unique() if x not in removelist]
        tmppandas_q = tmppandas.query('stn_name in @stations')


        npartitions= len(tmppandas) // self._np_size + 1
        loaded_dask = dd.from_pandas(tmppandas_q, npartitions=npartitions)
        return loaded_dask

        vals = {}
        if metadatafile:


            F=['HebName','ITM_E','ITM_N','LAT_deg','LON_deg','MASL','Station_Open_date','Rain','Temperature','Wind',\
               'Humidity','Pressure','Radiation','Screen_Model','InstLoc_AnemometeLoc','Anemometer_h','comments']

            MD = pandas.read_csv(metadatafile,delimiter="\t",names=["Serial_Num","ENVISTA_ID","Stn_name_Heb",\
                                                                  "Stn_name_Eng","ITM_E","ITM_N","Lon_deg",\
                                                                  "Lat_deg","MASL","Open_Date","vars","Screen_Model",\
                                                                  "Instruments_loc_and_Anemometer_loc","Anemometer_height_m","comments"])

            for stnname,data in tmppandas_q.groupby("stn_name"):
                stnname = "".join(filter(lambda x: not x.isdigit(), stnname)).strip()
                Station=MD.query("Stn_name_Eng==@stnname")


                for x in F:
                    updator = getattr(self, "_process_%s" % x)
                    vals[x]=updator(Station)



        return vals

    def LoadData(self, newdata_path, outputpath, Projectname, metadatafile=None, type='meteorological', DataSource='IMS', station_column='stn_name', time_coloumn='time_obs', **metadata):
        """
        This function:
        - Loads the new data to dask
        - Adds it to the old data (if exists) by station name
        - Saves parquet files to the request location
        - Add a description to the metadata

        :param newdata_path: the path to the new data. in future might also be a web address.
        :param outputpath: Destination folder path for saving files
        :param Projectname: The project to which the data is associated. Will be saved in Matadata
        :param metadatafile: The path to a metadata file, if exist
        :param type: the data type. set by default to 'meteorological'
        :param DataSource: The source of the data. Will be saved to metadata. set by default to 'IMS'
        :param station_column: The name of the 'Station Name' column, for the groupby method. set by default to 'stn_name'
        :param time_column: The name of the Time column for indexing. set by default to 'time_obs'
        :param metadata: These parameters will be added to the metadata desc.
        :return:
        """

        metadata.update({'DataSource':DataSource})
        # 1- load the data

        loaded_dask,stations=self.getFromDir(newdata_path, time_coloumn)


        groupby_data=loaded_dask.groupby(station_column)
        for stnname in stations:
            stn_dask=groupby_data.get_group(stnname)

            filtered_stnname = "".join(filter(lambda x: not x.isdigit(), stnname)).strip()

            import pdb
            pdb.set_trace()

            dir_path = os.path.join(outputpath, filtered_stnname).replace(' ','_')
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            # 2- check if station exist in DataBase

            docList = datalayer.Measurements.getDocuments(Projectname,
                                                          type=type,
                                                          DataSource=DataSource,
                                                          StationName=filtered_stnname)

            import pdb
            pdb.set_trace()

            if docList:
                if len(docList)>1:
                    raise ValueError("the list should be at max length of 1. Check your query.")
                else:

                    # get current data from database
                    Data=[docList[0].getData(),stn_dask]
                    new_Data=dd.concat(Data,interleave_partitions=True)\
                                 .reset_index().set_index(time_coloumn)\
                                 .drop_duplicates()\
                                 .repartition(partition_size=self._np_size)

                    import pdb
                    pdb.set_trace()
                    shutil.rmtree(dir_path)

                    # fileformat = 'parquet'
                    # parquet_files = glob.glob(os.path.join(dir_path, "*" + fileformat))
                    # for file in parquet_files:
                        # os.remove(file)

                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)

                    new_Data.to_parquet(dir_path, engine='pyarrow')

            else:

                # create meta data
                desc=self._CreateMD(metadatafile, filtered_stnname, **metadata)

                new_Data=stn_dask.repartition(partition_size=self._np_size)
                new_Data.to_parquet(dir_path, engine='pyarrow')


                datalayer.Measurements.addDocument(projectName=Projectname,
                                                  resource=dir_path,
                                                  dataFormat='parquet',
                                                  type=type,
                                                 desc=desc
                                                  )

    def getFromDir(self,path,time_coloumn):
        """
        This function converts json/csv data into dask
        :param path:
        :param time_coloumn:
        :return:
        """

        """

        this function converts json/csv data into dask

        :param path: the path to data (json/csv)
        :return: tmpdata: dask pandas
        """
        fileformat='json'
        all_files = glob.glob(os.path.join(path,"*"+fileformat))

        L = []

        for filename in all_files:
            df = pandas.read_json(filename)
            L.append(df)

        tmppandas = pandas.concat(L, axis=0, ignore_index=True)
        tmppandas[time_coloumn] = pandas.to_datetime(tmppandas[time_coloumn])
        tmppandas=tmppandas.set_index(time_coloumn)

        ##################################################
        ##'temp solution: removing stations with issues'##
        ##################################################

        stations = [x for x in tmppandas['stn_name'].unique() if x not in self._removelist]
        tmppandas_q = tmppandas.query('stn_name in @stations')


        loaded_dask = dd.from_pandas(tmppandas_q,npartitions=1)
        return loaded_dask,stations

    def _CreateMD(self,metadatafile,stnname,**metadata):

        colums_dict={'BP':'Barometric pressure[hPa]',
                    'DiffR':'Scattered radiation[W/m^2]',
                    'Grad':'Global radiation[W/m^2]',
                    'NIP':'Direct radiation[W/m^2]',
                    'RH':'Relative Humidity[%]',
                    'Rain':'Accumulated rain[mm/10minutes]',
                    'STDwd':'Wind direction standard deviation[degrees]',
                    'TD':'Average temperature in 10 minutes[c]',
                    'TDmax':'Maximum temperature in 10 minutes[c]',
                    'TDmin':'Minimum temperature in 10 minutes[c]',
                    'TG':'Average near ground temperature in 10 minutes[c]',
                    'Time':"End time of maximum 10 minutes wind running average[hhmm], see 'Ws10mm'",
                    'WD':'Wind direction[degrees]',
                    'WDmax':'Wind direction of maximal gust[degrees]',
                    'WS':'Wind speed[m/s]',
                    'WS1mm':'Maximum 1 minute average Wind speed[m/s]',
                    'WSmax':'Maximal gust speed[m/s]',
                    'Ws10mm':"Maximum 10 minutes wind running average[m/s], see 'Time''",}

        colums_dict.update({"StationName":stnname})


        vals = {}

        if metadatafile:


            F=['HebName','ITM_E','ITM_N','LAT_deg','LON_deg','MASL','Station_Open_date','Rain_instrument','Temperature_instrument','Wind_instrument',\
               'Humidity_instrument','Pressure_instrument','Radiation_instrument','Screen_Model','InstLoc_AnemometeLoc','Anemometer_h','comments']

            MD = pandas.read_csv(metadatafile,delimiter="\t",names=["Serial_Num","ENVISTA_ID","Stn_name_Heb",\
                                                                  "Stn_name_Eng","ITM_E","ITM_N","Lon_deg",\
                                                                  "Lat_deg","MASL","Open_Date","vars","Screen_Model",\
                                                                  "Instruments_loc_and_Anemometer_loc","Anemometer_height_m","comments"])


            Station=MD.query("Stn_name_Eng==@stnname")


            for x in F:
                updator = getattr(self, "_process_%s" % x)
                vals[x]=updator(Station)


        vals.update(colums_dict)
        vals.update(metadata)
        return vals


        pass

    def _getFromWeb(self):
        """
        This function (to be implemented) load data from web and converts it to pandas
        :return:
        """
        pass



def getDocFromFile(path,time_coloumn='time_obs',**kwargs):

    """
    Reads the data from file
    :return:
    """
    dl = DataLoader()
    loaded_dask, _ = dl.getFromDir(path, time_coloumn)
    return datalayer.document.metadataDocument.nonDBMetadata(loaded_dask,**kwargs)



def getDocFromDB(projectName,type='meteorological',DataSource='IMS',StationName=None,**kwargs):
    """
    Reads the data from the database
    :return:
    """

    desc={}
    desc.update(kwargs)
    import pdb
    pdb.set_trace()
    if StationName:
        desc.update(StationName=StationName)

    docList = datalayer.Measurements.getDocuments(projectName=projectName,
                                                  DataSource=DataSource,
                                                  type=type,
                                                  **desc
                                                 )
    return docList




