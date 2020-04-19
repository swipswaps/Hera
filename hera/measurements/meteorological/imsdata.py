import pandas
import glob
import numpy
import dask.dataframe as dd
import os
import shutil
import matplotlib.pyplot as plt
import seaborn
from itertools import product
seaborn.set()


from ... import datalayer
from ... utils import andClause
from ... analytics import statistics

from hera.measurements.meteorological.analytics.statistics import calcHourlyDist
from hera.measurements import meteorological

class DataLoader(object):

    ## to do: add None for all properties from init
    """

    This class handles loading IMS data into the database

    """
    def __init__(self, np_size=None):
        """

        :param np_size:
        """

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

        vals = dict()
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

        metadata.update(dict(DataSource=DataSource))
        # 1- load the data

        loaded_dask,stations=self.getFromDir(newdata_path, time_coloumn)


        groupby_data=loaded_dask.groupby(station_column)
        for stnname in stations:
            stn_dask=groupby_data.get_group(stnname)

            filtered_stnname = "".join(filter(lambda x: not x.isdigit(), stnname)).strip()
            print('updating %s data' %filtered_stnname)


            dir_path = os.path.join(outputpath, filtered_stnname).replace(' ','_')
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)

            # 2- check if station exist in DataBase

            docList = datalayer.Measurements.getDocuments(Projectname,
                                                          type=type,
                                                          DataSource=DataSource,
                                                          StationName=filtered_stnname)


            if docList:
                if len(docList)>1:
                    raise ValueError("the list should be at max length of 1. Check your query.")
                else:

                    # get current data from database
                    stn_db=docList[0].getData()
                    Data=[stn_db,stn_dask]
                    new_Data=dd.concat(Data,interleave_partitions=True)\
                                 .reset_index().set_index(time_coloumn)\
                                 .drop_duplicates()\
                                 .repartition(partition_size=self._np_size)

                    # shutil.rmtree(dir_path)

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

        colums_dict=dict(BP='Barometric pressure[hPa]',
                         DiffR='Scattered radiation[W/m^2]',
                         Grad='Global radiation[W/m^2]',
                         NIP='Direct radiation[W/m^2]',
                         RH='Relative Humidity[%]',
                         Rain='Accumulated rain[mm/10minutes]',
                         STDwd='Wind direction standard deviation[degrees]',
                         TD='Average temperature in 10 minutes[c]',
                         TDmax='Maximum temperature in 10 minutes[c]',
                         TDmin='Minimum temperature in 10 minutes[c]',
                         TG='Average near ground temperature in 10 minutes[c]',
                         Time="End time of maximum 10 minutes wind running average[hhmm], see 'Ws10mm'",
                         WD='Wind direction[degrees]',
                         WDmax='Wind direction of maximal gust[degrees]',
                         WS='Wind speed[m/s]',
                         WS1mm='Maximum 1 minute average Wind speed[m/s]',
                         WSmax='Maximal gust speed[m/s]',
                         Ws10mm="Maximum 10 minutes wind running average[m/s], see 'Time''")

        colums_dict.update(dict(StationName=stnname))


        vals = dict()

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

    desc=dict()
    desc.update(kwargs)
    if StationName is not None:
        desc.update(StationName=StationName)

    docList = datalayer.Measurements.getDocuments(projectName=projectName,
                                                  DataSource=DataSource,
                                                  type=type,
                                                  **desc
                                                 )
    return docList


class plots(object):

    pass

class SeasonalPlots(plots):

    def __init__(self):

        self.seasonsdict=dict(Winter=dict(monthes=[12,1,2],strmonthes='[DJF]'),
                              Spring=dict(monthes=[3,4,5],strmonthes='[MAM]'),
                              Summer=dict(monthes=[6,7,8],strmonthes='[JJA]'),
                              Autumn=dict(monthes=[9,10,11],strmonthes='[SOM]')
                              )

    def plotProbContourf_bySeason(self, data, plotField, levels = None, scatter = True, withLabels = True, colorbar=True,
                                  ax=None,scatter_properties = dict(),contour_values = dict(),contour_properties = dict(),
                                  contourf_properties = dict(), labels_properties = dict(), ax_properties = dict(),
                                  normalization = 'max_normalized', figsize=[15, 10]):


        """
        applying plotProbContourf by season. see plotProbContourf for full documentation

        Parameters
        ----------

        data : dask or pandas dataframe
        plotField : the data column name to plot
        levels : list, optional.
        scatter : boolean. default is True
        withLabels : boolean. default is True
        colorbar : boolean. default is True
        ax : optional
        scatter_properties: dict, optional
        contour_values: dict, optional
        contour_properties: dict, optional
        contourf_properties:dict, optional
        labels_properties:dict, optional
        ax_properties:dict, optional
        normalization:string, default 'max_normalized'
        figsize: list, default is [15,10]

        returns
        -------
        ax :
        """

        if ax is None:
            fig, ax = plt.subplots(2,2,figsize=figsize)
        else:
            plt.sca(ax)

        curdata=data
        curdata = curdata.assign(curdate=curdata.index)
        curdata = curdata.assign(monthonly=curdata.curdate.dt.month)

        axPositionList = [x for x in product(range(ax.shape[0]), range(ax.shape[1]))]

        for axPosition, season in zip(axPositionList, self.seasonsdict):
            qstring = 'monthonly in %s' % self.seasonsdict.get(season)['monthes']
            seasondata=curdata.query(qstring)

            CS,CSF,ax_i=meteorological.dailyplots.plotProbContourf(seasondata, plotField, ax=ax[axPosition[0], axPosition[1]],
                                                                   colorbar=False, levels = levels, scatter = scatter, withLabels = withLabels,
                                                                   scatter_properties = scatter_properties, contour_values = contour_values,
                                                                   contour_properties = contour_properties, contourf_properties = contourf_properties,
                                                                   labels_properties = labels_properties,
                                                                   ax_functions_properties= ax_properties, normalization = normalization)

            ax_i.set_title('%s %s' %(season,self.seasonsdict.get(season)['strmonthes']))

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        if colorbar==True:
            plt.colorbar(ax=ax,ticks=CSF.levels)

        return ax



class DailyPlots(object):


    def __init__(self):

        self.contourvalsdict=dict(under_value=0.1,
                                  contourskip=2,
                                  contourfnum=10,
                                  max_value=1.0
                                  )

        self.axfuncdict=dict(set_xlim= [0, 24],
                             set_xticks=[x for x in range(0,25)],
                             set_xticklabels= [str(x) if x % 2 == 0 else "" for x in range(0, 25)],
                             set_xlabel= 'Time [Hours]'
                             )

        self.plotfieldaxfuncdict=dict(WD=dict(set_ylim=[0, 360],
                                              set_yticks=[x for x in range(0, 361, 30)],
                                              set_ylabel= 'Wind Direction [° from N]'
                                              ),
                                      WS=dict(set_ylim=[0,20],
                                          set_ylabel='Wind Speed [m/s]',
                                          set_yticks= [x for x in range(0, 21, 4)],
                                          set_yticklabels= [str(x) if x % 2 == 0 else "" for x in range(0, 21,4)]
                                          ),
                                      RH=dict(set_ylim=[0,100],
                                          set_yticks= [x for x in range(0, 101,5)],
                                          set_yticklabels= [str(x) if x % 10 == 0 else "" for x in range(0, 101,5)],
                                          set_ylabel= 'Relative Humidity [%]')
                                      )


        self.scatterdict=dict(zorder=1,
                              color='k',
                              marker='.',
                              size=0.5,
                              edgecolors='k',
                              alpha=0.55,
                              legend=False
                              )

        self.labelsdict=dict(levels= numpy.round(numpy.linspace(self.contourvalsdict['under_value'],
                                                                self.contourvalsdict['max_value'],
                                                                self.contourvalsdict['contourfnum']), 2)[::self.contourvalsdict['contourskip']],
                             inline=True,
                             fontsize=8,
                             fmt='%1.2f'
                             )


    def _getCountourDict(self, params):

        """
        Make a dictionary with the properties for contour plot
        :param params: parametes from the user
        :return: dict
        """

        return dict(zorder=3,
                    levels=numpy.round(numpy.linspace(params['under_value'],
                                                      params['max_value'],
                                                      params['contourfnum']), 2)[::self.contourvalsdict['contourskip']],
                    linewidths=0.5,
                    colors='k')

    def _getContourfDict(self, params):

        """
        Make a dictionary with the properties for contourf plot

        :param params: parametes from the user
        :return: dict
        """

        return  dict(zorder=2,
                     cmap=self._getcmap('jet',under=True,undercolor='0.9',over=False,overcolor=None,alpha=0.05),
                     levels=numpy.round(numpy.linspace(params['under_value'],
                                                       params['max_value'],
                                                       params['contourfnum']), 2),
                     extend='min'
                     )

    def _getcmap(self,name,under=False,undercolor='0.9',over=False,overcolor='0.9',alpha=0.05):

        """
        Creates a colormap object with under/over range properties

        Parameters
        ----------

        name: string.
        The name of requested colormap (for ex: 'jet')
        under: boolean, default False.
         Whether or not to set a low out-of-range color
        undercolor: String.
         The requested under color name
        over: boolean, default False.
         Whether or not to set a high out-of-range color
        overcolor: String.
         The requested over color name
        alpha: float, default 0.05.
         Out-of-range color transparency

        Returns
        -------
        cmap: colormap object
        """

        cmap=plt.get_cmap(name)
        if under is not False:
            cmap.set_under(color=undercolor,alpha=alpha)
        if over is not False:
            cmap.set_over(color=overcolor, alpha=alpha)

        return cmap


    def plotScatter(self,data,plotField,ax=None,scatter_properties=dict(),ax_functions_properties=dict()):


        """
        Make a scatter plot for selected plotField vs. daily time (in 24 hours)

        Parameters
        ----------
        data : dask or pandas dataframe
        plotField : string.
            The data column name to plot
        ax : Axes object, optional
            Axes object to plot in
        scatter_properties : dict, optional
            dict with parameters to add/update the default dict passed to seaborn.scatterplot
        ax_functions_properties : dict, optional
            A dict with axes functions to add/replace the default functions applied on the ax

        returns
        -------
        ax : The axes object
        """

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        scatter_props = dict(self.scatterdict)
        scatter_props.update(scatter_properties)

        curdata = data.dropna(subset=[plotField])

        curdata = curdata.query("%s > -9990" % plotField)
        curdata = curdata.assign(curdate=curdata.index)
        curdata = curdata.assign(houronly=curdata.curdate.dt.hour + curdata.curdate.dt.minute / 60.)

        ax_func_props = dict(self.plotfieldaxfuncdict.get(plotField, dict()))
        ax_func_props.update(self.axfuncdict)
        ax_func_props.update(ax_functions_properties)


        ax= seaborn.scatterplot(curdata['houronly'], curdata[plotField], ax=ax, **scatter_props)

        for func in ax_func_props:
            getattr(ax, func)(ax_func_props[func])

        return ax


    def plotProbContourf(self, data, plotField, levels=None, scatter=True, withLabels=True, colorbar=True, ax=None, scatter_properties=dict(),
                         contour_values=dict(), contour_properties=dict(), contourf_properties=dict(), labels_properties=dict(),
                         ax_functions_properties=dict(), normalization='max_normalized'):


        """
        Make a probability contour plot for unique values in 24 hours

        Parameters
        ----------

        data: dask or pandas dataframe.
            The data to plot
        plotField : string.
            The data column name to plot
        levels : list, optional.
            Overrides the defaults values of countour and countourf levels
        scatter : boolean. default is True
            Whether or not to add a scatterplot of the data
        withLabels : boolean. default is True
            Whether or not to add a labels to the contour lines
        colorbar : boolean. default is True
            Whether or not to add a colorbar
        ax : Axes object, optional
            Axes object to plot in
        scatter_properties : dict, optional
            dict with parameters to add/update the default dict passed to seaborn.scatterplot
        contour_values : dict, optional
            A dict with parameters to add/update the default contour and contourf *values*
            passed to _getCountourDict and _getCountourfDict
        contour_properties : dict, optional
            A dict with contour parameters to add/replace the default contour parmeters passed to plt.contour
        contourf_properties : dict, optional
            A dict with contourf parameters to add/replace the default contour parmeters passed to plt.contourf
        labels_properties : dict, optional
            A dict with labels parameters to add/replace the default labels parmeters passed to ax.clabel
        ax_functions_properties : dict, optional
            A dict with axes functions to add/replace the default functions applied on the ax
        normalization : string, default 'max_normalized'
            The method of data normalization. see hera.analytics.statistics.calcDist2d for more details

        Returns
        ------

        CS : contour set
        CSF : contourf set
        ax : The axes object
        """

        # Compute histogram #

        x_hist, y_hist, M_hist = calcHourlyDist(data, plotField, normalization=normalization)


        # Read and update contour and contourf properties #

        conrourvals_props=dict(self.contourvalsdict)
        conrourvals_props.update(contour_values)

        contour_props = self._getCountourDict(conrourvals_props)
        contourf_props = self._getContourfDict(conrourvals_props)


        if normalization=='y_normalized':
            contour_props.update(dict(levels= numpy.linspace(conrourvals_props['under_value'],
                                                             numpy.ceil(M_hist.max() * 100) / 100,
                                                             conrourvals_props['contourfnum'])[::conrourvals_props['contourskip']]))


            contourf_props.update(dict(levels=numpy.round(numpy.linspace(conrourvals_props['under_value'],
                                                                         numpy.ceil(M_hist.max() * 100) / 100,
                                                                         conrourvals_props['contourfnum']), 2)))


        contour_props.update(contour_properties)
        contourf_props.update(contourf_properties)

        # Read and update axes properties #

        ax_func_props = dict(self.plotfieldaxfuncdict.get(plotField, dict()))
        ax_func_props.update(self.axfuncdict)
        ax_func_props.update(ax_functions_properties)


        countour_levels = contour_props.pop("levels")
        countourf_levels = contourf_props.pop("levels")

        # Read and update levels and labels properties #

        labels_props = dict(self.labelsdict)
        if normalization == 'y_normalized':
            labels_props.update(dict(levels= numpy.linspace(conrourvals_props['under_value'],
                                                            numpy.ceil(M_hist.max() * 100) / 100,
                                                            conrourvals_props['contourfnum'])[::conrourvals_props['contourskip']]))

        if levels is not None:
            countour_levels = levels
            countourf_levels = levels
            del labels_props['levels']

        # Plot scatter, conrour and contourf #

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        if scatter==True:

            ax = self.plotScatter(data,plotField,ax=ax,scatter_properties=scatter_properties)

        CS = plt.contour(x_hist, y_hist, M_hist,levels=countour_levels,**contour_props)

        if withLabels==True:

            labels_props.update(labels_properties)
            labels_list = labels_props.pop("levels",CS.levels)

            ax.clabel(CS, labels_list,**labels_props)

        CSF= plt.contourf(x_hist, y_hist, M_hist,levels=countourf_levels ,**contourf_props)


        # Apply ax functions from ax_func_props

        for func in ax_func_props:
            getattr(ax, func)(ax_func_props[func])

        if colorbar==True:
            plt.colorbar(ax=ax, ticks=countourf_levels)


        return CS,CSF,ax





