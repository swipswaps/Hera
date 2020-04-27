import numpy
import wrf
import pandas
import geopandas
from netCDF4 import Dataset
import xarray

class wrfDatalayer():

    def getPandas(self, datapath, timelist, lat=None, lon=None, heightLimit=None):

        d = pandas.DataFrame()
        data = Dataset(datapath)
        xdata = xarray.open_dataset(datapath)
        vals = []
        if lat is not None:
            changes = ["south_north", "south_north", "south_north_stag"]
            l = wrf.getvar(data, "lat")
            for i in range(len(xdata.south_north)):
                if lat > float(l.isel(south_north=i).min())-0.01 and lat < float(l.isel(south_north=i).max())+0.01:
                    vals.append(i)
        elif lon is not None:
            changes = ["west_east", "west_east_stag", "west_east"]
            l = wrf.getvar(data, "lon")
            for i in range(len(xdata.west_east)):
                if lon > float(l.isel(west_east=i).min())-0.01 and lon < float(l.isel(west_east=i).max())+0.01:
                    vals.append(i)

        H_vals = []
        if heightLimit is not None:
            for val in vals:
                query_dict = {changes[0]: val}
                ht = wrf.getvar(data, "z").isel(**query_dict)
                for i in range(len(xdata.bottom_top)):
                    if ht.isel(bottom_top=i).min()<heightLimit:
                        H_vals.append(i)

        for i in timelist:
            ht = wrf.getvar(data, "z", units="m", timeidx=i)
            U = wrf.getvar(data, "U", timeidx=i)
            V = wrf.getvar(data, "V", timeidx=i)
            ter = wrf.getvar(data, "ter", timeidx=i)
            time = wrf.getvar(data, "times", timeidx=i)
            for j in range(max(H_vals)):
                for val in vals:
                    query_dict = {changes[0]: val}
                    query_dictU = {changes[1]: val}
                    query_dictV = {changes[2]: val}
                    if lat is not None:
                        new_u = U.isel(bottom_top=j, **query_dictU)[:-1]
                        new_v = V.isel(bottom_top=j, **query_dictV)
                    elif lon is not None:
                        new_u = U.isel(bottom_top=j, **query_dictU)
                        new_v = V.isel(bottom_top=j, **query_dictV)[:-1]
                    new_d = pandas.DataFrame(dict(height=ht.isel(bottom_top=j, **query_dict),
                                                  U=new_u,
                                                  V=new_v,
                                                  Time=time,
                                                  terrain=ter.isel(**query_dict),
                                                  LAT=xdata.isel(bottom_top=j, **query_dict,
                                                                 Time=i).XLAT,
                                                  LONG=xdata.isel(bottom_top=j, **query_dict,
                                                                  Time=i).XLONG))
                    d = pandas.concat([d, new_d])

        gdf = geopandas.GeoDataFrame(d, geometry=geopandas.points_from_xy(d.LONG, d.LAT))
        gdf.crs = 4326
        gdf = gdf.to_crs(epsg=2039)
        gdf["LAT"] = gdf.geometry.y
        gdf["LONG"] = gdf.geometry.x
        gdf["height_over_terrain"] = gdf.height - gdf.terrain

        return gdf