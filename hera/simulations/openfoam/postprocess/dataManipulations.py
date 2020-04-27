import numpy
import wrf

def addBase(data, base_data):
    """
    Adding the base height for points on a slice.

    Parameters
    ----------
    data: the data of the inletMesh patch of the slice
    base_data: the data of the terrain patch of the slice

    Returns the data with the base
    -------

    """

    data["base"] = [numpy.nan for x in range(len(data))]

    for i in range(len(base_data)):
        base = base_data.loc[i]["z"]
        x = base_data.loc[i]["x"]
        y = base_data.loc[i]["y"]
        if data.loc[data["x"] == x].loc[data["y"] == y].empty:
            pass
        else:
            index = list(data.loc[data["x"] == x].loc[data["y"] == y].index)[0]
            data.at[index, "base"] = base
    data["distance"] = numpy.sqrt(data["x"] * data["x"] + data["y"] * data["y"])
    data = data.set_index("distance").interpolate(method='index').reset_index().dropna()
    return data



long = wrf.getvar(data, "lon")
vals = []

for i in range(129):
    if request > float(long.isel(west_east=i).min())  and request > float(long.isel(west_east=1).max()):
        vals.append(i)

for i in range(1):
    ht = wrf.getvar(data, "z", units="m", timeidx=i)
    U = wrf.getvar(data, "U", timeidx=i)
    V = wrf.getvar(data, "V", timeidx=i)
    ter = wrf.getvar(data, "ter", timeidx=i)
    for j in range(20):
        for val in vals:
            new_d = pandas.DataFrame(dict(height=ht.isel(bottom_top=j,west_east=val),
                                          U = U.isel(bottom_top=j,west_east_stag=val),
                                          V = V.isel(bottom_top=j,west_east=val)[:126],
                                          bottom_top = j,
                                          Time=i,
                                          val=val,
                                          terrain = ter.isel(west_east=val),
                                          LAT = data2.isel(bottom_top=j,west_east=val, west_east_stag=val, Time=i).XLAT,
                                          LONG = data2.isel(bottom_top=j,west_east=val, west_east_stag=val, Time=i).XLONG))
            d = pandas.concat([d, new_d])

gdf = geopandas.GeoDataFrame(d, geometry=geopandas.points_from_xy(d.LONG, d.LAT))
gdf.crs = 4326
gdf = gdf.to_crs(epsg=2039)
gdf["LAT"] = gdf.geometry.y
gdf["LONG"] = gdf.geometry.x
gdf["height_over_terrain"] = gdf.height - gdf.terrain