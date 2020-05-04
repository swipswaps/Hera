import numpy

def addBase(data, base_data, xdir=True, ydir=True):
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
    if xdir:
        data["x2"] = data["x"] - data['x'].min()
    else:
        data["x2"] = data["x"].max() - data['x']

    if ydir:
        data["y2"] = data["y"] - data['y'].min()
    else:
        data["y2"] = data["y"].max() - data['y']

    data["distance"] = numpy.sqrt(data["x2"] * data["x2"] + data["y2"] * data["y2"])
    data = data.set_index("distance").interpolate(method='index').reset_index().dropna()
    data["height"] = data["z"] - data["base"]
    data["Velocity"] = numpy.sqrt(data["U_x"] * data["U_x"] + data["U_y"] * data["U_y"] + data["U_z"] * data["U_z"])
    return data