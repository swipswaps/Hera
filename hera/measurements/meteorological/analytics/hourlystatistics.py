from ....analytics.statistics import calcDist2d

def calcHourlyDist(data, Field,bins=30, normalization='density'):
    """

    :param data:
    :param Field:
    :param bins:
    :param normalization:
    :return:
    """


    curdata = data.dropna(subset=[Field])
    y = curdata[Field]
    x = (curdata.index.hour + (curdata.index.minute / 60))

    return calcDist2d(data,x,y,bins=bins,normalization=normalization)

