

def calculateFB(data):
    """
    Calculates the fractional mean bias.

    :param data: pandas of the model and raw data together.
    :return: float FB.
    """

    FB = 2*(data['model']-data['measure']).mean()/(data['model'].mean()+data['measure'].mean())
    return FB


def calculateNMSE(data):
    """
    Calculates the normalized mean-square error.

    :param data: pandas of the model and raw data together.
    :return: float NMSE.
    """

    NMSE = ((data['model']-data['measure'])**2).mean()/(data['model'].mean()*data['measure'].mean())
    return NMSE


def calculateFAC2(data):
    """
    Calculates the FAC2 criteria.

    :param data: pandas of the model and raw data together.
    :return: float FAC2.
    """

    model_over_measure = data['model']/data['measure']
    FAC2 = model_over_measure.apply(lambda x: 0.5<x<2).sum()/model_over_measure.count()
    return FAC2


def calculateNAD(data):
    """
    Calculates the normalized absolute difference.

    :param data: pandas of the model and raw data together.
    :return: float NAD.
    """

    NAD = abs((data['model']-data['measure']).mean())/(data['model'].mean()+data['measure'].mean())
    return NAD


def calculateR(data):
    """
    Calculates the linear correlation.

    :param data: pandas of the model and raw data together.
    :return: float R.
    """

    R = ((data['model']-data['model'].mean())*(data['measure']-data['measure'].mean())).mean()/(data['model'].std()*data['height'].std)**0.5
    return R


def calculateFB_FN(data):
    """
    Calculates FB_FN that tell us about under estimate in FB criteria.

    :param data: pandas of the model and raw data together.
    :return: float FB_FN.
    """

    FB_FN = sum(abs(data['model']-data['measure'])+(data['model']-data['measure']))/sum(data['model']+data['measure'])
    return FB_FN


def calculateFB_FP(data):
    """
    Calculates FB_FP that tell us about over estimate in FB criteria.

    :param data: pandas of the model and raw data together.
    :return: float FB_FP.
    """

    FB_FP = sum(abs(data['model']-data['measure'])+(data['measure']-data['model']))/sum(data['model']+data['measure'])
    return FB_FP