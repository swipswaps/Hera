from ..analytics.turbulencecalculator import TurbulenceCalculator
from ... import datalayer
import pandas

def getTurbulenceCalculator(projectName, samplingWindow, start=None, end=None, usePandas=None, isMissingData=False, **kwargs):
    """
    This method loads the raw data that corresponds to the requirements (projectName, station, instrument.. ) and
    creates a turbulence calculator with the desirable sampling window.

    :param projectName:     The name of the project.
    :param samplingWindow:  The desirable sampling window (as string).
    :param start:           Datetime of the begin.
    :param end:             Datetime of the end.
    :param usePandas:       A flag of whether or not to use pandas.
    :param isMissingData    A flag if there is a missing data to compute accordingly.

    :return: A turbulence calculator of the loaded raw data.
    """
    projectData = datalayer.Project[projectName]

    if type(start) is str:
        start = pandas.Timestamp(start)

    if type(end) is str:
        end = pandas.Timestamp(end)

    if start is None or end is None:
        start = projectData['start']
        end = projectData['end']

    rawData = datalayer.Experimental.getData(projectName = projectName, usePandas = usePandas, start__lte=end, end__gte=start, **kwargs)[start:end]

    identifier = {'projectName': projectName,
                  'samplingWindow': samplingWindow,
                  'station': None,
                  'instrument': None,
                  'height': None,
                  'start': start,
                  'end': end
                  }
    identifier.update(kwargs)

    if identifier['station'] is not None:
        stationData = projectData['stations'].query("station=='%s'" % identifier['station']).iloc[0]
        identifier['buildingHeight'] = stationData.get('buildingHeight', None)
        identifier['averagedHeight'] = stationData.get('averagedHeight', None)

    return TurbulenceCalculator(rawData = rawData, metadata=projectData, identifier=identifier, isMissingData=isMissingData)