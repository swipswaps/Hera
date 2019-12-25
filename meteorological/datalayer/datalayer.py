from ..analytics.turbulencecalculator import TurbulenceCalculator
from pyhera import datalayer
import pandas

def getTurbulenceCalculator(projectName, samplingWindow, start=None, end=None, usePandas=None, isMissingData=False, **kwargs):
    """
    This method loads the raw data that corresponds to the requirements (campaign, station, instrument.. ) and
    creates a turbulence calculator with the desirable sampling window.

    :param campaign:        The name of the campaign.
    :param station:         The name of the station.
    :param instrument:      The name of the instrument.
    :param height:          The station height.
    :param startTime:       Datetime of the begin.
    :param endTime:         Datetime of the end.
    :param samplingWindow:  The desirable sampling window (as string).
    :param usePandas:       A flag of whether or not to use pandas.

    :return: A turbulence calculator of the loaded raw data.
    """
    projectData = datalayer.Project[projectName]#self.Metadata(campaign = campaign)

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

    return TurbulenceCalculator(rawData = rawData, metadata = projectData, identifier = identifier, isMissingData=isMissingData)