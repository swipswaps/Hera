import os
import xarray
import numpy
from unum.units import *

from ..utils import toUnum,toNumber

rescaleD = lambda D, data, time_units, q_units: toNumber(toUnum(D, 1 * kg) * min / m ** 3,
                                                         q_units * time_units / m ** 3) * data
rescaleC = lambda C, data, q_units: toNumber(toUnum(C, 1 * kg) / m ** 3, q_units / m ** 3) * data

# def toVTK(self, data, outputdir, name, fields):
#     from pyevtk.hl import gridToVTK
#
#     for ii, tt in enumerate(data.datetime):
#         curdata = data.sel(datetime=tt)
#         outputpath = os.path.join(outputdir, "%s_%s" % (name, ii))
#         fieldsmap = dict([(key, curdata[key].values) for key in fields])
#
#         gridToVTK(outputpath, \
#                   curdata.x.values, \
#                   curdata.y.values, \
#                   curdata.z.values, \
#                   pointData=fieldsmap)

class SingleSimulation(object):
    _finalxarray = None

    def __init__(self, resource):
        if type(resource) is str:
            # pattern = os.path.join(resource, '*.nc')
            #
            # filenameList = []
            # times = []
            # for infilename in glob.glob(pattern):
            #     filenameList.append(infilename)
            #     timepart = float(".".join(infilename.split(".")[0].split("_")[-2:]))
            #     times.append(timepart)
            #
            # # Sort according to time.
            # combined = sorted([x for x in zip(filenameList, times)], key=lambda x: x[1])
            #
            # self._xray = xarray.open_mfdataset([x[0] for x in combined])
            self._finalxarray = xarray.open_mfdataset(os.path.join(resource, '*.nc'))
        else:
            self._finalxarray = resource.getData()
            if type(self._finalxarray) is str:
                self._finalxarray = xarray.open_mfdataset(self._finalxarray)

    def getDosage(self, Q=1 * kg, time_units=min, q_units=mg):
        """
        Calculates the dosage

        Parameters
        ----------
        Q : unum.units
            Default value is 1*kg

        time_units: unum.units
            Default value is min

        q_units: unum.units
            Default value is mg

        Returns
        -------
        self._finalxarray: xarray
            The calculated dosage in 'Dosage' key
        """
        dt_minutes = (self._finalxarray.datetime.diff('datetime')[0].values / numpy.timedelta64(1, 'm')) * min

        self._finalxarray.attrs['dt'] = dt_minutes.asUnit(time_units)
        self._finalxarray.attrs['Q'] = Q.asUnit(q_units)
        self._finalxarray['Dosage'] = rescaleD(Q * (min).asNumber(time_units), self._finalxarray['Dosage'], time_units,
                                               q_units)

        return self._finalxarray

    def getConcentration(self, Q=1*kg, time_units=min, q_units=mg):
        """
        Calculates the concentration

        Parameters
        ----------
        Q : unum.units
            Default value is 1*kg

        time_units: unum.units
            Default value is min

        q_units: unum.units
            Default value is mg

        Returns
        -------
        dDosage: xarray
            The calculated concentration in 'C' key
        """
        if 'dt' not in self._finalxarray.attrs.keys():
            self.getDosage(Q=Q, time_units=time_units, q_units=q_units)

        dDosage = self._finalxarray['Dosage'].diff('datetime').to_dataset().rename({'Dosage': 'dDosage'})
        dDosage['C'] = dDosage['dDosage'] / self._finalxarray.attrs['dt'].asNumber()

        return dDosage