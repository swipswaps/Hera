import os
import xarray
import json
import numpy
import glob
from unum.units import *

from ..utils import toUnum,toNumber

rescaleD = lambda D, data, time_units, q_units: toNumber(toUnum(D, 1 * kg) * min / m ** 3,
                                                         q_units * time_units / m ** 3) * data
rescaleC = lambda C, data, q_units: toNumber(toUnum(C, 1 * kg) / m ** 3, q_units / m ** 3) * data


class DataLayerLSM(object):
    _Sets = None

    def __init__(self, time_units=min, q_units=mg):

        self.time_units = time_units
        self.q_units = q_units

        conf = os.path.join(os.path.expanduser("~"), ".pynumericalmodels")
        self._Sets = json.load(open(os.path.join(conf, "LSM_datasets.json"), "r"))

    def getRepositories(self):
        return self._Sets.keys()

    def getSets(self, setname):
        return self._Sets[setname]["dataset"].keys()

    def getData(self, repository, dataset, Q=1 * kg):
        """
            Returns the data of the LSM model.

            Rescale  the source to Q if it is not None.

            Change the units of the dosage to mg*min/m**3 from []*s/m**3

        :param repository: The name of the repository.
        :param datasetname: The data set name.
        :param Q: rescaling of the source  (if not None)
        :return: xarray with LSM data
        """
        time_units = self.time_units
        q_units = self.q_units

        set_dir = self._Sets[repository]['path']
        filename = self._Sets[repository]["dataset"][dataset]["file_basename"]

        pattern = os.path.expanduser(os.path.join(set_dir, dataset, "%s*.nc" % filename))

        Q = 1 * kg if Q is None else toUnum(Q, kg)

        filenameList = []
        times = []
        for infilename in glob.glob(pattern):
            filenameList.append(infilename)
            timepart = float(".".join(infilename.split(".")[0].split("_")[-2:]))
            times.append(timepart)

        # Sort according to time.
        combined = sorted([x for x in zip(filenameList, times)], key=lambda x: x[1])

        finalxarray = xarray.open_mfdataset([x[0] for x in combined])

        dt_min = (finalxarray.datetime.diff('datetime')[0].values / numpy.timedelta64(1, 'm')) * min

        finalxarray.attrs['dt'] = dt_min.asUnit(time_units)
        finalxarray['Dosage'] = rescaleD(Q * (min).asNumber(time_units), finalxarray['Dosage'], time_units, q_units)

        return finalxarray

    def getConcentration(self, repository, dataset, Q=None):
        finalxarray = self.getData(repository, dataset, Q)

        dDosage = finalxarray['Dosage'].diff('datetime').to_dataset().rename({'Dosage': 'dDosage'})
        dDosage['C'] = dDosage['dDosage'] / finalxarray.attrs['dt'].asNumber()

        return dDosage

    def toVTK(self, data, outputdir, name, fields):
        from pyevtk.hl import gridToVTK

        for ii, tt in enumerate(data.datetime):
            curdata = data.sel(datetime=tt)
            outputpath = os.path.join(outputdir, "%s_%s" % (name, ii))
            fieldsmap = dict([(key, curdata[key].values) for key in fields])

            gridToVTK(outputpath, \
                      curdata.x.values, \
                      curdata.y.values, \
                      curdata.z.values, \
                      pointData=fieldsmap)
