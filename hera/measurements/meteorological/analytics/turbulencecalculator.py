import numpy
import pandas
from scipy.stats import circmean, circstd
from scipy.constants import g
from .abstractcalculator import AbstractCalculator


class TurbulenceCalculator(AbstractCalculator):
    _isMissingData = False

    def __init__(self, rawData, metadata, identifier, isMissingData=False):
        self._isMissingData = isMissingData
        super(TurbulenceCalculator, self).__init__(rawData=rawData, metadata=metadata, identifier=identifier)

    def fluctuations(self, inMemory=None):
        """
        Calculates the mean of u,v,w,T and the fluctuations u',v',w',T'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'up' not in self._RawData.columns:
            avg = self._RawData
            avg = avg if self.SamplingWindow is None else avg.resample(self.SamplingWindow)
            avg = avg.mean().rename(columns={'u': 'u_bar', 'v': 'v_bar', 'w': 'w_bar', 'T': 'T_bar'})

            self._TemporaryData = avg
            self._CalculatedParams += [['u_bar',{}], ['v_bar',{}], ['w_bar',{}], ['T_bar',{}]]
            if self._isMissingData:
                self._RawData = self._RawData.merge(avg, how='outer', left_index=True, right_index=True)
                self._RawData = self._RawData.dropna(how='all')
                self._RawData[['u_bar', 'v_bar', 'w_bar', 'T_bar']] = self._RawData[['u_bar', 'v_bar', 'w_bar', 'T_bar']].ffill()
                self._RawData = self._RawData.dropna(how='any')
            else:
                self._RawData = self._RawData.merge(avg, how='left', left_index=True, right_index=True)
                self._RawData = self._RawData.ffill()

            self._RawData['up'] = self._RawData['u'] - self._RawData['u_bar']
            self._RawData['vp'] = self._RawData['v'] - self._RawData['v_bar']
            self._RawData['wp'] = self._RawData['w'] - self._RawData['w_bar']
            self._RawData['Tp'] = self._RawData['T'] - self._RawData['T_bar']

        return self

    def sigma(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaU' not in self._TemporaryData.columns:
            sigmaU = self._RawData['u'].resample(self.SamplingWindow).std()
            self._TemporaryData['sigmaU'] = sigmaU
            self._CalculatedParams.append(['sigmaU',{}])

            sigmaV = self._RawData['v'].resample(self.SamplingWindow).std()
            self._TemporaryData['sigmaV'] = sigmaV
            self._CalculatedParams.append(['sigmaV',{}])

            sigmaW = self._RawData['w'].resample(self.SamplingWindow).std()
            self._TemporaryData['sigmaW'] = sigmaW
            self._CalculatedParams.append(['sigmaW',{}])

        return self

    def sigmaH(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaH' not in self._TemporaryData.columns:
            self.sigma()
            self.Ustar()
            sigmaH = 0.5*numpy.hypot(self._TemporaryData['sigmaU'], self._TemporaryData['sigmaW'])
            self._TemporaryData['sigmaH'] = sigmaH
            self._CalculatedParams.append(['sigmaH',{}])

        return self

    def sigmaHOverUstar(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaHOverUstar' not in self._TemporaryData.columns:
            self.fluctuations()
            self.sigmaH()
            sigmaHOverUstar = self._TemporaryData['sigmaH']/self._TemporaryData['Ustar']
            self._TemporaryData['sigmaHOverUstar'] = sigmaHOverUstar
            self._CalculatedParams.append(['sigmaHOverUstar',{}])

        return self

    def sigmaWOverUstar(self, inMemory=None):
        """



        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaWOverUstar' not in self._TemporaryData.columns:
            self.fluctuations()
            self.sigma()
            sigmaWOverUstar = self._TemporaryData['sigmaW']/self._TemporaryData['Ustar']
            self._TemporaryData['sigmaWOverUstar'] = sigmaWOverUstar
            self._CalculatedParams.append(['sigmaWOverUstar',{}])

        return self

    def wind_speed(self, inMemory=None):
        """
        Calculates the mean and the std of the horizontal speed.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wind_speed' not in self._RawData.columns:
            self.fluctuations()

            self._RawData = self._RawData.assign(wind_speed=lambda x: numpy.hypot(x['u'], x['v']))

            resampled = self._RawData['wind_speed']
            resampled = resampled if self.SamplingWindow is None else resampled.resample(self.SamplingWindow)

            avg = resampled.mean()
            self._TemporaryData['wind_speed'] = avg
            self._CalculatedParams.append(['wind_speed',{}])

            std = resampled.std()#.rename(columns={'wind_speed':'wind_speed_std'})
            self._TemporaryData['wind_speed_std']=std
            self._CalculatedParams.append(['wind_speed_std',{}])

        return self

    def wind_dir(self, inMemory=None):
        """
        Calculates the mean and the std of the wind direction in mathematical and meteorological form.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wind_dir_mathematical' not in self._RawData.columns:
            self.fluctuations()

            self._RawData = self._RawData.assign(wind_dir_mathematical=lambda x: numpy.arctan2(x['v'], x['u']))
            resampled = self._RawData['wind_dir_mathematical']
            resampled = resampled if self.SamplingWindow is None else resampled.resample(self.SamplingWindow)

            if self._DataType=='pandas':
                avg = resampled.apply(lambda x: circmean(x, high=numpy.pi, low=-numpy.pi))
            else:
                avg = resampled.agg(lambda x: circmean(x, high=numpy.pi, low=-numpy.pi))

            self._TemporaryData['wind_dir_mathematical'] = avg
            self._CalculatedParams.append(['wind_dir_mathematical',{}])

            if self._DataType == 'pandas':
                avg = numpy.rad2deg(avg + numpy.pi)
                self._TemporaryData['wind_dir_meteorological'] = [int(270 - x) if 270 - x >= 0 else int(630 - x) for x in avg.values]
            else:
                self._TemporaryData['wind_dir_meteorological'] = self._TemporaryData['wind_dir_mathematical'].apply(
                    lambda x: numpy.rad2deg(x + numpy.pi), meta=('wind_dir_mathematical', 'float64'))
                self._TemporaryData['wind_dir_meteorological'] = self._TemporaryData['wind_dir_meteorological'].apply(
                    lambda x: int(270 - x) if 270 - x >= 0 else int(630 - x), meta=('wind_dir_meteorological', 'int64'))

            self._CalculatedParams.append(['wind_dir_meteorological',{}])

            if self._DataType == 'pandas':
                std = resampled.apply(lambda x: circstd(x, high=numpy.pi, low=-numpy.pi))
            else:
                std = resampled.agg(lambda x: circstd(x, high=numpy.pi, low=-numpy.pi))
            self._TemporaryData['wind_dir_mathematical_std'] = std
            self._CalculatedParams.append(['wind_dir_mathematical_std',{}])
            std = numpy.rad2deg(std) #numpy.rad2deg(std+numpy.pi)
            self._TemporaryData['wind_dir_meteorological_std'] = std #[int(270-x) if 270-x>=0 else int(630-x) for x in std.values]
            self._CalculatedParams.append(['wind_dir_meteorological_std',{}])

        return self

    def sigmaHOverWindSpeed(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaHOverWindSpeed' not in self._TemporaryData.columns:
            self.fluctuations()
            self.sigmaH()
            sigmaHOverWindSpeed = self._TemporaryData['sigmaH']/self._TemporaryData['wind_speed']
            self._TemporaryData['sigmaHOverWindSpeed'] = sigmaHOverWindSpeed
            self._CalculatedParams.append(['sigmaHOverWindSpeed',{}])

        return self

    def sigmaWOverWindSpeed(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'sigmaWOverWindSpeed' not in self._TemporaryData.columns:
            self.fluctuations()
            self.sigma()
            sigmaWOverWindSpeed = self._TemporaryData['sigmaW']/self._TemporaryData['wind_speed']
            self._TemporaryData['sigmaWOverWindSpeed'] = sigmaWOverWindSpeed
            self._CalculatedParams.append(['sigmaWOverWindSpeed',{}])

        return self

    def w3OverSigmaW3(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'w3OverSigmaW3' not in self._TemporaryData.columns:
            self.w3()
            self.sigma()
            w3OverSigmaW3 = self._TemporaryData['w3']/self._TemporaryData['sigmaW']**3
            self._TemporaryData['w3OverSigmaW3'] = w3OverSigmaW3
            self._CalculatedParams.append(['w3OverSigmaW3',{}])

        return self

    def uStarOverWindSpeed(self, inMemory=None):
        """


        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uStarOverWindSpeed' not in self._TemporaryData.columns:
            self.Ustar().wind_speed()
            uStarOverWindPeed = self._TemporaryData['Ustar']/self._TemporaryData['wind_speed']
            self._TemporaryData['uStarOverWindSpeed'] = uStarOverWindPeed
            self._CalculatedParams.append(['uStarOverWindSpeed',{}])

        return self

    def uu(self, inMemory=None):
        """
        Calculates the mean of u'*u'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uu' not in self._TemporaryData.columns:
            self.fluctuations()
            uu = (self._RawData['up'] * self._RawData['up']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uu'] = uu
            self._CalculatedParams.append(['uu',{}])

        return self

    def vv(self, inMemory=None):
        """
        Calculates the mean of v'*v'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'vv' not in self._TemporaryData.columns:
            self.fluctuations()
            vv = (self._RawData['vp'] * self._RawData['vp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['vv'] = vv
            self._CalculatedParams.append(['vv',{}])

        return self

    def ww(self, inMemory=None):
        """
        Calculates the mean of w'*w'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'ww' not in self._TemporaryData.columns:
            self.fluctuations()
            ww = (self._RawData['wp'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['ww'] = ww
            self._CalculatedParams.append(['ww',{}])

        return self

    def wT(self, inMemory=None):
        """
        Calculates the mean of w'*T'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wT' not in self._TemporaryData.columns:
            self.fluctuations()
            wT = (self._RawData['wp'] * self._RawData['Tp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['wT'] = wT
            self._CalculatedParams.append(['wT',{}])

        return self

    def uv(self, inMemory=None):
        """
        Calculates the mean of u'*v'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uv' not in self._TemporaryData.columns:
            self.fluctuations()
            uv = (self._RawData['up'] * self._RawData['vp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uv'] = uv
            self._CalculatedParams.append(['uv',{}])

        return self

    def uw(self, inMemory=None):
        """
        Calculates the mean of u'*w'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uw' not in self._TemporaryData.columns:
            self.fluctuations()
            uw = (self._RawData['up'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uw'] = uw
            self._CalculatedParams.append(['uw',{}])

        return self

    def vw(self, inMemory=None):
        """
        Calculates the mean of v'*w'.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'vw' not in self._TemporaryData.columns:
            self.fluctuations()
            vw = (self._RawData['vp'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['vw'] = vw
            self._CalculatedParams.append(['vw',{}])

        return self

    def w3(self, inMemory=None):
        """
        Calculates the mean of w'^3.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'w3' not in self._TemporaryData.columns:
            self.fluctuations()
            www = (self._RawData['wp'] ** 3).resample(self.SamplingWindow).mean()
            self._TemporaryData['w3'] = www
            self._CalculatedParams.append(['w3',{}])

        return self

    def w4(self, inMemory=None):
        """
        Calculates the mean of w'^4.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'w4' not in self._TemporaryData.columns:
            self.fluctuations()
            wwww = (self._RawData['wp'] ** 4).resample(self.SamplingWindow).mean()
            self._TemporaryData['w4'] = wwww
            self._CalculatedParams.append(['w4',{}])

        return self

    def TKE(self, inMemory=None):
        """
        Calculates the turbulence kinetic energy.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'TKE' not in self._TemporaryData.columns:
            self.uu().vv().ww()
            TKE = 0.5 * (self._TemporaryData['uu'] + self._TemporaryData['vv'] + self._TemporaryData['ww'])
            self._TemporaryData['TKE'] = TKE
            self._CalculatedParams.append(['TKE',{}])

        return self

    def wTKE(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wTKE' not in self._TemporaryData.columns:
            self.fluctuations()
            uu = self._RawData['up'] ** 2
            vv = self._RawData['vp'] ** 2
            ww = self._RawData['wp'] ** 2
            wp = self._RawData['wp']
            wTKE = (0.5 * (uu + vv + ww) * wp).resample(self.SamplingWindow).mean()
            self._TemporaryData['wTKE'] = wTKE
            self._CalculatedParams.append(['wTKE',{}])

        return self

    def Ustar(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Ustar' not in self._TemporaryData.columns:
            self.uw().vw()
            Ustar = (self._TemporaryData['uw'] ** 2 + self._TemporaryData['vw'] ** 2) ** 0.25
            self._TemporaryData['Ustar'] = Ustar
            self._CalculatedParams.append(['Ustar',{}])

        return self

    def Rvw(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Rvw' not in self._TemporaryData.columns:
            self.vw().vv().ww()
            Rvw = self._TemporaryData['vw'] / numpy.sqrt(self._TemporaryData['vv'] * self._TemporaryData['ww'])
            self._TemporaryData['Rvw'] = Rvw
            self._CalculatedParams.append(['Rvw',{}])

        return self

    def Ruw(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Ruw' not in self._TemporaryData.columns:
            self.uw().uu().ww()
            Ruw = self._TemporaryData['uw'] / numpy.sqrt(self._TemporaryData['uu'] * self._TemporaryData['ww'])
            self._TemporaryData['Ruw'] = Ruw
            self._CalculatedParams.append(['Ruw',{}])

        return self

    def MOLength(self, inMemory=None):
        """
        Calculates the Monin-Obukhov length.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'L' not in self._TemporaryData.columns:
            self.wT().Ustar()
            L = -(self._TemporaryData['T_bar']+273.15) * self._TemporaryData['Ustar'] ** 3 / (
                        self.Karman * g * self._TemporaryData['wT'])
            self._TemporaryData['L'] = L
            self._CalculatedParams.append(['L',{}])

        return self

    def zoL(self, zmd, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        zmd: float
            Height.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        i = 1

        while 'zoL%s' % i in self._TemporaryData.columns:
            if ['zoL%s' % i, {'zmd': zmd}] in self._AllCalculatedParams:
                return self
            i += 1

        self.MOLength()
        zoL = zmd / self._TemporaryData['L']
        self._TemporaryData['zoL%s' % i] = zoL
        self._CalculatedParams.append(['zoL%s' % i, {'zmd': zmd}])

        return self

    def zOverL(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        import warnings
        warnings.warn(
            "Remember that we use the 'buildingHeight' property as the averaged height. Correct by adding this field to the properties")

        if 'zOverL' not in self._TemporaryData.columns:
            self.MOLength()

            H = int(self.Identifier['buildingHeight'])
            instrumentHeight = int(self.Identifier['height'])
            # averagedHeight = H  # see warning.
            averagedHeight = int(self.Identifier['averagedHeight'])
            effectivez = instrumentHeight + H - 0.7 * averagedHeight
            zOverL = effectivez / self._TemporaryData['L']
            self._TemporaryData['zOverL'] = zOverL
            self._CalculatedParams.append(['zOverL',{}])

        return self

    def Lminus1_masked(self, inMemory=None):
        """

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Lminus1_masked' not in self._TemporaryData.columns:
            self.MOLength()
            mask = ((numpy.abs(self._TemporaryData['wT']) > 0.05) & (numpy.abs(self._TemporaryData['Ustar']) > 0.15))
            maskedData = self._TemporaryData[mask]
            Lminus1_masked = -self.Karman * (g / maskedData['T_bar']) * maskedData['wT'] / maskedData['Ustar'] ** 3
            self._TemporaryData['Lminus1_masked'] = Lminus1_masked
            self._CalculatedParams.append('Lminus1_masked')

        return self

    def StabilityMOLength(self, inMemory=None):
        """
        Calculates the MOlength stability.

        Parameters
        ----------
        inMemory : boolean
            Default value is None.

        Returns
        -------
        TurbulenceCalculator
            The object himself.
        """
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'StabilityMOLength' not in self._TemporaryData.columns:
            self.MOLength()
            stability = self._TemporaryData['L'].apply(self._ClassifyStability) if self._DataType is 'pandas' \
                else self._TemporaryData['L'].apply(self._ClassifyStability, meta='str')
            self._TemporaryData['StabilityMOLength'] = stability

            # Now drop all the fields in which wT <= 0.01 or Ustar <= 0.01
            # dropMethod = lambda x: x['StabilityMOLength'] if (numpy.abs(x['wT']) > 0.01) and (x['Ustar'] > 0.01) else None
            #dropMethod = lambda x: x['StabilityMOLength'] if (x['Ustar'] > 0.15) else None
            #dropMethod = lambda x: x['StabilityMOLength'] if (x['Ustar'] > 0.01) else None
            # self._TemporaryData['StabilityMOLength'] = self._TemporaryData.apply(dropMethod,
            #                                                                      axis=1) if self._DataType is 'pandas' \
            #     else self._TemporaryData.apply(dropMethod, meta='str', axis=1)
            self._CalculatedParams.append(['StabilityMOLength',{}])

        return self

    # ====================================================================
    #								Private
    # ====================================================================

    def _ClassifyStability(self, L):
        """
            According to 1/L categories:
            0 - Very Unstable
            1 - Unstable
            2 - Near Neutral
            3 - Stable
            4 - Very Stable
        """

        # For Z_0=1 (Irwin1979 Table 1)
        ret = 0
        if L is None:
            return "No Stability"
        if 1. / L < -.0875:
            ret = "very unstable"  # very un stable (A)
        elif 1. / L < -0.0081:
            ret = "unstable"  # un stable (C,B)
        elif 1. / L < 0.0081:
            ret = "neutral/near neutral"  # Neutral/Near Neutral (D)
        elif 1. / L < 0.25:  # (Mahrt1999: z/L>O(1)) #(z-d)/L<0.1667 from Delft Conference
            ret = "stable"  # stable (E,F)
        else:
            ret = "very stable"  # very stable (G)

        return ret

    def StrucFunDir(self, tau_range = None, dir1_data = None, u_dir1 = "u_dir1", v_dir1 = "v_dir1", w_dir1 = "w_dir1",
                    dir2_data = None, u_dir2 = "u_dir2", v_dir2 = "v_dir2", w_dir2 = "w_dir2", title = "", inMemory = None):

        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if dir2_data is None:
            dir2_data = dir1_data
            u_dir2 = u_dir1
            v_dir2 = v_dir1
            w_dir2 = w_dir1

        col_names = {tau:"D" + title + "_" + str(tau) + "s" for tau in tau_range}
        if set(col_names.values()).issubset(set(self._TemporaryData.columns)):
            # self._TemporaryData = self._TemporaryData.loc[(self._TemporaryData.index >= self.Identifier["start"]) &
            #                                               (self._TemporaryData.index < self.Identifier["end"])]
            return self

        self.fluctuations()

        # Extracting ...
        dir1_data_new = dir1_data[[u_dir1,v_dir1,w_dir1]]\
                       .rename(columns = {u_dir1: "u_dir1", v_dir1: "v_dir1", w_dir1: "w_dir1"})\
                       .loc[(dir1_data.index >= self.Identifier["start"]) & (dir1_data.index < self.Identifier["end"])]

        # Computing ...
        dir1_data_new["dir1_mag"] = (dir1_data_new["u_dir1"] ** 2 + dir1_data_new["v_dir1"] ** 2 + dir1_data_new["w_dir1"] ** 2) ** 0.5

        dir1_data_new.loc[:,['u_dir1','v_dir1','w_dir1']] = dir1_data_new.loc[:,['u_dir1','v_dir1','w_dir1']].div(dir1_data_new["dir1_mag"], axis=0)

        dir1_data_new = dir1_data_new.drop(columns = 'dir1_mag')

        dir2_data_new = dir2_data[[u_dir2,v_dir2,w_dir2]]\
                       .rename(columns = {u_dir2: "u_dir2", v_dir2: "v_dir2", w_dir2: "w_dir2"})\
                       .loc[(dir2_data.index >=self.Identifier["start"]) & (dir2_data.index < self.Identifier["end"])]

        dir2_data_new["dir2_mag"] = (dir2_data_new["u_dir2"] ** 2 + dir2_data_new["v_dir2"] ** 2 + dir2_data_new["w_dir2"] ** 2) ** 0.5

        dir2_data_new.loc[:,['u_dir2','v_dir2','w_dir2']] = dir2_data_new.loc[:,['u_dir2','v_dir2','w_dir2']].div(dir2_data_new["dir2_mag"], axis=0)

        dir2_data_new = dir2_data_new.drop(columns = 'dir2_mag')
        # u_mag_new = u_magnitude_data.rename("u_mag").loc[(u_magnitude_data.index >=  self.Identifier["start"]) &
        #                         (u_magnitude_data.index < self.Identifier["end"])]

        to_drop = set(self._RawData.columns) & {"u_dir1", "v_dir1", "w_dir1", "u_dir2", "v_dir2", "w_dir2"}
        if bool(to_drop):
            self._RawData = self._RawData.drop(columns = to_drop)


        united_data = self._RawData.merge(dir1_data_new, how = "outer", left_index = True, right_index = True)\
                           .merge(dir2_data_new, how = "outer", left_index = True, right_index = True)\
                           .dropna(how='all')

        united_data[["u_dir1", "v_dir1", "w_dir1", "u_dir2", "v_dir2", "w_dir2"]].ffill().dropna(how='any')

        united_data["ui"] = 0
        united_data["uj"] = 0
        for component in ["u","v","w"]:
            united_data["ui"] += united_data[component]*united_data["%s_dir1" % component]
            united_data["uj"] += united_data[component] * united_data["%s_dir2" % component]

        #print("RawData",[self._RawData.get_partition(n) for n in range(self._RawData.npartitions)])
        for tau in tau_range:
            if col_names[tau] not in self._TemporaryData.columns:
                data_tau = united_data[["ui","uj"]].reset_index()
                data_tau["Time"] -= pandas.Timedelta(tau, unit="s")

                data_tau = data_tau.set_index("Time").rename(columns = {"ui":"ui_shifted","uj":"uj_shifted"})
                print("data_tau", [data_tau.get_partition(n) for n in range(data_tau.npartitions)])
                to_drop = set(united_data.columns) & {"ui_shifted", "uj_shifted"}
                if bool(to_drop):
                    united_data = united_data.drop(columns = to_drop)
                united_data = united_data.merge(data_tau, how = "left", left_index = True, right_index = True).repartition(freq = "1D")

                #print("RawData", [self._RawData.get_partition(n) for n in range(self._RawData.npartitions)])

                self._TemporaryData[col_names[tau]] = ((united_data["ui_shifted"] - united_data["ui"]) *\
                                                 (united_data["uj_shifted"] - united_data["uj"])).resample(self.SamplingWindow).mean()
                self._CalculatedParams.append([col_names[tau],{}])

        # self._TemporaryData = self._TemporaryData.loc[(self._TemporaryData.index >= self.Identifier["start"]) &
        #                                               (self._TemporaryData.index < self.Identifier["end"])]
        return self

    def StrucFun(self, tau_range = None, ubar_data = None, u_bar = "u_bar", v_bar = "v_bar", w_bar = "w_bar",
                     mode = "MeanDir", title_additions = "", inMemory = None):

        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if ubar_data is None:
            self.fluctuations()
            ubar_data = self._TemporaryData[["u_bar","v_bar","w_bar"]].compute()

        if mode == "MeanDir":
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_data, u_dir1=u_bar, v_dir1=v_bar, w_dir1=w_bar, title = "11" + title_additions)

        if mode == "3dMeanDir":
            ubar_new = ubar_data[[u_bar,v_bar,w_bar]].rename(columns = {u_bar:"x_hat1",v_bar:"x_hat2",w_bar:"x_hat3"})
            ubar_new["y_hat1"] = - ubar_new["x_hat2"]
            ubar_new["y_hat2"] = ubar_new["x_hat1"]
            ubar_new["y_hat3"] = 0
            ubar_new["z_hat1"] = - ubar_new["x_hat1"] * ubar_new["x_hat3"]
            ubar_new["z_hat2"] = - ubar_new["x_hat2"] * ubar_new["x_hat3"]
            ubar_new["z_hat3"] = ubar_new["x_hat1"] ** 2 +  ubar_new["x_hat2"] ** 2

            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="x_hat1", v_dir1="x_hat2", w_dir1="x_hat3", title="11" + title_additions)
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="y_hat1", v_dir1="y_hat2", w_dir1="y_hat3", title="22" + title_additions)
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="z_hat1", v_dir1="z_hat2", w_dir1="z_hat3", title="33" + title_additions)
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="x_hat1", v_dir1="x_hat2", w_dir1="x_hat3",
                             dir2_data = ubar_new, u_dir2="y_hat1", v_dir2="y_hat2", w_dir2="y_hat3",title="12" + title_additions)
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="x_hat1", v_dir1="x_hat2", w_dir1="x_hat3",
                             dir2_data = ubar_new, u_dir2="z_hat1", v_dir2="z_hat2", w_dir2="z_hat3",title="13" + title_additions)
            self.StrucFunDir(tau_range=tau_range, dir1_data=ubar_new, u_dir1="y_hat1", v_dir1="y_hat2", w_dir1="y_hat3",
                             dir2_data = ubar_new, u_dir2="z_hat1", v_dir2="z_hat2", w_dir2="z_hat3",title="23" + title_additions)

        self._TemporaryData["u_mag" + title_additions] = ((ubar_data[u_bar] ** 2 + ubar_data[v_bar] ** 2 + ubar_data[w_bar] ** 2) ** 0.5).loc[(ubar_data.index >=
                                self.Identifier["start"]) & (ubar_data.index < self.Identifier["end"])]
        self._TemporaryData["u_mag" + title_additions] = self._TemporaryData["u_mag" + title_additions].ffill()
        self._CalculatedParams.append(["u_mag" + title_additions,{}])
        return self

    def StrucFun_eps(self, tau_range = None, ubar_data = None, u_bar = "u_bar", v_bar = "v_bar", w_bar = "w_bar",
                     mode = "MeanDir", title_additions = "", rmin = 0, rmax = 10, inMemory = None):
        """

        :param tau_range: date
               date
        :param ubar_data:
        :param u_bar:
        :param v_bar:
        :param w_bar:
        :param mode:
        :param title_additions:
        :param rmin:
        :param rmax:
        :param inMemory:
        :return:
        """

        self.StrucFun(tau_range = tau_range, ubar_data = ubar_data, u_bar = u_bar, v_bar = v_bar, w_bar = w_bar,
                     mode = mode, title_additions = title_additions)

        a = 0.52
        col_names = {tau:"D11" + title_additions + "_" + str(tau) + "s" for tau in tau_range}
        data = self._TemporaryData[list(col_names.values()) + ["u_mag" + title_additions]].compute()
        # estimations = pandas.DataFrame(index=self._TemporaryData.index.compute(), columns = col_names.values())
        estimations = pandas.DataFrame(index=data.index, columns=col_names.values())
        for tau in tau_range:
            data_temp = ((a * data[col_names[tau]]) ** (3 / 2)) / (tau * data["u_mag" + title_additions])
            mask = (tau * data["u_mag" + title_additions] < rmax) & (tau * data["u_mag" + title_additions] > rmin)
            # estimations[col_names[tau]] = ((((a * self._TemporaryData[col_names[tau]]) ** (3 / 2)) / (tau * self._TemporaryData["u_mag"])).compute())\
            #     .loc[((tau * self._TemporaryData["u_mag" + title_additions] < rmax) & (tau * self._TemporaryData["u_mag" + title_additions] > rmin)).compute()]
            estimations[col_names[tau]] = data_temp.loc[mask]

        self._TemporaryData["eps_D11"] = estimations.mean(axis=1)
        self._CalculatedParams.append(["eps_D11",{}])

        return self

    def ThirdStrucFun(self, tau_range = None, ubar_data = None, u_bar = "u_bar", v_bar = "v_bar", w_bar = "w_bar",
                     title_additions = "", inMemory = None):

        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        self.fluctuations()
        if ubar_data is None:
            ubar_data = self._TemporaryData[["u_bar","v_bar","w_bar"]].compute()

        col_names = {tau:"D111" + title_additions + "_" + str(tau) + "s" for tau in tau_range}
        if set(col_names.values()).issubset(set(self._TemporaryData.columns)):
            # self._TemporaryData = self._TemporaryData.loc[(self._TemporaryData.index >= self.Identifier["start"]) &
            #                                               (self._TemporaryData.index < self.Identifier["end"])]
            return self

        dir_data = ubar_data[[u_bar,v_bar,w_bar]].rename(columns={u_bar: "u_dir", v_bar: "v_dir", w_bar: "w_dir"}).loc[
            (ubar_data.index >=self.Identifier["start"]) & (ubar_data.index < self.Identifier["end"])]
        dir_data["u_mag"] = (dir_data["u_dir"] ** 2 + dir_data["v_dir"] ** 2 + dir_data["w_dir"] ** 2) ** 0.5
        dir_data.loc[:, ['u_dir', 'v_dir', 'w_dir']] = dir_data.loc[:, ['u_dir', 'v_dir', 'w_dir']].div(dir_data["u_mag"], axis=0)

        to_drop = set(self._RawData.columns) & {"u_dir", "v_dir", "w_dir", "u1_shifted"}
        if bool(to_drop):
            self._RawData = self._RawData.drop(columns = to_drop)
        self._RawData = self._RawData.merge(dir_data[['u_dir', 'v_dir', 'w_dir']], how="outer", left_index=True, right_index=True)
        self._RawData = self._RawData.dropna(how='all')
        self._RawData[["u_dir", "v_dir", "w_dir"]] = self._RawData[["u_dir", "v_dir", "w_dir"]].ffill()
        self._RawData = self._RawData.dropna(how='any')

        self._RawData["u1"] = self._RawData["u"] * self._RawData["u_dir"] + self._RawData["v"] * self._RawData["v_dir"] + \
                              self._RawData["w"] * self._RawData["w_dir"]

        for tau in tau_range:
            if col_names[tau] not in self._TemporaryData.columns:
                data_tau = self._RawData[["u1"]].reset_index()
                data_tau["Time"] -= pandas.Timedelta(tau, unit="s")
                data_tau = data_tau.set_index("Time").rename(columns = {"u1":"u1_shifted"})
                if "u1_shifted" in self._RawData.columns:
                    self._RawData = self._RawData.drop(columns = "u1_shifted")
                self._RawData = self._RawData.merge(data_tau, how = "left", left_index = True, right_index = True)
                self._TemporaryData[col_names[tau]] = ((self._RawData["u1_shifted"] - self._RawData["u1"]) ** 3).resample(self.SamplingWindow).mean()
                self._CalculatedParams.append([col_names[tau],{}])

        self._TemporaryData["u_mag" + title_additions] = dir_data["u_mag"]
        self._TemporaryData["u_mag" + title_additions] = self._TemporaryData["u_mag" + title_additions].ffill()
        self._CalculatedParams.append(["u_mag" + title_additions,{}])

        # self._TemporaryData = self._TemporaryData.loc[(self._TemporaryData.index >= self.Identifier["start"]) &
        #                                               (self._TemporaryData.index < self.Identifier["end"])]

        return self

    def ThirdStrucFun_eps(self, tau_range = None, ubar_data = None, u_bar = "u_bar", v_bar = "v_bar", w_bar = "w_bar",
                      title_additions = "", rmin = 0, rmax = 10, inMemory = None):

        self.ThirdStrucFun(tau_range = tau_range, ubar_data = ubar_data, u_bar = u_bar, v_bar = v_bar, w_bar = w_bar,
                     title_additions = title_additions)

        col_names = {tau:"D111" + title_additions + "_" + str(tau) + "s" for tau in tau_range}
        data = self._TemporaryData[list(col_names.values()) + ["u_mag" + title_additions]].compute()
        estimations = pandas.DataFrame(index=data.index, columns=col_names.values())
        for tau in tau_range:
            data_temp = 1.25 * data[col_names[tau]] / (tau * data["u_mag" + title_additions])
            mask = (tau * data["u_mag" + title_additions] < rmax) & (tau * data["u_mag" + title_additions] > rmin)
            estimations[col_names[tau]] = data_temp.loc[mask]

        self._TemporaryData["eps_D111"] = estimations.mean(axis=1)
        self._CalculatedParams.append(["eps_D111",{}])

        return self


class TurbulenceCalculatorSpark(TurbulenceCalculator):

    def fluctuations(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'up' not in self._RawData.columns:
            avg = self._RawData
            avg = avg if self.SamplingWindow is None else avg.resample(self.SamplingWindow)
            avg = avg.mean().rename(columns={'u': 'u_bar', 'v': 'v_bar', 'w': 'w_bar', 'T': 'T_bar'})

            self._TemporaryData = avg
            self._CalculatedParams += [['u_bar',{}], ['v_bar',{}], ['w_bar',{}], ['T_bar',{}]]

            # correcting the first index to be the same as the avg.
            self._RawData = self._RawData.reset_index()
            self._RawData.at[0,'Time'] = avg.index[0]
            self._RawData = self._RawData.set_index("Time")

            self._RawData = self._RawData.merge(avg, how='left', left_index=True, right_index=True)
            self._RawData = self._RawData.ffill()

            self._RawData['up'] = self._RawData['u'] - self._RawData['u_bar']
            self._RawData['vp'] = self._RawData['v'] - self._RawData['v_bar']
            self._RawData['wp'] = self._RawData['w'] - self._RawData['w_bar']
            self._RawData['Tp'] = self._RawData['T'] - self._RawData['T_bar']

        return self
