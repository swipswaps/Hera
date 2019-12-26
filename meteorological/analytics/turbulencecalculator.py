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

    def _ffillNew(self, data):
        try:
            return data.ffill()
        except ValueError:
            return data.repartition(freq=pandas.to_timedelta(self.SamplingWindow.replace('T', 'min'))).map_partitions(
                self._ffillNew2)

    def _ffillNew2(data):
        try:
            return data.ffill()
        except:
            return data.dropna()

    def sigma(self, inMemory=None):
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
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wind_speed' not in self._RawData.columns:
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
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wind_dir_mathematical' not in self._RawData.columns:
            self._RawData = self._RawData.assign(wind_dir_mathematical=lambda x: numpy.arctan2(x['v'], x['u']))
            resampled = self._RawData['wind_dir_mathematical']
            resampled = resampled if self.SamplingWindow is None else resampled.resample(self.SamplingWindow)

            avg = resampled.apply(lambda x: circmean(x, high=numpy.pi, low=-numpy.pi))
            self._TemporaryData['wind_dir_mathematical'] = avg
            self._CalculatedParams.append(['wind_dir_mathematical',{}])
            avg = numpy.rad2deg(avg+numpy.pi)
            self._TemporaryData['wind_dir_meteorological'] = [int(270-x) if 270-x>=0 else int(630-x) for x in avg.values]
            self._CalculatedParams.append(['wind_dir_meteorological',{}])

            std = resampled.apply(lambda x: circstd(x, high=numpy.pi, low=-numpy.pi)).rename(columns={'wind_dir_mathematical': 'wind_dir_mathematical_std'})
            self._TemporaryData['wind_dir_mathematical_std'] = std
            self._CalculatedParams.append(['wind_dir_mathematical_std',{}])
            std = numpy.rad2deg(std+numpy.pi)
            self._TemporaryData['wind_dir_meteorological_std'] = [int(270-x) if 270-x>=0 else int(630-x) for x in std.values]
            self._CalculatedParams.append(['wind_dir_meteorological_std',{}])

        return self

    def sigmaHOverWindSpeed(self, inMemory=None):
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
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uStarOverWindSpeed' not in self._TemporaryData.columns:
            self.Ustar().wind_speed()
            uStarOverWindPeed = self._TemporaryData['Ustar']/self._TemporaryData['wind_speed']
            self._TemporaryData['uStarOverWindSpeed'] = uStarOverWindPeed
            self._CalculatedParams.append(['uStarOverWindSpeed',{}])

        return self

    def uu(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uu' not in self._TemporaryData.columns:
            self.fluctuations()
            uu = (self._RawData['up'] * self._RawData['up']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uu'] = uu
            self._CalculatedParams.append(['uu',{}])

        return self

    def vv(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'vv' not in self._TemporaryData.columns:
            self.fluctuations()
            vv = (self._RawData['vp'] * self._RawData['vp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['vv'] = vv
            self._CalculatedParams.append(['vv',{}])

        return self

    def ww(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'ww' not in self._TemporaryData.columns:
            self.fluctuations()
            ww = (self._RawData['wp'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['ww'] = ww
            self._CalculatedParams.append(['ww',{}])

        return self

    def wT(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'wT' not in self._TemporaryData.columns:
            self.fluctuations()
            wT = (self._RawData['wp'] * self._RawData['Tp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['wT'] = wT
            self._CalculatedParams.append(['wT',{}])

        return self

    def uv(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uv' not in self._TemporaryData.columns:
            self.fluctuations()
            uv = (self._RawData['up'] * self._RawData['vp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uv'] = uv
            self._CalculatedParams.append(['uv',{}])

        return self

    def uw(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'uw' not in self._TemporaryData.columns:
            self.fluctuations()
            uw = (self._RawData['up'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['uw'] = uw
            self._CalculatedParams.append(['uw',{}])

        return self

    def vw(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'vw' not in self._TemporaryData.columns:
            self.fluctuations()
            vw = (self._RawData['vp'] * self._RawData['wp']).resample(self.SamplingWindow).mean()
            self._TemporaryData['vw'] = vw
            self._CalculatedParams.append(['vw',{}])

        return self

    def w3(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'w3' not in self._TemporaryData.columns:
            self.fluctuations()
            www = (self._RawData['wp'] ** 3).resample(self.SamplingWindow).mean()
            self._TemporaryData['w3'] = www
            self._CalculatedParams.append(['w3',{}])

        return self

    def w4(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'w4' not in self._TemporaryData.columns:
            self.fluctuations()
            wwww = (self._RawData['wp'] ** 4).resample(self.SamplingWindow).mean()
            self._TemporaryData['w4'] = wwww
            self._CalculatedParams.append(['w4',{}])

        return self

    def TKE(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'TKE' not in self._TemporaryData.columns:
            self.uu().vv().ww()
            TKE = 0.5 * (self._TemporaryData['uu'] + self._TemporaryData['vv'] + self._TemporaryData['ww'])
            self._TemporaryData['TKE'] = TKE
            self._CalculatedParams.append(['TKE',{}])

        return self

    def wTKE(self, inMemory=None):
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
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Ustar' not in self._TemporaryData.columns:
            self.uw().vw()
            Ustar = (self._TemporaryData['uw'] ** 2 + self._TemporaryData['vw'] ** 2) ** 0.25
            self._TemporaryData['Ustar'] = Ustar
            self._CalculatedParams.append(['Ustar',{}])

        return self

    def Rvw(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Rvw' not in self._TemporaryData.columns:
            self.vw().vv().ww()
            Rvw = self._TemporaryData['vw'] / numpy.sqrt(self._TemporaryData['vv'] * self._TemporaryData['ww'])
            self._TemporaryData['Rvw'] = Rvw
            self._CalculatedParams.append(['Rvw',{}])

        return self

    def Ruw(self, inMemory=None):
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'Ruw' not in self._TemporaryData.columns:
            self.uw().uu().ww()
            Ruw = self._TemporaryData['uw'] / numpy.sqrt(self._TemporaryData['uu'] * self._TemporaryData['ww'])
            self._TemporaryData['Ruw'] = Ruw
            self._CalculatedParams.append(['Ruw',{}])

        return self

    def MOLength(self, inMemory=None):
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
        if self._InMemoryAvgRef is None:
            self._InMemoryAvgRef = inMemory

        if 'zoL' not in self._TemporaryData.columns:
            self.MOLength()
            zoL = zmd / self._TemporaryData['L']
            self._TemporaryData['zoL'] = zoL
            self._CalculatedParams.append(['zoL',{}])

        return self

    def zOverL(self, inMemory=None):
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

            Version 1.4.0
            -------------
                    Drop the data if |U*| <= 0.15

            Version 1.3.3
            --------------
                    Drop the data if |wT| < 0.01 or |U*| <= 0.01

	    :math:`L = \frac{\overline{w'T'}}{\frac{du}{dz}}`

            .. versionadded:: 1.1.3

               Drop the data if :math:`|\overline{w'T'}| < 0.01` or :math:`|U^*| < 0.01`

        :param inMemory:
        :return:
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
