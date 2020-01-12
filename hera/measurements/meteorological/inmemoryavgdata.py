from .inmemoryrawdata import InMemoryRawData

class InMemoryAvgData(InMemoryRawData):
    _TurbulenceCalculator = None

    def __init__(self, data = None, index = None, columns = None, dtype = None, copy = False, turbulenceCalculator = None):
        super(InMemoryAvgData, self).__init__(data = data, index = index, columns = columns, dtype = dtype, copy = copy)
        self._TurbulenceCalculator = turbulenceCalculator
        self._Attrs['samplingWindow'] = turbulenceCalculator.SamplingWindow

    def __getattr__(self, item):
        if self._TurbulenceCalculator is None:
            raise AttributeError("The attribute '_TurbulenceCalculator' is None.")
        elif not item in dir(self._TurbulenceCalculator):
            raise NotImplementedError("The attribute '%s' is not implemented." % item)
        elif item == 'compute':
            ret = getattr(self._TurbulenceCalculator, item)
        else:
            ret = lambda *args, **kwargs: getattr(self._TurbulenceCalculator, item)(inMemory = self, *args, **kwargs)

        return ret
