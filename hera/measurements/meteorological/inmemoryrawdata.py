import os
import json
import pandas


class InMemoryRawData(pandas.DataFrame):
    _Attrs = None

    def __init__(self, data=None, index=None, columns=None, dtype=None, copy=False):
        super(InMemoryRawData, self).__init__(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
        self._Attrs = {}

    def append(self, other, ignore_index=False, verify_integrity=False):
        ret = super(InMemoryRawData, self).append(other, ignore_index=ignore_index, verify_integrity=verify_integrity)
        ret = InMemoryRawData(ret)
        ret._Attrs = other._Attrs
        ret._Attrs.update(self._Attrs)

        return ret

    @classmethod
    def read_hdf(cls, path_or_buf, key=None, **kwargs):
        ret = InMemoryRawData(pandas.read_hdf(path_or_buf, key, **kwargs))
        path_or_buf = '%s%s' % (path_or_buf.rpartition('.')[0], '.json')

        if os.path.isfile(path_or_buf):
            with open(path_or_buf, 'r') as jsonFile:
                ret._Attrs = json.load(jsonFile)

        return ret

    def to_hdf(self, path_or_buf, key, **kwargs):
        pandasCopy = self.copy()
        path_or_buf = '%s%s' % (path_or_buf.rpartition('.')[0], '.hdf')
        pandasCopy.to_hdf(path_or_buf, key, **kwargs)
        path_or_buf = '%s%s' % (path_or_buf.rpartition('.')[0], '.json')
        attrsToSave = self._Attrs

        if len(self._Attrs) > 0:
            if os.path.isfile(path_or_buf):
                with open(path_or_buf, 'r') as jsonFile:
                    attrsFile = json.load(jsonFile)
                    attrsFile.update(attrsToSave)
                    attrsToSave = attrsFile

            with open(path_or_buf, 'w') as jsonFile:
                json.dump(attrsToSave, jsonFile, indent=4, sort_keys=True)
