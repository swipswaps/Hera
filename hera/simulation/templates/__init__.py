import os
from ...datalayer.document.metadataDocument import Analysis as AnalysisDoc
from ... import datalayer
from ..LSM import LagrangianReader
from .inputForModelsCreation import InputForModelsCreator
import xarray


class Template():
    _document = None

    def __init__(self, document):
        self._document = document
        pass

    @property
    def params(self):
        return self._document['desc']['params']

    @property
    def version(self):
        return self._document['desc']['version']

    @property
    def modelName(self):
        return self._document['projectName']

    @property
    def templateFolder(self):
        return self._document['desc']['templateFolder']

    def run(self, saveDir, to_xarray=False, to_database=False):
        os.makedirs(saveDir, exist_ok=True)
        os.system('cp -rf %s %s' % (self.templateFolder, saveDir))

        # create the input file.
        # paramsMap['wind_dir'] = self.paramsMap['wind_dir_meteorological']
        ifmc = InputForModelsCreator(os.path.dirname(__file__))
        ifmc.setParamsMap(self.params)
        ifmc.setTemplate(self.version)

        if to_database:
            doc = dict(projectName=self.modelName,
                       resource='',
                       dataFormat='',
                       desc=dict(params=self.params,
                                 version=self.version
                                 )
                       )
            doc = AnalysisDoc(**doc).save()
            if to_xarray:
                saveDir = '%s_%s' % (saveDir, doc.id)
                doc['resource'] = os.path.join(saveDir, 'netcdf', '*')
                doc['dataFormat'] = 'netcdf'
            else:
                doc['resource'] = saveDir
                doc['dataFormat'] = 'string'

            doc.save()

        # write to file.
        ifmc.render(os.path.join(saveDir, 'INPUT'))

        # run the model.
        #os.chdir(saveDir)
        os.system(os.path.join(saveDir, 'a.out'))

        if to_xarray:
            results_full_path = os.path.join(saveDir, "tozaot", "machsan", "OUTD2d")
            netcdf_output = os.path.join(saveDir, "netcdf")
            os.makedirs(netcdf_output, exist_ok=True)

            L = []
            i = 0
            for xray in LagrangianReader.toNetcdf(basefiles=results_full_path):
                L.append(xray)

                if len(L) == 100:  # args.chunk:
                    finalxarray = xarray.concat(L, dim="datetime")
                    finalxarray.to_netcdf(os.path.join(netcdf_output, "data%s.nc" % i))
                    L = []
                    i += 1


            # save the rest.
            finalxarray = xarray.concat(L, dim="datetime")
            finalxarray.to_netcdf(os.path.join(netcdf_output, "data%s.nc" % i))