import pandas
import os
import json
from tinydb import TinyDB, Query
import paraview.simple as pvsimple
import numpy

from pynumericalmodels.OpenFOAM.postprocess.pvOpenFOAMBase import pvOFBase


class VTKpipeline(object):
    """This class executes a pipeline (runs and saves the outputs).
    It also  holds the metadata.

    Currently works only for the JSON pipeline. The XML (paraview native pipelines) will be built in the future.

    The pipeline is initialized with a reader.
    The metadata holds the names of all the filters that has to be executed.

    The VTKpipeline structure:
        {
            "metadata" : { see below },
            "VTKpipelines" : {
                    <pipeline1> : { pipeline definition, see VTKpipeline },
                    <pipeline2> : { pipeline definition, see VTKpipeline },
            }
        }

    The VTK pipeline JSON structure.
        {
            <guiName> : {
                            "type" : The type of the filter. (clip,slice,...).
                            "write"   : None/hdf (pandas)/netcdf (xarray),
                            "params" : [

                                    ("key","value"),
                                          .
                                          .
                                          .
                            ],...
                            "downstream" : [Another pipeline]
                        }
        }


    The meta data:
         {
            "timelist" : None, a list of time steps to use or a range B:E
            "fields"   : The fields to write.
            "datadir": The directory for the nc/hdf files. The default is subdir of the current directory
         }

     The write to writes the requested filters to the disk.
     The files are saved to a single .nc/hdf file with keys/fields in it. The file name is the
     pipelinename.

     As before, the hdf/nc is for one timestep.
    """

    _Metadata = None  # Holds the pipeline metadata. as JSON
    _VTKpipelineJSON = None  # Holds the json of the VTK pipeline.
    _pvOFBase = None  # Holds the OF base.

    _name = None

    @property
    def name(self):
        return self._name

    @property
    def pvOFBase(self):
        return self._pvOFBase

    def __init__(self, name, metadata, pipelineJSON, caseName):
        """
            Initializes a VTK pipeline.

        :param metadata:
            JSON of the metadata.
        :param pipelineJSON:
            JSON of the pipeline.
        :param caseName: the name of the case on which the pipeline executes.
        """
        self._pvOFBase = pvOFBase()
        self._VTKpipelineJSON = pipelineJSON
        self._Metadata = metadata
        self._name = name

        outputdir = metadata.get("datadir", "None")
        if outputdir != "None":
            self._pvOFBase.hdfdir = os.path.join(outputdir, caseName, "hdf")
            self._pvOFBase.netcdfdir = os.path.join(outputdir, caseName, "netcdf")

    def execute(self, source):
        """
            Builds the pipeline from the JSON vtk.

        :param source:
            The source filter guiName that the pipeline will be build on.

        """

        # build the pipeline.
        reader = pvsimple.FindSource(source)

        filterWrite = {}
        self._buildFilterLayer(father=reader, structureJson=self._VTKpipelineJSON, filterWrite=filterWrite)

        # Now execute the pipeline.
        timelist = self._Metadata.get("timelist", "None")
        if (timelist == "None"):
            timelist = None

        elif isinstance(timelist, str) or isinstance(timelist, unicode):
            # a bit of parsing.
            BandA = [0, 1e6]

            for i, val in enumerate(timelist.split(":")):
                BandA[i] = BandA[i] if len(val) == 0 else float(val)

            tl = pandas.Series(reader.TimestepValues)
            timelist = tl[tl.between(*BandA)].values

        # else just take it from the json (it should be a list).

        # Get the mesh regions.
        if "MeshRegions" in self._Metadata:
            reader.MeshRegions = self._Metadata["MeshRegions"]

        for frmt, datasourceslist in filterWrite.items():
            writer = getattr(self._pvOFBase, "write_%s" % frmt)
            if writer is None:
                raise ValueError("The write %s is not found" % writer)
            writer(readername=source, datasourcenamelist=datasourceslist, timelist=timelist,
                   fieldnames=self._Metadata.get('fields', None), outfile=self.name)

    def _buildFilterLayer(self, father, structureJson, filterWrite):
        """
            Recursively builds the structure of the leaf.
            Populates the self._filterWrite map

            Since the order of setting the params might be of importance (for example, setting the
            plane type determine the rest of the parameters), we set it as a list.

        :param father:
                The current filter father of the layer.

        :param structureJson:
                The portion of Json to build.

        :param[output]   filterWrite
                an  dictionary with the names of the filters that are about
                to be printed according to format.

        """
        if structureJson is None:
            return

        for filterGuiName in structureJson:
            # build the filter.
            paramPairList = structureJson[filterGuiName]['params']  # must be a list to enforce order in setting.
            filtertype = structureJson[filterGuiName]['type']
            filter = getattr(pvsimple, filtertype)(Input=father, guiName=filterGuiName)

            for param, pvalue in paramPairList:
                pvalue = str(pvalue) if isinstance(pvalue, unicode) else pvalue  # python2, will be removed in python3.
                paramnamelist = param.split(".")
                paramobj = filter
                for pname in paramnamelist[:-1]:
                    paramobj = getattr(paramobj, pname)
                setattr(paramobj, paramnamelist[-1], pvalue)
            filter.UpdatePipeline()

            writeformat = structureJson[filterGuiName].get("write", None)
            if (writeformat is not None) and (str(writeformat) != "None"):
                filterlist = filterWrite.setdefault(writeformat, [])
                filterlist.append(filterGuiName)

            self._buildFilterLayer(filter, structureJson[filterGuiName].get("downstream", None), filterWrite)


# ======================================================================================
# Pipeline Center
# ======================================================================================

class VTKpipelineCenter(object):
    """
        Retrieves the filter from the datalayer.

        TODO: Finish the class.
        Currently, each application has to manage its own pipelines.

    """
    _VTKPipelineJSON = None

    def __init__(self, VTKpipelineJSON):
        self._VTKPipelineJSON = {}

        self._VTKPipelineJSON = VTKpipelineJSON

    def getFilter(self, piplineName, filterName):
        pass


# ======================================================================================
# Pipeline factory.
# ======================================================================================

class pipelineFactory_JSON(object):
    """
        A Factory for a VTK pipeline that is described in a unified JSON.
        That is, the metadata and the pipeline definition are in one file.
        Gets the reader as an input.


            JSON format,
            {
                "metadata" : { see VTKpipeline },
                "VTKpipelines" : {
                        <pipeline1> : { pipeline definition, see VTKpipeline } or filename,
                        <pipeline2> : { pipeline definition, see VTKpipeline } or filename,
                }
            }



    """

    def __init__(self, caseName):
        self._caseName = caseName

    def getPipelines(self, filename, pipeline=None):
        """
            Reads a JSON description file of the pipeline and return the object.


        :param filename:
                The JSON file descriptor. can be file object or a file name.
        :param pipelinename:
                str, list or None.
                str - build the pipeline with that name.
                list - build the pipelins with that name
                None - build all the pipelines.

        :return:
                a map {pipeline name -> pipeline object}
        """

        if isinstance(filename, str):
            if os.path.exists(filename):
                with open(filename, 'r') as thefile:
                    configuration = json.load(thefile)
            else:
                json.loads(filename)

        else:
            configuration = json.load(filename)

        ret = {}

        pipelineList = [x for x in configuration['VTKpipelines'].keys()] if pipeline is None else numpy.atleast_1d(
            pipeline)

        for pipelineName in pipelineList:
            pipelineData = configuration['VTKpipelines'][pipelineName]
            if isinstance(pipelineData, unicode):
                # file name that contains the pipeline.
                with open(pipelineData, 'r') as pipefile:
                    pipelinefile = json.load(pipefile)
                    pipelineJSON = pipelinefile
            else:
                pipelineJSON = pipelineData

            ret[pipelineName] = VTKpipeline(pipelineName, configuration['metadata'], pipelineJSON, self._caseName)

        return ret


class pipelineFactory_DB(object):
    """
    This class manages the access to the pipeline results DB.

    The DB is a noSQL DB that holds the results of the

    The table is the casename.

    Each record holds the following: metadata and pipeline data.

    """
    _db = None

    @staticmethod
    def getDB(cls, dbname, dbfilepath=None):
        """
                Looks for the DB in the db directory $HOME/.pynumericalmodels/OF_piplinedirectory.json

                Tries to add it to the directory if does not exist (using dbfilepath).


                The directory structure is:

                {
                    <dbname> : full path to the DB


                }

        """
        conf = os.path.join(os.path.expanduser("~"), ".pynumericalmodels")
        with open(os.path.join(conf, "OF_piplines.json"), "r") as dbdirectoryfile:
            pipelineDB = json.load(dbdirectoryfile)

        if dbname not in pipelineDB:
            if dbfilepath is None:
                raise ValueError("Database %s not found (and no initialization path is given)" % dbname)
            else:
                pipelineDB[dbname] = dbfilepath
                with open(os.path.join(conf, "OF_piplinedirectory.json"), "w") as dbdirectoryfile:
                    json.dump(pipelineDB)

        return pipelineDB[dbname]

    def __init__(self, dbFile):
        if not os.path.exists(os.path.dirname(dbFile)):
            os.makedirs(os.path.dirname(dbFile))
        self._db = TinyDB(dbFile)

    def storeRecord(self, metadata, pipeline):
        """
            Stores a record (metadata + VTK pipeline) in the DB

        :param metadata:
                pipeline metadata (JSON)
        :param pipeline:
                The pipeline itself (JSON or XML).
        :return:
                The ID of the new entry.
        """

        table = self._db.table(metadata['casename'])

        ID = self.exists(metadata, pipeline)
        if ID is None:
            ID = table.insert({"metadata": metadata, "VTKpipeline": pipeline})
        else:
            table.update({"metadata": metadata, "VTKpipeline": pipeline}, doc_ids=[ID])

        return ID

    def getPipeline(self, casename, pipelinename):
        """
            Return the VTK pipeline for the requested case.

        :param casename:
            The case name
        :param pipelinename:
            The pipeline name
        :return:
            A map with pipelinename->VTKpipeline object
        """
        table = self._db.table(casename)
        res = table.search(Query().metadata.name == pipelinename)
        if len(res) == 0:
            raise ValueError("VTK Pipeline %s is not found in case %s" % (casename, pipelinename))

        return {pipelinename: VTKpipeline(res[0].metadata, res[0].VTKpipeline)}

    def exists(self, metadata):
        """
        Check whether the specific pipeline exists.
        The pipeline is identified by its name.

        :param metadata

        :return:
                ID   - The combination exists
                None - The combination does not exist.
        """
        table = self._db.table(metadata['casename'])
        res = table.search(Query().metadata.name == metadata['name'])
        return None if len(res) == 0 else res[0].doc_id


if __name__ == "__main__":
    bse = pvOFBase()
    reader = bse.ReadCase("Test", "AC4_3Da.foam", CaseType='Decomposed Case')  # 'Reconstructed Case')

    R = pipelineFactory_JSON().getPipeline("VTKPipe.json")
    print(R)
