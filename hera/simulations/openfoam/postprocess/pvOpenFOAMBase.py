#### import the simple module from the paraview
import paraview.simple as pvsimple
from paraview import servermanager

#### disable automatic camera reset on 'Show'
pvsimple._DisableFirstRenderCameraReset()
import vtk.numpy_interface.dataset_adapter as dsa
from itertools import product
import pandas
import numpy
import xarray
import os
import glob


class pvdatastructures(object):
    """
        A utility class to hols the components of
        a scalar and a vector.

        The components will be selected based on the length of the field.

        length  |  type
        ----------------
        1       | scalar.
        2       | vector.
        3       | tensor.
    """
    componentsNames = None

    def __init__(self):
        self.componentsNames = {(): "",
                                (0,): "_x",
                                (1,): "_y",
                                (2,): "_z",
                                (0, 0): "_xx",
                                (0, 1): "_xy",
                                (0, 2): "_xz",
                                (1, 0): "_yx",
                                (1, 1): "_yy",
                                (1, 2): "_yz",
                                (2, 0): "_zx",
                                (2, 1): "_zy",
                                (2, 2): "_zz"}


class pvOFBase(object):
    """

    """
    _Fields = None
    _FileName = None

    _componentsNames = None

    netcdfdir = None
    hdfdir    = None


    @property
    def fields(self):
        return self._Fields

    def __init__(self, servername=None):
        if servername is not None:
            pvsimple.Connect(servername)

        self._componentsNames = {}

        self.netcdfdir = "netcdf"
        self.hdfdir = "hdf"

        # Array shape length 1 - scalar.
        #					 2 - vector.
        #					 3 - tensor.
        # the dict holds their names.
        self._componentsNames = {(): "",
                                 (0,): "_x",
                                 (1,): "_y",
                                 (2,): "_z",
                                 (0, 0): "_xx",
                                 (0, 1): "_xy",
                                 (0, 2): "_xz",
                                 (1, 0): "_yx",
                                 (1, 1): "_yy",
                                 (1, 2): "_yz",
                                 (2, 0): "_zx",
                                 (2, 1): "_zy",
                                 (2, 2): "_zz"}

    # CaseType = 'Decomposed Case' ,CaseType = 'Reconstructed Case'
    def ReadCase(self, casename, filename, CaseType='Decomposed Case', fieldnames=None):
        reader = pvsimple.OpenFOAMReader(FileName=filename, CaseType=CaseType, guiName=casename)
        reader.MeshRegions = ['internalMesh']
        if fieldnames is not None:
            reader.CellArrays = fieldnames

        reader.UpdatePipeline()
        return reader

    def ReadLagrangianVTK(self, casename, filenames):  # ['U','p_rgh','T','Tmean']):
        self._readername = casename
        reader = pvsimple.LegacyVTKReader(FileNames=filenames, guiName=self._readername)
        reader.UpdatePipeline()
        return reader

    def to_pandas(self, readername, datasourcenamelist, timelist=None, fieldnames=None):
        return self.readTimeSteps(readername, datasourcenamelist, timelist, fieldnames, xarray=False)

    def to_xarray(self, readername, datasourcenamelist, timelist=None, fieldnames=None):
        return self.readTimeSteps(readername, datasourcenamelist, timelist, fieldnames, xarray=True)


    def readTimeSteps(self,readername,datasourcenamelist,timelist=None, fieldnames=None,xarray=False):
        """
            reads a list of datasource lists to a dictionary

        :param readername:
                The source filter (or its name)

        :param datasourcenamelist:
                A list of names of filters to get.

        :param timelist:
                The list of times to read.
        :param fieldnames:
                The list of fieldsto write.
        :param xarray
                convert pandas results to xarray (works only for regular grids).
        :return:
                For each time step.
                    A map datasourcename -> pandas
        """

        if isinstance(readername,str) or isinstance(readername,unicode):
            reader = pvsimple.FindSource(readername)
        else:
            reader = readername

        datasourcenamelist = numpy.atleast_1d(datasourcenamelist)

        timelist = reader.TimestepValues if timelist is None else numpy.atleast_1d(timelist)
        for timeslice in timelist:
            # read the timestep.
            print("\r Reading time slice %s" % timeslice)

            ret = {}

            for datasourcename in datasourcenamelist:
                datasource = pvsimple.FindSource(datasourcename)
                ret[datasourcename] = self.readTimeStep(reader,datasource,timeslice,fieldnames,xarray)
            yield ret

    def readTimeStep(self, readername, datasource,timeslice, fieldnames=None, xarray=False):

        if isinstance(readername,str):
            reader = pvsimple.FindSource(readername)
        else:
            reader = readername

        # read the timestep.
        datasource.UpdatePipeline(timeslice)

        rawData = servermanager.Fetch(datasource)
        data = dsa.WrapDataObject(rawData)

        if isinstance(data.Points, dsa.VTKArray):
            points = numpy.array(data.Points).squeeze()
        else:
            points = numpy.concatenate([numpy.array(x) for x in data.Points.GetArrays()]).squeeze()

        curstep = pandas.DataFrame()

        # create index
        curstep['x'] = points[:, 0]
        curstep['y'] = points[:, 1]
        curstep['z'] = points[:, 2]
        curstep['time'] = timeslice

        fieldlist = data.PointData.keys() if fieldnames is None else fieldnames
        for field in fieldlist:

            if isinstance(data.PointData[field], dsa.VTKNoneArray):
                continue
            elif isinstance(data.PointData[field], dsa.VTKArray):
                arry = numpy.array(data.PointData[field]).squeeze()
            else:
                arry = numpy.concatenate([numpy.array(x) for x in data.PointData[field].GetArrays() if not isinstance(x,dsa.VTKNoneArray)]).squeeze()

            # Array shape length 1 - scalar.
            #					 2 - vector.
            #					 3 - tensor.
            # the dict holds their names.
            TypeIndex = len(arry.shape) - 1
            for indxiter in product(*([range(3)] * TypeIndex)):
                L = [slice(None, None, None)] + list(indxiter)
                try:
                    curstep["%s%s" % (field, self._componentsNames[indxiter])] = arry[L]
                except ValueError:
                    print("Field %s is problematic... ommiting" % field)


        curstep = curstep.set_index(['time', 'x', 'y', 'z']).to_xarray() if xarray else curstep
        return curstep

    def write_netcdf(self, readername, datasourcenamelist, outfile=None, timelist=None, fieldnames=None,batch=100):

        def writeList(theList,batchID):

            data = xarray.concat(theList, dim="time")
            curfilename = os.path.join(self.netcdfdir, "%s_%s.nc" % (filtername, batchID))
            print("Writing %s " % curfilename)
            data.to_netcdf(curfilename)
            batchID += 1

        self._outfile = readername if outfile is None else outfile

        if not os.path.isdir(self.netcdfdir):
            os.makedirs(self.netcdfdir)

        batchID = 0
        L = []
        for xray in self.to_xarray(readername, datasourcenamelist=datasourcenamelist, timelist=timelist, fieldnames=fieldnames):

            L.append(xray)
            if len(L) == batch:
                if isinstance(L[0],dict):
                    filterList = [k for k in L[0].keys()]
                    for filtername in filterList:
                        writeList([item[filtername] for item in L],batchID)
                else:
                    writeList(L)
                L = []

        if isinstance(L[0],dict):
            filterList = [k for k in L[0].keys()]
            for filtername in filterList:
                writeList([item[filtername] for item in L],batchID)
        else:
            writeList(L)

    def write_hdf(self, readername, datasourcenamelist, outfile=None, timelist=None, fieldnames=None,batch=100):

        def writeList(theList, batchID):
            data = pandas.concat(theList, ignore_index=True,sort=True)
            curfilename = "%s_%s.hdf" % (outfile, batchID)
            print("\tWriting filter %s in file %s" % (filtername, curfilename))
            data.to_hdf(os.path.join(self.hdfdir, curfilename), key=filtername, format='table')
            batchID += 1

        outfile = readername if outfile is None else outfile
        if not os.path.isdir(self.hdfdir):
            os.makedirs(self.hdfdir)

        batchID = 0
        L = []
        for pnds in self.to_pandas(readername, datasourcenamelist=datasourcenamelist, timelist=timelist, fieldnames=fieldnames):

            L.append(pnds)

            if len(L) == batch:
                filterList = [x for x in L[0].keys()]
                for filtername in filterList:
                    writeList([item[filtername] for item in L], batchID)
                L=[]

        writeList(L, batchID)



    def open_dataset(self, outfile=None, timechunk=10):

        filenames = [filename for filename in glob.glob(os.path.join(self.netcdfdir, "%s*.nc" % outfile))]
        filenames = sorted(filenames, key=lambda x: float(x.split(".")[0].split("_")[-1]))

        dataset = xarray.open_mfdataset(filenames, chunks={'time': timechunk})

        return dataset




if __name__ == "__main__":
    bse = pvOFBase()

    print("Creating Reader")
    reader = bse.ReadCase("A", "A.foam",'Reconstructed Case') # CaseType='Decomposed Case')  # 'Reconstructed Case')
    cellCenters1 = pvsimple.CellCenters(Input=reader,guiName="cellcenter")
    #	print(reader.TimestepValues)
    #	reader = bse.ReadLagrangianVTK("W300StrongDispersion",["O2300_150_1-5_D101-5_P15000_Continuous_7260.vtk"])
    #	import pdb
    #	pdb.set_trace()
    #	print(reader.
    #	bse.write_hdf("W300StrongDispersion",reader)

    # clip = pvsimple.Clip(Input=reader)
    # clip.ClipType='Box'
    # clip.Crinkleclip=True
    # clip.ClipType.Bounds = [1000,3000,0,300,0,300]
    # clip.InsideOut = True
    # clip.UpdatePipeline()

    bse.write_netcdf("A","cellcenter")

    # bse = pvOFBase()
    # reader = bse.ReadCase("Straight100m", "Straight100m.openfoam")
    #print("...Done")
    #clip = pvsimple.Clip(Input=reader,guiName="clip")
    #clip.ClipType='Box'
    #clip.Crinkleclip=True

    #clip.ClipType.Bounds = [-0.10000000149011612, 5.099999904632568, -0.10000000149011612, 4.099999904632568,-0.10000000149011612, 2.5999999046325684]
    #clip.ClipType.Position = [2.4557588668766424, 1.6246336788311293, -0.020431968730497685]
    #clip.ClipType.Scale    = [0.14685547909596913, 0.18196658248910527, 0.8352306395969601]

    #clip.InsideOut = True
    #clip.UpdatePipeline()

    #cellcntr = pvsimple.CellCenters(reader)
    #cellcntr.UpdatePipeline()

    #for x in bse.to_pandas("A", "clip"):
    #    print("x")
    # data = bse.open_dataset(timechunk=10)
