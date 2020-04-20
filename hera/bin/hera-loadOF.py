import argparse
import json
from hera import datalayer
import dask.dataframe as dd
import pandas
import os
import shutil

"""
Converts hdf files with OpenFOAM results to parquet format, adds the paths to the database.
Unless -keepHDF is used, deletes the hdf files.
path is the path of the directory in which the hdf directory rests.
name is the name used for the formation of the hdf files.
projectName is used for saving the data to the database.
"""

parser = argparse.ArgumentParser()
parser.add_argument('path', nargs=1, type=str)
parser.add_argument('name', nargs=1, type=str)
parser.add_argument('projectName', nargs=1, type=str)
parser.add_argument('-keepHDF', action='store_false')

path = parser.parse_args().path[0]
name = parser.parse_args().name[0]
projectName = parser.parse_args().projectName[0]

keys = pandas.HDFStore("%s/hdf/%s_0.hdf" % (path, name)).keys()
fixed_keys = []
for key in keys:
    if key[0]=="/":
        fixed_keys.append(key[1:])
    else:
        fixed_keys.append(key)

if not os.path.isdir("%s/parquet" % path):
    os.makedirs("%s/parquet" % path)


jsondata = pandas.read_json("%s/meta.json" % path)
metadata = jsondata["metadata"].dropna().to_dict() # Reading the metadata

# Making a list of the pipeline filter names

pipeline = []

def makepipeline(pipe):
    pipeline.append(pipe["guiname"])
    if "downstream" in pipe.keys():
        makepipeline(pipe["downstream"]["pipeline"])

makepipeline(jsondata["pipeline"])

# Making the parquet files

for key in fixed_keys:
    data = dd.read_hdf("%s/hdf/%s_0.hdf" % (path, name), key=key)
    for i in range(1, len([name for name in os.listdir('%s/hdf' % path) if os.path.isfile(os.path.join('%s/hdf' % path, name))])):
        new_data = dd.read_hdf("%s/hdf/%s_%s.hdf" % (path, name, (i)), key=key)
        data = dd.concat([data, new_data],interleave_partitions=True).reset_index().set_index("time").repartition(partition_size="100Mb")
    data.to_parquet("%s/parquet/%s.parquet" % (path, key))

    # Finding the history of the filter

    new_pipe = pipeline[:pipeline.index(key) + 1]
    pipelinestr = "_".join(new_pipe)

    # Adding a link to the parquet to the database

    datalayer.Measurements.addDocument(projectName=projectName, desc=metadata, type="OFsimulation", filter=key, pipeline=pipelinestr,
                                       resource="%s/parquet/%s.parquet" % (path, key), dataFormat="parquet")

#   Delete hdf directory
if parser.parse_args().keepHDF:
    shutil.rmtree("%s/hdf" % path, ignore_errors=True)
    shutil.rmtree("%s/meta.json" % path, ignore_errors=True)


