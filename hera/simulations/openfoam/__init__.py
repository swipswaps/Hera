from .process.process import process
import sys
version = sys.version_info[0]
if version==2:
    from .postprocess.extractVTK import VTKpipeline