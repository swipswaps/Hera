from .analysis.Plotting import Plotting
import sys
version = sys.version_info[0]
if version==2:
    from .postprocess.extractVTK import VTKpipeline
else:
    from .process.process import process