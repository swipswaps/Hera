from .... import GIS
import matplotlib.pyplot as plt

class Plotting():
    _projectName = None
    _GISdatalayer = None

    def __init__(self, projectName):

        self._projectName = projectName
        self._GISdatalayer = GIS.GIS_datalayer(projectName=self._projectName, FilesDirectory="")

    def variableAgainstDistance(self, data, variable, colors=["red", "blue"], signedColors=["blue", "orange", "green", "red"], signedDists=None, labels=None, topography=True, ylim=None, ax=None):

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        data = data.sort_values(by="distance")
        labels = labels if labels is not None else ["Distance", variable, "Terrain"]

        ax.plot(data.distance, data[variable], color=colors[0])
        ax.tick_params(axis='y', labelcolor=colors[0])
        ax.set_xlabel(labels[0])
        ax.set_ylabel(labels[1], color=colors[0])
        if topography:
            ax2 = ax.twinx()
            ax2.plot(data.distance, data.base, color=colors[1])
            ax2.tick_params(axis='y', labelcolor=colors[1])
            ax2.set_ylabel(labels[2], color=colors[1])
        if ylim is not None:
            ax.set_ylim(ylim)
        if signedDists is not None:
            for i in range(len(signedDists)):
                ax2.plot([signedDists[i],signedDists[i]], [data.z.min(), data.z.max()], color=signedColors[i], linestyle="--")

    def variableAgainstHeight(self, data, variable, nOfPoints=4, labels=None, ax=None):

        if ax is None:
            fig, ax = plt.subplots()
        else:
            plt.sca(ax)

        labels = labels if labels is not None else ["Height (m)", variable]

        optional = []
        dists = []
        for d in data.distance.drop_duplicates():
            if len(data.query("distance==@d")) > 20:
                optional.append(d)
        delta = int(len(optional)/(nOfPoints+1))
        optional.sort()
        data = data.sort_values(by="height")
        for i in range(nOfPoints):
            dist = optional[delta*(i+1)]
            dists.append(dist)
            ax.plot(data.query("distance==@dist").height, data.query("distance==@dist")[variable], label=int(dist))
        ax.legend()
        ax.set_xlabel(labels[0])
        ax.set_ylabel(labels[1])

        return dists