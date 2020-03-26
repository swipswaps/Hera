from ... import datalayer
import matplotlib.pyplot as plt


def loadImage(projectName, path, locationName, extents):
    """
    Loads an image to the database.

    :param projectName: The project name
    :param path: The image path
    :param locationName: The location name
    :param extents: The extents of the image [left, right, bottom, top]
    :return:
    """
    doc = dict(projectName='',
               resource=path,
               dataFormat='image',
               type='GIS',
               desc=dict(locationName=locationName,
                         left=extents[0],
                         right=extents[1],
                         bottom=extents[2],
                         top=extents[3]
                         )
               )
    datalayer.Measurements.addDocument(**doc)


def plotImageLocationFromDocument(doc, ax=None):
    """
    Plots an image from a document

    :param doc: The document with the image metadata
    :param ax: The ax to plot to
    :return:
    """
    if ax is None:
        fig = plt.figure()

    path = doc.resource
    extents = [doc.desc['right'], doc.desc['left'], doc.desc['bottom'], doc.desc['top']]

    plt.imshow(path, extents=extents)
    ax = fig.axes[0]

    return fig, ax


def plotImageLocation(projectName, locationName, ax=None, **query):
    """
    Plots an image of the location called [locationName]

    :param projectName: The projectName
    :param locationName: The location name
    :param query: Some more specific details to query on
    :return:
    """
    doc = datalayer.Measurements.getDocuments(projectName=projectName,
                                              dataFormat='image',
                                              type='GIS',
                                              locationName=locationName,
                                              **query
                                              )
    if doc:
        raise ValueError('No documents for those requirements')
    elif len(doc) > 1:
        raise ValueError('More than 1 documents fills those requirements')

    doc = doc[0]

    return plotImageLocationFromDocument(doc=doc, ax=ax)