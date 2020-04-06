Datalayer Architecture
======================

The datalayer module abstracts the access to the resource of the user.


Each record represents a certain piece of data and thereofre it holds the metadata that describes it
and a reference to the data itself.

The metadata is determined by the user that added that data and stored in the __desc__ property

The data is stores in the __resource__ attribute and its format is stored in the 'dataFormat'
property. A data can be either a link to a file on the disk, or the data itself.

The dataFormat determine how the data is retrived when using the getData function of the record.

When a user adds a data to the database, he describes the metadata and the data itself.
A data that is stored in the database can be either a file or the data itself.


10-minute tutorial
------------------

This tutorial demonstrate how to make some basic actions using the datalayer.

The 'Hera' project aims to make the departmental data more accessible and to standardize its storage
An item in the database is called a 'document' and it stores all the information about the data
Each data is associated with a project and is stored in the 'document' under the 'projectName' category.
The document also contains a data description under the 'desc' dictionary which should hold any information that could be relevant to use when querying,
the data format under 'dataFormat', and a pointer to  the data location itself in the 'resource' property.

Adding new document to the Metadata collection:
*************************

first, import the requested module:

.. code-block:: python

    from hera import datalayer

Each data is classified into one of the following categories.

- Measurements - Any acquisition of data from the 'real world'. Satellites, meteorological measurments and dispersion measurements and ect.
- Simulations  - Any output of a model. (OpenFOAM, WRF, LSM and ect).
- Analysis     - Any data that is created during work and analyis and needed to be cached to accelerate the computations.

Since each category can hold different types of data, each data document
holds its type in  a 'type' property
when saving a new document in the database, you must provide 'projectName', 'desc', 'dataFormat' and 'resource'

for example, a Measurements data from type Meteorology and parquet data format should be added like this:

.. code-block:: python

    datalayer.Measurements.addDocument(projectName='myProject',
                                       resource='path-to-parquet-files',
                                       dataFormat='parquet',
                                       type='Meteorology'
                                       desc={'property1': 'value1',
                                             'property2': 'value2'
                                             }
                                       )

Retrieving data
***************

When reading from the database, you have to provide the 'projectName' and create a query using the function 'getDocuments'.
First, you get all of the documents in the project that property1 equals 'value1'

.. code-block:: python

    docList = datalayer.Measurements.getDocuments(projectName='projectName',property1='value1')

Now, you can use the function getData() to retrieve data presented by the document.
For example, we took the first document from our docList and got its data.

.. code-block:: python

    doc = docList[0]
    data = doc.getData()

MongoDB query language
***********************





Reference to code
-----------------

.. toctree::
    :maxdepth: 2

    collection
    datahandlers


