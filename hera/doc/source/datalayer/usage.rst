1. Python code
==============

First, import the hera package:

.. code-block:: python

    from hera import datalayer


After that, you should choose the desired collection to load the metadata to and use the addDocument function.
For example, we load metadata to the Measurements collection as shown below:

.. code-block:: python

    datalayer.Measurements.addDocument(projectName='myProject',
                                       resource='path-to-parquet-files',
                                       dataFormat='parquet',
                                       type='meteorology'
                                       desc={'property1': 'value1',
                                             'property2': 'value2'
                                             }
                                       )

