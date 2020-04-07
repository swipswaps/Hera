Loading metadata to the database
----------------------------

There are 2 ways to load metadata to the database:

1. Through python code
2. Through CLI(command line interface)

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


2. Command line interface
=========================

First, open terminal.
After that, you should use the command hera-datalayer with the load option as shown below:

hera-datalayer load docsToLoad.json

docsToload.json should contain the metadata to load, and should be in the form of:

{
"Measurements": [{"projectName='myProject',
                  "resource='path-to-parquet-files',
                  "dataFormat='parquet',
                  "type='meteorology'
                  "desc={"property1": "value1",
                         "property2": "value2"
                         }
                  },
                 measurementsDoc2,
                 .
                 .
                 .
                 ],
"Simulations": [simulationsDoc1,
                simulationsDoc2,
                .
                .
                .
                ],
"Analysis": [analysisDoc1,
             analysisDoc2,
             .
             .
             .
             ]
}


Note that a metadata document should contain the following keys:
1. "projectName"
2. "resource"
3. "dataFormat"
4. "type"
5. "desc"