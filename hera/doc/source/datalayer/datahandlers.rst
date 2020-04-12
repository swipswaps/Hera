Data handlers
==============

Each record in the mongodb represents a single item of data. In order to
simplify the loading procedure, we keep track of the data format (which is determined by the user when it is created).

The following data handlers are implemented:

- string :py:class:`hera.datalayer.datahandler.DataHandler_string`
- time   :py:class:`hera.datalayer.datahandler.DataHandler_time`
- HDF
- dict
- netcdf_xarray
- JSON_dict
- JSON_pandas
- geopandas
- parquet


string
------
.. autoclass:: hera.datalayer.datahandler.DataHandler_string
    :members:
    :undoc-members:

time
-----
.. autoclass:: hera.datalayer.datahandler.DataHandler_time
    :members:
    :undoc-members:

HDF
---
.. autoclass:: hera.datalayer.datahandler.DataHandler_HDF
    :members:
    :undoc-members:

dict
----
.. autoclass:: hera.datalayer.datahandler.DataHandler_dict
    :members:
    :undoc-members:

