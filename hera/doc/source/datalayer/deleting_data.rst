Deleting metadata from the database
-------------------------------

In order to delete metadata from the database you should use the CLI(command line interface).

There are 2 steps :
1. Query the metadata you want to delete.
2. Delete the result of the query.

1. Query the metadata
=====================

In order to query the metadata from the database you should use the following command:

hera-datalayer delete <query>

The query should filter the metadata documents you want to delete.
After you execute the command, it will save a json file called 'docsToDelete.json' with those metadata documents to your current directory.

Note that this json file is in the same format as the json for loading metadata through the CLI.

2. Delete the results of the query
==================================

In order to delete the results of the query you should use the following CLI command:

hera-datalayer delete <JSON>

The <JSON> should be the json file of the results of the query which called 'docsToDelete.json' unless you changed its name.



Example
=======

For example in order to delete all the experimental metadata of station 'Check_Post' in project 'Haifa' :

1. Query the metadata
*********************

hera-datalayer delete projectName="'Haifa'" type="'meteorological'" station="'Check_Post'"

2. Delete the results of the query
**********************************

hera-datalayer delete docsToDelete.json
