Listing metadata
----------------

In order to list metadata documents according to a wanted query you should use the CLI(command line interface) with the following command:

hera-datalayer list <query>


Example
=======

For example in order to list all the experimental metadata of station 'Check_Post' in project 'Haifa':

hera-datalayer list projectName="'Haifa'" type="'meteorological'" station="'Check_Post'"