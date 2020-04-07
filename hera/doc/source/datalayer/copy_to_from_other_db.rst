Copy metadata documents To/From other database
----------------------------------------------

In order to copy metadata documents to/from others databases you should use the CLI(command line interface) with the following commands:

1. Copy to
==========

hera-datalayer copyTo <others database login info> <query>

2. Copy from
============

hera-datalayer <others database login info> <query>


<others databse login info> should be in the following format:

username:password@IP/dbName



username, password and dbName belongs to the person you want to copy from/to.
IP is the ip where the database.