..
   # -*- coding: utf-8 -*- --------------------------------------------------===#
   #
   #  Copyright 2022 Trovares Inc.
   #
   #  Licensed under the Apache License, Version 2.0 (the "License");
   #  you may not use this file except in compliance with the License.
   #  You may obtain a copy of the License at
   #
   #      http://www.apache.org/licenses/LICENSE-2.0
   #
   #  Unless required by applicable law or agreed to in writing, software
   #  distributed under the License is distributed on an "AS IS" BASIS,
   #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   #  See the License for the specific language governing permissions and
   #  limitations under the License.
   #
   #===----------------------------------------------------------------------===#

.. _odbc:

ODBC Connector
==============

The extra ODBC connector module allows for connecting to databases that support ODBC.
The ODBC connector requires at least xGT 1.11.0.

Installation
------------

You can install the connector with dependencies for ODBC by executing this command:

.. code-block:: bash

   python -m pip install 'trovares_connector[odbc]'

Examples
--------

These examples show typical usage patterns.

Copy a SQL table
^^^^^^^^^^^^^^^^

This example copies test_table from the test database into test_table on xGT.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt(['test_table'])

To map to a specific xGT table type pass a tuple of (SQL table, xGT table):

.. code-block:: python

   conn.transfer_to_xgt([('test_table', 'xgt_table')])

Or as a dictionary:

.. code-block:: python

   conn.transfer_to_xgt([('test_table', {'frame' : 'xgt_table'} )])

Copy a SQL table to vertices
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example copy Person into the vertex frame Person.
The simplest way is to pass the key column as tuple with the table name.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt([('Person', ('ID',))])

The key here may be a column number.
To map to a specific xGT table type, pass a triple tuple of (SQL table, xGT vertex, key tuple):

.. code-block:: python

   conn.transfer_to_xgt([('Person', 'xgt_person', ('ID',))])

Dictionary remap:

.. code-block:: python

   conn.transfer_to_xgt([('Person', { 'frame' : 'xgt_person', 'key' : 'ID' })])

The frame may be omitted in the last example if the frame name is the same as SQL table.

Copy a SQL table to edges
^^^^^^^^^^^^^^^^^^^^^^^^^

This example copy Person into the vertex frame Person and the edge Friend into the edge frame Friend.
The simplest way is to pass the frames and source and target columns as tuple with the table name.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt([('Person', ('ID',)), ('Friend', ('Person', 'Person', 'src_key', 'trg_key'))])

The tuple format for the argument tuple is (Source Frame, Target Frame, Source Key, Target Key)

Similarly, a triple tuple may be given to rename:

.. code-block:: python

   conn.transfer_to_xgt([('Person', ('ID',)), ('Friend', 'xgt_friend', ('Person', 'Person', 'src_key', 'trg_key'))])

Or a dictionary:

.. code-block:: python

   conn.transfer_to_xgt([('Person', ('ID',)), ('Friend', {'frame' : 'xgt_friend', 'source': 'Person', 'target' : 'Person', 'source_key' : 'src_key', 'target_key' : 'trg_key'})])


Copy a SQL table to edges without a vertex table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example will create the corresponding vertex frame for the source and target with a key column named Key using the source and target keys when creating the edge.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt([('Friend', ('Person', 'Person', 'src_key', 'trg_key'))], easy_edges=True)


Appending data
^^^^^^^^^^^^^^

By default a transfer will drop any associated frame on xGT.
To append to a frame, set `append` to True on the transfer.

.. code-block:: python

   conn.transfer_to_xgt(['Person'], append=True)

Connecting to Snowflake
^^^^^^^^^^^^^^^^^^^^^^^

After installing `their ODBC driver <https://docs.snowflake.com/en/user-guide/odbc.html>`_, connect like so:

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string="DSN=snowflake;Database=test;Uid=test;Pwd=test;"
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt([('my_schema.my_table', 'test_table')])

This would transfer the table, `my_table`, in the `my_schema` schema, under the `test` database to the xGT table named `test_table`.

Connecting to MongoDB
^^^^^^^^^^^^^^^^^^^^^

This example uses MongoDB 5 with CData's MongoDB ODBC driver.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, MongoODBCDriver

   connection_string="DSN=MongoDB;Database=test;Uid=test;Pwd=test;"
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = MongoODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_xgt([('my_collection', 'test_table')])

This would transfer the collection, `my_collection`, under the `test` database to the xGT table named `test_table`.
The driver provides an parameter, include_id, that by default is false.
This parameter removes the _id field returned by the driver.
If this field is present when saving to MongoDB, it will replace documents with corresponding _ids in the database.

Transferring data from xGT to ODBC
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example copies test_table from xGT to a test database.

.. code-block:: python

   import xgt
   from trovares_connector import ODBCConnector, SQLODBCDriver

   connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('odbc')
   odbc_server = SQLODBCDriver(connection_string)
   conn = ODBCConnector(xgt_server, odbc_server)

   conn.transfer_to_odbc(tables=['test_table'])

To map to a specific xGT table type pass a tuple of (xGT frame, SQL table):

.. code-block:: python

   conn.transfer_to_odbc(tables=[('xgt_table', 'sql_table')])

Parameters for transferring edges and vertices exist as well.
Some limitations exist.
See below for more details.

Additional Examples
^^^^^^^^^^^^^^^^^^^

More detailed examples can be found in :ref:`jupyter` or on github:

* `Python Examples <https://github.com/trovares/trovares_connector/tree/main/examples/odbc>`_
* `Jupyter Notebooks <https://github.com/trovares/trovares_connector/tree/main/jupyter/odbc>`_

Limitations
-----------

* Transferring null to a database from xGT is not supported.
* When transferring to a database, the table must already be created.
* Transfer sizes/times are estimates and may not be available.

API Details
-----------

.. currentmodule:: trovares_connector

.. autosummary::
  :toctree:

  ODBCConnector
  SQLODBCDriver
  MongoODBCDriver
