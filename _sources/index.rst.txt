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

.. trovares_connector documentation master file, created by
   sphinx-quickstart on Fri Apr 29 15:54:24 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 1
   :numbered:


trovares_connector Package
===========================

This package is for connecting the Trovares xGT graph analytics engine with the Neo4j graph database.

The `source code <http://github.com/trovares/trovares_connector/>`_ is available on github.

Requirements
------------

* `Neo4j Python package <https://pypi.org/project/neo4j/>`_
* `xGT Python package <https://pypi.org/project/xgt/>`_
* `Pyarrow package <https://pypi.org/project/pyarrow/>`_

These can be installed through pip:

.. code-block:: bash

   python -m pip install xgt neo4j pyarrow

Optional
--------

* `APOC <https://github.com/neo4j-contrib/neo4j-apoc-procedures>`_
   Improves schema querying.
   Automatically used if installed as a plugin for Neo4j.
* `Py2neo Python package <https://pypi.org/project/py2neo/>`_
   Alternative Neo4j driver that provides http or bolt connections.
   Transfers are 2X faster, but the memory requirements can be excessive for large transfers.
   This can be selected via the driver parameter in the Trovares Neo4jDriver class.
* `Neo4j-arrow plugin and Python package <https://github.com/neo4j-field/neo4j-arrow>`_
   Alternative driver for transfers that is very experimental.
   This requires GDS and the jar plugin found in the above link to be installed as part of Neo4j.
   In addition it requires the neo4j-arrow python package found in the above link.
   At the moment, this provides very fast transfer speeds, but is limited to only int and string data types (Nulls do not work for these types).
   This can be selected via the driver parameter in the Trovares Neo4jDriver class.

Installation
------------

You can install this python package by executing this command:

.. code-block:: bash

   python -m pip install trovares_connector


Using the trovares_connector
-----------------------------

From any Python environment, simply importing both `xgt` and `trovares_connector` is all that is needed to operate this connector.

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector

Examples
========

These examples show typical usage patterns.

Copy entire graph from Neo4j to Trovares xGT
--------------------------------------------

This example uses two properties of the connector object that return a list of all of the node labels (vertex types) and all of the relationship types (edge types) in the Neo4j data.
All of these data frames are created in Trovares xGT and then all of the data is copied from Neo4j to xGT.

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector, Neo4jDriver

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   neo4j_server = Neo4jDriver(auth=('neo4j', 'foo'))
   conn = Neo4jConnector(xgt_server, neo4j_server)

   conn.transfer_to_xgt(vertices=conn.neo4j_node_labels,
                        edges=conn.neo4j_relationship_types)


Copy a portion of a graph based on node labels and/or relationship types
------------------------------------------------------------------------

In this example, only some of the node labels (vertex types) and some of the relationship types (edge types) are copied into Trovares xGT.
Using this idiom requires knowing some schema information about the graph data stored in Neo4j.

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector, Neo4jDriver

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   neo4j_server = Neo4jDriver(auth=('neo4j', 'foo'))
   conn = Neo4jConnector(xgt_server, neo4j_server)

   nodes_to_copy = ['Person']
   edges_to_copy = ['KNOWS']
   conn.transfer_to_xgt(vertices=nodes_to_copy, edges=edges_to_copy)

Using various Neo4j drivers
---------------------------

The connector supports passing a trovares_connector.Neo4jDriver, neo4j.BoltDriver, or a neo4j.Neo4jDriver.
The Trovares Neo4jDriver provides support for connecting to the Neo4j server through a combination of choices such as http, arrow, bolt, or other drivers.
These additional drivers can provide much faster performance than the default neo4j.Neo4jDriver, but may require the optional components explained above.

Some examples of connecting:

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector, Neo4jDriver
   from neo4j import GraphDatabase

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')

   neo4j_driver = GraphDatabase.driver("bolt://localhost", auth=('neo4j', 'foo'))

   # Using Neo4j's Python driver.
   conn = Neo4jConnector(xgt_server, neo4j_driver)

   # Using Neo4j's Python driver with a specific database.
   conn = Neo4jConnector(xgt_server, (neo4j_driver, 'my_database'))

   # Using the Trovares Neo4j driver with bolt.
   neo4j_driver = Neo4jDriver(auth=('neo4j', 'foo'))
   conn = Neo4jConnector(xgt_server, neo4j_driver, database='my_database')

   # Using the Trovares Neo4j driver with py2neo http.
   neo4j_driver = Neo4jDriver(auth=('neo4j', 'foo'), driver='py2neo-http')
   conn = Neo4jConnector(xgt_server, neo4j_driver)

   # Using the Trovares Neo4j driver with arrow.
   neo4j_driver = Neo4jDriver(auth=('neo4j', 'foo'), driver='neo4j-arrow')
   conn = Neo4jConnector(xgt_server, neo4j_driver)

These additional connectors will connect to Neo4j with a combination of connections currently and may have some limitations.

Additional Examples
-------------------

More detailed examples can be found here:

* `Python Examples <https://github.com/trovares/trovares_connector/tree/main/examples>`_
* `Jupyter Notebooks <https://github.com/trovares/trovares_connector/tree/main/jupyter>`_

Limitations
===========

Doesn't support the following:

* Multiple types for a single property.
* Untyped vertices.

Other limitations:

* Duration data type is converted into an integer representing nanoseconds.
* Multiple labels on a single node will translate into multiple nodes in xGT.
* Multiple node types for a single relationship type will translate the single relationship into multiple distinct relationship types.
  See below for more details.
* Point data type is converted into a list.

Single relationship with multiple node conversion
-------------------------------------------------

xGT is schema based and requires an edge to have distinct types.
Neo4j is schema-less so can have multiple node types per edge type.
In the case where the are multiple node types for single edge type, the connector will convert the single edge type in Neo4j into multiple edge types in xGT.
xGT will take the original edge type and create edge types in the format: source_label + _ + edge_label + _ target_label.
This only occurs for these cases.

An example would be the following schema in Neo4j:

.. code-block:: cypher

  (:A)-[:PART_OF]->(:B)-[:PART_OF]->(:C)-[:PART_OF]->(:D)

Would get converted to the following when transferring to xGT:

.. code-block:: cypher

  (:A)-[:A_PART_OF_B]->(:B)-[:B_PART_OF_C]->(:C)-[:C_PART_OF_D]->(:D)

In addition, multiple labels on a single node will cause a similar behavior:

.. code-block:: cypher

  (:A:B)-[:PART_OF]->(:C)

Would get converted to the following when transferring to xGT:

.. code-block:: cypher

  (:A)-[:A_PART_OF_C]->(:C), (:B)-[:B_PART_OF_C]->(:C)

API Details
===========

.. currentmodule:: trovares_connector

.. automodule:: trovares_connector
  :no-members:
  :no-inherited-members:
  :noindex:

.. autosummary::
  :toctree:

  Neo4jConnector
  Neo4jDriver

