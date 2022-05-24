.. xgt_neo4j_connector documentation master file, created by
   sphinx-quickstart on Fri Apr 29 15:54:24 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 1
   :numbered:


xgt_neo4j_connector Package
===========================

This package is for connecting the Trovares xGT graph analytics engine with the Neo4j graph database.

The `source code <http://github.com/trovares/xgt_neo4j_connector/>`_ is available on github.

Requirements
------------

* `Neo4j Python package <https://pypi.org/project/neo4j/>`_
* `xGT Python package <https://pypi.org/project/xgt/>`_

These can be installed through pip:

.. code-block:: bash

   python -m pip install xgt neo4j

Optional
--------

* `APOC <https://github.com/neo4j-contrib/neo4j-apoc-procedures>`_
   Improves schema querying.
   Automatically used if installed as a plugin for Neo4j.
* `Py2neo Python package <https://pypi.org/project/py2neo/>`_
   Alternative Neo4j driver that provides http or bolt connections.
   Transfers are 2X as fast, but the memory requirements can be excessive for large transfers.
   This can be selected via the driver parameter in the connector.
* `Neo4j-arrow plugin and Python package <https://github.com/neo4j-field/neo4j-arrow>`_
   Alternative driver for transfers that is very experimental.
   This requires GDS and the jar plugin found in the above link to be installed as part of Neo4j.
   In addition it requires the neo4j-arrow python package found in the above link.
   At the moment, this provides very fast transfer speeds, but is limited to only int and string data types (Nulls do not work for these types).
   This can be selected via the driver parameter in the connector.

Installation
------------

You can install this python package by executing this command:

.. code-block:: bash

   python -m pip install -e git+https://github.com/trovares/xgt_neo4j_connector.git#egg=xgt_neo4j_connector


Using xgt_neo4j_connector
-------------------------

From any Python environment, simply importing both `xgt` and `xgt_neo4j_connector` is all that is needed to operate this connector.

.. code-block:: python

   import xgt
   from xgt_neo4j_connector import Neo4jConnector



Examples
========

These examples show typical usage patterns.

Copy entire graph from Neo4j to Trovares xGT
--------------------------------------------

This example uses two properties of the connector object that return a list of all of the node labels (vertex types) and all of the relationshp types (edge types) in the neo4j data.
All of these data frames are created in Trovares xGT and then all of the data is copied from neo4j to xGT.

.. code-block:: python

   import xgt
   from xgt_neo4j_connector import Neo4jConnector

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   conn = Neo4jConnector(xgt_server, neo4j_auth=('neo4j', 'foo'))

   conn.transfer_from_neo4j_to_xgt_for(vertices = conn.neo4j_node_labels,
                                       edges = conn.neo4j_relationship_types)


Copy a portion of a graph based on node labels and/or relationship types
------------------------------------------------------------------------

In this example, only some of the node labels (vertex types) and some of the relationship types (edge types) are copied into Trovares xGT.
Using this idiom requires knowing some schema information about the graph data stored in Neo4j.

.. code-block:: python

   import xgt
   from xgt_neo4j_connector import Neo4jConnector

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   conn = Neo4jConnector(xgt_server, neo4j_auth=('neo4j', 'foo'))

   nodes_to_copy = ['Person']
   edges_to_copy = ['KNOWS']
   conn.transfer_from_neo4j_to_xgt_for(vertices = nodes_to_copy,
                                       edges = edges_to_copy)


Limitations
===========

Doesn't support the following:

* Multiple node types for a single relationship type.
* Multiple types for a single property.
* Point data type.
* Transferring data from xGT to Neo4j.

Other limitations:

* Duration data type is converted into an integer representing nanoseconds.

API Details
===========

.. currentmodule:: xgt_neo4j_connector

.. automodule:: xgt_neo4j_connector
  :no-members:
  :no-inherited-members:
  :noindex:

.. autosummary::
  :toctree:

  Neo4jConnector

