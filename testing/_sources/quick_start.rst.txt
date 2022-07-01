.. _quick_start:

Quick Start
===========

Installing
----------

In a seperate terminal install the `Developer version of xGT with Docker <https://hub.docker.com/r/trovares/xgt>`_ and run it:

.. code-block:: bash

   docker pull trovares/xgt
   docker run --publish=4367:4367 trovares/xgt

In another terminal install the connector:

.. code-block:: bash

   python -m pip install trovares_connector

Run a query
-----------

A simple example below shows connecting to Neo4j and xGT, transferring the whole graph database to xGT, running a query in xGT, and printing the results:

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector, Neo4jDriver

   # Connect to xGT and Neo4j.
   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   neo4j_server = Neo4jDriver(auth=('neo4j', 'foo'))
   conn = Neo4jConnector(xgt_server, neo4j_server)

   # Transfer the whole graph.
   conn.transfer_to_xgt()

   # Run the query.
   query = "match(a:foo) return a"
   job = xgt_server.run_job(query)

   # Print results.
   print("Results: ")
   for row in job.get_data():
       print(row)
