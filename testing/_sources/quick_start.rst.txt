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

.. code-block:: python

   import xgt
   from trovares_connector import Neo4jConnector, Neo4jDriver

   xgt_server = xgt.Connection()
   xgt_server.set_default_namespace('neo4j')
   neo4j_server = Neo4jDriver(auth=('neo4j', 'foo'))
   conn = Neo4jConnector(xgt_server, neo4j_server)

   # Transfer the whole graph.
   conn.transfer_to_xgt()
   query = "match(a) return a"
   job = conn.run_job(query)
   print("Results: " + ','.join(str(row[0]) for row in job.get_data()))
