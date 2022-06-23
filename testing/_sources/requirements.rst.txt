.. _requirements:

Requirements
============

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
   Available as an option to be turned on with the connector.
   Does not support nodes without labels.
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

