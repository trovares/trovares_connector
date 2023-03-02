..
   # -*- coding: utf-8 -*- --------------------------------------------------===#
   #
   #  Copyright 2022-2023 Trovares Inc.
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

.. _requirements:

Requirements
============

* `Neo4j Python package <https://pypi.org/project/neo4j/>`_ >= 4.4.1
* `xGT Python package <https://pypi.org/project/xgt/>`_ >= 1.10.0
* `Pyarrow package <https://pypi.org/project/pyarrow/>`_ >= 7.0.0
* `Trovares xGT <https://www.trovares.com>`_ >= 1.10.0

The Python packages can be installed through pip:

.. code-block:: bash

   python -m pip install xgt neo4j pyarrow

If you don't have Trovares xGT, you can install and run the `Developer version <https://hub.docker.com/r/trovares/xgt>`_ from Docker:

.. code-block:: bash

   docker pull trovares/xgt
   docker run --publish=4367:4367 trovares/xgt

Optional
--------

* `APOC <https://github.com/neo4j-contrib/neo4j-apoc-procedures>`_
   Improves schema querying.
   Available as an option to be turned on with the connector.
* `Py2neo Python package <https://pypi.org/project/py2neo/>`_
   Alternative Neo4j driver that provides http or bolt connections.
   Transfers are 2X faster, but the memory requirements can be excessive for large transfers.
   This can be selected via the driver parameter in the Trovares Neo4jDriver class.

ODBC
^^^^

To use the optional ODBC connection features, the following packages are required:

* `Arrow ODBC <https://pypi.org/project/arrow_odbc/>`_
* `Pandas <https://pypi.org/project/pandas/>`_
* ODBC Driver for your database and an ODBC Driver manager such unixODBC.
