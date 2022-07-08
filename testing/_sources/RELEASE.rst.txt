Release Notes
=============

1.3.0 (07-08-2022)
------------------
New Features
^^^^^^^^^^^^
  - Added ODBC connector for transferring from applications that support ODBC to xGT.

1.2.1 (07-01-2022)
------------------
Fixed
^^^^^
  - Fixed python dependencies not installing on pip install.

1.2.0 (06-24-2022)
------------------

New Features
^^^^^^^^^^^^
  - Added support for unlabeled nodes.
  - Added support for mapping Neo4j labels and types to xGT.
  - Added option to disable auto-downloading edges' source and target vertices.

Changed
^^^^^^^
  - Improved download estimates for single relationships with multiple nodes.
  - Rename disable_apoc to enable_apoc.
  - Endpoints are now returned as a tuple of source and target instead of a string.
  - Documentation improvements.

1.1.0 (06-17-2022)
------------------

New Features
^^^^^^^^^^^^
  - Added support for point and list data types.

Changed
^^^^^^^
  - Documentation improvements.

Fixed
^^^^^
  - Transferring empty frame/graph causes divide by 0.
  - When transferring to Neo4j from xGT use the default namespace when all values are None.

1.0.0 (06-13-2022)
------------------

New Features
^^^^^^^^^^^^
  - Initial Release.
  - Added support for transferring graph data from Neo4j to xGT.
  - Added support for transferring graph data from xGT to Neo4j.
  - Provided methods for querying Neo4j's data schema.
