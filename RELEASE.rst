Trovares Connector Release Notes
================================

1.1.0 (06-17-2022)
------------------

New Features
^^^^^^^^^^^^
  - Add support for point and list data types.

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
  - Add support for transferring graph data from Neo4j to xGT.
  - Add support for transferring graph data from xGT to Neo4j.
  - Provide methods for querying Neo4j's data schema.
