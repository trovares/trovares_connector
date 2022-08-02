Release Notes
=============

1.5.1 (07-29-2022)
------------------

New Features
^^^^^^^^^^^^
  - Added null support when writing to ODBC applications.

1.5.0 (07-29-2022)
------------------

New Features
^^^^^^^^^^^^
  - Added query translator for Neo4j connector to better support running queries on data with multiple labels.

1.4.0 (07-26-2022)
------------------

New Features
^^^^^^^^^^^^
  - Added support for transferring data from xGT to applications that support ODBC.
  - Add MongoDB driver for improved MongoDB support.

Changed
^^^^^^^
  - Can now transfer tables/databases via ODBC without data.

Fixed
^^^^^
  - Fixed tables not dropping automatically on transfer for ODBC.

1.3.1 (07-11-2022)
------------------
Fixed
^^^^^
  - Fixed odbc import issue if ODBC not installed.

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
