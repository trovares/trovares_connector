# trovares_connector Package

[![CI](https://github.com/trovares/trovares_connector/actions/workflows/pytest.yml/badge.svg)](https://github.com/trovares/trovares_connector/actions/workflows/pytest.yml)
[![Available on Pypi](https://img.shields.io/pypi/v/trovares_connector)](https://pypi.python.org/pypi/trovares_connector)
[![Pypi Versions](https://img.shields.io/pypi/pyversions/trovares_connector)](https://pypi.python.org/pypi/trovares_connector)
[![License](https://img.shields.io/github/license/trovares/trovares_connector)](https://github.com/trovares/trovares_connector/blob/main/LICENSE)
[![Twitter Follow](https://img.shields.io/twitter/follow/TrovaresxGT)](https://twitter.com/TrovaresxGT)

This Python package is for connecting the Trovares xGT graph analytics engine to other applications.
Trovares xGT can [significantly speedup Neo4j queries](https://www.trovares.com/trovaresvneo4j).

The default connector provided is for Neo4j or AuraDB.
The package also provides an optional ODBC connector for connecting to databases or applications that support ODBC.
Information about the ODBC connector can be found [in the documentation](https://trovares.github.io/trovares_connector/odbc). 

<table>
  <tr>
    <td><b>Homepage:</b></td>
    <td><a href="https://www.trovares.com">trovares.com</a></td>
  </tr>
  <tr>
    <td><b>Documentation:</b></td>
    <td><a href="https://trovares.github.io/trovares_connector/">trovares.github.io/trovares_connector</a></td>
  </tr>
  <tr>
    <td><b>General Help:</b></td>
    <td><a href="https://github.com/trovares/trovares_connector/discussions">github.com/trovares/trovares_connector/discussions</a></td>
  </tr>
</table>

## Requirements

 - [Neo4j Python](https://pypi.org/project/neo4j/)
 - [xGT Python](https://pypi.org/project/xgt/)
 - [Pyarrow](https://pypi.org/project/pyarrow/)
 - [Trovares xGT](https://www.trovares.com)

## Installation

You can install this python package by executing this command:

```bash
python -m pip install trovares_connector
```

If you want to use the ODBC connector, you can install the optional dependencies like so:
```bash
python -m pip install 'trovares_connector[odbc]'
```

If you don't have Trovares xGT, you can install and run the [Developer version](https://hub.docker.com/r/trovares/xgt) from Docker:

```bash
docker pull trovares/xgt
docker run --publish=4367:4367 trovares/xgt
```
## Using the trovares_connector

From any Python environment, simply importing both `xgt` and `trovares_connector` is all that is needed to operate this connector.

A simple example below shows connecting to Neo4j and xGT, transferring the whole graph database to xGT, running a query in xGT, and printing the results:

```python
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
```

## API

The available properties are:

  - neo4j_relationship_types
  - neo4j_node_labels
  - neo4j_property_keys
  - neo4j_node_type_properties
  - neo4j_rel_type_properties
  - neo4j_nodes
  - neo4j_edges

The available methods are:

  - get_xgt_schemas
  - create_xgt_schemas
  - copy_data_to_xgt
  - transfer_to_xgt
  - transfer_to_neo4j

## Examples

Some examples can be found here:

  - [Python examples](https://github.com/trovares/trovares_connector/tree/main/examples)
  - [Jupyter notebooks](https://github.com/trovares/trovares_connector/tree/main/jupyter)
