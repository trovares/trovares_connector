# xgt_neo4j_connector Package

This package is for connecting the Trovares xGT graph analytics engine with the Neo4j graph database.

Detailed documentation is available [here](https://trovares.github.io/xgt_neo4j_connector/).

## Requirements

 - [Neo4j Python](https://pypi.org/project/neo4j/)
 - [xGT Python](https://pypi.org/project/xgt/)
 - [Pyarrow](https://pypi.org/project/pyarrow/)

## Installation

You can install this python package by executing this command:

```bash
python -m pip install -e git+https://github.com/trovares/xgt_neo4j_connector.git#egg=xgt_neo4j_connector
```

## Using the xgt_neo4j_connector

From any Python environment, simply importing both `xgt` and `xgt_neo4j_connector` is all that is needed to operate this connector.

```python
import xgt
from xgt_neo4j_connector import Neo4jConnector
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

  - [Python examples](https://github.com/trovares/xgt_neo4j_connector/tree/main/examples)
  - [Jupyter notebooks](https://github.com/trovares/xgt_neo4j_connector/tree/main/jupyter)
