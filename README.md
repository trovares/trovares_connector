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
python -m pip install trovares_neo4j_connector
```

## Using xgt_neo4j_connector

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

  - get_xgt_schema_for
  - create_xgt_schemas
  - copy_data_from_neo4j_to_xgt
  - transfer_from_neo4j_to_xgt_for
  - transfer_from_xgt_to_neo4j_for
