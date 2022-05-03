import unittest
import time

import neo4j
import xgt
from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  def setup_class(cls):
    # Create a connection to Trovares xGT
    cls.xgt = xgt.Connection()
    cls.xgt.drop_namespace('test', force_drop = True)
    cls.xgt.set_default_namespace('test')
    cls.neo4j = cls._setup_connector()
    cls.neo4j_driver = cls.neo4j.neo4j_driver
    return

  def teardown_class(cls):
    del cls.neo4j
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  @classmethod
  def _setup_connector(cls, retries = 20):
    try:
      conn = Neo4jConnector(cls.xgt, neo4j_auth=('neo4j', 'foo'))
      return conn
    except (neo4j.exceptions.ServiceUnavailable):
      print(f"Neo4j Unavailable, retries = {retries}")
      if retries > 0:
        time.sleep(3)
        return cls._setup_connector(retries - 1)
    conn = Neo4jConnector(cls.xgt, neo4j_auth=('neo4j', 'foo'))
    return conn

  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()

  def test_neo4j_properties(self):
    assert isinstance(self.neo4j_driver, neo4j.Neo4jDriver)
    rel = self.neo4j.neo4j_relationship_types
    assert isinstance(rel, list)
    assert len(rel) == 0
    labels = self.neo4j.neo4j_node_labels
    assert isinstance(labels, list)
    assert len(labels) == 0
    props = self.neo4j.neo4j_property_keys
    assert isinstance(props, list)
    assert len(props) == 0
    props = self.neo4j.neo4j_node_type_properties
    assert isinstance(props, list)
    assert len(props) == 0
    props = self.neo4j.neo4j_rel_type_properties
    assert isinstance(props, list)
    assert len(props) == 0
    nodes = self.neo4j.neo4j_nodes
    assert isinstance(nodes, dict)
    assert len(nodes) == 0
    edges = self.neo4j.neo4j_edges
    assert isinstance(edges, dict)
    assert len(edges) == 0

  def gt_free_memory(self):
    return self.xgt.free_user_memory_size
