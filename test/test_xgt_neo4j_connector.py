import unittest

import xgt
from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  def setup_class(cls):
    cls.xgt = xgt.Connection()
    cls.xgt.set_default_namespace('test')

  def teardown_class(cls):
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()
#   c = Neo4jConnector(x)

