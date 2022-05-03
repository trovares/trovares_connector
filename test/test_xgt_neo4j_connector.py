import unittest

import xgt
from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()
    x = xgt.Connection()
    c = Neo4jConnector(x)

