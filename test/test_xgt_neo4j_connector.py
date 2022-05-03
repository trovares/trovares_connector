import unittest

from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  def test_one(self) -> None:
    print("Test #1")
    with self.assertRaises(Exception):
      c = Neo4jConnector()

