import unittest
import time

import neo4j
import xgt
from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  def setup_class(cls):
    cls.xgt = xgt.Connection()
    cls.xgt.drop_namespace('test', force_drop = True)
    cls.xgt.set_default_namespace('test')
    cls.create_connector()

  def teardown_class(cls):
    del cls.neo4j
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()

  def create_connector(self, max_retries = 20):
    retries = max_retries
    while retries >= 0:
      try:
        self.neo4j = Neo4jConnector(self.xgt)
      except (neo4j.exceptions.ServiceUnavailable):
        if retries == 0:
          raise
        retries -= 1
        time.sleep(5)
