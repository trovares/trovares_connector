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
    # Create a connector, retrying until neo4j becomes ready.
    retries = 20
    while retries >= 0:
      try:
        cls.neo4j = Neo4jConnector(cls.xgt, neo4j_auth=('neo4j', 'foo'))
      except (neo4j.exceptions.ServiceUnavailable):
        if retries == 0:
          raise
        retries -= 1
        time.sleep(5)
    # Create a connection to neo4j (via python driver).
    cls.neo4j_driver = cls.neo4j.neo4j_driver

  def teardown_class(cls):
    del cls.neo4j
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()

  def test_neo4j(self):
    driver = self.neo4j_driver
    assert isinstance(driver, neo4j.Neo4jDriver)

  def xgt_free_memory(self):
    return self.xgt.free_user_memory_size