import unittest
import time

import neo4j
import xgt
from xgt_neo4j_connector import Neo4jConnector

class TestXgtNeo4jConnector(unittest.TestCase):
  @classmethod
  def setup_class(cls):
    # Create a connection to Trovares xGT
    cls.xgt = xgt.Connection()
    cls.xgt.drop_namespace('test', force_drop = True)
    cls.xgt.set_default_namespace('test')
    cls.neo4j = cls._setup_connector()
    cls.neo4j_driver = cls.neo4j.neo4j_driver
    return

  @classmethod
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

  def setup_method(self, method):
    self._erase_neo4j_database()

  def teardown_method(self, method):
    self._erase_neo4j_database()

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
    assert len(props) >= 0
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

  def test_node_attributes(self):
    self._populate_node()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'))
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'])
    print(xgt_schema)
    vertices = xgt_schema['vertices']['Node']
    schema = dict(vertices['schema'])
    assert len(schema) == 11
    assert schema['int'] == 'int'
    assert schema['real'] == 'float'
    assert schema['str'] == 'text'
    assert schema['bool'] == 'boolean'
    assert schema['date_attr'] == 'date'
    assert schema['time_attr'] == 'time'
    assert schema['datetime_attr'] == 'datetime'
    assert schema['localtime_attr'] == 'time'
    assert schema['localdatetime_attr'] == 'datetime'
    assert schema['duration_attr'] == 'int'
    print(schema)
    print(c.neo4j_node_type_properties)

  def test_graph_update_after_connector_created(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'))
    self._populate_node()
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'])
    vertices = xgt_schema['vertices']['Node']
    schema = dict(vertices['schema'])
    assert len(schema) == 11
    assert schema['int'] == 'int'
    assert schema['real'] == 'float'
    assert schema['str'] == 'text'
    assert schema['bool'] == 'boolean'
    assert schema['date_attr'] == 'date'
    assert schema['time_attr'] == 'time'
    assert schema['datetime_attr'] == 'datetime'
    assert schema['localtime_attr'] == 'time'
    assert schema['localdatetime_attr'] == 'datetime'
    assert schema['duration_attr'] == 'int'

  def test_graph_delete_after_connector_created(self):
    self._populate_node()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'))
    with self.neo4j_driver.session() as session:
      result = session.run("MATCH (n) DETACH DELETE n")
    with self.assertRaises(ValueError):
      xgt_schema = c.get_xgt_schema_for(vertices=['Node'])

  def disable_test_transfer_node(self):
    self._populate_node()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=True)
    #c.transfer_from_neo4j_to_xgt_for(['Node'])
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_from_neo4j_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 1

  def test_transfer_node_working_types_bolt(self):
    self._populate_node_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_from_neo4j_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 2
    print(node_frame.get_data())

  def test_transfer_relationship_working_types_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_from_neo4j_to_xgt(xgt_schema)
    node_frame = self.xgt.get_edge_frame('Relationship')
    assert node_frame.num_rows == 2
    print(node_frame.get_data())

  def test_transfer_node_working_types_arrow(self):
    self._populate_node_working_types_arrow()
    #self._populate_relationship_working_types_arrow()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_from_neo4j_to_xgt(xgt_schema, use_bolt=False)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 1
    print(node_frame.get_data())

  def test_transfer_relationship_working_types_arrow(self):
    self._populate_relationship_working_types_arrow()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schema_for(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_from_neo4j_to_xgt(xgt_schema, use_bolt=False)
    node_frame = self.xgt.get_edge_frame('Relationship')
    assert node_frame.num_rows == 1
    print(node_frame.get_data())

  def _populate_node(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String, Boolean, Point, Date, Time, LocalTime,
      # DateTime, LocalDateTime, and Duration.
      # FIXME: Point listed in comment above, but not in the list
      result = session.run(
        'CREATE (:Node{int: 343, real: 3.14, str: "string", bool: true, ' +
        'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
        'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
        'localtime_attr: localtime("12:50:35.556"), ' +
        'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
        'duration_attr: duration("P14DT16H12M")})')
      return result
    return None

  # Duration not working for bolt.
  def _populate_node_working_types_bolt(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, and LocalDateTime.
      result = session.run(
        'CREATE (:Node{int: 343, real: 3.14, str: "string", bool: true, ' +
        'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
        'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
        'localtime_attr: localtime("12:50:35.556"), ' +
        'localdatetime_attr: localdatetime("2015185T19:32:24")}), ' +
        '(:Node{})')
      return result

  def _populate_node_working_types_arrow(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String
      result = session.run(
        'CREATE (:Node{int: 343, real: 3.14, str: "string"})')
        # TODO(someone) : none values don't work in arrow.
        #'CREATE (node1:Node{int: 343, real: 3.14, str: "string"}), (node2:Node{})')
      return result

  # Duration not working for bolt.
  def _populate_relationship_working_types_bolt(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, and LocalDateTime.
      result = session.run(
        'CREATE (:Node{})-' +
        '[rel1:Relationship{int: 343, real: 3.14, str: "string", bool: true, ' +
        'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
        'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
        'localtime_attr: localtime("12:50:35.556"), ' +
        'localdatetime_attr: localdatetime("2015185T19:32:24")}]' +
        '->(:Node{}), (:Node{})-[:Relationship{}]->(:Node{})')

  def _populate_relationship_working_types_arrow(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String
      result = session.run(
        'CREATE (:Node{})-' +
        '[:Relationship{int: 343, real: 3.14, str: "string"}]' +
        '->(:Node{})')
        # TODO(someone) : none values don't work in arrow.
        #'->(:Node{}), (:Node{})-[:Relationship{}]->(:Node{})')

  def _erase_neo4j_database(self):
    with self.neo4j_driver.session() as session:
      result = session.run("MATCH (n) DETACH DELETE n")
