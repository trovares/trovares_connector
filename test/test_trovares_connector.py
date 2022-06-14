#!/usr/bin/env python
# -*- coding: utf-8 -*- --------------------------------------------------===#
#
#  Copyright 2022 Trovares Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===----------------------------------------------------------------------===#

import unittest
from parameterized import parameterized_class
import time

import neo4j
import xgt
from trovares_connector import Neo4jConnector, Neo4jDriver

@parameterized_class([
   { "driver": "neo4j" },
   { "driver": "neo4j-bolt" },
   { "driver": "py2neo-bolt" }
])
class TestXgtNeo4jConnector(unittest.TestCase):
  @classmethod
  def setup_class(cls):
    # Create a connection to Trovares xGT
    cls.xgt = xgt.Connection()
    cls.xgt.drop_namespace('test', force_drop = True)
    cls.xgt.set_default_namespace('test')
    cls.neo4j_driver, cls.conn, cls.conn_arrow = cls._setup_connector(cls.driver)
    return

  @classmethod
  def teardown_class(cls):
    del cls.conn
    del cls.conn_arrow
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  @classmethod
  def _setup_connector(cls, connector_type, retries = 20):
    try:
      if connector_type == "neo4j":
        driver = neo4j.GraphDatabase.driver("bolt://localhost", auth=('neo4j', 'foo'))
      else:
        driver = Neo4jDriver(auth=('neo4j', 'foo'), driver=connector_type)
      arrow_driver = Neo4jDriver(auth=('neo4j', 'foo'), driver="neo4j-arrow")
      conn = Neo4jConnector(cls.xgt, driver)
      conn_arrow = Neo4jConnector(cls.xgt, arrow_driver)
      # Validate the db can run queries.
      with conn._neo4j_driver.bolt.session() as session:
        session.run("call db.info()")
      return (conn._neo4j_driver, conn, conn_arrow)
    except (neo4j.exceptions.ServiceUnavailable):
      print(f"Neo4j Unavailable, retries = {retries}")
      if retries > 0:
        time.sleep(3)
        return cls._setup_connector(retries - 1)
    if connector_type == "neo4j":
        driver = neo4j.GraphDatabase.driver("bolt://localhost", auth=('neo4j', 'foo'))
    else:
        driver = Neo4jDriver(auth=('neo4j', 'foo'), driver=connector_type)
    arrow_driver = Neo4jDriver(auth=('neo4j', 'foo'), driver="neo4j-arrow")
    conn = Neo4jConnector(cls.xgt, driver)
    conn_arrow = Neo4jConnector(cls.xgt, arrow_driver)
    return (conn._neo4j_driver, conn, conn_arrow)

  def setup_method(self, method):
    self._erase_neo4j_database()

  def teardown_method(self, method):
    self._erase_neo4j_database()

  def test_connector_creation(self) -> None:
    # Must pass at least one parameter to constructor.
    with self.assertRaises(TypeError):
      c = Neo4jConnector()

  def test_neo4j_properties(self):
    assert isinstance(self.neo4j_driver, Neo4jDriver)
    rel = self.conn.neo4j_relationship_types
    assert isinstance(rel, list)
    assert len(rel) == 0
    labels = self.conn.neo4j_node_labels
    assert isinstance(labels, list)
    assert len(labels) == 0
    props = self.conn.neo4j_property_keys
    assert isinstance(props, list)
    assert len(props) >= 0
    props = self.conn.neo4j_node_type_properties
    assert isinstance(props, list)
    assert len(props) == 0
    props = self.conn.neo4j_rel_type_properties
    assert isinstance(props, list)
    assert len(props) == 0
    nodes = self.conn.neo4j_nodes
    assert isinstance(nodes, dict)
    assert len(nodes) == 0
    edges = self.conn.neo4j_edges
    assert isinstance(edges, dict)
    assert len(edges) == 0

  def test_node_attributes(self):
    self._populate_node()
    c = self.conn
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
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

  def test_neo4j_relationship_types(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1}]->(:Node2{})').finalize()
    self.assertCountEqual(c.neo4j_relationship_types, ['Relationship1', 'Relationship2'])

  def test_neo4j_node_labels(self):
    c = self.conn
    self.neo4j_driver.query('CREATE (:Node1{}), (:Node2{int : 1})').finalize()
    self.assertCountEqual(c.neo4j_node_labels, ['Node1', 'Node2'])

  def test_neo4j_property_keys(self):
    c = self.conn
    self.assertCountEqual(
        c.neo4j_property_keys,
        ['bool', 'date_attr', 'datetime_attr', 'duration_attr', 'int', 'localdatetime_attr',
         'localtime_attr', 'real', 'str', 'time_attr', 'x', 'y'])

  def test_neo4j_rel_type_properties(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1, str: "hello"}]->(:Node2{}),'
        '(:Node1{})-[:Relationship2{str: "goodbye"}]->(:Node2{})').finalize()
    self.assertCountEqual(
        c.neo4j_rel_type_properties,
        [{'relType': ':`Relationship1`', 'propertyName': None, 'propertyTypes': None, 'mandatory': False},
         {'relType': ':`Relationship2`', 'propertyName': 'int', 'propertyTypes': ['Long'], 'mandatory': False},
         {'relType': ':`Relationship2`', 'propertyName': 'str', 'propertyTypes': ['String'], 'mandatory': True}])

  def test_neo4j_node_type_properties(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node1{}), (:Node2{int : 1, str : "hello"}), (:Node2{str : "goodbye"})').finalize()
    self.assertCountEqual(
        c.neo4j_node_type_properties,
        [{'nodeType': ':`Node1`', 'nodeLabels': ['Node1'], 'propertyName': None, 'propertyTypes': None, 'mandatory': False},
         {'nodeType': ':`Node2`', 'nodeLabels': ['Node2'], 'propertyName': 'int', 'propertyTypes': ['Long'], 'mandatory': False},
         {'nodeType': ':`Node2`', 'nodeLabels': ['Node2'], 'propertyName': 'str', 'propertyTypes': ['String'], 'mandatory': True}])

  def test_neo4j_edges(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1}]->(:Node2{})').finalize()
    assert len(c.neo4j_edges) == 2
    assert c.neo4j_edges['Relationship1']['endpoints'] == {'Node2->Node1'}
    assert c.neo4j_edges['Relationship1']['sources'] == {'Node2'}
    assert c.neo4j_edges['Relationship1']['targets'] == {'Node1'}
    assert c.neo4j_edges['Relationship1']['schema'] == {}
    assert c.neo4j_edges['Relationship2']['endpoints'] == {'Node1->Node2'}
    assert c.neo4j_edges['Relationship2']['sources'] == {'Node1'}
    assert c.neo4j_edges['Relationship2']['targets'] == {'Node2'}
    assert c.neo4j_edges['Relationship2']['schema'] == {'int' : 'Long'}

  def test_neo4j_edges_multi(self):
    c = self.conn
    self.neo4j_driver.query(
          'CREATE (:Node2{})-[:Relationship1{int: 2}]->(:Node1{}), (:Node1{})-[:Relationship1{int: 1}]->(:Node2{})').finalize()
    assert len(c.neo4j_edges) == 1
    assert c.neo4j_edges['Relationship1']['endpoints'] == {'Node1->Node2', 'Node2->Node1'}
    assert c.neo4j_edges['Relationship1']['sources'] == {'Node1', 'Node2'}
    assert c.neo4j_edges['Relationship1']['targets'] == {'Node1', 'Node2'}
    assert c.neo4j_edges['Relationship1']['schema'] == {'int' : 'Long'}

  def test_neo4j_nodes(self):
    c = self.conn
    self.neo4j_driver.query('CREATE (:Node1{}), (:Node2{int : 1})').finalize()
    assert len(c.neo4j_nodes) == 2
    assert c.neo4j_nodes['Node1'] == {}
    assert c.neo4j_nodes['Node2'] == {'int' : 'Long'}

  def test_graph_update_after_connector_created(self):
    c = self.conn
    self._populate_node()
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
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
    c = self.conn
    self.neo4j_driver.query("MATCH (n) DETACH DELETE n").finalize()
    with self.assertRaises(ValueError):
      xgt_schema = c.get_xgt_schemas(vertices=['Node'])

  def disable_test_transfer_node(self):
    self._populate_node()
    c = self.conn
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 1

  def test_transfer_node_working_types_bolt(self):
    self._populate_node_working_types_bolt()
    c = self.conn
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    print(node_frame.get_data())

  def test_transfer_nodes_to_neo4j(self):
    self._populate_node_working_types_bolt()
    c = self.conn
    c.transfer_to_xgt(vertices=['Node'])
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    expected = [row[1:] for row in node_frame.get_data()]
    self.neo4j_driver.query("MATCH (n) DETACH DELETE n").finalize()
    c.transfer_to_neo4j(vertices=['Node'])
    self.xgt.drop_frame("Node")
    c.transfer_to_xgt(vertices=['Node'])
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    result = [row[1:] for row in node_frame.get_data()]
    self.assertCountEqual(expected, result)

  def test_transfer_edges_to_neo4j(self):
    self._populate_relationship_working_types_bolt()
    c = self.conn
    c.transfer_to_xgt(edges=['Relationship'])
    node_frame = self.xgt.get_vertex_frame('Node')
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 3
    node_expected = [row[1:] for row in node_frame.get_data()]
    edge_expected = [row[2:] for row in edge_frame.get_data()]
    self.neo4j_driver.query("MATCH (n) DETACH DELETE n").finalize()
    c.transfer_to_neo4j(edges=['Relationship'])
    self.xgt.drop_frame("Relationship")
    self.xgt.drop_frame("Node")
    c.transfer_to_xgt(edges=['Relationship'])
    node_frame = self.xgt.get_vertex_frame('Node')
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 3
    node_result = [row[1:] for row in node_frame.get_data()]
    edge_result = [row[2:] for row in edge_frame.get_data()]
    self.assertCountEqual(node_expected, node_result)
    self.assertCountEqual(edge_expected, edge_result)

  def test_transfer_relationship_working_types_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = self.conn
    xgt_schema = c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 3
    print(edge_frame.get_data())
    self.xgt.drop_frame("Relationship")

  def test_transfer_relationship_without_vertex_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = self.conn
    xgt_schema = c.get_xgt_schemas(edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert node_frame.num_rows == 6
    assert edge_frame.num_rows == 3
    assert node_frame.get_data(length=1)[0][1] == 1
    self.xgt.drop_frame("Relationship")

  def test_transfer_everything_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = self.conn
    xgt_schema = c.get_xgt_schemas()
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert node_frame.num_rows == 6
    assert edge_frame.num_rows == 3
    self.xgt.drop_frame("Relationship")

  def test_transfer_node_working_types_arrow(self):
    self._populate_node_working_types_arrow()
    c = self.conn_arrow
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, t in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 1
    print(node_frame.get_data())

  def test_append(self):
    c = self.conn
    self.neo4j_driver.query(
      'CREATE (:Node{int: 343, str: "string"})').finalize()
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)

    c.copy_data_to_xgt(xgt_schema)
    c.create_xgt_schemas(xgt_schema, append=True)

    self.neo4j_driver.query("MATCH (n) DETACH DELETE n").finalize()
    self.neo4j_driver.query(
        'CREATE (:Node{int: 344, str: "string"})').finalize()
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 2

    c.create_xgt_schemas(xgt_schema, append=False)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 0

  def test_dropping(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node{})-[:Relationship]->(:Node{})').finalize()
    xgt_schema1 = c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])
    xgt_schema2 = c.get_xgt_schemas(vertices=['Node'])

    c.create_xgt_schemas(xgt_schema1)
    self.xgt.get_vertex_frame('Node')
    self.xgt.get_edge_frame('Relationship')

    c.create_xgt_schemas(xgt_schema1)
    self.xgt.get_vertex_frame('Node')
    self.xgt.get_edge_frame('Relationship')

    with self.assertRaises(xgt.XgtFrameDependencyError):
        c.create_xgt_schemas(xgt_schema2)
    c.create_xgt_schemas(xgt_schema2, force=True)
    self.xgt.get_vertex_frame('Node')
    with self.assertRaises(xgt.XgtNameError):
        self.xgt.get_edge_frame('Relationship')

  def test_transfer_relationship_working_types_arrow(self):
    self._populate_relationship_working_types_arrow()
    c = self.conn_arrow
    xgt_schema = c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 1
    print(edge_frame.get_data())
    self.xgt.drop_frame("Relationship")

  def test_multiple_node_labels_to(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node1{})-[:Relationship{}]->(:Node1{}), (:Node1{})-[:Relationship{}]->(:Node2{})').finalize()
    schema = c.get_xgt_schemas(vertices=['Node1', 'Node2'], edges=['Relationship'])
    c.create_xgt_schemas(schema)
    c.copy_data_to_xgt(schema)

    node_frame = self.xgt.get_edge_frame('Node1_Relationship_Node1')
    assert node_frame.num_rows == 1
    node_frame = self.xgt.get_edge_frame('Node1_Relationship_Node2')
    assert node_frame.num_rows == 1
    self.xgt.drop_frame("Node1_Relationship_Node1")
    self.xgt.drop_frame("Node1_Relationship_Node2")

  def test_multiple_node_labels_from(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node1{})-[:Relationship{}]->(:Node1{}), (:Node2{})-[:Relationship{}]->(:Node1{})').finalize()
    schema = c.get_xgt_schemas(vertices=['Node1', 'Node2'], edges=['Relationship'])
    c.create_xgt_schemas(schema)
    c.copy_data_to_xgt(schema)

    node_frame = self.xgt.get_edge_frame('Node1_Relationship_Node1')
    assert node_frame.num_rows == 1
    node_frame = self.xgt.get_edge_frame('Node2_Relationship_Node1')
    assert node_frame.num_rows == 1
    self.xgt.drop_frame("Node1_Relationship_Node1")
    self.xgt.drop_frame("Node2_Relationship_Node1")

  def test_multiple_property_types_vertex_negative(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node{x: 1})-[:Relationship{}]->(:Node{x: "hello"})').finalize()
    with self.assertRaises(ValueError):
        c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])

  def test_multiple_property_types_edge_negative(self):
    c = self.conn
    self.neo4j_driver.query(
        'CREATE (:Node{})-[:Relationship{x: 1}]->(:Node{}), (:Node{})-[:Relationship{x: "hello"}]->(:Node{})').finalize()
    with self.assertRaises(ValueError):
      c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])

  def test_different_properties_combine_into_single_schema(self):
    c = self.conn
    self.neo4j_driver.query('CREATE (:Node{x: 1}), (:Node{y: "hello"})').finalize()
    schema = c.get_xgt_schemas(vertices=['Node'])
    node_schema = schema['vertices']['Node']['schema']
    assert len(node_schema) == 3

  def test_transfer_no_data(self):
    c = self.conn
    c.transfer_to_xgt()
    c.transfer_to_neo4j()

  def _populate_node(self):
    self.neo4j_driver.query(
      # Integer, Float, String, Boolean, Point, Date, Time, LocalTime,
      # DateTime, LocalDateTime, and Duration.
      # FIXME: Point listed in comment above, but not in the list
      'CREATE (:Node{int: 343, real: 3.14, str: "string", bool: true, ' +
      'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
      'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
      'localtime_attr: localtime("12:50:35.556"), ' +
      'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
      'duration_attr: duration("P14DT16H12M")})').finalize()

  # Point not working for bolt.
  def _populate_node_working_types_bolt(self):
    self.neo4j_driver.query(
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, and LocalDateTime.
      'CREATE (:Node{}), (:Node{int: 343, real: 3.14, str: "string", bool: true, ' +
      'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
      'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
      'localtime_attr: localtime("12:50:35.556"), ' +
      'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
      'duration_attr: duration({days: 14, hours:16, minutes: 12})}), ' +
      '(:Node{})').finalize()

  def _populate_node_working_types_arrow(self):
    self.neo4j_driver.query(
      # Integer, Float, String.
      'CREATE (:Node{int: 343, str: "string"})').finalize()
      # TODO(someone) : none values don't work in arrow and float64 don't work in xGT 10.1.
      #'CREATE (:Node{int: 343, real: 3.14, str: "string"}), (:Node{})')

  # Point not working for bolt.
  def _populate_relationship_working_types_bolt(self):
    self.neo4j_driver.query(
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, and LocalDateTime.
      'CREATE (:Node{int: 1})-[:Relationship{}]->(:Node{int: 1}), (:Node{int: 1})-' +
      '[:Relationship{int: 343, real: 3.14, str: "string", bool: true, ' +
      'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
      'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
      'localtime_attr: localtime("12:50:35.556"), ' +
      'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
      'duration_attr: duration({days: 14, hours:16, minutes: 12})}]' +
      '->(:Node{int: 1}), (:Node{int: 1})-[:Relationship{}]->(:Node{int: 1})').finalize()

  def _populate_relationship_working_types_arrow(self):
    self.neo4j_driver.query(
      # Integer, String.
      'CREATE (:Node{})-' +
      '[:Relationship{int: 343, str: "string"}]' +
      '->(:Node{})', True).finalize()
      # TODO(someone) : float64 in xGT 10.1 aren't supported in arrow.
      #'[:Relationship{int: 343, real: 3.14, str: "string"}]' +
      # TODO(someone) : none values don't work in arrow.
      #'->(:Node{}), (:Node{})-[:Relationship{}]->(:Node{})')

  def _erase_neo4j_database(self):
    self.neo4j_driver.query("MATCH (n) DETACH DELETE n").finalize()
