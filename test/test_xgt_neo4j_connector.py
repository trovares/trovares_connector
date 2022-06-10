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
      # Validate the db can run queries.
      with conn.neo4j_driver.session() as session:
        session.run("call db.info()")
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1}]->(:Node2{})')
    self.assertCountEqual(c.neo4j_relationship_types, ['Relationship1', 'Relationship2'])

  def test_neo4j_node_labels(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run('CREATE (:Node1{}), (:Node2{int : 1})')
    self.assertCountEqual(c.neo4j_node_labels, ['Node1', 'Node2'])

  def test_neo4j_property_keys(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    self.assertCountEqual(
        c.neo4j_property_keys,
        ['bool', 'date_attr', 'datetime_attr', 'duration_attr', 'int', 'localdatetime_attr',
         'localtime_attr', 'real', 'str', 'time_attr', 'x', 'y'])

  def test_neo4j_rel_type_properties(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1, str: "hello"}]->(:Node2{}),'
          '(:Node1{})-[:Relationship2{str: "goodbye"}]->(:Node2{})')
    self.assertCountEqual(
        c.neo4j_rel_type_properties,
        [{'relType': ':`Relationship1`', 'propertyName': None, 'propertyTypes': None, 'mandatory': False},
         {'relType': ':`Relationship2`', 'propertyName': 'int', 'propertyTypes': ['Long'], 'mandatory': False},
         {'relType': ':`Relationship2`', 'propertyName': 'str', 'propertyTypes': ['String'], 'mandatory': True}])

  def test_neo4j_node_type_properties(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
        session.run('CREATE (:Node1{}), (:Node2{int : 1, str : "hello"}), (:Node2{str : "goodbye"})')
    self.assertCountEqual(
        c.neo4j_node_type_properties,
        [{'nodeType': ':`Node1`', 'nodeLabels': ['Node1'], 'propertyName': None, 'propertyTypes': None, 'mandatory': False},
         {'nodeType': ':`Node2`', 'nodeLabels': ['Node2'], 'propertyName': 'int', 'propertyTypes': ['Long'], 'mandatory': False},
         {'nodeType': ':`Node2`', 'nodeLabels': ['Node2'], 'propertyName': 'str', 'propertyTypes': ['String'], 'mandatory': True}])

  def test_neo4j_edges(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node2{})-[:Relationship1{}]->(:Node1{}), (:Node1{})-[:Relationship2{int: 1}]->(:Node2{})')
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node2{})-[:Relationship1{int: 2}]->(:Node1{}), (:Node1{})-[:Relationship1{int: 1}]->(:Node2{})')
    assert len(c.neo4j_edges) == 1
    assert c.neo4j_edges['Relationship1']['endpoints'] == {'Node1->Node2', 'Node2->Node1'}
    assert c.neo4j_edges['Relationship1']['sources'] == {'Node1', 'Node2'}
    assert c.neo4j_edges['Relationship1']['targets'] == {'Node1', 'Node2'}
    assert c.neo4j_edges['Relationship1']['schema'] == {'int' : 'Long'}

  def test_neo4j_nodes(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run('CREATE (:Node1{}), (:Node2{int : 1})')
    assert len(c.neo4j_nodes) == 2
    assert c.neo4j_nodes['Node1'] == {}
    assert c.neo4j_nodes['Node2'] == {'int' : 'Long'}

  def test_graph_update_after_connector_created(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'))
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'))
    with self.neo4j_driver.session() as session:
      result = session.run("MATCH (n) DETACH DELETE n")
    with self.assertRaises(ValueError):
      xgt_schema = c.get_xgt_schemas(vertices=['Node'])

  def disable_test_transfer_node(self):
    self._populate_node()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=True)
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)
    for vertex, schema in xgt_schema['vertices'].items():
      table_schema = schema['schema']
      attributes = {_:t for _, *t  in table_schema}
      print(f"\nAttributes: {attributes}")
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    print(node_frame.get_data())

  def test_transfer_nodes_to_neo4j(self):
    self._populate_node_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    c.transfer_to_xgt(vertices=['Node'])
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    expected = [row[1:] for row in node_frame.get_data()]
    with self.neo4j_driver.session() as session:
      session.run("MATCH (n) DETACH DELETE n")
    c.transfer_to_neo4j(vertices=['Node'])
    self.xgt.drop_frame("Node")
    c.transfer_to_xgt(vertices=['Node'])
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 3
    result = [row[1:] for row in node_frame.get_data()]
    # The column ordering can change when uploading to neo4j
    # So we just need to verify the None rows are correct
    # and the same elements are in the row with data, but with a different order.
    expected_pos = 0
    result_pos = 0
    for i in range(0,3):
        if result[i] != None:
            result_pos = i
        else:
            assert result[i].count(None) == len(result[i])
        if expected[i] != None:
            expected_pos = i
        else:
            assert expected[i].count(None) == len(expected[i])
    self.assertCountEqual(expected[expected_pos], result[result_pos])

  def test_transfer_edges_to_neo4j(self):
    self._populate_relationship_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    c.transfer_to_xgt(edges=['Relationship'])
    node_frame = self.xgt.get_vertex_frame('Node')
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 3
    node_expected = [row[1:] for row in node_frame.get_data()]
    edge_expected = [row[2:] for row in edge_frame.get_data()]
    with self.neo4j_driver.session() as session:
      session.run("MATCH (n) DETACH DELETE n")
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
    # The column ordering can change when uploading to neo4j
    # So we just need to verify the None rows are correct
    # and the same elements are in the row with data, but with a different order.
    expected_pos = 0
    result_pos = 0
    for i in range(0,3):
        if edge_result[i] != None:
            result_pos = i
        else:
            assert edge_result[i].count(None) == len(edge_result[i])
        if edge_expected[i] != None:
            expected_pos = i
        else:
            assert edge_expected[i].count(None) == len(edge_expected[i])
    self.assertCountEqual(edge_expected[expected_pos], edge_result[result_pos])

  def test_transfer_relationship_working_types_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    xgt_schema = c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 3
    print(edge_frame.get_data())
    self.xgt.drop_frame("Relationship")

  def test_transfer_relationship_without_vertex_bolt(self):
    self._populate_relationship_working_types_bolt()
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False,
                       driver="neo4j-arrow")
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with c.neo4j_driver.session() as session:
      session.run('CREATE (:Node{int: 343, str: "string"})')
    xgt_schema = c.get_xgt_schemas(vertices=['Node'])
    c.create_xgt_schemas(xgt_schema)

    c.copy_data_to_xgt(xgt_schema)
    c.create_xgt_schemas(xgt_schema, append=True)

    with c.neo4j_driver.session() as session:
      result = session.run("MATCH (n) DETACH DELETE n")
    with c.neo4j_driver.session() as session:
      session.run('CREATE (:Node{int: 344, str: "string"})')
    c.copy_data_to_xgt(xgt_schema)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 2

    c.create_xgt_schemas(xgt_schema, append=False)
    node_frame = self.xgt.get_vertex_frame('Node')
    assert node_frame.num_rows == 0

  def test_dropping(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with c.neo4j_driver.session() as session:
      session.run('CREATE (:Node{})-[:Relationship]->(:Node{})')
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False,
                       driver="neo4j-arrow")
    xgt_schema = c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])
    c.create_xgt_schemas(xgt_schema)
    c.copy_data_to_xgt(xgt_schema)
    edge_frame = self.xgt.get_edge_frame('Relationship')
    assert edge_frame.num_rows == 1
    print(edge_frame.get_data())
    self.xgt.drop_frame("Relationship")

  def test_multiple_node_labels_to(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node1{})-[:Relationship{}]->(:Node1{}), (:Node1{})-[:Relationship{}]->(:Node2{})')
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node1{})-[:Relationship{}]->(:Node1{}), (:Node2{})-[:Relationship{}]->(:Node1{})')
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
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run('CREATE (:Node{x: 1})-[:Relationship{}]->(:Node{x: "hello"})')
    with self.assertRaises(ValueError):
      c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])

  def test_multiple_property_types_edge_negative(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node{})-[:Relationship{x: 1}]->(:Node{}), (:Node{})-[:Relationship{x: "hello"}]->(:Node{})')
    with self.assertRaises(ValueError):
      c.get_xgt_schemas(vertices=['Node'], edges=['Relationship'])

  def test_different_properties_combine_into_single_schema(self):
    c = Neo4jConnector(self.xgt, neo4j_auth=('neo4j', 'foo'), verbose=False)
    with self.neo4j_driver.session() as session:
      session.run(
          'CREATE (:Node{x: 1}), (:Node{y: "hello"})')
    schema = c.get_xgt_schemas(vertices=['Node'])
    node_schema = schema['vertices']['Node']['schema']
    assert len(node_schema) == 3

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

  # Point not working for bolt.
  def _populate_node_working_types_bolt(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, LocalDateTime, various Points, and Lists.
      # TODO(landwehrj) : support datetime/point lists.
      result = session.run(
        'CREATE (:Node{}), (:Node{int: 343, real: 3.14, str: "string", bool: true, ' +
        'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
        'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
        'localtime_attr: localtime("12:50:35.556"), ' +
        'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
        'duration_attr: duration({days: 14, hours:16, minutes: 12}), ' +
        'point_attr2: point({x:0.5, y:1.2}), ' +
        'point_attr3: point({x:0.23, y:1.5, z:1.2}), ' +
        'geo_attr2: point({latitude:7.23, longitude:3.5}), ' +
        'geo_attr3: point({latitude:0.23, longitude:1.5, height:10.2}), ' +
        'bool_array: [true, false, false], ' +
        'long_array: [7, 1, 5], ' +
        'string_array: ["ad", "bc", "de"], ' +
        'double_array: [0.7, 1.9, 5.2]}), ' +
        '(:Node{})')

  def _populate_node_working_types_arrow(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String.
      result = session.run(
        'CREATE (:Node{int: 343, str: "string"})')
        # TODO(someone) : none values don't work in arrow and float64 don't work in xGT 10.1.
        #'CREATE (:Node{int: 343, real: 3.14, str: "string"}), (:Node{})')
      return result

  # Point not working for bolt.
  def _populate_relationship_working_types_bolt(self):
    with self.neo4j_driver.session() as session:
      # Integer, Float, String, Boolean, Date, Time, LocalTime,
      # DateTime, LocalDateTime, various Points, and Lists.
      # TODO(landwehrj) : support datetime/point lists.
      result = session.run(
        'CREATE (:Node{int: 1})-[:Relationship{}]->(:Node{int: 1}), (:Node{int: 1})-' +
        '[:Relationship{int: 343, real: 3.14, str: "string", bool: true, ' +
        'date_attr: date("+2015-W13-4"), time_attr: time("125035.556+0100"), ' +
        'datetime_attr: datetime("2015-06-24T12:50:35.556+0100"), ' +
        'localtime_attr: localtime("12:50:35.556"), ' +
        'localdatetime_attr: localdatetime("2015185T19:32:24"), ' +
        'duration_attr: duration({days: 14, hours:16, minutes: 12}),' +
        'point_attr2: point({x:0.5, y:1.2}), ' +
        'point_attr3: point({x:0.23, y:1.5, z:1.2}), ' +
        'geo_attr2: point({latitude:7.23, longitude:3.5}), ' +
        'geo_attr3: point({latitude:0.23, longitude:1.5, height:10.2}),' +
        'bool_array: [true, false, false], ' +
        'long_array: [7, 1, 5], ' +
        'string_array: ["ad", "bc", "de"], ' +
        'double_array: [0.7, 1.9, 5.2]}]' +
        '->(:Node{int: 1}), (:Node{int: 1})-[:Relationship{}]->(:Node{int: 1})')

  def _populate_relationship_working_types_arrow(self):
    with self.neo4j_driver.session() as session:
      # Integer, String.
      result = session.run(
        'CREATE (:Node{})-' +
        '[:Relationship{int: 343, str: "string"}]' +
        '->(:Node{})')
        # TODO(someone) : float64 in xGT 10.1 aren't supported in arrow.
        #'[:Relationship{int: 343, real: 3.14, str: "string"}]' +
        # TODO(someone) : none values don't work in arrow.
        #'->(:Node{}), (:Node{})-[:Relationship{}]->(:Node{})')

  def _erase_neo4j_database(self):
    with self.neo4j_driver.session() as session:
      session.run("MATCH (n) DETACH DELETE n")
