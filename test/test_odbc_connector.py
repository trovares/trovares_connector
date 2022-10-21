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

import xgt
import pyodbc
from trovares_connector.odbc import ODBCConnector, SQLODBCDriver

class TestXgtODBCConnector(unittest.TestCase):
  # Print all diffs on failure.
  maxDiff=None
  @classmethod
  def setup_class(cls):
    # Create a connection to Trovares xGT
    cls.xgt = xgt.Connection()
    cls.xgt.set_default_namespace('test')
    cls.odbc_driver, cls.conn = cls._setup_connector()
    return

  @classmethod
  def teardown_class(cls):
    del cls.conn
    cls.xgt.drop_namespace('test', force_drop = True)
    del cls.xgt

  @classmethod
  def _setup_connector(cls):
    connection_string = 'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
    odbc_driver = pyodbc.connect(connection_string)
    driver = SQLODBCDriver(connection_string)
    conn = ODBCConnector(cls.xgt, driver)
    return (odbc_driver, conn)

  def setup_method(self, method):
    self._erase_mysql_database()

  def teardown_method(self, method):
    self._erase_mysql_database()

  def _erase_mysql_database(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("DROP TABLE IF EXISTS Node")
    cursor.execute("DROP TABLE IF EXISTS Relationship")
    self.odbc_driver.commit()
    self.xgt.drop_namespace('test', force_drop = True)

  def test_transfer(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53), "
                   "TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE, "
                   "TestDatetime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)")
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06', '1986-05-06 12:56:34', "
                   "'1989-05-06 12:56:34', '12:56:34', 1999)")
    cursor.execute("INSERT INTO test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = ['test'])
    assert self.xgt.get_table_frame('test').num_rows == 2
    print(self.xgt.get_table_frame('test').get_data())

  def test_rename(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hello')")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test','test1')])
    assert self.xgt.get_table_frame('test1').num_rows == 1
    print(self.xgt.get_table_frame('test1').get_data())

    self.conn.transfer_to_xgt(tables = [('test', {'frame' : 'test2'})])
    assert self.xgt.get_table_frame('test2').num_rows == 1
    print(self.xgt.get_table_frame('test2').get_data())

  def test_vertex(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', (0,))])
    assert self.xgt.get_vertex_frame('test').num_rows == 2
    print(self.xgt.get_vertex_frame('test').get_data())

    self.conn.transfer_to_xgt(tables = [('test','test1', (0,))])
    assert self.xgt.get_vertex_frame('test1').num_rows == 2
    print(self.xgt.get_vertex_frame('test1').get_data())

    self.conn.transfer_to_xgt(tables = [('test','test2', ('Value1',))])
    assert self.xgt.get_vertex_frame('test2').num_rows == 2
    print(self.xgt.get_vertex_frame('test2').get_data())

    self.conn.transfer_to_xgt(tables = [('test', {'frame' : 'test3', 'key' : 0 } )])
    assert self.xgt.get_vertex_frame('test3').num_rows == 2
    print(self.xgt.get_vertex_frame('test3').get_data())

    self.conn.transfer_to_xgt(tables = [('test', {'frame' : 'test4', 'key' : 'Value1' } )])
    assert self.xgt.get_vertex_frame('test4').num_rows == 2
    print(self.xgt.get_vertex_frame('test4').get_data())

  def test_edge(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', ('Vertex', 'Vertex', 0, 1))], easy_edges=True)
    assert self.xgt.get_edge_frame('test').num_rows == 2
    print(self.xgt.get_edge_frame('test').get_data())

    self.conn.transfer_to_xgt(tables = [('test','test1', ('Vertex', 'Vertex', 0, 1))])
    assert self.xgt.get_edge_frame('test1').num_rows == 2
    print(self.xgt.get_edge_frame('test1').get_data())

    self.conn.transfer_to_xgt(tables = [('test', 'Vertex1', (0,)), ('test','test2', ('Vertex1', 'Vertex1', 'Value1', 'Value2'))])
    assert self.xgt.get_edge_frame('test2').num_rows == 2
    print(self.xgt.get_edge_frame('test2').get_data())

    self.conn.transfer_to_xgt(tables = [('test', {'frame' : 'test3', 'source' : 'Vertex', 'target' : 'Vertex', 'source_key' : 0, 'target_key' : 1 } )])
    assert self.xgt.get_edge_frame('test3').num_rows == 2
    print(self.xgt.get_edge_frame('test3').get_data())

    self.conn.transfer_to_xgt(tables = [('test', {'frame' : 'test4', 'source' : 'Vertex', 'target' : 'Vertex', 'source_key' : 'Value1', 'target_key' : 'Value2' } )])
    assert self.xgt.get_edge_frame('test4').num_rows == 2
    print(self.xgt.get_edge_frame('test4').get_data())

  def test_append(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', ('Vertex', 'Vertex', 0, 1))], easy_edges=True)
    assert self.xgt.get_edge_frame('test').num_rows == 1
    print(self.xgt.get_edge_frame('test').get_data())

    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()
    self.conn.transfer_to_xgt(tables = [('test', ('Vertex', 'Vertex', 0, 1))], append=True)
    assert self.xgt.get_edge_frame('test').num_rows == 2
    print(self.xgt.get_edge_frame('test').get_data())

  def test_dropping(self):
    c = self.conn
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE Node (id INT)")
    cursor.execute("INSERT INTO Node VALUES (0)")
    cursor.execute("CREATE TABLE Relationship (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO Relationship VALUES (0, 0, 'hola')")
    self.odbc_driver.commit()

    xgt_schema1 = self.conn.get_xgt_schemas(tables = [('Node', (0,)), ('Relationship', ('Node', 'Node', 0, 1))])
    xgt_schema2 = self.conn.get_xgt_schemas(tables = [('Node', (0,))])

    c.create_xgt_schemas(xgt_schema1)
    self.xgt.get_vertex_frame('Node')
    self.xgt.get_edge_frame('Relationship')

    c.create_xgt_schemas(xgt_schema1)
    self.xgt.get_vertex_frame('Node')
    self.xgt.get_edge_frame('Relationship')

    with self.assertRaises(xgt.XgtFrameDependencyError):
        c.create_xgt_schemas(xgt_schema2)
    c.create_xgt_schemas(xgt_schema2, force = True)
    self.xgt.get_vertex_frame('Node')
    with self.assertRaises(xgt.XgtNameError):
        self.xgt.get_edge_frame('Relationship')

  def test_transfer_no_data(self):
    c = self.conn
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE Node (id INT)")
    self.conn.transfer_to_xgt(tables = [('Node', (0,))])
    assert self.xgt.get_vertex_frame('Node').num_rows == 0

  def test_transfer_to_odbc(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    cursor.execute("INSERT INTO test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = ['test'])
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(tables = ['test'])
    self.conn.transfer_to_xgt(tables = ['test'])
    assert self.xgt.get_table_frame('test').num_rows == 2
    print(self.xgt.get_table_frame('test').get_data())

  def test_transfer_to_odbc_rename(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', 'test1')])
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(tables = [('test1', 'test')])
    self.conn.transfer_to_xgt(tables = ['test'])
    assert self.xgt.get_table_frame('test').num_rows == 1
    print(self.xgt.get_table_frame('test').get_data())

  def test_transfer_to_odbc_vertex(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', (0,))])
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(vertices = ['test'])
    self.conn.transfer_to_xgt(tables = [('test', (0,))])
    assert self.xgt.get_vertex_frame('test').num_rows == 1
    print(self.xgt.get_vertex_frame('test').get_data())

  def test_transfer_to_odbc_vertex_rename(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', 'test1', (0,))])
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(vertices = [('test1', 'test')])
    self.conn.transfer_to_xgt(tables = [('test', (0,))])
    assert self.xgt.get_vertex_frame('test').num_rows == 1
    print(self.xgt.get_vertex_frame('test').get_data())

  def test_transfer_to_odbc_edges(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables = [('test', ('Vertex1', 'Vertex2', 1, 2))], easy_edges = True)
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(edges = ['test'])
    self.conn.transfer_to_xgt(tables = [('test', ('Vertex1', 'Vertex2', 1, 2))], easy_edges = True)
    assert self.xgt.get_edge_frame('test').num_rows == 1
    print(self.xgt.get_edge_frame('test').get_data())

  def test_transfer_to_odbc_edges_rename(self):
    cursor = self.odbc_driver.cursor()
    create_statement = """CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53),
                       TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE,
                       TestDateTime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)"""
    cursor.execute(create_statement)
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06',"
                   "'1989-05-06 12:56:34', '1989-05-06 12:56:34', '12:56:34', 1999)")
    self.odbc_driver.commit()

    self.conn.transfer_to_xgt(tables=[('test', 'test1', ('Vertex1', 'Vertex2', 1, 2))], easy_edges = True)
    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute(create_statement)
    self.conn.transfer_to_odbc(edges = [('test1', 'test')])
    self.conn.transfer_to_xgt(tables = [('test', ('Vertex1', 'Vertex2', 1, 2))], easy_edges = True, force = True)
    assert self.xgt.get_edge_frame('test').num_rows == 1
    print(self.xgt.get_edge_frame('test').get_data())

  def test_transfer_query(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (TestBool BOOL, TestInt INT, TestBigInt BIGINT, TestFloat FLOAT(24), TestDouble FLOAT(53), "
                   "TestFixedString char(5), TestString varchar(255), TestDecimal DECIMAL(10, 6), TestDate DATE, "
                   "TestDatetime DATETIME, TestTimestamp TIMESTAMP NULL, TestTime TIME, TestYear YEAR)")
    cursor.execute("INSERT INTO test VALUES (True, 32, 5000, 1.7, 1.98, 'vdxs', 'String', 1.78976, '1989-05-06', '1986-05-06 12:56:34', "
                   "'1989-05-06 12:56:34', '12:56:34', 1999)")
    cursor.execute("INSERT INTO test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    self.odbc_driver.commit()

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = 'my_test')
    assert self.xgt.get_table_frame('my_test').num_rows == 2
    print(self.xgt.get_table_frame('my_test').get_data())

  def test_vertex_query(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', (0,)))
    assert self.xgt.get_vertex_frame('test').num_rows == 2
    print(self.xgt.get_vertex_frame('test').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test1', (0,)))
    assert self.xgt.get_vertex_frame('test1').num_rows == 2
    print(self.xgt.get_vertex_frame('test1').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test2', ('Value1',)))
    assert self.xgt.get_vertex_frame('test2').num_rows == 2
    print(self.xgt.get_vertex_frame('test2').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', {'frame' : 'test3', 'key' : 0 } ))
    assert self.xgt.get_vertex_frame('test3').num_rows == 2
    print(self.xgt.get_vertex_frame('test3').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', {'frame' : 'test4', 'key' : 'Value1' } ))
    assert self.xgt.get_vertex_frame('test4').num_rows == 2
    print(self.xgt.get_vertex_frame('test4').get_data())

  def test_edge_query(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', ('Vertex', 'Vertex', 0, 1)), easy_edges=True)
    assert self.xgt.get_edge_frame('test').num_rows == 2
    print(self.xgt.get_edge_frame('test').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test1', ('Vertex', 'Vertex', 0, 1)))
    assert self.xgt.get_edge_frame('test1').num_rows == 2
    print(self.xgt.get_edge_frame('test1').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('Vertex1', (0,)))
    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', 'test2', ('Vertex1', 'Vertex1', 'Value1', 'Value2')))
    assert self.xgt.get_edge_frame('test2').num_rows == 2
    print(self.xgt.get_edge_frame('test2').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', {'frame' : 'test3', 'source' : 'Vertex', 'target' : 'Vertex', 'source_key' : 0, 'target_key' : 1 }))
    assert self.xgt.get_edge_frame('test3').num_rows == 2
    print(self.xgt.get_edge_frame('test3').get_data())

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', {'frame' : 'test4', 'source' : 'Vertex', 'target' : 'Vertex', 'source_key' : 'Value1', 'target_key' : 'Value2' }))
    assert self.xgt.get_edge_frame('test4').num_rows == 2
    print(self.xgt.get_edge_frame('test4').get_data())

  def test_append_query(self):
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (0, 0, 'hola')")
    self.odbc_driver.commit()

    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', ('Vertex', 'Vertex', 0, 1)), easy_edges=True)
    assert self.xgt.get_edge_frame('test').num_rows == 1
    print(self.xgt.get_edge_frame('test').get_data())

    cursor.execute("DROP TABLE IF EXISTS test")
    cursor.execute("CREATE TABLE test (Value1 INT, Value2 INT, Value3 varchar(255))")
    cursor.execute("INSERT INTO test VALUES (1, 0, 'adios')")
    self.odbc_driver.commit()
    self.conn.transfer_query_to_xgt("SELECT * FROM test", mapping = ('test', ('Vertex', 'Vertex', 0, 1)), append=True)
    assert self.xgt.get_edge_frame('test').num_rows == 2
    print(self.xgt.get_edge_frame('test').get_data())

  def test_transfer_no_data_query(self):
    c = self.conn
    cursor = self.odbc_driver.cursor()
    cursor.execute("CREATE TABLE Node (id INT)")
    self.conn.transfer_query_to_xgt("SELECT * FROM Node", mapping = ('Node', (0,)))
    assert self.xgt.get_vertex_frame('Node').num_rows == 0
