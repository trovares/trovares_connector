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
from trovares_connector import Neo4jConnector, Neo4jDriver
import xgt

xgt_server = xgt.Connection()
xgt_server.set_default_namespace('test')

neo4j_driver = Neo4jDriver(auth=('neo4j', 'foo'))
c = Neo4jConnector(xgt_server, neo4j_driver)

show_results = False

def print_results(result):
    if show_results:
        print(result)

def cleanup_neo4j_database():
    # clean up neo4j database
    neo4j_driver.query("MATCH (a) DETACH DELETE a").finalize()

def add_vertex():
    neo4j_driver.query("create (a:Node{int : 1})").finalize()

def add_edge():
    neo4j_driver.query("create (a:Node{prop : 1})-[b:Edge{prop : 1}]->(a)").finalize()

class TestTranslationFailures(unittest.TestCase):
    def teardown_method(self, method):
        cleanup_neo4j_database()

    def test_loop_data(self):
        add_edge()
        query = "match(a)-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_loop_no_data(self):
        query = "match(a)-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_data(self):
        add_edge()
        query = "match(a)-->()-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_no_data(self):
        query = "match(a)-->()-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_loop_data_labeled(self):
        add_edge()
        query = "match(a:Node)-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_loop_no_data_labeled(self):
        query = "match(a:Node)-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_data_labeled(self):
        add_edge()
        query = "match(a:Node)-->()-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_no_data_labeled(self):
        query = "match(a:Node)-->()-->()-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_loop_data_all_labeled(self):
        add_edge()
        query = "match(a:Node)-->(e:Edge)-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_loop_no_data_all_labeled(self):
        query = "match(a:Node)-->(e:Edge)-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_data_all_labeled(self):
        add_edge()
        query = "match(a:Node)-->(e1:Edge)-->(e2:Edge)-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_longer_loop_no_data_all_labeled(self):
        query = "match(a:Node)-->(e1:Edge)-->(e2:Edge)-->(a) return a"
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data_basic(self):
        add_edge()
        query = \
        """
        MATCH (v0)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data_basic(self):
        query = \
        """
        MATCH (v0)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data_basic_labeled(self):
        add_edge()
        query = \
        """
        MATCH (v0:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data_basic_labeled(self):
        query = \
        """
        MATCH (v0:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data(self):
        add_edge()
        query = \
        """
        MATCH (v0)-[]->(v1)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data(self):
        query = \
        """
        MATCH (v0)-[]->(v1)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data_labeled(self):
        add_edge()
        query = \
        """
        MATCH (v0:Node)-[]->(v1)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data_more_labeled(self):
        add_edge()
        query = \
        """
        MATCH (v0:Node)-[]->(v1:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_data_all_labeled(self):
        add_edge()
        query = \
        """
        MATCH (v0:Node)-[e:Edge]->(v1:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data_labeled(self):
        query = \
        """
        MATCH (v0:Node)-[]->(v1)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data_more_labeled(self):
        query = \
        """
        MATCH (v0:Node)-[]->(v1:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_vertex_no_data_all_labeled(self):
        query = \
        """
        MATCH (v0:Node)-[e:Edge]->(v1:Node)
        RETURN v0
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_edge_no_data(self):
        query = \
        """
        MATCH ()-[e]->()
        RETURN e
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_edge_data(self):
        add_edge()
        query = \
        """
        MATCH ()-[e]->()
        RETURN e
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_edge_no_data_labeled(self):
        query = \
        """
        MATCH ()-[e:Edge]->()
        RETURN e
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

    def test_edge_data_labeled(self):
        add_edge()
        query = \
        """
        MATCH ()-[e:Edge]->()
        RETURN e
        """
        translated_query = c.translate_query(query)
        print_results(f"Translated Query:\n{translated_query}")

