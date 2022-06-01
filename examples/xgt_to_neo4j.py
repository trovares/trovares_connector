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

# This example just creates a graph in xGT, transfers it to Neo4j, and
# runs the same query in xGT in Neo4j.

from xgt_neo4j_connector import Neo4jConnector
import xgt

xgt_server = xgt.Connection()
xgt_server.set_default_namespace('neo4j')
database = "test"

c=Neo4jConnector(xgt_server, neo4j_auth=('neo4j', 'foo'), neo4j_database=database)

# Uncomment to delete the database
"""
with c.neo4j_driver.session(database=database) as session:
    session.run("MATCH (n) DETACH DELETE n")
"""

xgt_server.drop_frame("Knows")
xgt_server.drop_frame("Person")
xgt_server.create_vertex_frame(name='Person', schema=[['id', xgt.INT]], key='id')
e_frame = xgt_server.create_edge_frame(name='Knows', schema=[['src', xgt.INT], ['trg', xgt.INT]],
                                       source_key='src', target_key='trg', source="Person", target="Person")
end = 10
# Create a chain with with a loop in Neo4j
for i in range(0, end):
    e_frame.insert([[i, i+1]])
e_frame.insert([[2, 0]])

# Transfer graph from Neo4j to xGT
c.transfer_from_xgt_to_neo4j_for(edges=["Knows"], vertex_keys=True)

# Look for the loop
query = "match(a)-->()-->()-->(a) return a.id"

# Get results with Neo4j
with c.neo4j_driver.session(database=database) as session:
    job = session.run(query)
    print("Neo4j found the following nodes in a triangle: " + ','.join(str(row[0]) for row in job))

# Get results with xGT
job = xgt_server.run_job(query)
print("xGT found the following nodes in a triangle: " + ','.join(str(row[0]) for row in job.get_data()))
