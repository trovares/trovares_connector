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

import datetime
import pyarrow as pa
import pyarrow.flight as pf

import neo4j
import xgt
import os
import time

class Neo4jConnector(object):
    _NEO4J_TYPE_TO_XGT_TYPE = {
        'INTEGER': xgt.INT,
        'Long': xgt.INT,
        'FLOAT': xgt.FLOAT,
        'Double': xgt.FLOAT,
        'STRING': xgt.TEXT,
        'String': xgt.TEXT,
        'Boolean': xgt.BOOLEAN,
        'Date': xgt.DATE,
        'Time': xgt.TIME,
        'DateTime': xgt.DATETIME,
        'LocalTime': xgt.TIME,
        'LocalDateTime': xgt.DATETIME,
        'Duration': xgt.INT,
    }

    _NEO4J_TYPE_TO_ARROW_TYPE = {
        'INTEGER': pa.int64(),
        'Long': pa.int64(),
        'FLOAT': pa.float32(),
        'Double': pa.float32(),
        'STRING': pa.string(),
        'String': pa.string(),
        'Boolean': pa.bool_(),
        'Date': pa.date32(),
        'Time': pa.time64('us'),
        'DateTime': pa.timestamp('us'),
        'LocalTime': pa.time64('us'),
        'LocalDateTime': pa.timestamp('us'),
        'Duration': pa.int64(),
    }

    def __init__(self, xgt_server,
                       neo4j_host = 'localhost',
                       neo4j_bolt_port = 7687, neo4j_http_port = 7474,
                       neo4j_arrow_port = 9999, neo4j_auth = None,
                       neo4j_database = neo4j.DEFAULT_DATABASE,
                       verbose = False, disable_apoc = False,
                       driver = 'neo4j-bolt',
                       neo4j_protocol = 'neo4j',
                       neo4j_http_protocol = 'http'):
        """
        Initializes the connector class.

        Parameters
        ----------
        xgt_server : xgt.Connection
            Connection object to xGT.
        neo4j_host : str
            Host address of the Neo4j server.
        neo4j_bolt_port : int
            Bolt port of the Neo4j server.
        neo4j_http_port : int
            HTTP port of the Neo4j server.
            Used with HTTP drivers.
        neo4j_arrow_port : int
            Arrow port of the Neo4j server.
            Used with Arrow driver.
        neo4j_auth : neo4j.Auth
            Authentication details.
        neo4j_database : str
            Neo4j database to connect to.
        verbose : bool
            Print detailed information during calls.
        disable_apoc : bool
            If the connector finds APOC, it will use that to improve schema queries.
            If set to True this disables that feature.
        driver : str
            Driver to use for transferring data from Neo4j.
            Options include 'neo4j-bolt', 'py2neo-bolt', 'py2neo-http', and 'neo4j-arrow'.
            Some connections, such as schema querying will still go through 'neo4j-bolt',
            but all data transferring will use the method selected here.
            'neo4j-arrow' is considered very experimental.
            See the documentation for requirements for using.
        neo4j_protocol : str
            Protocol used when connecting to Neo4j through bolt.
            Acceptable values include: neo4j, neo4j+s, neo4j+ssc, bolt, bolt+s, and bolt+ssc.
        neo4j_http_protocol : str
            Protocol used when connecting to Neo4j through http with http drivers.
            Acceptable values include: http, https, http+s, http+ssc.
        """

        self._xgt_server = xgt_server
        self._neo4j_host = neo4j_host
        self._neo4j_bolt_port = neo4j_bolt_port
        self._neo4j_http_port = neo4j_http_port
        self._neo4j_arrow_port = neo4j_arrow_port
        self._neo4j_auth = neo4j_auth
        self._neo4j_database = neo4j_database
        self._neo4j_protocol = neo4j_protocol
        self._neo4j_http_protocol = neo4j_http_protocol
        # neo4j arrow can't take none as a parameter for the database.
        self._neo4j_database_arrow = 'neo4j' if neo4j_database == None else neo4j_database
        self.__verbose = verbose

        self._default_namespace = xgt_server.get_default_namespace()
        # These are just kept as seperate variables because they may be needed
        self._neo4j_driver = None
        self._py2neo_driver = None
        self._arrow_driver = None

        if self.__verbose:
            print('Using ' + driver + ' for transfers of data.')

        self._neo4j_driver = neo4j.GraphDatabase.driver(f"{self._neo4j_protocol}://{self._neo4j_host}",
                                                        auth=self._neo4j_auth)
        if driver == 'neo4j-bolt':
            pass
        elif driver == 'py2neo-bolt':
            from py2neo import Graph
            self._py2neo_driver = Graph(
                self._neo4j_protocol + "://" + self._neo4j_host + ":" +str(neo4j_bolt_port),
                auth=neo4j_auth, name=neo4j_database)
        elif driver == 'py2neo-http':
            from py2neo import Graph
            self._py2neo_driver = Graph(
                self._neo4j_http_protocol + "://" + self._neo4j_host + ":" +str(neo4j_http_port),
                auth=neo4j_auth, name=neo4j_database)
        elif driver == 'neo4j-arrow':
            import neo4j_arrow as na
            self._arrow_driver = na.Neo4jArrow(self._neo4j_auth[0],
                                               self._neo4j_auth[1])
        else:
            raise ValueError(f"Unknown driver, {driver}.")

        self._neo4j_has_apoc = False if disable_apoc else self.__neo4j_check_for_apoc()
        if self.__verbose and self._neo4j_has_apoc:
            print("Using apoc to query schema.")

        self._neo4j_relationship_types = None
        self._neo4j_node_type_properties = None
        self._neo4j_rel_type_properties = None
        self._neo4j_property_keys = None
        self._neo4j_node_labels = None

    def __str__(self) -> str:
        result = ""
        result += f"Neo4j Node Labels: {self.neo4j_node_labels}\n\n"
        result += f"Neo4j Relationship Types: {self.neo4j_relationship_types}\n\n"
        result += f"Neo4j Property Keys: {self.neo4j_property_keys}\n\n"
        result += f"Neo4j Schema nodes: {self.neo4j_nodes}\n\n"
        result += f"Neo4j Schema edges: {self.neo4j_edges}\n\n"
        return result

    @property
    def neo4j_driver(self) -> neo4j.Neo4jDriver:
        """
        Retrieve the Python bolt driver connected to the Neo4j database.

        Returns
        -------
        neo4j.Neo4jDriver
          The Python bolt driver object that is connected to the Neo4j database.
        """
        return self._neo4j_driver

    @property
    def neo4j_relationship_types(self) -> list():
        """
        Retrieve a list of the Neo4j relationship types.

        Returns
        -------
        list
          List of the string names of relationship types in the connected Neo4j.
        """
        return self.__neo4j_relationship_types()

    @property
    def neo4j_node_labels(self) -> list():
        """
        Retrieve a list of the Neo4j node labels.

        Returns
        -------
        list
          List of the string names of node labels in the connected Neo4j.
        """
        return self.__neo4j_node_labels()

    @property
    def neo4j_property_keys(self) -> list():
        """
        Retrieve a list of the Neo4j property keys.

        Returns
        -------
        list
          List of the string names of property keys in the connected Neo4j.
        """
        return self.__neo4j_property_keys()

    @property
    def neo4j_node_type_properties(self) -> list():
        """
        Retrieve a list of the property types attached to the nodes in Neo4j.

        Each element of this list is a dictionary describing the property,
        including its name, its possible data types, and which node labels
        it may be attached to.

        Returns
        -------
        list
          List of the string names of node property types in the connected Neo4j.
        """

        return self.__neo4j_node_type_properties()

    @property
    def neo4j_rel_type_properties(self) -> list():
        """
        Retrieve a list of the property types attached to the relationships in
        Neo4j.

        Each element of this list is a dictionary describing the property,
        including its name, its possible data types, and which relationship(s)
        it may be attached to.

        Returns
        -------
        list
          List of the string names of relationship property types in the
          connected Neo4j.
        """
        return self.__neo4j_rel_type_properties()

    @property
    def neo4j_nodes(self) -> dict():
        """
        Retrieve a dictionary of the node labels (types) mapped to a
        description of the node's schema.

        Each dictionary entry has a key, which is the node label, and a value
        that is a dictionary of property-names mapped to property-types.

        Returns
        -------
        dict
          Dictionary mapping the node labels to a description of the
          node's schema.
        """
        return self.__neo4j_nodes()

    @property
    def neo4j_edges(self) -> dict():
        """
        Retrieve a dictionary of the edges (relationships) mapped to a
        description of the edge's metadata, including its property schema and
        its endpoint node labels (for both source and target).

        Returns
        -------
        dict
          Dictionary mapping the edge names to a description of the
          edge's schema and edge endpoints.
        """
        return self.__neo4j_edges()

    def get_xgt_schemas(self, vertices = None, edges = None,
                        neo4j_id_name = 'neo4j_id',
                        neo4j_source_node_name = 'neo4j_source',
                        neo4j_target_node_name = 'neo4j_target') -> dict():
        """
        Retrieve a dictionary containing the schema information for all of
        the nodes/vertices and all of the edges requested.
        If both vertices and edges is None, it will retrieve them all.

        Parameters
        ----------
        vertices : iterable
            List of requested node labels (vertex frame names).
        edges : iterable
            List of requested relationship type (edge frame) names.
            Any vertices not given for an edge will be automatically requested.
        neo4j_id_name : str
            The name of the xGT column holding the Neo4j node's ID value.
        neo4j_source_node_name : str
            The name of the xGT column holding the source node's ID value.
        neo4j_source_node_name: str
            The name of the xGT column holding the target node's ID value.

        Returns
        -------
        dict
            Dictionary containing the schema information both all of the nodes/
            vertices and all of the edges requested.
        """
        result = {'vertices' : dict(), 'edges' : dict()}

        self.__update_cache_state()

        if vertices is None and edges is None:
           vertices = {vertex : None for vertex in self.__neo4j_node_labels(False)}
           edges = {edge : None for edge in self.__neo4j_relationship_types(False)}
        elif edges is None:
            edges = { }
        elif vertices is None:
            vertices = { }

        # Any vertices not given for an edge will be added to vertices.
        for edge in edges:
            if edge not in self.__neo4j_relationship_types(False):
                raise ValueError(f"Neo4j Relationship {edge} is not found.")
            schemas = self.__extract_xgt_edge_schemas(edge, vertices,
                neo4j_source_node_name, neo4j_target_node_name, False)
            result['edges'][edge] = schemas

        for vertex in vertices:
            if vertex not in self.__neo4j_node_labels(False):
                raise ValueError(f"Neo4j Node Label {vertex} is not found.")
            table_schema = self.__extract_xgt_vertex_schema(vertex, neo4j_id_name, False)
            result['vertices'][vertex] = table_schema
            if self.__verbose:
                print(f"xGT graph schema for vertex {vertex}: {table_schema}")

        return result

    def create_xgt_schemas(self, xgt_schemas, append = False,
                           force = False) -> None:
        """
        Creates vertex and/or edge frames in Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.
        Finally, all of the nodes and all of the relationships are copied,
        one frame at a time, from Neo4j to xGT.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the
            :py:meth:`~Neo4jConnector.get_xgt_schemas` method.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).
        force : boolean
            Set to true to force xGT to drop edges when a vertex frame has dependencies.

        Returns
        -------
            None
        """
        if not append:
            for edge in xgt_schemas['edges']:
                self._xgt_server.drop_frame(edge)
                schemas = xgt_schemas['edges'][edge]
                # Frame name refers to multiple edges:
                if (len(schemas) > 0):
                    for schema in schemas:
                        multi_edge_name = self.__edge_name_transform(edge, schema['source'], schema['target'], True)
                        self._xgt_server.drop_frame(multi_edge_name)

            for vertex in xgt_schemas['vertices']:
                try:
                    self._xgt_server.drop_frame(vertex)
                except xgt.XgtFrameDependencyError as e:
                    if force:
                        # Would be better if this could be done without doing this.
                        edge_frames = str(e).split(':')[-1].split(' ')[1:]
                        for edge in edge_frames:
                            self._xgt_server.drop_frame(edge)
                        self._xgt_server.drop_frame(vertex)
                    else:
                        raise e

        for vertex, schema in xgt_schemas['vertices'].items():
            table_schema = schema['schema']
            key = schema['key']
            create_frame = True
            if append:
                try:
                    frame = self._xgt_server.get_vertex_frame(vertex)
                    if frame.schema != table_schema:
                        raise ValueError(
                            f"Vertex Frame {vertex} has a schema {frame.schema}"
                            + f" that is incompatible with Neo4j: {table_schema}"
                        )
                    create_frame = False
                except:
                    pass
            if create_frame:
                self._xgt_server.create_vertex_frame(
                    name = vertex, schema = table_schema,
                    key = key, attempts = 5,
                )

        for edge, schema_list in xgt_schemas['edges'].items():
            transform = True if len(schema_list) > 1 else False
            for schema in schema_list:
                table_schema = schema['schema']
                if self.__verbose:
                    print(f"xGT graph schema for edge {edge}: {table_schema}")
                name = self.__edge_name_transform(edge, schema['source'], schema['target'], transform)
                self._xgt_server.create_edge_frame(
                        name = name, schema = table_schema,
                        source = schema['source'],
                        source_key = schema['source_key'],
                        target = schema['target'],
                        target_key = schema['target_key'],
                        attempts = 5,
                    )

        return None

    def copy_data_to_xgt(self, xgt_schemas) -> None:
        """
        Copies data from Neo4j to the requested vertex and/or edge frames
        in Trovares xGT.

        This function copies data from Neo4j to xGT for all of the nodes and
        all of the relationships, one frame at a time.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the
            :py:meth:`~Neo4jConnector.get_xgt_schemas` method.

        Returns
        -------
            None
        """
        def xlate_result_property(attr, attr_type) -> str:
            if self._arrow_driver is not None and (attr_type == 'datetime' or attr_type == 'date' or attr_type == 'time'):
                return f", toString(v.{a}) as {a}"
            return f", v.{a} AS {a}"
        # Use the count store to get totals.
        estimated_counts = 0
        for vertex in xgt_schemas['vertices']:
            q = f"MATCH (v:{vertex}) RETURN count(v)"
            with self.__query(q) as query:
                for record in query.result():
                    estimated_counts += record[0]
        for edge in xgt_schemas['edges']:
            q = f"MATCH ()-[e:{edge}]->() RETURN count(e)"
            with self.__query(q) as query:
                for record in query.result():
                    estimated_counts += record[0]

        with  self.progress_display(estimated_counts) as progress_bar:
            for vertex, schema in xgt_schemas['vertices'].items():
                if self.__verbose:
                    print(f'Copy data for vertex {vertex} into schema: {schema}')
                table_schema = schema['schema']
                attributes = {_:t for _, t in table_schema}
                key = schema['key']
                query = f"MATCH (v:{vertex}) RETURN id(v) AS {key}"  # , {', '.join(attributes)}"
                for a in attributes:
                    if a != key:
                        query += xlate_result_property(a, attributes[a]) # f", v.{a} AS {a}"
                self.__copy_data(query, vertex, schema['neo4j_schema'], progress_bar)
            for edge, schema_list in xgt_schemas['edges'].items():
                if self.__verbose:
                    print(f'Copy data for node {edge} into schema: {schema_list}')
                transform = True if len(schema_list) > 1 else False
                for schema in schema_list:
                    name = self.__edge_name_transform(edge, schema['source'], schema['target'], transform)
                    table_schema = schema['schema']
                    attributes = {_:t for _, t in table_schema}
                    source = schema['source']
                    source_key = schema['source_key']
                    target = schema['target']
                    target_key = schema['target_key']
                    query = f"MATCH (u:{source})-[e:{edge}]->(v:{target}) RETURN"
                    query += f" id(u) AS {source_key}"
                    query += f", id(v) AS {target_key}"
                    for a in attributes:
                        if a != source_key and a != target_key:
                            query += f", e.{a} AS {a}"
                    self.__copy_data(query, name, schema['neo4j_schema'], progress_bar)
        return  None

    def transfer_to_xgt(self, vertices = None, edges = None,
                        neo4j_id_name = 'neo4j_id',
                        neo4j_source_node_name = 'neo4j_source',
                        neo4j_target_node_name = 'neo4j_target',
                        append = False, force = False) -> None:
        """
        Copies data from Neo4j to Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.
        Finally, all of the nodes and all of the relationships are copied,
        one frame at a time, from Neo4j to xGT.
        If both vertices and edges is None, it will retrieve them all.

        Parameters
        ----------
        vertices : iterable
            List of requested node labels (vertex frame names).
        edges : iterable
            List of requested relationship type (edge frame) names.
            Any vertices not given for an edge will be automatically requested.
        neo4j_id_name : str
            The name of the xGT column holding the Neo4j node's ID value.
        neo4j_source_node_name : str
            The name of the xGT column holding the source node's ID value.
        neo4j_source_node_name : str
            The name of the xGT column holding the target node's ID value.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).
        force : boolean
            Set to true to force xGT to drop edges when a vertex frame has dependencies.

        Returns
        -------
            None
        """
        xgt_schema = self.get_xgt_schemas(vertices, edges,
                neo4j_id_name, neo4j_source_node_name, neo4j_target_node_name)
        self.create_xgt_schemas(xgt_schema, append, force)
        self.copy_data_to_xgt(xgt_schema)
        return None

    def transfer_to_neo4j(self, vertices = None, edges = None, namespace = None,
                          edge_keys = False, vertex_keys = False):
        """
        Copies data from Trovares xGT to Neo4j.

        All of the nodes and all of the relationships are copied,
        one frame at a time, from xGT to Neo4j.
        If both vertices and edges is None, it will retrieve them all.

        Parameters
        ----------
        vertices : iterable
            List of requested node labels (vertex frame names).
        edges : iterable
            List of requested relationship type (edge frame) names.
            Any vertices not given for an edge will be automatically requested.
        namespace : str
            Namespace for the selected frames.
            If none will use the default namespace.
        edge_keys : boolean
            If true will transfer edge key columns.
        vertex_keys : boolean
            If true will transfer vertex key columns.

        Returns
        -------
            None
        """
        xgt_server = self._xgt_server
        if vertices == None and edges == None:
            vertices = [frame.name for frame in xgt_server.get_vertex_frames(namespace=namespace)]
            edges = [frame.name for frame in xgt_server.get_edge_frames(namespace=namespace)]
            namespace = None
        elif vertices == None:
            vertices = []
        elif edges == None:
            edges = []

        if namespace is not None:
            vertices = [namespace + "__" + vertex for vertex in vertices]
            edges = [namespace + "__" + edge for edge in vertices]

        id_neo4j_map = { }

        for edge in edges:
            edge_frame = xgt_server.get_edge_frame(edge)
            vertices.append(edge_frame.source_name)
            vertices.append(edge_frame.target_name)

        def convert(value):
            if value is None:
                return 'Null'
            elif isinstance(value, str):
                return '"' + value + '"'
            elif isinstance(value, datetime.datetime):
                format_string = 'datetime({{year:{0},month:{1},day:{2},hour:{3},minute:{4},second:{5},microsecond:{6}}})'
                return format_string.format(value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond)
            elif isinstance(value, datetime.date):
                return 'date({{year:{0},month:{1},day:{2}}})'.format(value.year, value.month, value.day)
            elif isinstance(value, datetime.time):
                return 'time({{hour:{0},minute:{1},second:{2},microsecond:{3}}})'.format(value.hour, value.minute, value.second, value.microsecond)
            else:
                return str(value)
        estimated_counts = 0
        for vertex in vertices:
            estimated_counts += xgt_server.get_vertex_frame(vertex).num_rows
        for edge in edges:
            estimated_counts += xgt_server.get_edge_frame(edge).num_rows
        with  self.progress_display(estimated_counts) as progress_bar:
            for vertex in vertices:
                if vertex in id_neo4j_map:
                    continue
                id_neo4j_map[vertex] = { }
                vertex_frame = xgt_server.get_vertex_frame(vertex)
                create_string = 'create (a:' + vertex + '{{{0}}}) return ID(a)'

                schema = vertex_frame.schema
                reader = self.__arrow_reader(vertex)

                labels = [val[0] for val in schema]

                for i, value in enumerate(schema):
                    if value[0] == vertex_frame.key:
                        key_pos = i
                        break

                with self._neo4j_driver.session(database=self._neo4j_database,
                                                default_access_mode=neo4j.WRITE_ACCESS) as session:
                    while (True):
                        try:
                            chunk = reader.read_chunk().data
                            rows = [None] * chunk.num_rows
                            for i in range(chunk.num_rows):
                                rows[i] = []
                            for i, x in enumerate(chunk):
                                for j, y in enumerate(x):
                                    rows[j].append(y.as_py())
                            tx = session.begin_transaction()
                            for row in rows:
                                elements = ",".join(labels[i] + ':' + convert(row[i], ) for i in range(len(row)) if vertex_keys or i != key_pos)
                                result = tx.run(create_string.format(elements))
                                for val in result:
                                    id_neo4j_map[vertex][row[key_pos]] = val[0]
                                progress_bar.show_progress(1)
                            tx.commit()
                            tx.close()
                        except StopIteration:
                            break

            for edge in edges:
                edge_frame = xgt_server.get_edge_frame(edge)
                source = edge_frame.source_name
                target = edge_frame.target_name
                source_frame = xgt_server.get_vertex_frame(source)
                target_frame = xgt_server.get_vertex_frame(target)
                source_map = id_neo4j_map[source]
                target_map = id_neo4j_map[target]
                create_string = 'match (a:' + source + '), (b:' + target + ') where ID(a) = {0} and ID(b) = {1} create (a)-[:' + edge + '{{{2}}}]->(b)'

                schema = edge_frame.schema
                reader = self.__arrow_reader(edge)
                labels = [val[0] for val in schema]

                for i, value in enumerate(schema):
                    if value[0] == edge_frame.source_key:
                        src_key_pos = i
                        break

                for i, value in enumerate(schema):
                    if value[0] == edge_frame.target_key:
                        trg_key_pos = i
                        break

                with self._neo4j_driver.session(database=self._neo4j_database,
                                                default_access_mode=neo4j.WRITE_ACCESS) as session:
                    while (True):
                        try:
                            chunk = reader.read_chunk().data
                            rows = [None] * chunk.num_rows
                            for i in range(chunk.num_rows):
                                rows[i] = []
                            for i, x in enumerate(chunk):
                                for j, y in enumerate(x):
                                    rows[j].append(y.as_py())
                            tx = session.begin_transaction()
                            for row in rows:
                                elements = ",".join(labels[i] + ':' + convert(row[i]) for i in range(len(row)) if edge_keys or (i != src_key_pos and i != trg_key_pos))
                                tx.run(create_string.format(source_map[row[src_key_pos]], target_map[row[trg_key_pos]], elements))
                                progress_bar.show_progress(1)
                            tx.commit()
                            tx.close()
                        except StopIteration:
                            break

    class py2neo_run_closure():
        def __init__(self, connector, query):
            self._result = connector._py2neo_driver.query(query)

        def __enter__(self):
            return self

        def __exit__(self, exc_type,exc_value, exc_traceback):
            pass

        def result(self):
            return self._result

    class neo4j_run_closure():
        def __init__(self, connector, query):
            self._query = query
            self._connector = connector

        def __enter__(self):
            self._session = self._connector._neo4j_driver.session(database=self._connector._neo4j_database,
                                                                  default_access_mode=neo4j.READ_ACCESS)
            return self
        def __exit__(self, exc_type,exc_value, exc_traceback):
            self._session.close()

        def result(self):
            return self._session.run(self._query)

    class progress_display():
        def __init__(self, total_count, bar_size = 60, prefix = "Transferring: "):
            self._bar_end = '\r'
            # When drawing the bar, we can only use 1 line so we need to shrink
            # the bar elements for cases where the terminal is too tiny.
            try:
                terminal_size = os.get_terminal_size().columns
            except:
                # Can't get terminal_size
                terminal_size = 0
            static_element_size = 80
            current_space_requirement = static_element_size + bar_size
            # Can't get terminal_size
            if terminal_size == 0:
                pass
            # Terminal is too tiny for a bar: just print the results.
            elif terminal_size < static_element_size:
                bar_size = 0
                self._bar_end = '\n'
            # Shrink bar to fit the print within the terminal.
            elif terminal_size < current_space_requirement:
                bar_size = terminal_size - static_element_size
            self._total_count = total_count
            self._count = 0
            self._bar_size = bar_size
            self._prefix = prefix
            self._start_time = time.time()

        def __enter__(self):
            self.show_progress()
            return self

        def __exit__(self, exc_type,exc_value, exc_traceback):
            print("", flush=True)

        def __format_time(self, seconds, digits=1):
            isec, fsec = divmod(round(seconds*10**digits), 10**digits)
            return f'{datetime.timedelta(seconds=isec)}.{fsec:0{digits}.0f}'

        def show_progress(self, count_to_add = 0):
            self._count += count_to_add
            # Counts are no longer accurate
            while (self._count > self._total_count):
                self._total_count *= 2
            progress = int(self._count * self._bar_size / self._total_count)
            current_elapsed = time.time() - self._start_time
            remaining = 0 if self._count == 0 else ((self._total_count - self._count) *
                                                   (current_elapsed)) / self._count
            rate = 0 if self._count == 0 else round(self._count / (current_elapsed), 1)
            remaining = self.__format_time(remaining)
            duration = self.__format_time(current_elapsed)
            print("{}[{}{}] {}/{} in {}s ({}/s, eta: {}s)     ".format(self._prefix,
                  u"#"*progress, "."*(self._bar_size-progress), self._count,
                  self._total_count, duration, rate, remaining), end=self._bar_end, flush=True)

    class BasicArrowClientAuthHandler(pf.ClientAuthHandler):
        def __init__(self, username, password):
            super().__init__()
            self.basic_auth = pf.BasicAuth(username, password)
            self.token = None
        def __init__(self):
            super().__init__()
            self.basic_auth = pf.BasicAuth()
            self.token = None
        def authenticate(self, outgoing, incoming):
            auth = self.basic_auth.serialize()
            outgoing.write(auth)
            self.token = incoming.read()
        def get_token(self):
            return self.token

    def __query(self, query, use_neo4j_always = False):
        if not use_neo4j_always and self._py2neo_driver is not None:
            return self.py2neo_run_closure(self, query)
        else:
            return self.neo4j_run_closure(self, query)

    def __neo4j_check_for_apoc(self):
        try:
            with self.__query("RETURN apoc.version()") as query:
                query.result()
                return True
        except Exception as e:
            pass
        return False

    def __neo4j_property_keys(self, flush_cache = True):
        if flush_cache:
            with self.__query("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey") as query:
                self._neo4j_property_keys = list([record["propertyKey"] for record in query.result()])
                self._neo4j_property_keys.sort()
        return self._neo4j_property_keys

    def __neo4j_node_type_properties(self, flush_cache = True):
        if flush_cache:
            fields = ('nodeType', 'nodeLabels', 'propertyName', 'propertyTypes', 'mandatory')
            if self._neo4j_has_apoc:
                q="CALL apoc.meta.nodeTypeProperties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory RETURN *"
            else:
                q="CALL db.schema.nodeTypeProperties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory RETURN *"
            with self.__query(q) as query:
                self._neo4j_node_type_properties = [{_ : record[_] for _ in fields} for record in query.result()]
        return self._neo4j_node_type_properties

    def __neo4j_rel_type_properties(self, flush_cache = True):
        if flush_cache:
            fields = ('relType', 'propertyName', 'propertyTypes', 'mandatory')
            if self._neo4j_has_apoc:
                q="CALL apoc.meta.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory RETURN *"
            else:
                q="CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory RETURN *"
            with self.__query(q) as query:
                self._neo4j_rel_type_properties = [{_ : record[_] for _ in fields} for record in query.result()]
        return self._neo4j_rel_type_properties

    def __add_neo4j_schema_connectivity_to_neo4j_edges(self) -> None:
        def extract_node_info(node):
            labels = node.labels
            if len(labels) == 1:
                return list(labels)[0]
            return labels
        if self._neo4j_has_apoc:
            q="CALL apoc.meta.graph()"
        else:
            q="CALL db.schema.visualization() YIELD nodes, relationships RETURN *"
        # TODO(landwehrj) Can schema be done with py2neo?
        with self.__query(q, True) as query:
            for record in query.result():
                has_multiple_relations = len(record['relationships']) > 1
                for e in record['relationships']:
                    nodes = e.nodes
                    source = nodes[0]
                    target = nodes[1]
                    e_type = e.type

                    source_name = extract_node_info(source)
                    target_name = extract_node_info(target)

                    if not self._neo4j_has_apoc and has_multiple_relations:
                        # Neo4j does not return the correct schema for multi-edges
                        # without APOC so we need to do additional queries to filter fake edges:
                        # See https://github.com/neo4j/neo4j/issues/9726
                        schema_exists = False
                        q = f"MATCH (:{source_name})-[e:{e_type}]->(:{target_name}) return count(e) > 0"
                        with self.__query(q) as query:
                            for result in query.result():
                                schema_exists = result[0]

                        if (not schema_exists):
                            continue

                    if self.__verbose:
                        print(f"Edge Connectivity: {e}")
                        print(f" -> type => {e_type}")
                        print(f" -> source nodes => {source}")
                        print(f" -> target nodes => {nodes[1]}")
                        print(f"  -> Edge {e_type}: {self._neo4j_edges[e_type]}\n")
                    if 'endpoints' not in self._neo4j_edges[e_type]:
                        self._neo4j_edges[e_type]['endpoints'] = set()
                        self._neo4j_edges[e_type]['sources'] = set()
                        self._neo4j_edges[e_type]['targets'] = set()
                    self._neo4j_edges[e_type]['endpoints'].add(
                        f"{extract_node_info(source)}->{extract_node_info(target)}")
                    self._neo4j_edges[e_type]['sources'].add(extract_node_info(source))
                    self._neo4j_edges[e_type]['targets'].add(extract_node_info(target))
        return None

    def __neo4j_nodes(self, flush_cache = True):
        if flush_cache:
            nodes = dict()
            for prop in self.__neo4j_node_type_properties(flush_cache):
                labels = prop['nodeLabels']
                propTypes = prop['propertyTypes']
                if propTypes is not None and len(propTypes) == 1:
                        propTypes = propTypes[0]
                for name in labels:
                    if name not in nodes:
                        nodes[name] = dict()
                    if propTypes is not None:
                        nodes[name][prop['propertyName']] = propTypes
            self._neo4j_nodes = nodes
        return self._neo4j_nodes

    def __neo4j_edges(self, flush_cache = True) -> dict():
        if flush_cache:
          self.__update_cache_state()
        return self._neo4j_edges

    def __neo4j_process_nodes(self, nodes):
        res = dict()
        for n in nodes:
            if self.__verbose:
                print(f"__neo4j_process_nodes: {n}")
            propName = n['propertyName']
            propType = n['propertyTypes']
            if propType is not None and len(propType) == 1:
                propType = propType[0]
            for name in n['nodeLabels']:
                if name not in res:
                    res[name] = dict()
                if propName is not None and propName not in res[name]:
                    res[name][propName] = propType
        return res

    def __neo4j_process_edges(self, edges):
        res = dict()
        for e in edges:
            if self.__verbose:
                print(f"__neo4j_process_edges: {e}")
            relType = e['relType']
            propName = e['propertyName']
            propType = e['propertyTypes']
            if relType[0:2] == ':`' and relType[-1] == '`':
                relType = relType[2:-1]
            if propType is not None and len(propType) == 1:
                propType = propType[0]
            if relType not in res:
                res[relType] = {'schema' : dict()}
            if propName is not None:
                res[relType]['schema'][propName] = propType
        return res

    def __extract_xgt_vertex_schema(self, vertex, neo4j_id_name, flush_cache = True):
        if flush_cache:
          self.__update_cache_state()
        if vertex in self.__neo4j_nodes(False):
            neo4j_node = self.__neo4j_nodes(False)[vertex]
            neo4j_node_attributes = list(neo4j_node.keys())
            if neo4j_id_name in neo4j_node:
                raise ValueError(
                        f"Neo4j ID name {neo4j_id_name} is an attribute of node {neo4j_node}")
        schema = [[key, self.__neo4j_type_to_xgt_type(type)]
                  for key, type in neo4j_node.items()]
        neo4j_schema = [[key, type] for key, type in neo4j_node.items()]
        schema.insert(0, [neo4j_id_name, xgt.INT])
        neo4j_schema.insert(0, [neo4j_id_name, 'INTEGER'])
        return {'schema' : schema, 'neo4j_schema' : neo4j_schema,
                'key' : neo4j_id_name}

    def __extract_xgt_edge_schema(self, edge, source, target,
                                  neo4j_source_node_name,
                                  neo4j_target_node_name,
                                  flush_cache = True):
        if flush_cache:
          self.__update_cache_state()
        result = dict()
        if edge not in self.__neo4j_relationship_types(False):
            raise ValueError(f"Edge {edge} is not a known relationship type")
        edge_info = self.__neo4j_edges(False)[edge]
        if self.__verbose:
            print(f"xGT graph schema for edge {edge}: {edge_info}")
        info_schema = edge_info['schema']
        edge_endpoints = edge_info['endpoints']
        endpoints = f"{source}->{target}"
        if endpoints not in edge_endpoints:
            if len(edge_endpoints) == 1:
                raise ValueError(f"Edge Type {edge} with endpoints: {endpoints} not found.")
            else:
                return

        schema = [[key, self.__neo4j_type_to_xgt_type(type)]
                  for key, type in info_schema.items()]
        neo4j_schema = [[key, type] for key, type in info_schema.items()]
        schema.insert(0, [neo4j_target_node_name, xgt.INT])
        schema.insert(0, [neo4j_source_node_name, xgt.INT])
        neo4j_schema.insert(0, [neo4j_target_node_name, 'INTEGER'])
        neo4j_schema.insert(0, [neo4j_source_node_name, 'INTEGER'])
        return {'schema' : schema, 'neo4j_schema' : neo4j_schema,
                'source' : source, 'target' : target,
                'source_key' : neo4j_source_node_name,
                'target_key' : neo4j_target_node_name}

    def __extract_xgt_edge_schemas(self, edge, vertices,
                                    neo4j_source_node_name,
                                    neo4j_target_node_name, flush_cache = True):
        if flush_cache:
          self.__update_cache_state()
        schemas = []
        neo4j_edge = self.__neo4j_edges(False)[edge]
        for source in neo4j_edge['sources']:
            if source not in vertices:
                vertices[source] = None
            for target in neo4j_edge['targets']:
                if target not in vertices:
                    vertices[target] = None
                result = self.__extract_xgt_edge_schema(edge,
                    source, target, neo4j_source_node_name,
                    neo4j_target_node_name, flush_cache)
                if result is not None:
                    schemas.append(result)
        return schemas

    def __neo4j_type_to_xgt_type(self, prop_type):
        if isinstance(prop_type, list):
                raise ValueError(
                    f"Multiple types for property not supported.")
        elif prop_type in self._NEO4J_TYPE_TO_XGT_TYPE:
            return self._NEO4J_TYPE_TO_XGT_TYPE[prop_type]
        raise TypeError(f'The "{prop_type}" Neo4j type is not yet supported')

    def __arrow_writer(self, frame_name, schema):
        arrow_conn = pf.FlightClient((self._xgt_server.host, self._xgt_server.port))
        arrow_conn.authenticate(self.BasicArrowClientAuthHandler())
        writer, _ = arrow_conn.do_put(
            pf.FlightDescriptor.for_path(self._default_namespace, frame_name),
            schema)
        return writer

    def __arrow_reader(self, frame_name):
        arrow_conn = pf.FlightClient((self._xgt_server.host, self._xgt_server.port))
        arrow_conn.authenticate(self.BasicArrowClientAuthHandler())
        return arrow_conn.do_get(pf.Ticket(self._default_namespace + '__' + frame_name))

    def __copy_data(self, cypher_for_extract, frame, neo4j_schema, progress_bar):
        if self._py2neo_driver is not None:
            self.__py2neo_copy_data(cypher_for_extract, neo4j_schema, frame, progress_bar)
        elif self._arrow_driver is not None:
            self.__arrow_copy_data(cypher_for_extract, frame, progress_bar)
        else:
            self.__bolt_copy_data(cypher_for_extract, neo4j_schema, frame, progress_bar)

    def __bolt_copy_data(self, cypher_for_extract, neo4j_schema, frame, progress_bar):
        with self._neo4j_driver.session(database=self._neo4j_database,
                                        default_access_mode=neo4j.READ_ACCESS) as session:
            schema = pa.schema([])
            # With xGT 10.1 we need to change double to float
            # so we infer the schema manually.
            for i, value in enumerate(neo4j_schema):
                arrow_type = self._NEO4J_TYPE_TO_ARROW_TYPE[value[1]]
                schema = schema.append(pa.field('col' + str(i), arrow_type))

            result = session.run(cypher_for_extract)
            first_record = result.peek()
            data = [None] * len(first_record)

            block_size = 10000
            for i in range(len(first_record)):
                data[i] = [None] * block_size

            xgt_writer = self.__arrow_writer(frame, schema)
            chunk_count = 0
            for record in result:
                for i, val in enumerate(record):
                    if isinstance(val, (neo4j.time.Date, neo4j.time.Time,
                                        neo4j.time.DateTime)):
                        data[i][chunk_count] = val.to_native()
                    elif isinstance(val, neo4j.time.Duration):
                        # For months this average seconds in a month.
                        val = (val.months * 2628288 + val.days * 86400 +
                               val.seconds) * 10**9 + val.nanoseconds
                        data[i][chunk_count] = val
                    else:
                        data[i][chunk_count] = val
                chunk_count = chunk_count + 1
                if chunk_count == block_size:
                    batch = pa.RecordBatch.from_arrays(data, schema=schema)
                    xgt_writer.write(batch)
                    progress_bar.show_progress(chunk_count)
                    chunk_count = 0

            if chunk_count > 0:
                for j in range(len(data)):
                    data[j] = data[j][:-(block_size - chunk_count)]
                batch = pa.RecordBatch.from_arrays(data, schema=schema)
                xgt_writer.write(batch)
                progress_bar.show_progress(chunk_count)

            xgt_writer.close()

    def __py2neo_copy_data(self, cypher_for_extract, neo4j_schema, frame, progress_bar):
        schema = pa.schema([])
        # With xGT 10.1 we need to change double to float
        # so we infer the schema manually.
        for i, value in enumerate(neo4j_schema):
            arrow_type = self._NEO4J_TYPE_TO_ARROW_TYPE[value[1]]
            schema = schema.append(pa.field('col' + str(i), arrow_type))

        result = self._py2neo_driver.query(cypher_for_extract)
        data = [None] * len(result.keys())

        block_size = 10000
        for i in range(len(result.keys())):
            data[i] = [None] * block_size

        xgt_writer = self.__arrow_writer(frame, schema)
        chunk_count = 0
        # Types Used by py2neo
        from interchange.time import Date, Time, DateTime, Duration
        for record in result:
            for i, val in enumerate(record):
                if isinstance(val, (Date, Time, DateTime)):
                    data[i][chunk_count] = val.to_native()
                elif isinstance(val, Duration):
                    # For months this average seconds in a month.
                    val = (val.months * 2628288 + val.days * 86400 +
                           val.seconds) * 10**9 + int(val.subseconds * 10**9)
                    data[i][chunk_count] = val
                else:
                    data[i][chunk_count] = val
            chunk_count = chunk_count + 1
            if chunk_count == block_size:
                batch = pa.RecordBatch.from_arrays(data, schema=schema)
                xgt_writer.write(batch)
                progress_bar.show_progress(chunk_count)
                chunk_count = 0

        if chunk_count > 0:
            for j in range(len(data)):
                data[j] = data[j][:-(block_size - chunk_count)]
            batch = pa.RecordBatch.from_arrays(data, schema=schema)
            xgt_writer.write(batch)
            progress_bar.show_progress(chunk_count)

        xgt_writer.close()

    def __arrow_copy_data(self, cypher_for_extract, frame, progress_bar):
        ticket = self._arrow_driver.cypher(cypher_for_extract,
                                           self._neo4j_database_arrow)
        ready = self._arrow_driver.wait_for_job(ticket, timeout=60)
        if not ready:
            raise Exception('something is wrong...did you submit a job?')
        neo4j_reader = self._arrow_driver.stream(ticket).to_reader()
        xgt_writer = self.__arrow_writer(frame, neo4j_reader.schema)
        # move data from Neo4j to xGT in chunks
        while (True):
            try:
                batch = neo4j_reader.read_next_batch()
                xgt_writer.write(batch)
                progress_bar.show_progress(batch.num_rows)
            except StopIteration:
                break
        xgt_writer.close()

    def __neo4j_relationship_types(self, flush_cache = True) -> list():
        if flush_cache:
          self.__update_cache_state()
        if self._neo4j_relationship_types is None:
            self._neo4j_relationship_types = list(self._neo4j_edges.keys())
            self._neo4j_relationship_types.sort()
        return self._neo4j_relationship_types

    def __neo4j_node_labels(self, flush_cache = True) -> list():
        if flush_cache:
          self.__update_cache_state()
        if self._neo4j_node_labels is None:
            self._neo4j_node_labels = list(self._neo4j_nodes.keys())
            self._neo4j_node_labels.sort()
        return self._neo4j_node_labels

    def __edge_name_transform(self, edge, source, target, transform_name):
        if transform_name:
            return source + '_' + edge + '_' + target
        else:
            return edge

    # TODO(landwehrj) : Is there a way to detect the cache is stale
    # One option is to use the Neo4j count store of relationship/nodes
    # to check if there are changes there. This wouldn't work in certain
    # cases. Is there a way to get database modification time?
    # Is it possible to query individual schema elements?
    def __update_cache_state(self):
        self._neo4j_relationship_types = None
        self._neo4j_node_type_properties = None
        self._neo4j_rel_type_properties = None
        self._neo4j_property_keys = None
        self._neo4j_node_labels = None
        self._neo4j_nodes = None
        n = self.neo4j_node_type_properties
        self._neo4j_nodes = self.__neo4j_process_nodes(n)
        e = self.neo4j_rel_type_properties
        self._neo4j_edges = self.__neo4j_process_edges(e)
        self.__add_neo4j_schema_connectivity_to_neo4j_edges()

