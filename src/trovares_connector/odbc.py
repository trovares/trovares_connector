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

import xgt
import pyarrow as pa
import pyarrow.flight as pf
from arrow_odbc import read_arrow_batches_from_odbc
from arrow_odbc import insert_into_table
from .common import ProgressDisplay
from .common import BasicArrowClientAuthHandler

class SQLODBCDriver(object):
    def __init__(self, connection_string):
        """
        Initializes the driver class.

        Parameters
        ----------
        connection_string : str
            Standard ODBC connection string used for connecting to the ODBC applications.
            Example:
            'Driver={MariaDB};Server=127.0.0.1;Port=3306;Database=test;Uid=test;Pwd=foo;'
        """
        self._connection_string = connection_string
        self._schema_query = "SELECT * FROM {0} LIMIT 1"
        self._data_query = "SELECT * FROM {0}"
        self._estimate_query="SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'"

class ODBCConnector(object):
    def __init__(self, xgt_server, odbc_driver):
        """
        Initializes the connector class.

        Parameters
        ----------
        xgt_server : xgt.Connection
            Connection object to xGT.
        odbc_driver : SQLODBCDriver
            Connection object to ODBC.
        """
        self._xgt_server = xgt_server
        self._default_namespace = xgt_server.get_default_namespace()
        self._driver = odbc_driver

    def __arrow_writer(self, frame_name, schema):
        arrow_conn = pf.FlightClient((self._xgt_server.host, self._xgt_server.port))
        arrow_conn.authenticate(BasicArrowClientAuthHandler())
        writer, _ = arrow_conn.do_put(
            pf.FlightDescriptor.for_path(self._default_namespace, frame_name),
            schema)
        return writer

    def __arrow_reader(self, frame_name):
        arrow_conn = pf.FlightClient((self._xgt_server.host, self._xgt_server.port))
        arrow_conn.authenticate(BasicArrowClientAuthHandler())
        return arrow_conn.do_get(pf.Ticket(self._default_namespace + '__' + frame_name))

    def get_xgt_schemas(self, tables = None):
        """
        Retrieve a dictionary containing the schema information for all of
        the tables requested and their mappings.

        Parameters
        ----------
        tables : iterable
            List of requested tables.

        Returns
        -------
        dict
            Dictionary containing the schema information of the tables,
            vertices, and edges requested.
        """
        if tables is None:
            tables = [ ]
        mapping_vertices = { }
        mapping_edges = { }
        mapping_tables = { }
        result = {'vertices' : dict(), 'edges' : dict(), 'tables' : dict()}

        for val in tables:
            if isinstance(val, str):
                mapping_tables[val] = {'frame' : val}
            elif isinstance(val, tuple):
                # ('table', X, ...)
                if isinstance(val[1], str):
                    # ('table', (...), ...)
                    if len(val) == 3:
                        # ('table', (), ...)
                        if len(val[2]) == 1:
                            mapping_vertices[val[0]] = {'frame' : val[1], 'key' : val[2][0]}
                        # ('table', (,), ...)
                        elif len(val[2]) == 4:
                            mapping_edges[val[0]] = {'frame' : val[1], 'source' : val[2][0],
                                                     'target' : val[2][1], 'source_key' : val[2][2],
                                                     'target_key' : val[2][3]}
                    else:
                        mapping_tables[val[0]] = {'frame' : val[1]}
                elif isinstance(val[1], tuple) and len(val[1]) == 1:
                    mapping_vertices[val[0]] = {'frame' : val[0], 'key' : val[1][0]}
                elif isinstance(val[1], tuple) and len(val[1]) == 4:
                    mapping_edges[val[0]] = {'frame' : val[0], 'source' : val[1][0],
                                             'target' : val[1][1], 'source_key' : val[1][2],
                                             'target_key' : val[1][3]}
                elif isinstance(val[1], dict):
                    if len(val[1]) == 1:
                        mapping_tables[val[0]] = val[1]
                    elif len(val[1]) == 2:
                        mapping_vertices[val[0]] = val[1]
                    elif len(val[1]) == 5:
                        mapping_edges[val[0]] = val[1]
                    else:
                        raise ValueError("Dictionary format incorrect for " + val[0])
                else:
                    raise ValueError("Argument format incorrect for " + val)

        for table in mapping_tables:
            schema = self.__extract_xgt_table_schema(table, mapping_tables)
            result['tables'][table] = schema

        for table in mapping_vertices:
            schema = self.__extract_xgt_table_schema(table, mapping_vertices)
            result['vertices'][table] = schema

        for table in mapping_edges:
            schema = self.__extract_xgt_table_schema(table, mapping_edges)
            result['edges'][table] = schema

        return result

    def create_xgt_schemas(self, xgt_schemas, append = False,
                           force = False, easy_edges = False) -> None:
        """
        Creates table, vertex and/or edge frames in Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the
            :py:meth:`~ODBCConnector.get_xgt_schemas` method.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).
        force : boolean
            Set to true to force xGT to drop edges when a vertex frame has dependencies.
        easy_edges : boolean
            Set to true to create a basic vertex class wtih key column for any edges
            without corresponding vertex frames.

        Returns
        -------
            None
        """
        if not append:
            if easy_edges:
                for edge, schema in xgt_schemas['edges'].items():
                    src = schema['mapping']['source']
                    trg = schema['mapping']['target']
                    if src not in xgt_schemas['vertices']:
                        xgt_schemas['vertices'][src] = { 'xgt_schema': [['key', 'int']], 'temp_creation' : True, 'mapping' : { 'frame' : src, 'key' : 'key' } }
                    if trg not in xgt_schemas['vertices']:
                        xgt_schemas['vertices'][trg] = { 'xgt_schema': [['key', 'int']], 'temp_creation' : True, 'mapping' : {'frame' : trg, 'key' : 'key' } }
                for _, schema in xgt_schemas['tables'].items():
                    self._xgt_server.drop_frame(schema['mapping']['frame'])

            for _, schema in xgt_schemas['edges'].items():
                self._xgt_server.drop_frame(schema['mapping']['frame'])

            for _, schema in xgt_schemas['vertices'].items():
                try:
                    self._xgt_server.drop_frame(schema['mapping']['frame'])
                except xgt.XgtFrameDependencyError as e:
                    if force:
                        # Would be better if this could be done without doing this.
                        edge_frames = str(e).split(':')[-1].split(' ')[1:]
                        for edge in edge_frames:
                            self._xgt_server.drop_frame(edge)
                        self._xgt_server.drop_frame(schema['mapping']['frame'])
                    else:
                        raise e
            for table, schema in xgt_schemas['tables'].items():
                self._xgt_server.create_table_frame(name = schema['mapping']['frame'], schema = schema['xgt_schema'])

            remove_list = []
            for vertex, schema in xgt_schemas['vertices'].items():
                key = schema['mapping']['key']
                if isinstance(key, int):
                    key = schema['xgt_schema'][key][0]
                self._xgt_server.create_vertex_frame(name = schema['mapping']['frame'], schema = schema['xgt_schema'], key = key)
                if 'temp_creation' in schema:
                    remove_list.append(vertex)

            for vertex in remove_list:
                xgt_schemas['vertices'].pop(vertex)

            for edge, schema in xgt_schemas['edges'].items():
                src = schema['mapping']['source']
                trg = schema['mapping']['target']
                src_key = schema['mapping']['source_key']
                trg_key = schema['mapping']['target_key']
                if isinstance(src_key, int):
                    src_key = schema['xgt_schema'][src_key][0]
                if isinstance(trg_key, int):
                    trg_key = schema['xgt_schema'][trg_key][0]
                self._xgt_server.create_edge_frame(name = schema['mapping']['frame'], schema = schema['xgt_schema'],
                                                   source = src, target = trg, source_key = src_key, target_key = trg_key)

    def transfer_to_xgt(self, tables = None, append = False, force = False, easy_edges = False) -> None:
        """
        Copies data from the ODBC application to Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.
        Finally, all of the tables, vertices, and all of the edges are copied,
        one frame at a time, from the ODBC application to xGT.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the
            :py:meth:`~ODBCConnector.get_xgt_schemas` method.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).
        force : boolean
            Set to true to force xGT to drop edges when a vertex frame has dependencies.
        easy_edges : boolean
            Set to true to create a basic vertex class wtih key column for any edges
            without corresponding vertex frames.

        Returns
        -------
            None
        """
        xgt_schema = self.get_xgt_schemas(tables)
        self.create_xgt_schemas(xgt_schema, append, force, easy_edges)
        self.copy_data_to_xgt(xgt_schema)

    def copy_data_to_xgt(self, xgt_schemas):
        """
        Copies data from the ODBC application to the requested table, vertex and/or edge frames
        in Trovares xGT.

        This function copies data from the ODBC application to xGT for all of the tables, vertices
        and edges, one frame at a time.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for table, vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the
            :py:meth:`~ODBCConnector.get_xgt_schemas` method.

        Returns
        -------
            None
        """
        estimate = 0
        def estimate_size(table):
            estimate = 0
            reader = read_arrow_batches_from_odbc(
                query=self._driver._estimate_query.format(table),
                connection_string=self._driver._connection_string,
                batch_size=10000,
            )
            for batch in reader:
                for _, item in batch.to_pydict().items():
                    estimate += item[0]
            return estimate
        try:
            for table, schema in xgt_schemas['tables'].items():
                estimate += estimate_size(table)
            for table, schema in xgt_schemas['vertices'].items():
                estimate += estimate_size(table)
            for table, schema in xgt_schemas['edges'].items():
                estimate += estimate_size(table)
        except Exception as e:
            pass

        with ProgressDisplay(estimate) as progress_bar:
            for table, schema in xgt_schemas['tables'].items():
                self.__copy_data(self._driver._data_query.format(table), schema['mapping']['frame'], schema['arrow_schema'], progress_bar)
            for table, schema in xgt_schemas['vertices'].items():
                self.__copy_data(self._driver._data_query.format(table), schema['mapping']['frame'], schema['arrow_schema'], progress_bar)
            for table, schema in xgt_schemas['edges'].items():
                self.__copy_data(self._driver._data_query.format(table), schema['mapping']['frame'], schema['arrow_schema'], progress_bar)

    def __copy_data(self, query_for_extract, frame, schema, progress_bar):
        reader = read_arrow_batches_from_odbc(
            query=query_for_extract,
            connection_string=self._driver._connection_string,
            batch_size=10000,
        )
        writer = self.__arrow_writer(frame, schema)
        for batch in reader:
            # Process arrow batches
            writer.write(batch)
            progress_bar.show_progress(batch.num_rows)
        writer.close()

    def __get_xgt_schema(self, table):
        reader = read_arrow_batches_from_odbc(
            query=self._driver._schema_query.format(table),
            connection_string=self._driver._connection_string,
            batch_size=1,
        )
        val = next(reader, None)
        if val != None:
            return (self._xgt_server.get_schema_from_data(pa.Table.from_batches([val])), val.schema)
        raise ValueError("Table " + table + " contains no data. Can't determine schema.")

    def __extract_xgt_table_schema(self, table, mapping):
        xgt_schema, arrow_schema = self.__get_xgt_schema(table)
        return {'xgt_schema' : xgt_schema, 'arrow_schema' : arrow_schema, 'mapping' : mapping[table]}

