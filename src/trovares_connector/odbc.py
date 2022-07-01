import pyarrow as pa
import pyarrow.flight as pf
from arrow_odbc import read_arrow_batches_from_odbc
from arrow_odbc import insert_into_table
from .common import ProgressDisplay
from .common import BasicArrowClientAuthHandler

class SQLODBCDriver(object):
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._schema_query = "SELECT * FROM {0} LIMIT 1"
        self._data_query = "SELECT * FROM {0}"
        self._estimate_query="SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'"

class ODBCConnector(object):
    def __init__(self, xgt_server, odbc_driver):
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
                            mapping_edges[val[0]] = {'frame' : val[1], 'source' : val[2][2],
                                                     'target' : val[2][3], 'source_key' : val[2][0],
                                                     'target_key' : val[2][1]}
                    else:
                        mapping_tables[val[0]] = {'frame' : val[1]}
                elif isinstance(val[1], tuple) and len(val[1]) == 1:
                    mapping_vertices[val[0]] = {'frame' : val[0], 'key' : val[1]}
                elif isinstance(val[1], tuple) and len(val[2]) == 2:
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
                           force = False) -> None:
        if not append:
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
                        self._xgt_server.drop_frame(schema['xgt_name'])
                    else:
                        raise e

            for table, schema in xgt_schemas['tables'].items():
                self._xgt_server.create_table_frame(name = schema['mapping']['frame'], schema = schema['xgt_schema'])
            for vertex, schema in xgt_schemas['vertices'].items():
                key = schema['mapping']['key']
                if isinstance(key, int):
                    key = schema['xgt_schema'][key][0]
                self._xgt_server.create_vertex_frame(name = schema['mapping']['frame'], schema = schema['xgt_schema'], key = key)
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

    def transfer_to_xgt(self, tables = None, append = False, force = False) -> None:
        xgt_schema = self.get_xgt_schemas(tables)
        self.create_xgt_schemas(xgt_schema, append, force)
        self.copy_data_to_xgt(xgt_schema)

    def transfer_to_odbc(self, vertices = None, edges = None,
                         tables = None, namespace = None) -> None:
        xgt_server = self._xgt_server
        if namespace == None:
            namespace = self._default_namespace
        if vertices == None and edges == None and tables == None:
            vertices = [frame.name for frame in xgt_server.get_vertex_frames(namespace=namespace)]
            edges = [frame.name for frame in xgt_server.get_edge_frames(namespace=namespace)]
            tables = [frame.name for frame in xgt_server.get_table_frames(namespace=namespace)]
            namespace = None
        if vertices == None:
            vertices = []
        if edges == None:
            edges = []
        if tables == None:
            tables = []

        for edge in edges:
            edge_frame = xgt_server.get_edge_frame(edge)
            vertices.append(edge_frame.source_name)
            vertices.append(edge_frame.target_name)

        for table in tables:
            if isinstance(table, tuple):
                frame, table = table
            else:
                frame = table
            reader = self.__arrow_reader(frame)
            reader1 = reader.to_reader()

            first_batch = reader1.read_next_batch()
            schema = first_batch.schema
            def iter_record_batches():
                count = 0
                l = pa.Table.from_pandas(first_batch.to_pandas()).to_batches()
                for x in l:
                    count += 1
                    yield x
                for batch in reader1:
                    l = pa.Table.from_pandas(batch.to_pandas()).to_batches()
                    for x in l:
                        count += 1
                        yield x

            final_reader = pa.RecordBatchReader.from_batches(schema, iter_record_batches())
            insert_into_table(
                connection_string=self._driver._connection_string,
                chunk_size=10000,
                table=table,
                reader=final_reader,
            )

    def copy_data_to_xgt(self, xgt_schemas):
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

    def __extract_xgt_table_schema(self, table, mapping):
        xgt_schema, arrow_schema = self.__get_xgt_schema(table)
        return {'xgt_schema' : xgt_schema, 'arrow_schema' : arrow_schema, 'mapping' : mapping[table]}

