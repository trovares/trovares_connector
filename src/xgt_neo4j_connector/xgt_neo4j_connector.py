import pyarrow as pa
import pyarrow.flight as pf

import neo4j
import neo4j_arrow as na
import xgt

class BasicClientAuthHandler(pf.ClientAuthHandler):
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

class Neo4jConnector(object):
    NEO4J_TYPE_TO_XGT_TYPE = {
        'INTEGER': xgt.INT,
        'Long': xgt.INT,
        'FLOAT': xgt.FLOAT,
        'STRING': xgt.TEXT,
        'String': xgt.TEXT,
    }

    def __init__(self, xgt_server,
                       neo4j_host = 'localhost', 
                       neo4j_port = 7474, neo4j_bolt_port = 7687,
                       neo4j_arrow_port = 9999, neo4j_auth = None,
                       verbose = False):
        self._xgt_server = xgt_server
        self._neo4j_host = neo4j_host
        self._neo4j_port = neo4j_port
        self._neo4j_bolt_port = neo4j_bolt_port
        self._neo4j_arrow_port = neo4j_arrow_port
        self._neo4j_auth = neo4j_auth
        self.__verbose = verbose

        self._default_namespace = xgt_server.get_default_namespace()
        self._neo4j_driver = neo4j.GraphDatabase.driver(f"neo4j://{self._neo4j_host}",
                                                        auth=self._neo4j_auth)
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
        

    def __str__(self) -> str:
        result = ""
        result += f"Neo4j Node Labels: {self.neo4j_node_labels}\n\n"
        result += f"Neo4j Relationship Types: {self.neo4j_relationship_types}\n\n"
        result += f"Neo4j Property Keys: {self.neo4j_property_keys}\n\n"
        result += f"Neo4j Schema nodes: {self.neo4j_nodes}\n\n"
        result += f"Neo4j Schema edges: {self.neo4j_edges}\n\n"
        return result

    @property
    def neo4j_relationship_types(self) -> list():
        """
        Retrieve a list of the neo4j relationship types.

        Returns
        -------
        list
          List of the string names of relationship types in the connected neo4j.
        """
        if self._neo4j_relationship_types is None:
            self._neo4j_relationship_types = list(self.neo4j_edges.keys())
            self._neo4j_relationship_types.sort()
        return self._neo4j_relationship_types

    @property
    def neo4j_node_labels(self) -> list():
        """
        Retrieve a list of the neo4j node labels.

        Returns
        -------
        list
          List of the string names of node labels in the connected neo4j.
        """
        if self._neo4j_node_labels is None:
            self._neo4j_node_labels = list(self._neo4j_nodes.keys())
            self._neo4j_node_labels.sort()
        return self._neo4j_node_labels

    @property
    def neo4j_property_keys(self) -> list():
        """
        Retrieve a list of the neo4j property keys.

        Returns
        -------
        list
          List of the string names of property keys in the connected neo4j.
        """
        if self._neo4j_property_keys is None:
            self._neo4j_property_keys = list(self.__neo4j_property_keys())
            self._neo4j_property_keys.sort()
        return self._neo4j_property_keys
    
    @property
    def neo4j_node_type_properties(self) -> list():
        """
        Retrieve a list of the property types attached to the nodes in neo4j.
        
        Each element of this list is a dictionary describing the property,
        including its name, its possible data types, and which node labels
        it may be attached to.

        Returns
        -------
        list
          List of the string names of node property types in the connected neo4j.
        """
        if self._neo4j_node_type_properties is None:
            self._neo4j_node_type_properties = self.__neo4j_nodeTypeProperties()
        return self._neo4j_node_type_properties

    @property
    def neo4j_rel_type_properties(self) -> list():
        """
        Retrieve a list of the property types attached to the relationships in
        neo4j.

        Each element of this list is a dictionary describing the property,
        including its name, its possible data types, and which relationship(s)
        it may be attached to.

        Returns
        -------
        list
          List of the string names of relationship property types in the
          connected neo4j.
        """
        if self._neo4j_rel_type_properties is None:
            self._neo4j_rel_type_properties = self.__neo4j_relTypeProperties()
        return self._neo4j_rel_type_properties

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
        if self._neo4j_nodes is None:
            self._neo4j_nodes = self.__neo4j_nodes()
        return self._neo4j_nodes
    
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
        return self._neo4j_edges

    def get_xgt_schema_for(self, vertices = None, edges = None,
                                 neo4j_id_name = 'neo4j_id',
                                 neo4j_source_node_name = 'neo4j_source',
                                 neo4j_target_node_name = 'neo4j_target',
                          ) -> dict():
        """
        Retrieve a dictionary containing the schema information for all of 
        the nodes/vertices and all of the edges requested.

        Parameters
        ----------
        vertices : iterable
            List of requested node labels (vertex frame names).  
        edges : iterable
            List of requested relationship type (edge frame) names.
        neo4j_id_name : str
            The name of the xGT column holding the neo4j node's ID value.
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
        if vertices is None:
            vertices = list()
        if edges is None:
            edges = dict()
        
        for vertex in vertices:
            if vertex not in self.neo4j_node_labels:
                raise ValueError(f"Neo4j Node Label {vertex} is not found.")
            table_schema = self.__extract_xgt_vertex_schema(vertex, neo4j_id_name)
            result['vertices'][vertex] = table_schema
            if self.__verbose:
                print(f"xgt graph schema for vertex {vertex}: {table_schema}")
        
        for edge in edges:
            schemas = self.__extract_xgt_edge_schemas(edge, vertices,
                                neo4j_source_node_name, neo4j_target_node_name)
            result['edges'][edge] = schemas
            if len(schemas) > 1:
                raise ValueError(
                    f"Relationship Types from/to multiple node labels not supported.")
        return result

    def create_xgt_schemas(self, xgt_schemas, append = False) -> None:
        """
        Creates vertex and/or edge frames in Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.
        Finally, all of the nodes and all of the relationships are copied,
        one frame at a time, from neo4j to xGT.

        Parameters
        ----------
        xgt_schemas : dict
            Dictionary containing schema information for vertex and edge frames
            to create in xGT.
            This dictionary can be the value returned from the 
            :py:meth:`~Neo4jConnector.get_xgt_schema_for` method.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).

        Returns
        -------
            None
        """
        if not append:
            for edge in xgt_schemas['edges']:
                self._xgt_server.drop_frame(edge)
            for vertex in xgt_schemas['vertices']:
                self._xgt_server.drop_frame(vertex)

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
                            + f" that is incompatible with neo4j: {table_schema}"
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
            schema = schema_list[0]
            table_schema = schema['schema']
            if self.__verbose or True:
                print(f"xgt graph schema for edge {edge}: {table_schema}")
            self._xgt_server.create_edge_frame(
                    name = edge, schema = table_schema,
                    source = schema['source'],
                    source_key = schema['source_key'],
                    target = schema['target'],
                    target_key = schema['target_key'],
                    attempts = 5,
                )

        return None

    def copy_data_from_neo4j_to_xgt(self, xgt_schemas):
        for vertex, schema in xgt_schemas['vertices'].items():
            if self.__verbose or True:
                print(f'Copy data for vertex {vertex} into schema: {schema}')
            table_schema = schema['schema']
            attributes = [_ for _, t in table_schema]
            key = schema['key']
            query = f"MATCH (v:{vertex}) RETURN id(v) AS {key}"  # , {', '.join(attributes)}"
            for a in attributes:
                if a != key:
                    query += f", v.{a} AS {a}"
            self.__arrow_copy_data(query, vertex)
        for edge, schema_list in xgt_schemas['edges'].items():
            if self.__verbose or True:
                print(f'Copy data for node {edge} into schema: {schema_list}')
            schema = schema_list[0]
            table_schema = schema['schema']
            attributes = [_ for _, t in table_schema]
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
            self.__arrow_copy_data(query, edge)

    def transfer_from_neo4j_to_xgt_for(self,
                            vertices = None, edges = None,
                            neo4j_id_name = 'neo4j_id',
                            neo4j_source_node_name = 'neo4j_source',
                            neo4j_target_node_name = 'neo4j_target',
                            append = False) -> None:
        """
        Copies data from neo4j to Trovares xGT.

        This function first infers the schemas for all of the needed frames in xGT to
        store the requested data.
        Then those frames are created in xGT.
        Finally, all of the nodes and all of the relationships are copied,
        one frame at a time, from neo4j to xGT.

        Parameters
        ----------
        vertices : iterable
            List of requested node labels (vertex frame names).  
        edges : iterable
            List of requested relationship type (edge frame) names.
        neo4j_id_name : str
            The name of the xGT column holding the neo4j node's ID value.
        neo4j_source_node_name : str
            The name of the xGT column holding the source node's ID value.
        neo4j_source_node_name : str
            The name of the xGT column holding the target node's ID value.
        append : boolean
            Set to true when the xGT frames are already created and holding data
            that should be appended to.
            Set to false when the xGT frames are to be newly created (removing
            any existing frames with the same names prior to creation).

        Returns
        -------
            None
        """
        xgt_schema = self.get_xgt_schema_for(vertices, edges,
                neo4j_id_name, neo4j_source_node_name, neo4j_target_node_name)
        self.create_xgt_schemas(xgt_schema, append)
        self.copy_data_from_neo4j_to_xgt(xgt_schema)
        return None

    def __neo4j_property_keys(self):
        q="CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"
        with self._neo4j_driver.session() as session:
            result = session.run(q)
            return [record["propertyKey"] for record in result]
        return None

    def __neo4j_nodeTypeProperties(self):
        fields = ('nodeType', 'nodeLabels', 'propertyName', 'propertyTypes', 'mandatory')
        q="CALL db.schema.nodeTypeProperties() YIELD nodeType, nodeLabels, propertyName, propertyTypes, mandatory RETURN *"
        with self._neo4j_driver.session() as session:
            result = session.run(q)
            node_props = [{_ : record[_] for _ in fields} for record in result]
            return node_props
        return None

    def __neo4j_relTypeProperties(self):
        fields = ('relType', 'propertyName', 'propertyTypes', 'mandatory')
        q="CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory RETURN *"
        with self._neo4j_driver.session() as session:
            result = session.run(q)
            return [{_ : record[_] for _ in fields} for record in result]
        return None

    def __add_neo4j_schema_connectivity_to_neo4j_edges(self) -> None:
        def extract_node_info(node):
            labels = node.labels
            if len(labels) == 1:
                return list(labels)[0]
            return labels
        q="CALL db.schema.visualization() YIELD nodes, relationships RETURN *"
        with self._neo4j_driver.session() as session:
            result = session.run(q)
            for record in result:
                for e in record['relationships']:
                    nodes = e.nodes
                    source = nodes[0]
                    target = nodes[1]
                    type = e.type
                    if self.__verbose:
                        print(f"Edge Connectivity: {e}")
                        print(f" -> type => {type}")
                        print(f" -> source nodes => {source}")
                        print(f" -> target nodes => {nodes[1]}")
                        print(f"  -> Edge {type}: {self._neo4j_edges[type]}\n")
                    if 'endpoints' not in self._neo4j_edges[type]:
                        self._neo4j_edges[type]['endpoints'] = set()
                        self._neo4j_edges[type]['sources'] = set()
                        self._neo4j_edges[type]['targets'] = set()
                    self._neo4j_edges[type]['endpoints'].add(
                        f"{extract_node_info(source)}->{extract_node_info(target)}")
                    self._neo4j_edges[type]['sources'].add(extract_node_info(source))
                    self._neo4j_edges[type]['targets'].add(extract_node_info(target))
        return None

    def __neo4j_schema_connectivity(self):
        fields = ('nodes', 'relationships')
        q="CALL db.schema.visualization() YIELD nodes, relationships RETURN *"
        nodes = []
        edges = []
        with self._neo4j_driver.session() as session:
            result = session.run(q)
            for record in result:
                for n in record['nodes']:
                    nodes.append(n)
                for e in record['relationships']:
                    edges.append(e)
            #return [[(_, record[_]) for _ in fields] for record in result]
        return (nodes, edges)

    def __neo4j_nodes(self):
        nodes = dict()
        for prop in self.neo4j_node_type_properties:
            labels = prop['nodeLabels']
            propTypes = prop['propertyTypes']
            if len(propTypes) == 1:
                    propTypes = propTypes[0]
            for name in labels:
                if name not in nodes:
                    nodes[name] = dict()
                nodes[name][prop['propertyName']] = propTypes
        return nodes
    
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

    def __extract_xgt_vertex_schema(self, vertex, neo4j_id_name):
        if vertex in self.neo4j_nodes:
            neo4j_node = self.neo4j_nodes[vertex]
            neo4j_node_attributes = list(neo4j_node.keys())
            if neo4j_id_name in neo4j_node:
                raise ValueError(
                        f"Neo4j ID name {neo4j_id_name} is an attribute of node {neo4j_node}")
        schema = [[key, self.__neo4j_type_to_xgt_type(type)] 
                                for key, type in neo4j_node.items()]
        schema.insert(0, [neo4j_id_name, xgt.INT])
        return {'schema' : schema, 'key' : neo4j_id_name}

    def __extract_xgt_edge_schema(self, edge, source, target,
                                  neo4j_source_node_name,
                                  neo4j_target_node_name):
        result = dict()
        if edge not in self.neo4j_relationship_types:
            raise ValueError(f"Edge {edge} is not a known relationship type")
        if self.__verbose:
            print(f"xgt graph schema for edge {edge}: {edge_info}")
        edge_info = self.neo4j_edges[edge]
        schema = edge_info['schema']
        edge_endpoints = edge_info['endpoints']
        endpoints = f"{source}->{target}"
        if endpoints not in edge_endpoints:
            raise ValueError(f"Edge Type {edge} with endpoints: {endpoints} not found.")

        schema = [[key, self.__neo4j_type_to_xgt_type(type)] 
                                for key, type in schema.items()]
        schema.insert(0, [neo4j_target_node_name, xgt.INT])
        schema.insert(0, [neo4j_source_node_name, xgt.INT])
        return {'schema' : schema, 'source' : source, 'target' : target,
                'source_key' : neo4j_source_node_name,
                'target_key' : neo4j_target_node_name}

    def __extract_xgt_edge_schemas(self, edge, vertices,
                                    neo4j_source_node_name,
                                    neo4j_target_node_name):
        schemas = []
        neo4j_edge = self.neo4j_edges[edge]
        for source in neo4j_edge['sources']:
            if source in vertices:
                for target in neo4j_edge['targets']:
                    if target in vertices:
                        schemas.append(
                            self.__extract_xgt_edge_schema(edge,
                                source, target, neo4j_source_node_name,
                                neo4j_target_node_name))
        return schemas

    def __neo4j_type_to_xgt_type(self, prop_type):
        if prop_type in self.NEO4J_TYPE_TO_XGT_TYPE:
            return self.NEO4J_TYPE_TO_XGT_TYPE[prop_type]
        return xgt.UNKNOWN

    def __arrow_writer(self, frame_name, schema):
        arrow_conn = pf.FlightClient((self._xgt_server.host, self._xgt_server.port))
        arrow_conn.authenticate(BasicClientAuthHandler())
        writer, _ = arrow_conn.do_put(
            pf.FlightDescriptor.for_path(self._default_namespace, frame_name),
            schema)
        return writer

    def __arrow_copy_data(self, cypher_for_extract, frame):
        import time
        t0 = time.time()
        neo4j_arrow_client = na.Neo4jArrow(self._neo4j_auth[0],
                                            self._neo4j_auth[1])
        ticket = neo4j_arrow_client.cypher(cypher_for_extract)
        ready = neo4j_arrow_client.wait_for_job(ticket, timeout=60)
        if not ready:
            raise Exception('something is wrong...did you submit a job?')
        neo4j_reader = neo4j_arrow_client.stream(ticket).to_reader()
        xgt_writer = self.__arrow_writer(frame, neo4j_reader.schema)
        # move data from neo4j to xGT in chunks
        count = 0
        while (True):
            try:
                batch = neo4j_reader.read_next_batch()
                xgt_writer.write(batch)
                count += 1
            except StopIteration:
                break
        xgt_writer.close()
        duration = time.time() - t0
        print(f"Time to transfer: {duration:,.2f}", flush=True)
        return duration