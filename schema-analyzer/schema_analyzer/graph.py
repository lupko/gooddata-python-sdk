# (C) 2021 GoodData Corporation
import functools
from collections import namedtuple

import networkx as nx

from schema_analyzer.metadata import (
    MetadataPrimaryKeyRow,
    MetadataForeignKeyRow,
)
from schema_analyzer.graph_base import (
    schema_id,
    table_id,
    column_id,
    type_id,
    NodeTypes,
    EdgeTypes,
)
from schema_analyzer.graph_metadata import (
    DatabaseGraphMetadata,
)
from schema_analyzer.graph_visitor import (
    VisitContext,
    DatabaseGraphVisitor,
    VisitNavigationDefinition,
    VisitError,
)
from schema_analyzer.scoring_cols import (
    CompositeColumnScoringVisitor,
)
from schema_analyzer.scoring_key_dq import (
    KeyDisqualificationScoring,
)
from schema_analyzer.scoring_keyword import (
    KeywordBasedColumnScoring,
)

from schema_analyzer.scoring_type import (
    TypeBasedColumnScoring,
)


def _filter_out_none(d):
    """
    Graph attributes must not contain 'None' values because the graph may then not be serializable in
    some formats (say GraphML)

    :param d: dict with node attributes
    :return: dict with only those keys whose value is not None
    """
    return {k: v for k, v in d.items() if v is not None}


VisitStackEntry = namedtuple(
    "VisitStackEntry",
    [
        "from_node",
        "to_node",
        "edge_type",
        "edge_data",
    ],
)


def _full_graph_factory(md: DatabaseGraphMetadata, include_type_nodes=False):
    g = nx.DiGraph()

    missing_types = set()

    if include_type_nodes:
        for _id, row in md.types.items():
            g.add_node(
                _id,
                node_type=NodeTypes.TYPE.value,
                **_filter_out_none(row._asdict()),
            )

    for _id, row in md.schemas.items():
        g.add_node(
            _id, node_type=NodeTypes.SCHEMA.value, **_filter_out_none(row._asdict())
        )

    # create node per table and edges between schema and table
    for _id, row in md.tables.items():
        from_schema = schema_id(row.table_cat, row.table_schem)
        # there is no node for a schema to which the table belongs; something's amiss
        assert from_schema in md.schemas

        g.add_node(
            _id, node_type=NodeTypes.TABLE.value, **_filter_out_none(row._asdict())
        )
        g.add_edge(from_schema, _id, edge_type=EdgeTypes.SCHEMA_TABLE.value)

    # create column per table and edges between tables and their columns
    for _id, row in md.columns.items():
        from_table = table_id(row.table_cat, row.table_schem, row.table_name)
        # there is no node for a table to which the column belongs; something's amiss
        assert from_table in md.tables

        g.add_node(
            _id, node_type=NodeTypes.COLUMN.value, **_filter_out_none(row._asdict())
        )
        g.add_edge(from_table, _id, edge_type=EdgeTypes.TABLE_COLUMN.value)
        g.add_edge(_id, from_table, edge_type=EdgeTypes.COLUMN_TABLE.value)

        if include_type_nodes:
            to_type = type_id(row.type_name)

            # some types may not be globally defined and only appear first on some column; if that is
            # the case, create a unique node for the type first before linking
            if to_type not in md.types and to_type not in missing_types:
                missing_types.add(to_type)
                g.add_node(to_type, wtftype=True)

            g.add_edge(_id, to_type, edge_type=EdgeTypes.COLUMN_TYPE.value)

    # create node per primary key, create edges to columns that make up a possibly composite primary key
    for _id, rows in md.pks.items():
        first_row: MetadataPrimaryKeyRow = rows[0]
        from_table = table_id(
            first_row.table_cat, first_row.table_schem, first_row.table_name
        )
        assert from_table in md.tables

        g.add_node(
            _id,
            node_type=NodeTypes.PK.value,
            **_filter_out_none(dict(pk_name=first_row.pk_name)),
        )
        g.add_edge(from_table, _id, edge_type=EdgeTypes.TABLE_PK.value)

        for row in rows:
            of_column = column_id(
                row.table_cat, row.table_schem, row.table_name, row.column_name
            )
            assert of_column in md.columns

            g.add_edge(
                _id,
                of_column,
                edge_type=EdgeTypes.PK_COLUMN.value,
                **_filter_out_none(row._asdict()),
            )

    # create node per foreign key, create edge from referencing table to the fk, from fk to the referenced table
    # then create edges
    for _id, rows in md.fks.items():
        first_row: MetadataForeignKeyRow = rows[0]
        pk_table = table_id(
            first_row.pktable_cat, first_row.pktable_schem, first_row.pktable_name
        )
        fk_table = table_id(
            first_row.fktable_cat, first_row.fktable_schem, first_row.fktable_name
        )

        g.add_node(
            _id,
            node_type=NodeTypes.FK.value,
            **_filter_out_none(dict(fk_name=first_row.fk_name)),
        )
        g.add_edge(fk_table, _id, edge_type=EdgeTypes.TABLE_FK.value)
        g.add_edge(_id, pk_table, edge_type=EdgeTypes.FK_TABLE.value)

        for row in rows:
            fk_column = column_id(
                row.fktable_cat,
                row.fktable_schem,
                row.fktable_name,
                row.fkcolumn_name,
            )
            pk_column = column_id(
                row.pktable_cat,
                row.pktable_schem,
                row.pktable_name,
                row.pkcolumn_name,
            )

            g.add_edge(
                fk_column,
                _id,
                edge_type=EdgeTypes.COLUMN_FK.value,
                **_filter_out_none(row._asdict()),
            )
            g.add_edge(
                _id,
                pk_column,
                edge_type=EdgeTypes.FK_COLUMN.value,
                **_filter_out_none(row._asdict()),
            )
            g.add_edge(
                fk_column,
                pk_column,
                edge_type=EdgeTypes.REFERENCE.value,
                **_filter_out_none(row._asdict()),
            )
            g.add_edge(
                pk_column,
                fk_column,
                edge_type=EdgeTypes.REFERENCE_BY.value,
                **_filter_out_none(row._asdict()),
            )

    return g


class DatabaseGraph:
    def __init__(self, md: DatabaseGraphMetadata, graph: nx.DiGraph):
        self._md = md
        self._graph = graph

    @classmethod
    def from_data(cls, md: DatabaseGraphMetadata, include_type_nodes=False):
        return DatabaseGraph(md=md, graph=_full_graph_factory(md, include_type_nodes))

    @property
    def graph(self) -> nx.DiGraph:
        """
        Returns directed graph containing full detail of the database layout.

        The graph may contain nodes of these types:

        -  schema
        -  table
        -  column
        -  pk
        -  fk
        -  index
        -  type

        Note how both primary keys and foreign keys are modeled as nodes. The edges between nodes always contain
        edge_type attribute with the informative type name for the edge:

        - schema-table : edge between schema and table
        - table-column : edge between table and its column
        - column-table : edge between column and table to which it belongs
        - table-pk     : edge between table and its primary key
        - pk-column    : edge between primary key and columns of which it is made; edge has additional key_seq attribute
          which indicates the order of the column within the primary key
        - table-fk     : edge between table that defines the foreign key relationship and the foreign key itself
        - fk-table     : edge between foreign key relationship and the referenced table
        - column-fk    : edge between foreign key column and the foreign key definition
        - fk-column    : edge between foreign key relationship and the column in the referenced table
        - reference    : edge between foreign key column and the column in the referenced table
        - reference-by : edge between column and the foreign key column that is referencing it

        :return: digraph containing the database schema
        """
        return self._graph

    def get_nodes_by_type(self, types: tuple[NodeTypes]):
        """
        Gets all nodes of the desired types present in this graph.

        :param types: node types
        :return: list of nodes
        """
        return [
            node
            for node in self._md.get_nodes_by_type(types=types)
            if self.graph.has_node(node)
        ]

    def get_tables(self):
        """
        Convenience function to get all table nodes present in the graph.

        :return: list of table nodes
        """
        return self.get_nodes_by_type((NodeTypes.TABLE,))

    def get_node_metadata(self, node: str):
        """
        Gets database metadata for the provided node.

        :param node: node to get metadata for
        :return:
        """
        if not self.graph.has_node(node):
            raise KeyError(f"Graph does not contain node {node}")

        return self._md.get_node_metadata(node_id=node)

    def get_isolated_submodels(self):
        """
        Returns a generator that yields new instance of DatabaseGraph for each submodel found in the database graph. A
        submodel is a group of one or more tables (and their associated columns, PKs and FKs) that are all connected
        to each other using foreign keys. Each table and its associated nodes are exactly in one submodel that is
        isolated (has no connection) with the other submodels.

        Given the node and edge types used in the graph, the bulk of this task is about finding the strongly connected
        components, trimming the reflexive strong components (single node) and including additional nodes connected to
        them (schemas, types) and creating a subgraph.

        This method is good to pin-point which parts of the database schema 'play together'.

        :return: generator yielding instances of DatabaseGraph for each found sub-model.
        """
        g = self.graph

        for component in nx.strongly_connected_components(g):
            # trim off single-node strong components (typically schemas and types)
            if len(component) == 1:
                continue

            # create sub-model. this consists of the nodes that make up the strong component +
            # nodes that only have either incoming or outgoing edges and can thus never be part
            # of the component.. schema and type nodes are two examples
            #
            # while they are not part of the component, the sub-model should include them for sakes
            # of completeness
            submodel = set()
            for node in component:
                for in_node, out_node, edge_data in g.in_edges(node, data=True):
                    if edge_data["edge_type"] in {EdgeTypes.SCHEMA_TABLE.value}:
                        submodel.add(in_node)
                for in_node, out_node, edge_data in g.out_edges(node, data=True):
                    if edge_data["edge_type"] in {EdgeTypes.COLUMN_TYPE.value}:
                        submodel.add(out_node)

                submodel.add(node)

            yield DatabaseGraph(self._md, g.subgraph(submodel))

    def add_fact_and_dim_scores(
        self,
        scorers=(
            TypeBasedColumnScoring,
            KeyDisqualificationScoring,
            KeywordBasedColumnScoring,
        ),
    ):
        """
        Calculates per-column scores indicating fitness of the column to be used as either fact or a dimension
        column in some star schema table.

        The calculated scores will be added as node attributes into the graph.

        :param scorers: scorers to use, defaults to all available, built-in scorers
        :return: nothing
        """
        scoring_visitor = CompositeColumnScoringVisitor(
            [scorer() for scorer in scorers]
        )
        self.accept_visitor(scoring_visitor)

        fact_score = dict(
            [
                (node_id, score)
                for node_id, score, _ in scoring_visitor.fact_scoring.get_node_scores()
            ]
        )
        dim_score = dict(
            [
                (node_id, score)
                for node_id, score, _ in scoring_visitor.dimension_scoring.get_node_scores()
            ]
        )

        # ensure nodes that are not subject to scoring (tables, pk, fk etc) have negative score;
        # this is good to have as graph visualizers typically work with union of all node attributes and do not
        # allow to distinguish between NULL attribute values
        for node in self.graph.nodes():
            if node not in fact_score:
                fact_score[node] = -1
            if node not in dim_score:
                dim_score[node] = -1

        nx.set_node_attributes(self.graph, fact_score, "fact_score")
        nx.set_node_attributes(self.graph, dim_score, "dim_score")

    def accept_visitor(
        self,
        visitor: DatabaseGraphVisitor,
        fallback_nav: VisitNavigationDefinition = None,
    ):
        """
        Accepts the provided visitor of the database graph. The visitation will always start at the graph roots - which
        are typically schema nodes. From then on, the visit method provides navigation data = the edges to follow.

        The visit is depth-first. As soon as a visit method returns edges to follow, the visit will dive into
        the nodes on the other side of the edges.

        For convenience, your visitor's visit method may omit returning the navigation data and instead rely
        only the fallback navigation mapping provided on the input to this method. If you use the fallback
        navigation, your visitor may still return navigation data for some of the visit methods and they
        will have preference over the fallback navigation.

        :param visitor: visitor to accept
        :param fallback_nav: optionally specify fallback navigation data to use
        :return:
        """
        # create fallback navigation dict. if the input is provided, it is expected to contain dict
        # mapping node type (enum) to set of edge types; since the graph nodes have contain enum _value_ as the
        # node_type, morph the dict to mapping of enum values to set of edge types
        fallback_nav = (
            {k.value: v for k, v in fallback_nav.items()}
            if fallback_nav is not None
            else dict()
        )
        g = self.graph
        n = g.nodes(data=True)
        md = self._md
        ctx = VisitContext(graph=g, graph_md=md)
        node_visit_funs = {
            NodeTypes.SCHEMA.value: functools.partial(visitor.visit_schema, ctx),
            NodeTypes.TABLE.value: functools.partial(visitor.visit_table, ctx),
            NodeTypes.COLUMN.value: functools.partial(visitor.visit_column, ctx),
            NodeTypes.PK.value: functools.partial(visitor.visit_pk, ctx),
            NodeTypes.FK.value: functools.partial(visitor.visit_fk, ctx),
        }
        edge_visit_funs = {
            EdgeTypes.REFERENCE.value: functools.partial(visitor.visit_reference, ctx)
        }

        # when one node has edges to multiple nodes of different types, then nodes will be grouped by type and
        # the groups will be visited in the order listed here
        visit_order = (
            NodeTypes.SCHEMA.value,
            NodeTypes.TABLE.value,
            NodeTypes.COLUMN.value,
            NodeTypes.PK.value,
            NodeTypes.FK.value,
        )

        roots = [node for node, degree in g.in_degree() if degree == 0]
        # small note on this reversing 'business' - this is in place to ensure that nodes are visited
        # in the order they are discovered. since the stack is used (append to end, pop from the end), code
        # must ensure the items are correctly placed from end of the stack towards the beginning
        stack = [
            VisitStackEntry(
                from_node=None, to_node=root, edge_type=None, edge_data=None
            )
            for root in roots
        ]

        while len(stack) > 0:
            from_node, to_node, edge_type, edge_data = stack.pop(-1)
            to_node_type = n[to_node]["node_type"]
            node_data = md.get_node_metadata(to_node)

            if edge_type in edge_visit_funs:
                retval = edge_visit_funs[edge_type](
                    to_node, node_data, from_node, edge_data
                )
            else:
                retval = node_visit_funs[to_node_type](to_node, node_data)

            if retval is not None:
                follow_edges = retval
            elif edge_type in fallback_nav:
                follow_edges = fallback_nav[edge_type]
            elif to_node_type in fallback_nav:
                follow_edges = fallback_nav[to_node_type]
            else:
                follow_edges = set()

            next_level = dict([(visitable_type, []) for visitable_type in visit_order])

            for in_node, out_node, edge_data in g.out_edges(to_node, data=True):
                edge_type = EdgeTypes(edge_data["edge_type"])
                out_node_type = n[out_node]["node_type"]

                if edge_type in follow_edges:
                    if out_node == from_node:
                        # this is too aggressive
                        raise VisitError(
                            "Visit navigation data leads to a cycle. Aborting."
                        )

                    next_level[out_node_type].append(
                        VisitStackEntry(
                            from_node=in_node,
                            to_node=out_node,
                            edge_type=edge_type.value,
                            edge_data=edge_data,
                        )
                    )

            for visitable_type in reversed(visit_order):
                next_level[visitable_type].reverse()
                stack.extend(next_level[visitable_type])
