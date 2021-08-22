# (C) 2021 GoodData Corporation
from collections import namedtuple
from typing import Set, Union, Dict

from schema_analyzer.metadata import (
    MetadataTableRow,
    MetadataColumnRow,
    MetadataPrimaryKeyRow,
    MetadataForeignKeyRow,
    MetadataSchemaRow,
)
from schema_analyzer.graph_base import EdgeTypes, NodeTypes

VisitContext = namedtuple(
    "VisitContext",
    ["graph", "graph_md"],
)

VisitReturnType = Union[Set[EdgeTypes], None]
VisitNavigationDefinition = Dict[Union[NodeTypes, EdgeTypes], Set[EdgeTypes]]

STD_VISIT_NAVIGATION = {
    NodeTypes.SCHEMA: {EdgeTypes.SCHEMA_TABLE},
    NodeTypes.TABLE: {EdgeTypes.TABLE_COLUMN, EdgeTypes.TABLE_PK, EdgeTypes.TABLE_FK},
    NodeTypes.COLUMN: {EdgeTypes.REFERENCE},
}


class VisitError(Exception):
    def __init__(self, message):
        super(VisitError, self).__init__(message)


class DatabaseGraphVisitor:
    """
    Base class of database graph visitors. Database graph will start visit on schema nodes and follow the path
    based on the navigation data that is either returned by the particular visit method OR is provided on the
    side in the accept visitor method.
    """

    def visit_schema(
        self, ctx: VisitContext, schema_id: str, schema_md: MetadataSchemaRow
    ) -> VisitReturnType:
        raise NotImplementedError()

    def visit_table(
        self, ctx: VisitContext, table_id: str, table_md: MetadataTableRow
    ) -> VisitReturnType:
        raise NotImplementedError()

    def visit_column(
        self, ctx: VisitContext, column_id: str, column_md: MetadataColumnRow
    ) -> VisitReturnType:
        raise NotImplementedError()

    def visit_pk(
        self, ctx: VisitContext, pk_id: str, pk_md: list[MetadataPrimaryKeyRow]
    ) -> VisitReturnType:
        raise NotImplementedError()

    def visit_fk(
        self, ctx: VisitContext, fk_id: str, fk_md: list[MetadataForeignKeyRow]
    ) -> VisitReturnType:
        raise NotImplementedError()

    def visit_reference(
        self,
        ctx: VisitContext,
        column_id: str,
        column_md: MetadataColumnRow,
        from_column_id: str,
        edge_data: dict,
    ):
        raise NotImplementedError()


class NoopDatabaseGraphVisitor(DatabaseGraphVisitor):
    def visit_schema(
        self, ctx: VisitContext, schema_id: str, schema_md: MetadataSchemaRow
    ) -> VisitReturnType:
        return

    def visit_table(
        self, ctx: VisitContext, table_id: str, table_md: MetadataTableRow
    ) -> VisitReturnType:
        return

    def visit_column(
        self, ctx: VisitContext, column_id: str, column_md: MetadataColumnRow
    ) -> VisitReturnType:
        return

    def visit_pk(
        self, ctx: VisitContext, pk_id: str, pk_md: list[MetadataPrimaryKeyRow]
    ) -> VisitReturnType:
        return

    def visit_fk(
        self, ctx: VisitContext, fk_id: str, fk_md: list[MetadataForeignKeyRow]
    ) -> VisitReturnType:
        return

    def visit_reference(
        self,
        ctx: VisitContext,
        column_id: str,
        column_md: MetadataColumnRow,
        from_column_id: str,
        edge_data: dict,
    ):
        return
