# (C) 2021 GoodData Corporation
import functools

from schema_analyzer.graph_base import EdgeTypes
from schema_analyzer.graph_visitor import (
    VisitContext,
    VisitReturnType,
    NoopDatabaseGraphVisitor,
)
from schema_analyzer.metadata import (
    MetadataSchemaRow,
    MetadataTableRow,
    MetadataColumnRow,
    MetadataPrimaryKeyRow,
)
from schema_analyzer.scoring import NodeScoringObjective


class ColumnScoringVisitor(NoopDatabaseGraphVisitor):
    """
    Abstract class for visitors that walk the graph looking for column nodes that may be used as either facts or
    dimensions in star schema tables.
    """

    def __init__(self):
        super(ColumnScoringVisitor, self).__init__()

    @property
    def fact_scoring(self) -> NodeScoringObjective:
        raise NotImplementedError()

    @property
    def dimension_scoring(self) -> NodeScoringObjective:
        raise NotImplementedError()

    def visit_schema(
        self, ctx: VisitContext, schema_id: str, schema_data: MetadataSchemaRow
    ) -> VisitReturnType:
        return {EdgeTypes.SCHEMA_TABLE}

    def visit_table(
        self, ctx: VisitContext, table_id: str, table_data: MetadataTableRow
    ) -> VisitReturnType:
        return {EdgeTypes.TABLE_COLUMN, EdgeTypes.TABLE_PK, EdgeTypes.TABLE_FK}

    def visit_column(
        self, ctx: VisitContext, col_id: str, column_data: MetadataColumnRow
    ) -> VisitReturnType:
        return {EdgeTypes.REFERENCE}


class BaseColumnScoringVisitor(ColumnScoringVisitor):
    """
    Base class to use for concrete column scoring visitors.

    This class implements the fact and dimension scoring objectives that the concrete implementations should
    use to add their scores.
    """

    def __init__(self):
        super(ColumnScoringVisitor, self).__init__()

        self._fact_scoring = NodeScoringObjective("fact scoring")
        self._dimension_scoring = NodeScoringObjective("dimension scoring")

    @property
    def fact_scoring(self):
        return self._fact_scoring

    @property
    def dimension_scoring(self):
        return self._dimension_scoring


class CompositeColumnScoringVisitor(ColumnScoringVisitor):
    """
    Composite column scoring visitor can take 1 to N concrete visitors, dispatch the scoring logic to them and
    then in the end return accumulated results.

    The use of composite visitor is preferred when employing multiple scoring strategies. The database graph will
    be walked once by this visitor.
    """

    def __init__(self, components: list[ColumnScoringVisitor]):
        super(CompositeColumnScoringVisitor, self).__init__()
        self._components = components

    @property
    def fact_scoring(self) -> NodeScoringObjective:
        return functools.reduce(
            lambda a, b: a.merge(b), [c.fact_scoring for c in self._components]
        )

    @property
    def dimension_scoring(self) -> NodeScoringObjective:
        return functools.reduce(
            lambda a, b: a.merge(b), [c.dimension_scoring for c in self._components]
        )

    def visit_schema(
        self, ctx: VisitContext, schema_id: str, schema_data: MetadataSchemaRow
    ) -> VisitReturnType:
        for component in self._components:
            component.visit_schema(ctx, schema_id, schema_data)

        return {EdgeTypes.SCHEMA_TABLE}

    def visit_table(
        self, ctx: VisitContext, table_id: str, table_data: MetadataTableRow
    ) -> VisitReturnType:
        for component in self._components:
            component.visit_table(ctx, table_id, table_data)

        return {EdgeTypes.TABLE_COLUMN, EdgeTypes.TABLE_PK, EdgeTypes.TABLE_FK}

    def visit_column(
        self, ctx: VisitContext, col_id: str, column_data: MetadataColumnRow
    ) -> VisitReturnType:
        for component in self._components:
            component.visit_column(ctx, col_id, column_data)

        return {EdgeTypes.REFERENCE}

    def visit_pk(
        self, ctx: VisitContext, pk_id: str, pk_data: list[MetadataPrimaryKeyRow]
    ) -> VisitReturnType:
        for component in self._components:
            component.visit_pk(ctx, pk_id, pk_data)

        return

    def visit_reference(
        self,
        ctx: VisitContext,
        col_id: str,
        column_data: MetadataColumnRow,
        from_column_id: str,
        edge_data: dict,
    ):
        for component in self._components:
            component.visit_reference(
                ctx, col_id, column_data, from_column_id, edge_data
            )
