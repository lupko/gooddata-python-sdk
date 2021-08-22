# (C) 2021 GoodData Corporation
from schema_analyzer.graph_base import column_id
from schema_analyzer.graph_visitor import VisitContext, VisitReturnType
from schema_analyzer.metadata import MetadataPrimaryKeyRow, MetadataColumnRow
from schema_analyzer.scoring import NodeDisqualified
from schema_analyzer.scoring_cols import BaseColumnScoringVisitor


class KeyDisqualificationScoring(BaseColumnScoringVisitor):
    """
    This implementation of column scoring visitor disqualifies any columns that play role of primary key
    or foreign key from being facts.
    """

    def __init__(self):
        super(KeyDisqualificationScoring, self).__init__()

    def visit_pk(
        self, ctx: VisitContext, pk_id: str, pk_data: list[MetadataPrimaryKeyRow]
    ) -> VisitReturnType:
        for pk in pk_data:
            # disqualify all columns that are part of PK from being facts
            self._fact_scoring.add(
                NodeDisqualified(
                    column_id(
                        pk.table_cat, pk.table_schem, pk.table_name, pk.column_name
                    ),
                    reason=f"column is part of primary key ({pk_id}); cannot be a fact",
                )
            )

        return

    def visit_reference(
        self,
        ctx: VisitContext,
        col_id: str,
        column_data: MetadataColumnRow,
        from_column_id: str,
        edge_data: dict,
    ):
        self._fact_scoring.add(
            NodeDisqualified(
                node_id=col_id,
                reason=f"column is referenced in a foreign key ({edge_data['fk_name']})",
            )
        )
        self._fact_scoring.add(
            NodeDisqualified(
                node_id=from_column_id,
                reason=f"column is a foreign key ({edge_data['fk_name']})",
            )
        )
