# (C) 2021 GoodData Corporation
from typing import Union

from schema_analyzer.graph_visitor import VisitContext, VisitReturnType
from schema_analyzer.metadata import MetadataColumnRow
from schema_analyzer.scoring import NodeScore, NodeDisqualified
from schema_analyzer.scoring_cols import BaseColumnScoringVisitor


class TypeScoringDefinition:
    def __init__(
        self, dim_scores: dict[str, int], fact_scores: dict[str, int], fact_dq: set[str]
    ):
        self._dim_scores = dim_scores
        self._fact_scores = fact_scores
        self._fact_dq = fact_dq

    def get_dim_score(self, type_name) -> Union[int, None]:
        return self._dim_scores[type_name] if type_name in self._dim_scores else None

    def get_fact_score(self, type_name) -> Union[int, None]:
        return self._fact_scores[type_name] if type_name in self._fact_scores else None

    def is_fact_dq(self, type_name) -> bool:
        return type_name in self._fact_dq


_DEFAULT_TYPE_SCORING = TypeScoringDefinition(
    dim_scores={
        "VARCHAR": NodeScore.SCORE_GOOD,
        "TEXT": NodeScore.SCORE_NORMAL,
        "BIT": NodeScore.SCORE_GOOD,
        "CHAR": NodeScore.SCORE_GOOD,
        "TIME": NodeScore.SCORE_GOOD,
        "TIMESTAMP": NodeScore.SCORE_GOOD,
        "DATE": NodeScore.SCORE_GOOD,
        "DATETIME": NodeScore.SCORE_GOOD,
    },
    fact_scores={
        "DECIMAL": NodeScore.SCORE_GOOD,
        "NUMERIC": NodeScore.SCORE_GOOD,
        "INT": NodeScore.SCORE_NORMAL,
        "SMALLINT": NodeScore.SCORE_NORMAL,
        "SMALLINT UNSIGNED": NodeScore.SCORE_NORMAL,
    },
    fact_dq={
        "VARCHAR",
        "TEXT",
        "TIME",
        "TIMESTAMP",
        "DATE",
        "DATETIME",
        "LONGTEXT",
        "BLOB",
        "LONGBLOB",
    },
)


class TypeBasedColumnScoring(BaseColumnScoringVisitor):
    """
    This visitor scores columns as viable facts or dimensions based on the data type used for the column.
    """

    def __init__(self, defs: TypeScoringDefinition = _DEFAULT_TYPE_SCORING):
        super(TypeBasedColumnScoring, self).__init__()
        self._defs = defs

    def _categorize_based_on_type(self, col_id: str, column_data: MetadataColumnRow):
        type_name = column_data.type_name

        if self._defs.get_dim_score(type_name) is not None:
            self._dimension_scoring.add(
                NodeScore(
                    node_id=col_id,
                    score=self._defs.get_dim_score(type_name),
                    reason=f"viable data type for a dimension ({type_name})",
                )
            )

        if self._defs.is_fact_dq(type_name):
            # data types that do not allow summarization (strings, dates etc) entirely disqualify
            # some columns from being facts
            self._fact_scoring.add(
                NodeDisqualified(
                    node_id=col_id,
                    reason=f"non-summarizable data type: ({type_name})",
                )
            )
        elif self._defs.get_fact_score(type_name) is not None:
            self._fact_scoring.add(
                NodeScore(
                    node_id=col_id,
                    score=self._defs.get_fact_score(type_name),
                    reason=f"viable data type for a fact ({type_name})",
                )
            )

    def visit_column(
        self, ctx: VisitContext, col_id: str, column_data: MetadataColumnRow
    ) -> VisitReturnType:
        self._categorize_based_on_type(col_id, column_data)

        return
