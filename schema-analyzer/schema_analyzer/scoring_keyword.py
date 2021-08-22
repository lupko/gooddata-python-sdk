# (C) 2021 GoodData Corporation
import functools

from schema_analyzer.graph_visitor import VisitContext, VisitReturnType
from schema_analyzer.metadata import MetadataColumnRow
from schema_analyzer.scoring import NodeDisqualified, NodeScore
from schema_analyzer.scoring_cols import BaseColumnScoringVisitor
from schema_analyzer.utils import _db_identifier_to_lc_words


class KeywordScoringDictionary:
    """
    Dictionary to use for keyword scoring.

    TODO 1: need to expand this infra to allow for different / multiple languages. ideally use something like langdetect
     to determine language used for naming tables/columns. then have per-lang dictionary + a straightforward way to
     pass it down as input to the visitor
    TODO 2: nice to have ability to load dictionaries from something like a json document
    """

    def __init__(self, lang: str, fact_scores: dict[str, int], fact_dq: set[str]):
        self._lang = lang
        self._fact_scores = fact_scores
        self._fact_dq = fact_dq

    @property
    def lang(self):
        return self._lang

    def get_strongly_matching_keywords(self, words: list[str]) -> dict[str, int]:
        return dict(
            [
                (word, self._fact_scores[word])
                for word in words
                if word in self._fact_scores
            ]
        )

    def get_weakly_matching_keywords(self, words: list[str]) -> dict[str, int]:
        # return dict of keyword->score for any keyword that can be found in some of the input words
        return dict(
            [
                (kw, score)
                for kw, score in self._fact_scores.items()
                if functools.reduce(
                    lambda a, b: a or b,
                    [False] + [True for word in words if kw in word],
                )
                is True
            ]
        )

    def get_disqualified_words(self, words: list[str]) -> list[str]:
        last_word = words[-1]

        return [last_word] if last_word in self._fact_dq else []


_DEFAULT_KEYWORD_SCORING = KeywordScoringDictionary(
    lang="en",
    fact_scores={
        "price": NodeScore.SCORE_GOOD,
        "qty": NodeScore.SCORE_GOOD,
        "quantity": NodeScore.SCORE_GOOD,
        "cost": NodeScore.SCORE_GOOD,
        "revenue": NodeScore.SCORE_NORMAL,
        "amount": NodeScore.SCORE_GOOD,
        "margin": NodeScore.SCORE_NORMAL,
        "discount": NodeScore.SCORE_NORMAL,
        "rate": NodeScore.SCORE_GOOD,
        "sale": NodeScore.SCORE_NORMAL,
        "quota": NodeScore.SCORE_NORMAL,
        "duration": NodeScore.SCORE_GOOD,
        "percent": NodeScore.SCORE_NORMAL,
        "pct": NodeScore.SCORE_NORMAL,
    },
    fact_dq={"id", "identifier", "key", "uid", "gid", "uuid"},
)


class KeywordBasedColumnScoring(BaseColumnScoringVisitor):
    """
    This implementation of column scoring visitor uses simple column name analysis to bump or sink the
    scoring of a column towards the fact objective.

    The logic here attempts to split the column name into words first; it can deal with either snake case or camel case
    naming convention. It then intersects the words used in the column name with the dictionary of keywords. Each
    word in the intersection has some score assigned; the scores are accumulated for the final column score.

    Additionally, the logic looks for 'disqualifying' keywords being the last word in the column name. ID columns
    are typically named 'something_id' or 'SomethingId'. ID columns cannot be facts so such columns are disqualified
    immediately.

    When code is not able to split col name into multiple words, it treats the whole name as a single word
    ('id', 'price) with some benefit of doubt that it may be a garbage-named column that does not follow any
    reasonable convention ('mysalary')

    Code will first check if the entire name matches disqualifying or a normal keyword - if it is so, then the col
    name is indeed a single word name with some recognized name and is scored accordingly.

    After that, code will check all keywords and try to find them in the col name. The evaluation tries to go on
    the safe side, not give false positives because dictionary words may overlap.
    """

    def __init__(self, defs: KeywordScoringDictionary = _DEFAULT_KEYWORD_SCORING):
        super(KeywordBasedColumnScoring, self).__init__()
        self._defs = defs

    def _analyze_multiword(self, col_id: str, words: list[str]):
        disq_words = self._defs.get_disqualified_words(words)

        if len(disq_words) > 0:
            # first see if the last word is one of the disqualifying keywords. e.g 'id', 'identifier'..
            # some_id, some_key, some_identifier
            self._fact_scoring.add(
                NodeDisqualified(
                    col_id,
                    reason=f"column contains disqualifying keyword: {', '.join(disq_words)}",
                )
            )

        found_keywords = self._defs.get_strongly_matching_keywords(words)

        if len(found_keywords) > 1:
            self._fact_scoring.add(
                NodeScore(
                    node_id=col_id,
                    score=sum([s for s in found_keywords.values()]),
                    reason=f"promising keywords exactly matched: {', '.join(found_keywords.keys())}",
                )
            )

    def _analyze_singleword(self, col_id: str, word: list[str]):
        # this method should be called only if the whole colname is a single word
        assert len(word) == 1

        disq_words = self._defs.get_disqualified_words(word)
        if len(disq_words) > 0:
            # first see if the column name equals some of the disqualifying keywords. e.g 'id', 'identifier'..
            self._fact_scoring.add(
                NodeDisqualified(
                    col_id,
                    reason=f"column is named using disqualifying keyword: {word[0]}",
                )
            )

            return

        found_keywords = self._defs.get_strongly_matching_keywords(word)
        if len(found_keywords) > 0:
            # then see if col name exactly matches a keyword; this may be the case with col names that are just
            # single word
            self._fact_scoring.add(
                NodeScore(
                    node_id=col_id,
                    score=sum([s for s in found_keywords.values()]),
                    reason=f"column name exactly matches a keyword: {word[0]}",
                )
            )
        else:
            # no exact match; this may be the case with col names with garbage names; look for keywords
            # within the string
            found_keywords = self._defs.get_weakly_matching_keywords(word)

            if len(word[0]) == sum([len(kw) for kw in found_keywords.keys()]):
                # when the length of the column name is equal to sum of lengths of all found keywords, then
                # it means the column name is made up entirely from the keywords;

                self._fact_scoring.add(
                    NodeScore(
                        node_id=col_id,
                        score=sum([s for s in found_keywords.values()]),
                        reason=f"column name exactly matches a keyword: {word[0]}",
                    )
                )

            elif len(found_keywords) > 0:
                # otherwise the colname is probably some kind of 'garbage' name; some keywords were found in
                # there but it may be false positives

                self._fact_scoring.add(
                    NodeScore(
                        node_id=col_id,
                        score=100,
                        reason=f"text search found some keywords: {', '.join(found_keywords.keys())}",
                    )
                )

    def visit_column(
        self, ctx: VisitContext, col_id: str, column_data: MetadataColumnRow
    ) -> VisitReturnType:
        col_name = column_data.column_name
        words = _db_identifier_to_lc_words(col_name)

        if len(words) > 1:
            self._analyze_multiword(col_id, words)
        else:
            self._analyze_singleword(col_id, words)

        return
