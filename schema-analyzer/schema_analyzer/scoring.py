# (C) 2021 GoodData Corporation


class NodeScore:
    """
    Node score is a result of evaluation of a graph node against some criteria. Scoring is a result of heuristic
    methods that aim to determine whether a node is suitable for 'something'.

    Example: is a column node a good candidate to be a fact?

    Multiple heuristics may be applied and each contributes its score; the scores are added together in the end
    and the higher the score, the higher chance of the node being suitable for 'something'.

    Note that scores assigned by different heuristics may be negative and thus interplay with scoring done by
    other heuristics. A very high negative score can effectively disqualify node.
    """

    SCORE_NORMAL = 100
    SCORE_GOOD = 200

    def __init__(self, node_id: str, score: int, reason: str):
        self._node_id = node_id
        self._score = score
        self._reason = reason

    @property
    def node_id(self):
        return self._node_id

    @property
    def score(self):
        return self._score

    @property
    def reason(self):
        return self._reason

    def __str__(self):
        if self.score > 0:
            sign = "+"
        elif self.score < 0:
            sign = "-"
        else:
            sign = "?"

        return f"{sign} - {self.node_id}: {self.reason}"

    def __repr__(self):
        return self.__str__()


class NodeDisqualified(NodeScore):
    """
    A convenience subclass of NodeScore that effectively disqualifies the node.

    Example: a column node fits several criteria to be a fact in a star schema nicely, it has positive score;
    then it turns out it is part of a primary key in a table and gets disqualified.
    """

    def __init__(self, node_id: str, reason: str):
        super(NodeDisqualified, self).__init__(
            node_id=node_id, score=-100000, reason=reason
        )

    def __str__(self):
        return f"! - {self.node_id} - {self.reason}"


class NodeScoringObjective:
    """
    Node scoring objective represents the reason why are some nodes being scores:

    -  Find column nodes that can be facts in star schema table
    -  Find column nodes that can be dimensions start schema tables

    Node scores are accumulated under this objective. There can be multiple scores for a single node.

    The scoring objective should have a descriptive name - this also plays a role of objective's identifier. Objectives
    with the same names can be merged, thus allowing multiple independent heuristics to evaluate nodes against the same
    objective and in the end merge the results.
    """

    def __init__(self, name: str, scores: dict = None):
        self._name = name
        self._node_scores = scores or dict()

    @property
    def name(self):
        return self._name

    def add(self, score: NodeScore):
        if score.node_id in self._node_scores:
            self._node_scores[score.node_id].append(score)
        else:
            self._node_scores[score.node_id] = [score]

    def get_node_scores(self, score_cutoff: int = None):
        results = []

        for col_id, scores in self._node_scores.items():
            total_score = sum([score.score for score in scores])

            if score_cutoff is not None and total_score < score_cutoff:
                continue

            sorted_scores = scores[:]
            sorted_scores.sort(key=lambda x: x.score)

            results.append((col_id, total_score, sorted_scores))

        results.sort(key=lambda x: x[1])

        return results

    def merge(self, other: "NodeScoringObjective"):
        """
        Merge two instances of node scores for the same objective.

        :param other: other scoring to merge with this instance
        :return: always new instance
        """
        if self.name != other.name:
            raise ValueError(
                f"attempting to merge incompatible column scorings: {self.name} and {other.name}"
            )

        merged_nodes = dict()

        for node_id, scores in self._node_scores.items():
            merged_nodes[node_id] = scores[:]

        for node_id, scores in other._node_scores.items():
            if node_id in merged_nodes:
                merged_nodes[node_id].extend(scores[:])
            else:
                merged_nodes[node_id] = scores[:]

        return NodeScoringObjective(self.name, merged_nodes)
