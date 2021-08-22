# (C) 2021 GoodData Corporation
import gooddata_sdk as sdk
from gooddata_fdw.logging import _log_debug, _log_info
from operator import itemgetter

from gooddata_fdw.environment import (
    ForeignDataWrapper,
    TableDefinition,
    ColumnDefinition,
)
from gooddata_fdw.naming import (
    DefaultInsightTableNaming,
    DefaultInsightColumnNaming,
    DefaultCatalogNamingStrategy,
)

_USER_AGENT = "gooddata-fdw/0.1"
"""
Extra segment of the User-Agent header that will be appended to standard gooddata-sdk user agent.
"""


def _col_as_computable(col: ColumnDefinition):
    item_type, item_id = col.options["id"].split("/")

    # since all cols are from the compute table, the uniqueness of local_id is ensured...
    if item_type == "label":
        return sdk.Attribute(local_id=col.column_name, label=item_id)
    else:
        aggregation = col.options["agg"] if "agg" in col.options else None

        return sdk.SimpleMetric(
            local_id=col.column_name,
            item=sdk.ObjId(item_id, item_type),
            aggregation=aggregation,
        )


class GoodDataForeignDataWrapper(ForeignDataWrapper):
    def __init__(self, options, columns):
        super(GoodDataForeignDataWrapper, self).__init__(options, columns)

        if "host" not in options or "token" not in options:
            raise ValueError("server OPTIONS must contain 'host' and 'token' keys.")

        if "workspace" not in options:
            raise ValueError(
                "attempting to work with an incorrectly defined foreign table."
                "Table must contain both 'workspace'."
            )

        _log_debug(f"initializing (options={options}, columns={columns})")

        self._host, self._token, self._workspace = itemgetter(
            "host", "token", "workspace"
        )(options)
        self._options = options
        self._columns = columns
        self._insight = options["insight"] if "insight" in options else None
        self._compute = options["compute"] if "compute" in options else None
        self._sdk = sdk.GoodDataSdk(
            host=self._host, token=self._token, extra_user_agent=_USER_AGENT
        )

        self._validate()

    def _validate(self):
        """
        Validates column definitions, making sure that the options contain all the essential mapping metadata.

        For table mapped to an insight, each column's OPTIONS must contain localId.
        For all other tables (including the 'compute' pseudo-table), each column's OPTIONS must contain 'id'
        :return:
        """
        if self._insight is not None:
            for c in self._columns.values():
                if "local_id" not in c.options:
                    raise ValueError(
                        f"Foreign table column '{c.column_name}' is not defined correctly. "
                        f"For tables that map GoodData.CN insight, the column OPTIONS must specify "
                        f"'localId' which is localIdentifier of the Insight's bucket item. If you created "
                        f"this table manually, please rather use the IMPORT FOREIGN SCHEMA and import "
                        f"from the 'gooddata_insights' schema. The import will set everything correctly."
                    )

            return

        for c in self._columns.values():
            if "id" not in c.options:
                raise ValueError(
                    f"Foreign table column '{c.column_name}' is not defined correctly. "
                    f"For tables mapping to GoodData.CN semantic layer, the column OPTIONS must specify "
                    f"'id' which in format: 'fact/your.fact.id', 'label/your.label.id', "
                    f"'metric/your.metric.id'."
                )
            else:
                split = c.options["id"].split("/")

                if len(split) > 2 or split[0] not in ("fact", "label", "metric"):
                    raise ValueError(
                        f"Foreign table column '{c.column_name}' is not defined correctly. "
                        f"For tables mapping to GoodData.CN semantic layer, the column OPTIONS must "
                        f"specify 'id' which in format: 'fact/your.fact.id', 'label/your.label.id', "
                        f"'metric/your.metric.id'. Instead got: {c.options['id']}"
                    )

    def _execute_insight(self, quals, columns, sortKeys=None):
        """
        Computes data for table mapped to an insight. Note that this execution maintains insight's filters - the
        table is implicitly filtered.
        """
        # TODO add validation that the table columns are consistent with insight bucket items

        col_to_local_id = dict(
            [(c.column_name, c.options["local_id"]) for c in self._columns.values()]
        )
        insight = self._sdk.insights.get_insight(self._workspace, self._insight)
        table = self._sdk.tables.for_insight(self._workspace, insight)

        for result_row in table.read_all():
            row = dict()

            # TODO: it is likely that conversion to DATE/TIMESTAMP will have to happen here if the column is of
            #  the respective type
            for column_name in columns:
                row[column_name] = result_row[col_to_local_id[column_name]]

            yield row

    def get_computable_for_col_name(self, column_name):
        return _col_as_computable(self._columns[column_name])

    def _execute_compute(self, quals, columns, sortKeys=None):
        """
        Computes data for the 'compute' pseudo-table. The 'compute' table is special. It does not behave as other
        relational tables: the input columns determine what data will be calculated and the cardinality of the result
        fully depends on the input columns.
        """
        # TODO: pushdown some of the filters that are included in quals
        items = [self.get_computable_for_col_name(col_name) for col_name in columns]
        table = self._sdk.tables.for_items(self._workspace, items)

        # TODO: it is likely that this has to change to support DATE and TIMESTAMP. have mapping that need to be
        #  timestamp/date, instead of returning generator, iterate rows, convert to dates and yield the converted row
        return table.read_all()

    def _execute_custom_report(self, quals, columns, sortKeys=None):
        """
        Computes data for manually created table that maps to particular workspace and its columns map to label, fact or
        metric in that workspace. The mapping conventions are same as for the 'compute' pseudo-table. Compared to the
        pseudo-table though, the custom report execution always computes data for all columns - thus appears like
        any other relational table.
        """
        # TODO: pushdown some of the filters that are included in quals
        items = [_col_as_computable(col) for col in self._columns.values()]
        table = self._sdk.tables.for_items(self._workspace, items)

        # TODO: it is likely that this has to change to support DATE and TIMESTAMP. have mapping that need to be
        #  timestamp/date, instead of returning generator, iterate rows, convert to dates and yield the converted row
        # note: no need to filter result rows to only those that are SELECTed.. multicorn/postgres takes care of
        # that
        return table.read_all()

    def execute(self, quals, columns, sortkeys=None):
        _log_debug(
            f"query in fdw with options {self._options}; columns {type(columns)}"
        )

        if self._insight:
            return self._execute_insight(quals, columns, sortkeys)
        if self._compute:
            return self._execute_compute(quals, columns, sortkeys)

        return self._execute_custom_report(quals, columns, sortkeys)

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        _log_info(
            f"import fdw schema {schema} (srv_options={srv_options}, "
            f"options={options}, restriction_type={restriction_type}, restricts={restricts})"
        )

        if "host" not in srv_options or "token" not in srv_options:
            raise ValueError("server OPTIONS must contain 'host' and 'token' keys.")

        if "workspace" not in options:
            raise ValueError(
                "gooddata_fdw: IMPORT SCHEMA OPTIONS must contain 'workspace' key "
                "to indicate workspace from which to import."
            )

        host = srv_options["host"]

        if not host.startswith("https://") and not host.startswith("http://"):
            raise ValueError(
                "gooddata_fdw: your server is not defined correctly. "
                "The host must start with https:// or http://"
            )

        if schema == "gooddata_insights":
            return cls.import_insights_from_workspace(
                schema, srv_options, options, restriction_type, restricts
            )
        elif schema == "gooddata_compute":
            return cls.import_semantic_layer_from_workspace(
                schema, srv_options, options, restriction_type, restricts
            )

        raise NotImplementedError(
            f"This FDW does not support IMPORT FOREIGN SCHEMA for {schema}"
        )

    @classmethod
    def import_insights_from_workspace(
        cls, schema, srv_options, options, restriction_type, restricts
    ):
        workspace = options["workspace"]
        table_naming = DefaultInsightTableNaming()

        _log_info(
            f"importing insights as tables from {srv_options['host']} workspace {options['workspace']}"
        )

        _sdk = sdk.GoodDataSdk(
            host=srv_options["host"],
            token=srv_options["token"],
            extra_user_agent=_USER_AGENT,
        )

        # TODO catalog will be needed to correctly identify cols that contain date/timestamp; skipping for now
        # _log_debug(f"loading full catalog")
        # catalog_service = sdk.CatalogService(client)
        # catalog = catalog_service.get_full_catalog(workspace)

        _log_debug("loading all insights")
        insights = _sdk.insights.get_insights(workspace)

        tables = []

        for insight in insights:
            table_name = table_naming.table_name_for_insight(insight)
            _log_info(f"creating table def {table_name} for insight {insight.title}")

            column_naming = DefaultInsightColumnNaming()
            columns = []

            for attr in insight.attributes:
                column_name = column_naming.col_name_for_attribute(attr)
                _log_debug(f"creating col def {column_name} for attribute {attr}")

                col = ColumnDefinition(
                    column_name=column_name,
                    type_name="VARCHAR(256)",
                    options=dict(local_id=attr.local_id),
                )
                columns.append(col)

            for metric in insight.metrics:
                column_name = column_naming.col_name_for_metric(metric)
                _log_debug(f"creating col def {column_name} for metric {metric}")

                col = ColumnDefinition(
                    column_name=column_name,
                    type_name="DECIMAL(15,5)",
                    options=dict(local_id=metric.local_id),
                )
                columns.append(col)

            table = TableDefinition(
                table_name=table_name,
                columns=columns,
                options=dict(workspace=workspace, insight=insight.id),
            )
            tables.append(table)

        return tables

    @classmethod
    def import_semantic_layer_from_workspace(
        cls, schema, srv_options, options, restriction_type, restricts
    ):
        workspace = options["workspace"]

        _log_info(
            f"importing semantic layer as tables from {srv_options['host']} workspace {options['workspace']}"
        )

        _sdk = sdk.GoodDataSdk(
            host=srv_options["host"],
            token=srv_options["token"],
            extra_user_agent=_USER_AGENT,
        )

        catalog = _sdk.catalog.get_full_catalog(workspace)
        columns = []
        naming = DefaultCatalogNamingStrategy()

        for metric in catalog.metrics:
            column_name = naming.col_name_for_metric(metric)

            _log_info(f"metric {metric.id} mapped to column {column_name}")

            columns.append(
                ColumnDefinition(
                    column_name=column_name,
                    type_name="DECIMAL(15,5)",
                    options=dict(id=f"metric/{metric.id}"),
                )
            )

        for dataset in catalog.datasets:
            for fact in dataset.facts:
                column_name = naming.col_name_for_fact(fact, dataset)

                _log_info(f"fact {fact.id} mapped to column {column_name}")

                columns.append(
                    ColumnDefinition(
                        column_name=column_name,
                        type_name="DECIMAL(15,5)",
                        options=dict(id=f"fact/{fact.id}"),
                    )
                )

            for attribute in dataset.attributes:
                # TODO: correctly identify cols that should be DATE or TIMESTAMP. skipping for now because
                #  can't be bothered doing the date conversions
                for label in attribute.labels:
                    column_name = naming.col_name_for_label(label, dataset)

                    _log_info(f"label {label.id} mapped to column {column_name}")

                    columns.append(
                        ColumnDefinition(
                            column_name=column_name,
                            type_name="VARCHAR(256)",
                            options=dict(id=f"label/{label.id}"),
                        )
                    )

        return [
            TableDefinition(
                table_name="compute",
                columns=columns,
                options=dict(workspace=workspace, compute="pseudo-table"),
            )
        ]

    @property
    def rowid_column(self):
        return super().rowid_column

    def insert(self, values):
        return super().insert(values)

    def update(self, oldvalues, newvalues):
        return super().update(oldvalues, newvalues)

    def delete(self, oldvalues):
        return super().delete(oldvalues)