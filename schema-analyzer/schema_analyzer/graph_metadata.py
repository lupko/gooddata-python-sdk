# (C) 2021 GoodData Corporation
import pickle

from schema_analyzer import (
    MetadataCatalogRow,
    MetadataSchemaRow,
    MetadataTableRow,
    MetadataColumnRow,
    MetadataPrimaryKeyRow,
    MetadataForeignKeyRow,
    MetadataIndexInfoRow,
    MetadataTypeInfoRow,
)
from schema_analyzer.connector import DbMetadata, DbConnector
from schema_analyzer.graph_base import (
    schema_id,
    table_id,
    column_id,
    pk_id,
    fk_id,
    index_id,
    type_id,
    NodeTypes,
)


def _catalog_row_to_node(row: MetadataCatalogRow) -> tuple[str, MetadataCatalogRow]:
    pass


def _schema_row_to_node(row: MetadataSchemaRow) -> tuple[str, MetadataSchemaRow]:
    return schema_id(row.table_catalog, row.table_schem), row


def _table_row_to_node(row: MetadataTableRow) -> tuple[str, MetadataTableRow]:
    return table_id(row.table_cat, row.table_schem, row.table_name), row


def _column_row_to_node(row: MetadataColumnRow) -> tuple[str, MetadataColumnRow]:
    return (
        column_id(row.table_cat, row.table_schem, row.table_name, row.column_name),
        row,
    )


def _column_pk_to_node(row: MetadataPrimaryKeyRow) -> tuple[str, MetadataPrimaryKeyRow]:
    return pk_id(row.table_cat, row.table_schem, row.table_name, row.pk_name), row


def _column_fk_to_node(row: MetadataForeignKeyRow) -> tuple[str, MetadataForeignKeyRow]:
    return (
        fk_id(row.fktable_cat, row.fktable_schem, row.fktable_name, row.fk_name),
        row,
    )


def _index_to_node(row: MetadataIndexInfoRow) -> tuple[str, MetadataIndexInfoRow]:
    return (
        index_id(row.table_cat, row.table_schem, row.table_name, row.index_name),
        row,
    )


def _type_to_node(row: MetadataTypeInfoRow) -> tuple[str, MetadataTypeInfoRow]:
    return type_id(row.type_name), row


def _load_schemas(md: DbMetadata, dbname: str) -> dict[str, MetadataSchemaRow]:
    schemas = dict(list(_schema_row_to_node(s) for s in md.get_schemas(catalog=dbname)))

    if not len(schemas):
        # if database has no schema explicitly defined but instead has tables defined 'globally' then
        # some (all?) databases (MariaDB) will return no schema data at all.
        #
        # the other metadata entries will have table_schema None; this entry exists to anchor those
        # tables
        dummy_schema_id = schema_id(table_catalog=dbname)
        schemas[dummy_schema_id] = MetadataSchemaRow(
            table_catalog=dbname, table_schem=None
        )

    return schemas


def _load_tables(md: DbMetadata, dbname: str) -> dict[str, MetadataTableRow]:
    return dict(list(_table_row_to_node(t) for t in md.get_tables(catalog=dbname)))


def _load_columns(md: DbMetadata, dbname: str) -> dict[str, MetadataColumnRow]:
    return dict(list(_column_row_to_node(c) for c in md.get_columns(catalog=dbname)))


def _load_pks(md: DbMetadata, dbname: str) -> dict[str, list[MetadataPrimaryKeyRow]]:
    result = dict()

    for pk in md.get_primary_keys(catalog=dbname):
        pk_id, row = _column_pk_to_node(pk)

        if pk_id not in result:
            result[pk_id] = list()

        result[pk_id].append(row)

    return result


def _load_fks(md: DbMetadata, dbname: str) -> dict[str, list[MetadataForeignKeyRow]]:
    result = dict()

    for fk in md.get_exported_keys(catalog=dbname):
        fk_id, row = _column_fk_to_node(fk)

        if fk_id not in result:
            result[fk_id] = list()

        result[fk_id].append(row)

    return result


def _load_indexes(
    md: DbMetadata, dbname: str, schema: str, table: str
) -> dict[str, MetadataIndexInfoRow]:
    return dict(
        list(
            _index_to_node(idx)
            for idx in md.get_index_info(
                table_name=table, catalog=dbname, schema=schema
            )
        )
    )


def _load_types(md: DbMetadata) -> dict[str, MetadataTypeInfoRow]:
    return dict(list(_type_to_node(t) for t in md.get_type_info()))


class DatabaseGraphMetadata:
    """
    Metadata storage for the database graph. The metadata is either loaded from a file or obtained from a live
    database using DbConnector.

    This class holds mappings between the different possible graph nodes and their metadata objects. It also has
    several indexes to facilitate efficient lookups.
    """

    def __init__(self):
        self._schemas: dict[str, MetadataSchemaRow] = dict()
        self._tables: dict[str, MetadataTableRow] = dict()
        self._columns: dict[str, MetadataColumnRow] = dict()
        self._pks: dict[str, list[MetadataPrimaryKeyRow]] = dict()
        self._fks: dict[str, list[MetadataForeignKeyRow]] = dict()
        self._types: dict[str, MetadataTypeInfoRow] = dict()
        self._has_data = False

        self._node_idx = dict()
        self._node_by_type_idx = dict()

    def _create_indexes(self):
        self._node_by_type_idx[NodeTypes.SCHEMA] = tuple(self._schemas.keys())
        self._node_by_type_idx[NodeTypes.TABLE] = tuple(self._tables.keys())
        self._node_by_type_idx[NodeTypes.COLUMN] = tuple(self._columns.keys())
        self._node_by_type_idx[NodeTypes.PK] = tuple(self._pks.keys())
        self._node_by_type_idx[NodeTypes.FK] = tuple(self._fks.keys())
        self._node_by_type_idx[NodeTypes.TYPE] = tuple(self._types.keys())

        self._node_idx = dict(
            list(self._schemas.items())
            + list(self._tables.items())
            + list(self._columns.items())
            + list(self._pks.items())
            + list(self._fks.items())
            + list(self._types.items())
        )

    def load(self, filename: str):
        """
        Loads all the raw data used to construct the graph from a file. This will use Python's pickle
        to deserialize the contents of the provided file.

        :param filename: file name, must exist
        :return: None
        """
        with open(filename, "rb") as fr:
            (
                self._schemas,
                self._tables,
                self._columns,
                self._pks,
                self._fks,
                self._types,
            ) = pickle.load(fr)
            self._has_data = True
            self._create_indexes()

    def dump(self, filename: str):
        """
        Stores all the raw data used to construct the graph into a file. This will use Python's pickle
        to serialize the contents.

        :param filename: file name, if exists will be overwritten
        :return:
        """
        if self._has_data is False:
            raise ValueError(
                "There is no data to save. You can use load_from_db() to read data from existing database."
            )

        with open(filename, "wb") as fw:
            pickle.dump(
                (
                    self._schemas,
                    self._tables,
                    self._columns,
                    self._pks,
                    self._fks,
                    self._types,
                ),
                fw,
            )

    def load_from_db(self, connector: DbConnector, dbname: str):
        """
        Loads all database metadata needed to construct database graph.

        :param connector: an instance of connector to use to retrieve database
        :param dbname: database name
        :return:
        """
        with connector.metadata() as md:
            self._schemas = _load_schemas(md, dbname)
            self._tables = _load_tables(md, dbname)
            self._columns = _load_columns(md, dbname)
            self._pks = _load_pks(md, dbname)
            self._fks = _load_fks(md, dbname)
            self._types = _load_types(md)
            self._has_data = True
            self._create_indexes()

    def get_nodes_by_type(self, types: tuple):
        """
        Gets all nodes in the database graph by their node type.

        :param types: node types to get
        :type types: tuple[NodeTypes]
        :return:
        """
        result = []

        for t in types:
            result.extend(self._node_by_type_idx[t])

        return result

    def get_node_metadata(self, node_id: str):
        """
        Gets data for node with the provided id.

        :param node_id:
        :return:
        """
        return self._node_idx[node_id]

    @property
    def is_empty(self) -> bool:
        return not self._has_data

    @property
    def schemas(self) -> dict[str, MetadataSchemaRow]:
        return self._schemas

    @property
    def schema_ids(self) -> set[str]:
        return set(self._schemas.keys())

    @property
    def tables(self) -> dict[str, MetadataTableRow]:
        return self._tables

    @property
    def table_ids(self) -> set[str]:
        return set(self._tables.keys())

    @property
    def columns(self) -> dict[str, MetadataColumnRow]:
        return self._columns

    @property
    def column_ids(self) -> set[str]:
        return set(self._columns.keys())

    @property
    def pks(self) -> dict[str, list[MetadataPrimaryKeyRow]]:
        return self._pks

    @property
    def pk_ids(self) -> set[str]:
        return set(self._pks.keys())

    @property
    def fks(self) -> dict[str, list[MetadataForeignKeyRow]]:
        return self._fks

    @property
    def fk_ids(self) -> set[str]:
        return set(self._fks.keys())

    @property
    def types(self) -> dict[str, MetadataTypeInfoRow]:
        return self._types

    @property
    def type_ids(self) -> set[str]:
        return set(self._types.keys())
