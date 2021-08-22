# (C) 2021 GoodData Corporation
from enum import Enum


def _sanitize_schema(schema: str) -> str:
    return schema or "@"


class NodeTypes(Enum):
    """
    Enumeration of all supported node types.
    """

    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    PK = "pk"
    FK = "fk"
    INDEX = "index"
    TYPE = "type"


class EdgeTypes(Enum):
    """
    Enumeration of all supported edge types.
    """

    SCHEMA_TABLE = "schema-table"
    """
    Edge between schema and table
    """

    TABLE_COLUMN = "table-column"
    """
    edge between table and its column
    """

    COLUMN_TABLE = "column-table"
    """edge between column and table to which it belongs"""

    COLUMN_TYPE = "column-type"
    """edge between column and its type definition"""

    TABLE_PK = "table-pk"
    """edge between table and its primary key"""

    PK_COLUMN = "pk-column"
    """edge between primary key and columns of which it is made; edge has additional key_seq attribute
          which indicates the order of the column within the primary key"""

    TABLE_FK = "table-fk"
    """edge between table that defines the foreign key relationship and the foreign key itself"""

    FK_TABLE = "fk-table"
    """edge between foreign key relationship and the referenced table"""

    COLUMN_FK = "column-fk"
    """edge between foreign key column and the foreign key definition"""

    FK_COLUMN = "fk-column"
    """edge between foreign key relationship and the column in the referenced table"""

    REFERENCE = "reference"
    """edge between foreign key column and the column in the referenced table"""

    REFERENCE_BY = "reference-by"
    """edge between column and the foreign key column that is referencing it"""


def _composite_id(
    node_type: NodeTypes, cat: str, schema: str, rest: tuple = None
) -> str:
    all_parts = [cat, _sanitize_schema(schema)] + list(rest or ())

    return node_type.value + "://" + ".".join(all_parts)


def schema_id(table_catalog, table_schem=None) -> str:
    return _composite_id(NodeTypes.SCHEMA, table_catalog, table_schem)


def table_id(table_cat, table_schem, table_name) -> str:
    return _composite_id(NodeTypes.TABLE, table_cat, table_schem, (table_name,))


def column_id(table_cat, table_schem, table_name, column_name) -> str:
    return _composite_id(
        NodeTypes.COLUMN, table_cat, table_schem, (table_name, column_name)
    )


def pk_id(table_cat, table_schem, table_name, pk_name) -> str:
    return _composite_id(
        NodeTypes.PK, table_cat, table_schem, (table_name, pk_name or "@")
    )


def fk_id(table_cat, table_schem, table_name, fk_name) -> str:
    return _composite_id(
        NodeTypes.FK, table_cat, table_schem, (table_name, fk_name or "@")
    )


def index_id(table_cat, table_schem, table_name, index_name) -> str:
    return _composite_id(
        NodeTypes.INDEX, table_cat, table_schem, (table_name, index_name or "@")
    )


def type_id(type_name) -> str:
    return f"{NodeTypes.TYPE}://{type_name}"
