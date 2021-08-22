# (C) 2021 GoodData Corporation
# there are some links here that make the lines too long so skipping this particular lint for the entire file
# flake8: noqa: E501
from collections import namedtuple

MetadataProductInfo = namedtuple(
    "DbProductInfo",
    ["product_name", "product_version", "major_version", "minor_version"],
)
"""
This is collection of four different pieces of JDBC metadata:

- product_name = getDatabaseProductName()
- product_version = getDatabaseMinorVersion()
- major_version = getDatabaseMajorVersion()
- minor_version = getDatabaseMinorVersion()
"""

MetadataDriverInfo = namedtuple(
    "DbDriverInfo",
    [
        "driver_name",
        "driver_version",
        "major_version",
        "minor_version",
        "jdbc_major_version",
        "jdbc_minor_version",
    ],
)
"""
This is collection of six different pieces of JDBC metadata:

- product_name = getDriverName()
- product_version = getDriverVersion()
- major_version = getDriverMajorVersion()
- minor_version = getDriverMinorVersion()
- jdbc_major_version = getJDBCMajorVersion(),
- jdbc_minor_version = getJDBCMinorVersion(),

"""

MetadataCatalogRow = namedtuple("MetadataCatalogRow", ["table_cat"])
"""
One row in the results of JDBC Metadata getCatalogs() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getCatalogs()

- TABLE_CAT String => catalog name
"""

MetadataSchemaRow = namedtuple("MetadataSchemaRow", ["table_schem", "table_catalog"])
"""
One row in the results of JDBC Metadata getSchemas() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getSchemas()

- TABLE_SCHEM String => schema name
- TABLE_CATALOG String => catalog name (may be null)
"""

MetadataTableRow = namedtuple(
    "MetadataTableRow",
    [
        "table_cat",
        "table_schem",
        "table_name",
        "table_type",
        "remarks",
        "type_cat",
        "type_schem",
        "type_name",
        "self_referencing_col_name",
        "ref_generation",
    ],
)
"""
One row in the results of JDBC Metadata getTables() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getTables(java.lang.String,java.lang.String,java.lang.String,java.lang.String%5B%5D)

- TABLE_CAT String => table catalog (may be null)
- TABLE_SCHEM String => table schema (may be null)
- TABLE_NAME String => table name
- TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
- REMARKS String => explanatory comment on the table
- TYPE_CAT String => the types catalog (may be null)
- TYPE_SCHEM String => the types schema (may be null)
- TYPE_NAME String => type name (may be null)
- SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed table (may be null)
- REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED". (may be null)
"""

MetadataColumnRow = namedtuple(
    "MetadataColumnRow",
    [
        "table_cat",
        "table_schem",
        "table_name",
        "column_name",
        "data_type",
        "type_name",
        "column_size",
        "buffer_length",
        "decimal_digits",
        "num_prec_radix",
        "nullable",
        "remarks",
        "column_def",
        "sql_data_type",
        "sql_datetime_sub",
        "char_octet_length",
        "ordinal_position",
        "is_nullable",
        "scope_catalog",
        "scope_schema",
        "scope_table",
        "source_data_type",
        "is_autoincrement",
        "is_generatedcolumn",
    ],
)
"""
One row in the results of Metadata getColumns() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getColumns(java.lang.String,java.lang.String,java.lang.String,java.lang.String)

- TABLE_CAT String => table catalog (may be null)
- TABLE_SCHEM String => table schema (may be null)
- TABLE_NAME String => table name
- COLUMN_NAME String => column name
- DATA_TYPE int => SQL type from java.sql.Types
- TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
- COLUMN_SIZE int => column size.
- BUFFER_LENGTH is not used.
- DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
- NUM_PREC_RADIX int => Radix (typically either 10 or 2)
- NULLABLE int => is NULL allowed.
    - columnNoNulls - might not allow NULL values
    - columnNullable - definitely allows NULL values
    - columnNullableUnknown - nullability unknown
- REMARKS String => comment describing column (may be null)
- COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
- SQL_DATA_TYPE int => unused
- SQL_DATETIME_SUB int => unused
- CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
- ORDINAL_POSITION int => index of column in table (starting at 1)
- IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
    - YES --- if the column can include NULLs
    - NO --- if the column cannot include NULLs
    - empty string --- if the nullability for the column is unknown
- SCOPE_CATALOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
- SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
- SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
- SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
- IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
    - YES --- if the column is auto incremented
    - NO --- if the column is not auto incremented
    - empty string --- if it cannot be determined whether the column is auto incremented
- IS_GENERATEDCOLUMN String => Indicates whether this is a generated column
    - YES --- if this a generated column
    - NO --- if this not a generated column
    - empty string --- if it cannot be determined whether this is a generated column
"""

MetadataPrimaryKeyRow = namedtuple(
    "MetadataPrimaryKeyRow",
    ["table_cat", "table_schem", "table_name", "column_name", "key_seq", "pk_name"],
)
"""
One row in the results of JDBC Metadata getPrimaryKeys() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getPrimaryKeys(java.lang.String,java.lang.String,java.lang.String)

- TABLE_CAT String => table catalog (may be null)
- TABLE_SCHEM String => table schema (may be null)
- TABLE_NAME String => table name
- COLUMN_NAME String => column name
- KEY_SEQ short => sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
- PK_NAME String => primary key name (may be null)
"""

MetadataForeignKeyRow = namedtuple(
    "MetadataForeignKeyRow",
    [
        "pktable_cat",
        "pktable_schem",
        "pktable_name",
        "pkcolumn_name",
        "fktable_cat",
        "fktable_schem",
        "fktable_name",
        "fkcolumn_name",
        "key_seq",
        "update_rule",
        "delete_rule",
        "fk_name",
        "pk_name",
        "deferrability",
    ],
)
"""
One row in the results of JDBC Metadata getImportedKeys() and getExportedKeys() methods.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getImportedKeys(java.lang.String,java.lang.String,java.lang.String)
See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getExportedKeys(java.lang.String,java.lang.String,java.lang.String)

- PKTABLE_CAT String => primary key table catalog being imported (may be null)
- PKTABLE_SCHEM String => primary key table schema being imported (may be null)
- PKTABLE_NAME String => primary key table name being imported
- PKCOLUMN_NAME String => primary key column name being imported
- FKTABLE_CAT String => foreign key table catalog (may be null)
- FKTABLE_SCHEM String => foreign key table schema (may be null)
- FKTABLE_NAME String => foreign key table name
- FKCOLUMN_NAME String => foreign key column name
- KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
- UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
    - importedNoAction - do not allow update of primary key if it has been imported
    - importedKeyCascade - change imported key to agree with primary key update
    - importedKeySetNull - change imported key to NULL if its primary key has been updated
    - importedKeySetDefault - change imported key to default values if its primary key has been updated
    - importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
- DELETE_RULE short => What happens to the foreign key when primary is deleted.
    - importedKeyNoAction - do not allow delete of primary key if it has been imported
    - importedKeyCascade - delete rows that import a deleted key
    - importedKeySetNull - change imported key to NULL if its primary key has been deleted
    - importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
    - importedKeySetDefault - change imported key to default if its primary key has been deleted
- FK_NAME String => foreign key name (may be null)
- PK_NAME String => primary key name (may be null)
- DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
    - importedKeyInitiallyDeferred - see SQL92 for definition
    - importedKeyInitiallyImmediate - see SQL92 for definition
    - importedKeyNotDeferrable - see SQL92 for definition
"""

MetadataTypeInfoRow = namedtuple(
    "MetadataTypeInfoRow",
    [
        "type_name",
        "data_type",
        "precision",
        "literal_prefix",
        "literal_suffix",
        "create_params",
        "nullable",
        "case_sensitive",
        "searchable",
        "unsigned_attribute",
        "fixed_prec_scale",
        "auto_increment",
        "local_type_name",
        "minimum_scale",
        "maximum_scale",
        "sql_data_type",
        "sql_datetime_sub",
        "num_prec_radix",
    ],
)
"""
One row in the results of JDBC Metadata getTypeInfo() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getTypeInfo()

- TYPE_NAME String => Type name
- DATA_TYPE int => SQL data type from java.sql.Types
- PRECISION int => maximum precision
- LITERAL_PREFIX String => prefix used to quote a literal (may be null)
- LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
- CREATE_PARAMS String => parameters used in creating the type (may be null)
- NULLABLE short => can you use NULL for this type.
    - typeNoNulls - does not allow NULL values
    - typeNullable - allows NULL values
    - typeNullableUnknown - nullability unknown
- CASE_SENSITIVE boolean=> is it case sensitive.
- SEARCHABLE short => can you use "WHERE" based on this type:
    - typePredNone - No support
    - typePredChar - Only supported with WHERE .. LIKE
    - typePredBasic - Supported except for WHERE .. LIKE
    - typeSearchable - Supported for all WHERE ..
- UNSIGNED_ATTRIBUTE boolean => is it unsigned.
- FIXED_PREC_SCALE boolean => can it be a money value.
- AUTO_INCREMENT boolean => can it be used for an auto-increment value.
- LOCAL_TYPE_NAME String => localized version of type name (may be null)
- MINIMUM_SCALE short => minimum scale supported
- MAXIMUM_SCALE short => maximum scale supported
- SQL_DATA_TYPE int => unused
- SQL_DATETIME_SUB int => unused
- NUM_PREC_RADIX int => usually 2 or 10
"""

MetadataIndexInfoRow = namedtuple(
    "MetadataIndexInfoRow",
    [
        "table_cat",
        "table_schem",
        "table_name",
        "non_unique",
        "index_qualifier",
        "index_name",
        "type",
        "ordinal_position",
        "column_name",
        "asc_or_desc",
        "cardinality",
        "pages",
        "filter_condition",
    ],
)
"""
One row in the results of JDBC Metadata getIndexInfo() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getIndexInfo(java.lang.String,java.lang.String,java.lang.String,boolean,boolean)

- TABLE_CAT String => table catalog (may be null)
- TABLE_SCHEM String => table schema (may be null)
- TABLE_NAME String => table name
- NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
- INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
- INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
- TYPE short => index type:
    - tableIndexStatistic - this identifies table statistics that are returned in conjunction with a table's index descriptions
    - tableIndexClustered - this is a clustered index
    - tableIndexHashed - this is a hashed index
    - tableIndexOther - this is some other style of index
- ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
- COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
- ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
- CARDINALITY long => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
- PAGES long => When TYPE is tableIndexStatistic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
- FILTER_CONDITION String => Filter condition, if any. (may be null)
"""

MetadataVersionColumnRow = namedtuple(
    "MetadataVersionColumnRow",
    [
        "scope",
        "column_name",
        "data_type",
        "type_name",
        "column_size",
        "buffer_length",
        "decimal_digits",
        "pseudo_column",
    ],
)
"""
One row in results of JDBC Metadata getVersionColumns() method.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html#getVersionColumns(java.lang.String,java.lang.String,java.lang.String)

- SCOPE short => is not used
- COLUMN_NAME String => column name
- DATA_TYPE int => SQL data type from java.sql.Types
- TYPE_NAME String => Data source-dependent type name
- COLUMN_SIZE int => precision
- BUFFER_LENGTH int => length of column value in bytes
- DECIMAL_DIGITS short => scale - Null is returned for data types where DECIMAL_DIGITS is not applicable.
- PSEUDO_COLUMN short => whether this is pseudo column like an Oracle ROWID
    - versionColumnUnknown - may or may not be pseudo column
    - versionColumnNotPseudo - is NOT a pseudo column
    - versionColumnPseudo - is a pseudo column
"""

MetadataConstants = namedtuple(
    "MetadataConstants",
    [
        "attributeNoNulls",
        "attributeNullable",
        "attributeNullableUnknown",
        "bestRowNotPseudo",
        "bestRowPseudo",
        "bestRowSession",
        "bestRowTemporary",
        "bestRowTransaction",
        "bestRowUnknown",
        "columnNoNulls",
        "columnNullable",
        "columnNullableUnknown",
        "functionColumnIn",
        "functionColumnInOut",
        "functionColumnOut",
        "functionColumnResult",
        "functionColumnUnknown",
        "functionNoNulls",
        "functionNoTable",
        "functionNullable",
        "functionNullableUnknown",
        "functionResultUnknown",
        "functionReturn",
        "functionReturnsTable",
        "importedKeyCascade",
        "importedKeyInitiallyDeferred",
        "importedKeyInitiallyImmediate",
        "importedKeyNoAction",
        "importedKeyNotDeferrable",
        "importedKeyRestrict",
        "importedKeySetDefault",
        "importedKeySetNull",
        "procedureColumnIn",
        "procedureColumnInOut",
        "procedureColumnOut",
        "procedureColumnResult",
        "procedureColumnReturn",
        "procedureColumnUnknown",
        "procedureNoNulls",
        "procedureNoResult",
        "procedureNullable",
        "procedureNullableUnknown",
        "procedureResultUnknown",
        "procedureReturnsResult",
        "sqlStateSQL",
        "sqlStateSQL99",
        "sqlStateXOpen",
        "tableIndexClustered",
        "tableIndexHashed",
        "tableIndexOther",
        "tableIndexStatistic",
        "typeNoNulls",
        "typeNullable",
        "typeNullableUnknown",
        "typePredBasic",
        "typePredChar",
        "typePredNone",
        "typeSearchable",
        "versionColumnNotPseudo",
        "versionColumnPseudo",
        "versionColumnUnknown",
    ],
)
"""
Values of all static constants in the defined on the database metadata object.

See: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html
"""


class MetadataRowTransformer:
    def __init__(self):
        pass

    def metadata_table_row_transformer(self, row: MetadataTableRow) -> MetadataTableRow:
        """
        Transform a row read from JDBC Metadata getTables() result set. Connectors may override this
        in order to perform sanitization / unification of DB specifics. Default implementation uses data returned
        by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_column_row_transformer(
        self, row: MetadataColumnRow
    ) -> MetadataColumnRow:
        """
        Transform a row read from JDBC Metadata getColumns() result set. Connectors may override this
        in order to perform sanitization / unification of DB specifics. Default implementation uses data returned
        by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_pk_row_transformer(
        self, row: MetadataPrimaryKeyRow
    ) -> MetadataPrimaryKeyRow:
        """
        Transform a row read from JDBC Metadata getPrimaryKeys() result set. Connectors may override this
        in order to perform sanitization / unification of DB specifics. Default implementation uses data returned
        by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_fk_row_transformer(
        self, row: MetadataForeignKeyRow
    ) -> MetadataForeignKeyRow:
        """
        Transform a row read from JDBC Metadata getImportedKeys() and getExportedKeys() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_type_info_row_transformer(
        self, row: MetadataTypeInfoRow
    ) -> MetadataTypeInfoRow:
        """
        Transform a row read from JDBC Metadata getTypeInfo() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_index_info_row_transformer(
        self, row: MetadataIndexInfoRow
    ) -> MetadataIndexInfoRow:
        """
        Transform a row read from JDBC Metadata getIndexInfo() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_version_column_row_transformer(
        self, row: MetadataVersionColumnRow
    ) -> MetadataVersionColumnRow:
        """
        Transform a row read from JDBC Metadata getVersionColumns() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_catalog_row_transformer(
        self, row: MetadataCatalogRow
    ) -> MetadataCatalogRow:
        """
        Transform a row read from JDBC Metadata getCatalogs() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row

    def metadata_schema_row_transformer(
        self, row: MetadataSchemaRow
    ) -> MetadataSchemaRow:
        """
        Transform a row read from JDBC Metadata getSchemas() result set.
        Connectors may override this in order to perform sanitization / unification of DB specifics. Default
        implementation uses data returned by the database as-is.

        It is OK for transformation to mutate the input row.

        :param row: row to transform
        :return: transformed row; may be the same instance as on input or may be a new instance
        """
        return row
