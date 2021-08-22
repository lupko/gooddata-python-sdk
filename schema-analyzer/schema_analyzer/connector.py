# (C) 2021 GoodData Corporation
from typing import Generator

import jaydebeapi

from schema_analyzer.metadata import (
    MetadataTableRow,
    MetadataColumnRow,
    MetadataPrimaryKeyRow,
    MetadataForeignKeyRow,
    MetadataTypeInfoRow,
    MetadataIndexInfoRow,
    MetadataConstants,
    MetadataVersionColumnRow,
    MetadataCatalogRow,
    MetadataSchemaRow,
    MetadataRowTransformer,
    MetadataProductInfo,
    MetadataDriverInfo,
)


class DbConnector:
    def __init__(
        self,
        classname=None,
        connection_string=None,
        driver_path=None,
        user=None,
        password=None,
    ):
        self._classname = classname
        self._connection_string = connection_string
        self._driver_path = driver_path
        self._user = user
        self._password = password

    @property
    def connection_string(self) -> str:
        return self._connection_string

    @connection_string.setter
    def connection_string(self, val: str):
        self._connection_string = val

    @property
    def driver_path(self) -> str:
        return self._driver_path

    @driver_path.setter
    def driver_path(self, val: str):
        self._driver_path = val

    @property
    def user(self) -> str:
        return self._user

    @user.setter
    def user(self, val: str):
        self._user = val

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, val: str):
        self._password = val

    def create_properties(self) -> dict[str, str]:
        """
        Create dict that will be eventually used as Properties sent to the driver. The default implementation
        fills in `user` and `password` properties.

        If concrete database connector has some specific properties that it allows to set, it should override
        this method, get default properties and enrich them with database specific stuff.

        :return: dict representing Properties that will be sent over to the driver
        """
        return dict(user=self.user, password=self.password)

    def metadata(self, row_transformer=MetadataRowTransformer()) -> "DbMetadata":
        """
        Access database metadata. This is an adapter on top of the JDBC DatabaseMetaData object.

        Use the returned object as a resource:
        >>> with connector.metadata() as md:
        >>>     for table in md.get_tables():
        >>>         # do something with the table
        >>>         pass

        :param row_transformer: optionally specify row transformer to use when reading the different metadata entries
        :return:
        """
        conn = jaydebeapi.connect(
            jclassname=self._classname,
            url=self.connection_string,
            driver_args=self.create_properties(),
            jars=self.driver_path,
        )

        return DbMetadata(metadata_transform=row_transformer, conn=conn)


def _convert_to_python(val):
    """
    Metadata results may contain java types that will not be picked up and auto-converted by jaydebeapi.
    Long, Integer, BigInteger and JLong encountered so far.

    Keeping java types around creates problems down the line because various python built-ins or third party
    code is not prepared to handle these types.

    :param val: value to convert
    :return:
    """
    t = str(type(val))

    if "java.lang.Integer" in t:
        return int(val.intValue())
    elif "java.lang.Long" in t:
        return int(val.longValue())
    elif "java.lang.Boolean" in t:
        return bool(val.booleanValue())
    elif "java.math.BigInteger" in t:
        return int(val.longValue())
    elif "JLong" in t:
        return int(val)
    elif "JInt" in t:
        return int(val)
    elif "java" in t:
        print("unexpected java type: " + t)

        # certainly this is not true, more java types incoming!?
        assert t is True

    return val


class DbMetadata:
    def __init__(
        self, metadata_transform: MetadataRowTransformer, conn: jaydebeapi.Connection
    ):
        self._t = metadata_transform
        self._conn = conn
        self._product_info = None
        self._driver_info = None
        self._constants = None

    def _process_result(self, result, db_row_factory):
        with self._conn.cursor() as cursor:
            cursor._rs = result
            cursor._meta = result.getMetaData()

            while True:
                row = cursor.fetchone()

                if row is None:
                    return None
                else:
                    with_python_types = [_convert_to_python(col) for col in row]

                    yield db_row_factory(with_python_types)

    @property
    def product_info(self) -> MetadataProductInfo:
        """
        Gets information about the database product running on the server.

        :return: DbProductInfo
        :rtype: DbProductInfo
        """
        if self._product_info is None:
            self._product_info = MetadataProductInfo(
                product_name=self._conn.jconn.getMetaData().getDatabaseProductName(),
                product_version=self._conn.jconn.getMetaData().getDatabaseProductVersion(),
                major_version=self._conn.jconn.getMetaData().getDatabaseMajorVersion(),
                minor_version=self._conn.jconn.getMetaData().getDatabaseMinorVersion(),
            )

        return self._product_info

    @property
    def driver_info(self) -> MetadataDriverInfo:
        """
        Gets information about the driver used to connect to the database.

        :return: DbDriverInfo
        :rtype: DbDriverInfo
        """
        if self._driver_info is None:
            self._driver_info = MetadataDriverInfo(
                driver_name=self._conn.jconn.getMetaData().getDriverName(),
                driver_version=self._conn.jconn.getMetaData().getDriverVersion(),
                major_version=self._conn.jconn.getMetaData().getDriverMajorVersion(),
                minor_version=self._conn.jconn.getMetaData().getDriverMinorVersion(),
                jdbc_major_version=self._conn.jconn.getMetaData().getJDBCMajorVersion(),
                jdbc_minor_version=self._conn.jconn.getMetaData().getJDBCMinorVersion(),
            )
        return self._driver_info

    def get_tables(
        self, catalog: str = None, schema: str = None, table_name_pattern: str = "%"
    ) -> Generator[MetadataTableRow, MetadataTableRow, None]:
        """
        Yields table metadata for the connected database.

        This metadata is obtained by calling getTables() JDBC method.

        :param catalog: optionally specify name of catalog from which to get tables; defaults to all catalogs
        :param schema: optionally specify name of schema from which to get tables; defaults to all schemas
        :param table_name_pattern: pattern for tables to return; default is '%' which should return all tables
        :return: yields row per table in the database
        """
        table_results = self._conn.jconn.getMetaData().getTables(
            catalog, schema, table_name_pattern, None
        )

        def table_row_factory(row):
            return self._t.metadata_table_row_transformer(MetadataTableRow(*row))

        return self._process_result(table_results, table_row_factory)

    def get_columns(
        self, catalog: str = None, schema: str = None, table_name="%"
    ) -> Generator[MetadataColumnRow, MetadataColumnRow, None]:
        """
        Yields column metadata for the connected database.

        This metadata is obtained by calling getColumns() JDBC method.

        :param catalog: optionally specify name of catalog from which to get tables; defaults to all catalogs
        :param schema: optionally specify name of schema from which to get tables; defaults to all schemas
        :param table_name: name or pattern for tables to return; default is '%' which should return all tables
        :return: yields row per column
        """
        column_results = self._conn.jconn.getMetaData().getColumns(
            catalog, schema, table_name, None
        )

        def column_row_factory(row):
            return self._t.metadata_column_row_transformer(MetadataColumnRow(*row))

        return self._process_result(column_results, column_row_factory)

    def get_primary_keys(
        self, catalog: str = None, schema: str = None, table_name="%"
    ) -> Generator[MetadataPrimaryKeyRow, MetadataPrimaryKeyRow, None]:
        """
        Yields primary key metadata for the connected database. Columns defined as PRIMARY KEY in various
        tables can be listed through this.

        This metadata is obtained by calling getPrimaryKeys() JDBC method.

        :param catalog: optionally specify name of catalog from which to get tables; defaults to all catalogs
        :param schema: optionally specify name of schema from which to get tables; defaults to all schemas
        :param table_name: name or pattern for tables whose primary keys to list
        :return: yields row per primary key
        """
        pk_results = self._conn.jconn.getMetaData().getPrimaryKeys(
            catalog, schema, table_name
        )

        def pk_row_factory(row):
            return self._t.metadata_pk_row_transformer(MetadataPrimaryKeyRow(*row))

        return self._process_result(pk_results, pk_row_factory)

    def get_imported_keys(
        self, catalog: str = None, schema: str = None, table_name="%"
    ) -> Generator[MetadataForeignKeyRow, MetadataForeignKeyRow, None]:
        ik_results = self._conn.jconn.getMetaData().getImportedKeys(
            catalog, schema, table_name
        )

        def fk_row_factory(row):
            return self._t.metadata_fk_row_transformer(MetadataForeignKeyRow(*row))

        return self._process_result(ik_results, fk_row_factory)

    def get_exported_keys(
        self, catalog: str = None, schema: str = None, table_name="%"
    ) -> Generator[MetadataForeignKeyRow, MetadataForeignKeyRow, None]:
        ek_results = self._conn.jconn.getMetaData().getExportedKeys(
            catalog, schema, table_name
        )

        def fk_row_factory(row):
            return self._t.metadata_fk_row_transformer(MetadataForeignKeyRow(*row))

        return self._process_result(ek_results, fk_row_factory)

    def get_type_info(
        self,
    ) -> Generator[MetadataTypeInfoRow, MetadataTypeInfoRow, None]:
        """
        Gets metadata about types available in the database.

        This metadata is obtained by calling getTypeInfo() JDBC method.

        :return: yields row per available type
        """
        type_info_results = self._conn.jconn.getMetaData().getTypeInfo()

        def type_info_row_factory(row):
            return self._t.metadata_type_info_row_transformer(MetadataTypeInfoRow(*row))

        return self._process_result(type_info_results, type_info_row_factory)

    def get_index_info(
        self,
        table_name,
        catalog: str = None,
        schema: str = None,
        unique=False,
        approximate=False,
    ) -> Generator[MetadataIndexInfoRow, MetadataIndexInfoRow, None]:
        """
        Gets metadata about indexes defined for a table in the database.

        This metadata is obtained by calling getIndexInfo() JDBC method.

        :param catalog: optionally specify name of catalog from which to get tables; defaults to all catalogs
        :param schema: optionally specify name of schema from which to get tables; defaults to all schemas
        :param table_name: table
        :param unique: when true, return only indices for unique values; when false, return indices regardless of
        whether unique or not
        :param approximate: when true, result is allowed to reflect approximate or out of data values; when false,
        results are requested to be accurate
        :return:
        """
        index_results = self._conn.jconn.getMetaData().getIndexInfo(
            catalog, schema, table_name, unique, approximate
        )

        def index_info_row_factory(row):
            return self._t.metadata_index_info_row_transformer(
                MetadataIndexInfoRow(*row)
            )

        return self._process_result(index_results, index_info_row_factory)

    def get_version_columns(
        self, catalog: str = None, schema: str = None, table_name="%"
    ) -> Generator[MetadataVersionColumnRow, MetadataVersionColumnRow, None]:
        """
        Gets metadata about version columns in a table in the database.

        This metadata is obtained by calling getVersionColumns() JDBC method.

        :param catalog: optionally specify name of catalog from which to get tables; defaults to all catalogs
        :param schema: optionally specify name of schema from which to get tables; defaults to all schemas
        :param table_name: table or expression
        :return:
        """
        vc_results = self._conn.jconn.getMetaData().getVersionColumns(
            catalog, schema, table_name
        )

        def vc_row_factory(row):
            return self._t.metadata_version_column_row_transformer(
                MetadataVersionColumnRow(*row)
            )

        return self._process_result(vc_results, vc_row_factory)

    def get_catalogs(self) -> Generator[MetadataCatalogRow, MetadataCatalogRow, None]:
        """
        Gets metadata about version columns in a table in the database.

        This metadata is obtained by calling getCatalogs() JDBC method.

        :return:
        """
        catalog_results = self._conn.jconn.getMetaData().getCatalogs()

        def catalog_row_factory(row):
            return self._t.metadata_catalog_row_transformer(MetadataCatalogRow(*row))

        return self._process_result(catalog_results, catalog_row_factory)

    def get_schemas(
        self, catalog: str = None, schema_pattern: str = None
    ) -> Generator[MetadataSchemaRow, MetadataSchemaRow, None]:
        """
        Gets metadata about version columns in a table in the database.

        This metadata is obtained by calling getCatalogs() JDBC method.

        :param catalog: optionally specify catalog name; must match the catalog name as it is stored in the database;
        "" retrieves those without a catalog; None means catalog name should not be used to narrow down the search.
        :param schema_pattern: a schema name; must match the schema name as it is stored in the database;
        None means schema name should not be used to narrow down the search.
        :return:
        """
        if catalog is not None or schema_pattern is not None:
            schema_results = self._conn.jconn.getMetaData().getSchemas(
                catalog, schema_pattern
            )
        else:
            schema_results = self._conn.jconn.getMetaData().getSchemas()

        def schema_row_factory(row):
            return self._t.metadata_schema_row_transformer(MetadataSchemaRow(*row))

        return self._process_result(schema_results, schema_row_factory)

    @property
    def constants(self):
        """
        Gets values of all constants defined on the JDBC DatabaseMetaData object. These are essentially used as
        enumerators in other metadata call results.

        See JDBC Documentation: https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/DatabaseMetaData.html.

        :return: MetadataConstants
        :rtype: MetadataConstants
        """
        if self._constants is None:
            self._constants = MetadataConstants(
                self._conn.jconn.getMetaData().attributeNoNulls,
                self._conn.jconn.getMetaData().attributeNullable,
                self._conn.jconn.getMetaData().attributeNullableUnknown,
                self._conn.jconn.getMetaData().bestRowNotPseudo,
                self._conn.jconn.getMetaData().bestRowPseudo,
                self._conn.jconn.getMetaData().bestRowSession,
                self._conn.jconn.getMetaData().bestRowTemporary,
                self._conn.jconn.getMetaData().bestRowTransaction,
                self._conn.jconn.getMetaData().bestRowUnknown,
                self._conn.jconn.getMetaData().columnNoNulls,
                self._conn.jconn.getMetaData().columnNullable,
                self._conn.jconn.getMetaData().columnNullableUnknown,
                self._conn.jconn.getMetaData().functionColumnIn,
                self._conn.jconn.getMetaData().functionColumnInOut,
                self._conn.jconn.getMetaData().functionColumnOut,
                self._conn.jconn.getMetaData().functionColumnResult,
                self._conn.jconn.getMetaData().functionColumnUnknown,
                self._conn.jconn.getMetaData().functionNoNulls,
                self._conn.jconn.getMetaData().functionNoTable,
                self._conn.jconn.getMetaData().functionNullable,
                self._conn.jconn.getMetaData().functionNullableUnknown,
                self._conn.jconn.getMetaData().functionResultUnknown,
                self._conn.jconn.getMetaData().functionReturn,
                self._conn.jconn.getMetaData().functionReturnsTable,
                self._conn.jconn.getMetaData().importedKeyCascade,
                self._conn.jconn.getMetaData().importedKeyInitiallyDeferred,
                self._conn.jconn.getMetaData().importedKeyInitiallyImmediate,
                self._conn.jconn.getMetaData().importedKeyNoAction,
                self._conn.jconn.getMetaData().importedKeyNotDeferrable,
                self._conn.jconn.getMetaData().importedKeyRestrict,
                self._conn.jconn.getMetaData().importedKeySetDefault,
                self._conn.jconn.getMetaData().importedKeySetNull,
                self._conn.jconn.getMetaData().procedureColumnIn,
                self._conn.jconn.getMetaData().procedureColumnInOut,
                self._conn.jconn.getMetaData().procedureColumnOut,
                self._conn.jconn.getMetaData().procedureColumnResult,
                self._conn.jconn.getMetaData().procedureColumnReturn,
                self._conn.jconn.getMetaData().procedureColumnUnknown,
                self._conn.jconn.getMetaData().procedureNoNulls,
                self._conn.jconn.getMetaData().procedureNoResult,
                self._conn.jconn.getMetaData().procedureNullable,
                self._conn.jconn.getMetaData().procedureNullableUnknown,
                self._conn.jconn.getMetaData().procedureResultUnknown,
                self._conn.jconn.getMetaData().procedureReturnsResult,
                self._conn.jconn.getMetaData().sqlStateSQL,
                self._conn.jconn.getMetaData().sqlStateSQL99,
                self._conn.jconn.getMetaData().sqlStateXOpen,
                self._conn.jconn.getMetaData().tableIndexClustered,
                self._conn.jconn.getMetaData().tableIndexHashed,
                self._conn.jconn.getMetaData().tableIndexOther,
                self._conn.jconn.getMetaData().tableIndexStatistic,
                self._conn.jconn.getMetaData().typeNoNulls,
                self._conn.jconn.getMetaData().typeNullable,
                self._conn.jconn.getMetaData().typeNullableUnknown,
                self._conn.jconn.getMetaData().typePredBasic,
                self._conn.jconn.getMetaData().typePredChar,
                self._conn.jconn.getMetaData().typePredNone,
                self._conn.jconn.getMetaData().typeSearchable,
                self._conn.jconn.getMetaData().versionColumnNotPseudo,
                self._conn.jconn.getMetaData().versionColumnPseudo,
                self._conn.jconn.getMetaData().versionColumnUnknown,
            )

        return self._constants

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()
