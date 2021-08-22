# (C) 2021 GoodData Corporation
from schema_analyzer.connector import DbConnector


class MariaDbConnector(DbConnector):
    def __init__(
        self, connection_string=None, driver_path=None, user=None, password=None
    ):
        super(MariaDbConnector, self).__init__(
            classname="org.mariadb.jdbc.Driver",
            connection_string=connection_string,
            driver_path=driver_path,
            user=user,
            password=password,
        )
