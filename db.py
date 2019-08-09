from psycopg2 import connect
from psycopg2.extensions import parse_dsn
from psycopg2.extras import DictCursor


class ReadOnlySQLDataBase:
    def __init__(self, *, connection_string, readonly=True, autocommit=False):
        self.connection = connect(**parse_dsn(connection_string), cursor_factory=DictCursor)
        self.connection.autocommit = autocommit
        self.connection.readonly = readonly
        self.cursor = self.connection.cursor()

    def sql(self, query):
        self.cursor.execute(query)

        if self.cursor.description:
            return self.cursor.fetchall()

    return None
