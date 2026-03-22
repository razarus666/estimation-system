"""
DB互換レイヤー: SQLite / PostgreSQL 自動切り替え
DATABASE_URL環境変数が設定されていればPostgreSQL、なければSQLiteを使用する。
app.pyやmatching_engine.pyの既存コード(sqlite3スタイル)を変更不要にする。
"""
import os
import re
import sqlite3

DATABASE_URL = os.environ.get('DATABASE_URL', '')
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras


# --- SQLite datetime -> PG互換 変換マップ ---------------------
_SQLITE_DT_PATTERNS = [
    (re.compile(r"datetime\('now'\s*,\s*'localtime'\)", re.IGNORECASE), "NOW()"),
    (re.compile(r"datetime\('now'\s*,\s*'-(\d+)\s+days?'\)", re.IGNORECASE),
     lambda m: f"NOW() - INTERVAL '{m.group(1)} days'"),
    (re.compile(r"datetime\('now'\)", re.IGNORECASE), "NOW()"),
    (re.compile(r"datetime\(([^,)]+)\s*,\s*'([^']+)'\)", re.IGNORECASE),
     lambda m: f"({m.group(1)})::timestamp"),  # fallback
]


def _convert_sql_for_pg(sql):
    """SQLite形式のSQLをPostgreSQL互換に変換"""
    converted = re.sub(r'(?<!\?)\?(?!\?)', '%s', sql)

    for pattern, replacement in _SQLITE_DT_PATTERNS:
        if callable(replacement):
            converted = pattern.sub(replacement, converted)
        else:
            converted = pattern.sub(replacement, converted)

    converted = re.sub(
        r'INSERT\s+OR\s+IGNORE\s+INTO',
        'INSERT INTO',
        converted,
        flags=re.IGNORECASE
    )
    if 'INSERT OR IGNORE' in sql.upper().replace(' ', ''):
        if 'ON CONFLICT' not in converted.upper():
            converted = converted.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

    converted = re.sub(
        r'INSERT\s+OR\s+REPLACE\s+INTO',
        'INSERT INTO',
        converted,
        flags=re.IGNORECASE
    )

    converted = re.sub(r'\bAUTOINCREMENT\b', '', converted, flags=re.IGNORECASE)

    if (re.search(r'CREATE\s+TABLE', converted, re.IGNORECASE)):
        converted = re.sub(
            r'(\w+)\s+INTEGER\s+PRIMARY\s+KEY\s*',
            r'\1 SERIAL PRIMARY KEY ',
            converted,
            flags=re.IGNORECASE
        )
        converted = re.sub(
            r"DEFAULT\s*\(\s*datetime\([^)]*\)\s*\)",
            "DEFAULT NOW()",
            converted,
            flags=re.IGNORECASE
        )

    return converted

class PgCursorWrapper:
    """psycopg2カーソルをsqlite3.Cursor互換にするラッパー"""

    def __init__(self, pg_cursor):
        self._cursor = pg_cursor
        self.lastrowid = None
        self.description = None
        self.rowcount = -1

    def execute(self, sql, params=None):
        converted = _convert_sql_for_pg(sql)
        is_insert = converted.strip().upper().startswith('INSERT')
        needs_returning = is_insert and 'RETURNING' not in converted.upper()
        if needs_returning:
            converted = converted.rstrip().rstrip(';') + ' RETURNING id'
        if params:
            self._cursor.execute(converted, params)
        else:
            self._cursor.execute(converted)
        self.description = self._cursor.description
        self.rowcount = self._cursor.rowcount
        if needs_returning:
            try:
                row = self._cursor.fetchone()
                if row:
                    self.lastrowid = row['id'] if isinstance(row, dict) else row[0]
            except Exception:
                pass
        return self

    def executemany(self, sql, params_list):
        converted = _convert_sql_for_pg(sql)
        for params in params_list:
            self._cursor.execute(converted, params)
        self.rowcount = self._cursor.rowcount
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return PgRowWrapper(row)

    def fetchall(self):
        rows = self._cursor.fetchall()
        return [PgRowWrapper(r) for r in rows]

    def close(self):
        self._cursor.close()

    def __iter__(self):
        return self

    def __next__(self):
        row = self._cursor.fetchone()
        if row is None:
            raise StopIteration
        return PgRowWrapper(row)

class PgRowWrapper:
    """psycopg2のDictRowをsqlite3.Row互換にするラッパー"""

    def __init__(self, dict_row):
        if isinstance(dict_row, dict):
            self._dict = dict_row
            self._keys = list(dict_row.keys())
        else:
            self._dict = dict(dict_row)
            self._keys = list(dict_row.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._dict[self._keys[key]]
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict.values())

    def keys(self):
        return self._keys

    def __repr__(self):
        return f"PgRowWrapper({self._dict})"


class PgConnectionWrapper:
    """psycopg2接続をsqlite3.Connection互換にするラッパー"""

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def cursor(self):
        pg_cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return PgCursorWrapper(pg_cursor)

    def execute(self, sql, params=None):
        cursor = self.cursor()
        cursor.execute(sql, params)
        return cursor

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def row_factory(self):
        return None

    @row_factory.setter
    def row_factory(self, value):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.close()
        return False


def get_connection():
    """DB接続を返す。DATABASE_URLがあればPG、なければSQLite"""
    if USE_PG:
        url = DATABASE_URL
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        pg_conn = psycopg2.connect(url)
        pg_conn.autocommit = False
        return PgConnectionWrapper(pg_conn)
    else:
        db_path = os.environ.get('DB_PATH', 'data/estimation.db')
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn


def get_db_type():
    """現在のDB種別を返す"""
    return 'postgresql' if USE_PG else 'sqlite'
