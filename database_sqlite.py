#!/usr/bin/env python
# coding=utf-8
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around SQLite3."""

import sqlite3
import copy
import itertools
import logging
import time

class Connection(object):

    """A lightweight wrapper around Sqlite3 connections.
    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage:

        db = database_sqlite3.ConnectionSqlite3("/path/to/test.db")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the sqlite3 API.

    We explicitly set the timezone to UTC and the character encoding to
    UTF-8 on all connections to avoid time zone and encoding errors.
    """


    def __init__(self, database=":memory:", host="localhost", user=None, password=None, max_idle_time=7*3600):
        self.host = host
        self.database = database
        self.max_idle_time = max_idle_time

        args = dict(db=database)
        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except:
            logging.error("Cannot connect to Sqlite3 on %s", self.host, exc_info=True)

    def __del__(self):
        self.close()

    def cursor(self):
        return self._cursor

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = sqlite3.connect(self.database)
        self.isolation_level = None

    def iter(self, query, *parameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            pass  # cursor.close()

    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def _ensure_connected(self):
        # if  coonection has been idle for too long (7 hours by default).
        # pre-emptive
        if (self._db is None or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters):
        try:
            cursor.execute(query, parameters)
            self._db.commit()
            return
        except OperationalError:
            logging.error("Error connecting to SQLite3 on %s", self.host)
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

# Alias some common Sqlite3 exceptions
IntegrityError = sqlite3.IntegrityError
OperationalError = sqlite3.OperationalError

if __name__ == '__main__':

    from tornado import database

    con = Connection("bar.db")
    insert = """
CREATE TABLE userdeatail (
    uid INT(10) NULL,
    intro TEXT NULL,
    profile TEXT NULL,
    PRIMARY KEY (uid)
);
    """

    
    #con.execute("drop table userdeatail")
   # con.execute(insert)
    #con.execute("delete from userdeatail")


    con = database.Connection("localhost", "test")
    con.executemany("insert into userdetail(profile, intro) values(%s,%s)", [["a'a", 'bb'], ("cc", "测试")])
    #con.executemany("insert into userdeatail(profile, intro) values(?,?)", [["a'a", 'bb'], ("cc", u"测试")])

    rows = con.query("select * from userdetail")
    for r in rows:
        print r["intro"]
