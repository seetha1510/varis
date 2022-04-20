import sqlite3
import os
import pytest
import sys

from mock import patch
from GetConnection import getConnection

def test_getConnection():
    dbName = "getConnectionTest.db"
    getConnection(dbName, isDeleteIfExists=False)
    assert os.stat(dbName).st_size == 0
    with open(dbName, "w") as f:
        f.write(" " * 100)
    getConnection(dbName, isDeleteIfExists=False)
    assert os.stat(dbName).st_size > 0
    getConnection(dbName, isDeleteIfExists=True)
    assert os.stat(dbName).st_size == 0

def test_getConnection_exists():
    conn = getConnection("test1", True)
    c = conn.cursor()
    c.execute('''CREATE TABLE testTable(
        name text
        )
        ''')
    conn.commit()
    conn = getConnection("test1", True)
    c = conn.cursor()
    with pytest.raises(sqlite3.OperationalError) as e:
        c.execute('''SELECT * from testTable''')
        # print(str(e))