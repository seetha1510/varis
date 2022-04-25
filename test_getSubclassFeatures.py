import pytest
import sqlite3
import os
import pandas

import GetSubclassFeatures

def test_checkTableExists_subclassFeatures():
    conn = GetSubclassFeatures.getConnection("test2.db")
    GetSubclassFeatures.createSubclassFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' and name='subclass_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "subclass_features"
    