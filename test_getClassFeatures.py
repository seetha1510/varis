import pytest
import sqlite3
import os
import pandas

import GetClassFeatures

def test_checkTableExists_classFeatures():
    conn = GetClassFeatures.getConnection("test2.db")
    GetClassFeatures.createClassFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' and name='class_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "class_features"

