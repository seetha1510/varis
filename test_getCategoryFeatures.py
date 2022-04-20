import pytest
import sqlite3
import os
import pandas

import GetCategoryFeatures

def test_checkTableExists_categoryFeatures():
    conn = GetCategoryFeatures.getConnection("test2.db")
    GetCategoryFeatures.createCategoryFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' and name='category_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "category_features"

