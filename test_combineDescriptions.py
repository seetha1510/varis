import pytest
import sqlite3
import pandas
import os

import CombineDescriptions
import extract_data

# check if CombinedDescription column exists in products table
def test_combineDescriptions():
    extract_data.main("Capstone_UAT_Short.csv", "test2.db")
    conn = extract_data.getConnection("test2.db", False)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "products"
    nullCount = 386
    stmt = "select count(*) from products where CombinedDescription is NULL"
    cursor.execute(stmt)
    result = cursor.fetchone()
    # print(result)
    assert result[0] == nullCount
    CombineDescriptions.combineDescriptions(conn)
    stmt = "select count(*) from products where CombinedDescription is NULL"
    cursor.execute(stmt)
    result = cursor.fetchone()
    # print(result)
    assert result[0] == 0