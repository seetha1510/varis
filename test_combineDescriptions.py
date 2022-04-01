import pytest
import sqlite3
import pandas
import os

import CombineDescriptions

def test_getConnection():
    CombineDescriptions.getConnection("test1")
    assert os.path.exists("test1")
    if os.path.exists("test1"):
        os.remove("test1")

# check if CombinedDescription column exists in products table
def test_combineDescriptions():
    conn = CombineDescriptions.getConnection("test2.db")
    # CombineDescriptions.combineDescriptions(conn)
    cursor = conn.cursor()
    stmt = "SELECT EXISTS(SELECT CombinedDescription FROM products)"
    cursor.execute(stmt)
    res = cursor.fetchone()
    assert res[0] == 1