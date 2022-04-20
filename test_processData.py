from cmath import nan
from numpy import extract
from mock import patch
import pytest
import extract_data
import pandas
import math
import os
import sqlite3
import sys
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import extract_data
import PreProcessDescriptions
import CombineDescriptions
import GetFeatures
from GetConnection import getConnection
import GetSubclassFeatures
import GetClassFeatures
import GetCategoryFeatures
import GetSegmentFeatures
import Accuracy_ML_v3

def test_createAndPopulateTables():
    conn = getConnection("test2.db", isDeleteIfExists=True)
    extract_data.main("Capstone_UAT_Short.csv", "test2.db")
    cursor = conn.cursor()
    # verify that extract_data has created tables for population of the products
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='segments'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "segments"

    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='categories'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "categories"

    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='classes'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "classes"

    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='subclasses'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "subclasses"

    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "products"

    # assert that CombinedDescription column is NULL before running PreProcessDescriptions.py
    stmt = "SELECT CombinedDescription FROM products"
    cursor.execute(stmt)
    result = cursor.fetchall()
    for value in result:
        assert value == (None,)

    CombineDescriptions.main("test2.db")
    stmt = "SELECT CombinedDescription FROM products"
    cursor.execute(stmt)
    result = cursor.fetchall()
    for value in result:
        assert value != (None,)

    PreProcessDescriptions.main("test2.db")
    GetFeatures.main("test2.db")
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "features"
    GetSubclassFeatures.main("test2.db")
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='subclass_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "subclass_features"
    GetClassFeatures.main("test2.db")
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='class_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "class_features"
    GetCategoryFeatures.main("test2.db")
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='category_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "category_features"
    GetSegmentFeatures.main("test2.db")
    stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name='segment_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    print(result)
    assert result[0] == "segment_features"

    Accuracy_ML_v3.main("test2.db", "testResults.csv")