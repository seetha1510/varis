from cmath import nan
from numpy import extract
import pytest
from sklearn.utils import resample
from mock import patch

import extract_data
import pandas
import math
import os
import sqlite3
import sys

from GetConnection import getConnection

def test_importDataSet():
    # check if a invalid file is inputted when importing data set
    fake_args = ["test", "badName.txt", "integrationTest.db"]
    with patch('sys.argv', fake_args):
        with pytest.raises(FileNotFoundError):
            extract_data.importDataSet(sys.argv[1])


def test_importDataSet_checkCorrectType():
    # check for correct type in data --> dataframe
    fake_args = ["test", "testData.csv", "integrationTest.db"]
    with patch('sys.argv', fake_args):
        data = extract_data.importDataSet(sys.argv[1])
        assert type(data) == pandas.core.frame.DataFrame
    # print(type(data))

def test_importDataSet_sampleDataSet():
    fake_args = ["test", "testData.csv", "integrationTest.db"]
    with patch('sys.argv', fake_args):
        data = extract_data.importDataSet(sys.argv[1])
        expected = [0, 11111, 'UNO', 'Card game', '<ul><li>Fun party game</li></ul>', 'Card game 2', 'Manufacturer', 123543, 'EA', 10.99, None,'SEG_1', 'Seg name', 'CAT_1', 'Cat name', 'CLS_1', 'Class name', 'SUB_1', 'Subclass name']
        for row in data.itertuples():
            # print(row)
            # assert expected == (row[0:10] + row[10:])
            for i, col in enumerate(row):
                if expected[i] == None:
                    assert math.isnan(col)
                    continue
                assert expected[i] == col

def test_segmentAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    extract_data.createTables(conn)
    expectedMsg = "Segment table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    assert expectedMsg not in captured.out
    
    extract_data.createTables(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out

def test_categoryAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    extract_data.createTables(conn)
    expectedMsg = "Category table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    assert expectedMsg not in captured.out
    
    extract_data.createTables(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out


def test_classAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    extract_data.createTables(conn)
    expectedMsg = "Class table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    assert expectedMsg not in captured.out
    
    extract_data.createTables(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out

def test_subclassAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    extract_data.createTables(conn)
    expectedMsg = "Subclass table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    assert expectedMsg not in captured.out
    
    extract_data.createTables(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out

def test_subclassAlreadyExists(capsys):
    db = "test2.db"
    conn = getConnection(db, True)
    extract_data.createTables(conn)
    expectedMsg = "Product table already exists, deleting and re-creating"
    captured = capsys.readouterr()
    assert expectedMsg not in captured.out
    
    extract_data.createTables(conn)
    captured = capsys.readouterr()
    assert expectedMsg in captured.out


