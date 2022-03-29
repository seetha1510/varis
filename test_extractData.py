from cmath import nan
from numpy import extract
import pytest
import extract_data
import pandas
import math
import os
import sqlite3

def test_importDataSet():
    # check if a invalid file is inputted when importing data set
    with pytest.raises(FileNotFoundError):
        extract_data.importDataSet("badName.txt")


def test_importDataSet_checkCorrectType():
    # check for correct type in data --> dataframe
    data = extract_data.importDataSet("testData.csv")
    assert type(data) == pandas.core.frame.DataFrame
    # print(type(data))

def test_importDataSet_sampleDataSet():
    data = extract_data.importDataSet("testData.csv")
    expected = [0, 11111, 'UNO', 'Card game', '<ul><li>Fun party game</li></ul>', 'Card game 2', 'Manufacturer', 123543, 'EA', 10.99, None,'SEG_1', 'Seg name', 'CAT_1', 'Cat name', 'CLS_1', 'Class name', 'SUB_1', 'Subclass name']
    for row in data.itertuples():
        # print(row)
        # assert expected == (row[0:10] + row[10:])
        for i, col in enumerate(row):
            if expected[i] == None:
                assert math.isnan(col)
                continue
            assert expected[i] == col

def test_getConnection():
    extract_data.getConnection("test1")
    assert os.path.exists("test1")
    if os.path.exists("test1"):
        os.remove("test1")

def test_getConnection_exists():
    conn = extract_data.getConnection("test1")
    c = conn.cursor()
    c.execute('''CREATE TABLE testTable(
        name text
        )
        ''')
    conn.commit()
    conn = extract_data.getConnection("test1")
    c = conn.cursor()
    with pytest.raises(sqlite3.OperationalError) as e:
        c.execute('''SELECT * from testTable''')
        # print(str(e))
    

    


