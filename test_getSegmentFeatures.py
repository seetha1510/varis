import pytest
import sqlite3
import os
import pandas

import GetSegmentFeatures

def test_checkTableExists_segmentFeatures():
    conn = GetSegmentFeatures.getConnection("test2.db")
    GetSegmentFeatures.createSegmentFeaturesTable(conn)
    cursor = conn.cursor()
    stmt = "SELECT name FROM sqlite_master WHERE type='table' and name='segment_features'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    assert result[0] == "segment_features"