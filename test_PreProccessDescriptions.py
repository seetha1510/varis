import numpy as np
import pandas as pd
import sqlite3
import re
import nltk
import string
import traceback
from bs4 import BeautifulSoup
import time
import pytest
import os

import PreProccessDescriptions

def test_getConnection(): #Checks the module can make a connection to the database
    PreProccessDescriptions.getConnection("test1")
    assert os.path.exists("test1")
    if os.path.exists("test1"):
        os.remove("test1")
        