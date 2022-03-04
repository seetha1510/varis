# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 15:58:48 2022

@author: Owner
"""

# import libraries
import os
from operator import index
import numpy as np
import pandas as pd
import sqlite3

# connect to sql server
# conn = sqlite3.connect(":memory:")
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()