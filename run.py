#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 20:31:02 2022

@author: cassidyfrier
"""

import sys
import extract_data
import PreProcessDescriptions
import CombineDescriptions
import GetFeatures
import GetSubclassFeatures
import GetClassFeatures
import GetCategoryFeatures
import GetSegmentFeatures
import Accuracy_ML_v3

def main():
    dataFile = sys.argv[1]
    database = sys.argv[2]
    resultFile = sys.argv[3]
    extract_data.main(dataFile, database)
    CombineDescriptions.main(database)
    PreProcessDescriptions.main(database)
    GetFeatures.main(database)
    GetSubclassFeatures.main(database)
    GetClassFeatures.main(database)
    GetCategoryFeatures.main(database)
    GetSegmentFeatures.main(database)
    Accuracy_ML_v3.main(database, resultFile)
    
if __name__ == "__main__":
    main()