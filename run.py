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

extract_data.main(sys.argv[1], sys.argv[2])
PreProcessDescriptions.main(sys.argv[2])
CombineDescriptions.main(sys.argv[2])
GetFeatures.main(sys.argv[2])
GetSubclassFeatures.main(sys.argv[2])
GetClassFeatures.main(sys.argv[2])
GetCategoryFeatures.main(sys.argv[2])
GetSegmentFeatures.main(sys.argv[2])
Accuracy_ML_v3.main(sys.argv[2], sys.argv[3])