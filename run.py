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
    # take in arguments in command line for datafile name, name of database to be created and name of file to write to 
    dataFile = sys.argv[1]
    database = sys.argv[2]
    resultFile = sys.argv[3]

    # runs other files in order by passing in required inputs
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