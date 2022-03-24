import sqlite3
import random
import csv

# connect to sql server
databaseName = "test2.db"
conn = sqlite3.connect(databaseName)
cursor = conn.cursor()

#used for interacting with database
def read_query(conn, query):
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except:
        conn.close()
        print("Unable to cexecute query.")

#used to compare features of a product to features of the bins its in
#currently set to return random value, will implement later
def compare_features(product_features, bin_features):
    return random.uniform(0.0,1.0)

#create empty arrays to keep track of sku, features, subclass, class, category, and segment
#of each product in the database
skus = []
features = []
subclasses = []
classes = []
categories = []
segments = []

#get product features
query = "SELECT SKU from features"
skus = read_query(conn, query)

query = "SELECT features from features"
features = read_query(conn, query)

#put features in dict for easy lookup
product_dic = dict(zip(skus, features))

#get subclass features
query = "SELECT subclass from products"
subclasses = read_query(conn, query)

query = "SELECT SUB_ID from subclass_features"
sub_ids = read_query(conn, query)

query = "SELECT SUB_Features from subclass_features"
sub_features = read_query(conn, query)

#put features in dict for easy lookup
subclass_dic = dict(zip(sub_ids, sub_features))

#get class features
query = "SELECT class from products"
classes = read_query(conn, query)

query = "SELECT CLS_ID from class_features"
cls_ids = read_query(conn, query)

query = "SELECT CLS_Features from class_features"
cls_features = read_query(conn, query)

#put features in dict for easy lookup
class_dic = dict(zip(cls_ids, cls_features))

#get category features
query = "SELECT category from products"
categories = read_query(conn, query)

query = "SELECT CAT_ID from category_features"
cat_ids = read_query(conn, query)

query = "SELECT CAT_Features from category_features"
cat_features = read_query(conn, query)

category_dic = dict(zip(cat_ids, cat_features))

#get segment features
query = "SELECT segment from products"
segments = read_query(conn, query)

query = "SELECT SEG_ID from segment_features"
seg_ids = read_query(conn, query)

query = "SELECT SEG_Features from segment_features"
seg_features = read_query(conn, query)

#put features in dict for easy lookup
segment_dic = dict(zip(seg_ids, seg_features))

#get manufacturer info
query = "SELECT Manufacturer from products"
manufacturers = read_query(conn, query)

query = "SELECT MfrPartNum from products"
mfrPartNums = read_query(conn, query)

#put features in dict for easy lookup
mfr_dic = dict(zip(manufacturers, mfrPartNums))

# open the file in the write mode
f = open('ClassificationAccuracy.csv', 'w', encoding='UTF8', newline='')

# create the csv writer
writer = csv.writer(f)

# write a row to the csv file
header = ['SKU','Manufacturer','MfrPartNum','Segment_Score','Category_Score','Class_Score','Sub_Class_Score']
writer.writerow(header)


#iterate through each product in the table
for i in range(len(skus)):
    #get the segment, category, class, subclass
    #compare to features
    data_seg_features = []
    data_cat_features = []
    data_cls_features = []
    data_sub_features = []
    data_seg_score = compare_features(features[i], data_seg_features)
    data_cat_score = compare_features(features[i], data_cat_features)
    data_cls_score = compare_features(features[i], data_cls_features)
    data_sub_score = compare_features(features[i], data_sub_features)

    segment_scores = compare_features(features[i], seg_features, seg_ids)
    segment_scores.sort()

    category_features = []
    category_ids = []
    for i in range(4):
        seg = segment_scores[i][1]
        #query for all the categories that belong to this segment
        #add the features of the category to the list and the ids

    category_scores = compare_features(features[i], category_features, category_ids)
    category_scores.sort()

    class_features = []
    class_ids = []
    
    for i in range(20):
        cat = category_scores[i][1]
        #query for all classes that belong to this category
        # add the features of the classes to the list and the ids
    
    class_scores = compare_features(features[i], class_features, class_ids)
    class_scores.sort()

    subclass_features = []
    subclass_ids = []
    for i in range(50):
        clas = class_scores[i][1]
        #query for all classes that belong to this category
        # add the features of the subclasses to the list and the ids

    subclass_scores = compare_features(features[i], subclass_features, subclass_ids)
    subclass_scores.sort()

    #output to csv file   
    
    segment_score = data_seg_score / segment_scores[0][0]
    category_score = data_cat_score / category_scores[0][0]
    class_score = data_cls_score / class_scores[0][0]
    subclass_score = data_sub_score / subclass_scores[0][0]
       
    row = [skus[i][0], manufacturers[i][0], mfrPartNums[i][0], segment_score, category_score, class_score, subclass_score]
    writer.writerow(row)
   

# close the file
f.close()
