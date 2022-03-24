import sqlite3
import random
import csv
import pandas as pd
from difflib import SequenceMatcher
import time

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
def compare_features(a_features, b_features):
    #return random.uniform(0.0,1.0)
    s = SequenceMatcher(lambda x: x == " ", a_features[0], b_features)
    #print(a_features[0])
    #print (s.ratio())
    return s.ratio()
    #return round(s.ratio(),4)

def compare_bin_level(product_features, bin_features, bin_id):
    scores = []
    for i in range(len(bin_features)):
        #print(product_features)
        s = compare_features(product_features, bin_features[i])
        #print(str(s) + " " + str(bin_id[i]))
        scores.append((s, bin_id[i]))
    return scores

def compare_seg_level(product_features, bin_features, bin_id):
    scores = []
    for i in range(len(bin_features)):
        s = compare_features(product_features, bin_features[i][0])
        #print(str(s) + " " + str(bin_id[i]))
        scores.append((s, bin_id[i][0]))
    return scores

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
#print(seg_ids)

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

categories_features_lookup = pd.read_sql_query("SELECT categories.CAT_ID, categories.SEG_ID, category_features.CAT_Features FROM categories INNER JOIN category_features USING(CAT_ID)", conn)

classes_features_lookup = pd.read_sql_query("SELECT classes.CLS_ID, classes.CAT_ID, class_features.CLS_Features FROM classes INNER JOIN class_features USING(CLS_ID)", conn)

subclasses_features_lookup = pd.read_sql_query("SELECT subclasses.SUB_ID, subclasses.CLS_ID, subclass_features.SUB_Features FROM subclasses INNER JOIN subclass_features USING(SUB_ID)", conn)


# open the file in the write mode
f = open('ClassificationAccuracy.csv', 'w', encoding='UTF8', newline='')

# create the csv writer
writer = csv.writer(f)

# write a row to the csv file
header = ['SKU','Manufacturer','MfrPartNum','Segment_Score','Category_Score','Class_Score','Sub_Class_Score']
writer.writerow(header)


#iterate through each product in the table
for i in range(len(skus)):
    if i == 50:
        break
    start_time = time.time()
    #get the segment, category, class, subclass features
    #compare to features
    data_seg_features = segment_dic.get(segments[i],[])
    data_cat_features = category_dic.get(categories[i],[])
    data_cls_features = class_dic.get(classes[i],[])
    data_sub_features = subclass_dic.get(subclasses[i],[])
    data_seg_score = compare_features(features[i], data_seg_features[0])
    data_cat_score = compare_features(features[i], data_cat_features[0])
    data_cls_score = compare_features(features[i], data_cls_features[0])
    data_sub_score = compare_features(features[i], data_sub_features[0])

    segment_scores = compare_seg_level(features[i], seg_features, seg_ids)
    #print(segment_scores)
    segment_scores.sort()

    category_features = []
    category_ids = []

    for j in range(4):
        seg = segment_scores[j][1]
        #print(seg)
        cat_temp = categories_features_lookup.loc[categories_features_lookup['SEG_ID'] == seg]
        #print(cat_temp)
        category_features.extend(cat_temp['CAT_Features'])
        category_ids.extend(cat_temp['CAT_ID'])
        #query for all the categories that belong to this segment
        #add the features of the category to the list and the ids

    category_scores = compare_bin_level(features[i], category_features, category_ids)
    category_scores.sort()
    
    #print(category_scores)
    
    class_features = []
    class_ids = []
    
    for j in range(4):
        cat = category_scores[j][1]
        cls_temp = classes_features_lookup.loc[classes_features_lookup['CAT_ID'] == cat]
        #cls_temp = classes_features_lookup.query(q)
        class_features.extend(cls_temp['CLS_Features'])
        class_ids.extend(cls_temp['CLS_ID'])
        #query for all classes that belong to this category
        # add the features of the classes to the list and the ids
    
    class_scores = compare_bin_level(features[i], class_features, class_ids)
    class_scores.sort()

    subclass_features = []
    subclass_ids = []
    for j in range(4):
        clas = class_scores[j][1]
        sub_temp = subclasses_features_lookup.loc[subclasses_features_lookup['CLS_ID'] == clas]
        #sub_temp = subclasses_features_lookup.query(q)
        subclass_features.extend(sub_temp['SUB_Features'])
        subclass_ids.extend(sub_temp['SUB_ID'])
        #query for all classes that belong to this category
        # add the features of the subclasses to the list and the ids

    subclass_scores = compare_bin_level(features[i], subclass_features, subclass_ids)
    subclass_scores.sort()

    #output to csv file   
    #print(segment_scores)
    ######################################
    ##### TO DO: add logic that shortcuts the rest of the evaluations to zero if misclassification at higher level is found (i.e. if misclassified at category, then class and subclass should be 0)
    ######################################
    segment_score = data_seg_score / segment_scores[len(segment_scores)-1][0]
    category_score = (data_cat_score / category_scores[len(category_scores)-1][0])
    class_score = (data_cls_score / class_scores[len(class_scores)-1][0])
    subclass_score = (data_sub_score / subclass_scores[len(subclass_scores)-1][0])
       
    row = [skus[i][0], manufacturers[i][0], mfrPartNums[i][0], segment_score, category_score, class_score, subclass_score]
    #print(category_score, data_cat_score, category_scores[len(category_scores)-1][0])
    end_time = time.time()
    print("---" + str(i) + ": " + str(end_time-start_time) + " seconds ---")
    writer.writerow(row)
   

# close the file
f.close()
