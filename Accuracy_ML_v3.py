import copy
import sqlite3
import random
import csv
import sys
import pandas as pd
from difflib import SequenceMatcher
import time
import string
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import random
    
#load sentence trasnformer model for comparing features
model = SentenceTransformer('bert-base-nli-mean-tokens')

print("Default model loaded")


#used for interacting with database
#reads given query and returns result from database
def read_query(conn, cursor, query):
    
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except:
        conn.close()
        print("Unable to execute query.")

#compares two sets of feature strings using cosine similarity
def compare_features(a_features, b_features):
    #add bin features and product features to sentences array
    sentences = copy.deepcopy(b_features)
    sentences.append(a_features)
    #encode and shape sentences for use with cosine similarity
    sentence_embeddings = model.encode(sentences)
    sentence_embeddings.shape
    #returns similarity score of the product compared to the bin
    return cosine_similarity([sentence_embeddings[-1]], sentence_embeddings[:-1])

#given one set of product features and all of the bin features/ids,
#returns similarity scores for the product compared with each bin
def compare_bin_level(product_features, bin_features, bin_id):
    #compare features
    scores = compare_features(product_features, bin_features)
    scores_id = []
    #store scores with their corresponding bin ids
    for i in range(len(scores[0])):
        scores_id.append((scores[0][i], bin_id[i]))

    return scores_id

def main(dbName, fileName):

    # connect to sql server
    databaseName = dbName
    conn = sqlite3.connect(databaseName)
    cursor = conn.cursor()

    
    #create empty arrays to keep track of sku, features, subclass, class, category, and segment
    #of each product in the database
    skus = []
    features = []
    subclasses = []
    classes = []
    categories = []
    segments = []
    

    #get product SKUs
    query = "SELECT SKU from features"
    skus = read_query(conn, cursor, query)

    #get features of each product
    query = "SELECT features from features"
    features = read_query(conn, cursor, query)
    for i in range(len(features)):
        features[i] = features[i][0]
    
    #put skus and features in dict for easy lookup
    product_dic = dict(zip(skus, features))
    
    #get subclass ids for each product
    query = "SELECT subclass from products"
    subclasses = read_query(conn, cursor, query)
    
    #get all subclass ids
    query = "SELECT SUB_ID from subclass_features"
    sub_ids = read_query(conn, cursor, query)
    
    #get all subclass products
    query = "SELECT SUB_Features from subclass_features"
    sub_features = read_query(conn, cursor, query)
    for i in range(len(sub_features)):
        sub_features[i] = sub_features[i][0]
    
    #put ids and features in dict for easy lookup
    subclass_dic = dict(zip(sub_ids, sub_features))
    
    #get class ids for each product
    query = "SELECT class from products"
    classes = read_query(conn, cursor, query)
    
    #get all class ids
    query = "SELECT CLS_ID from class_features"
    cls_ids = read_query(conn, cursor, query)
    
    #get all class features
    query = "SELECT CLS_Features from class_features"
    cls_features = read_query(conn, cursor, query)
    for i in range(len(cls_features)):
        cls_features[i] = cls_features[i][0]
    
    #put ids and features in dict for easy lookup
    class_dic = dict(zip(cls_ids, cls_features))
    
    #get category ids for each product
    query = "SELECT category from products"
    categories = read_query(conn, cursor, query)
    
    #get all category ids
    query = "SELECT CAT_ID from category_features"
    cat_ids = read_query(conn, cursor, query)
    
    #get all category features
    query = "SELECT CAT_Features from category_features"
    cat_features = read_query(conn, cursor, query)
    for i in range(len(cat_features)):
        cat_features[i] = cat_features[i][0]
    
    #put ids and features in dict for easy lookup
    category_dic = dict(zip(cat_ids, cat_features))
    
    #get segment ids for each product
    query = "SELECT segment from products"
    segments = read_query(conn, cursor, query)
    
    #get all segment ids
    query = "SELECT SEG_ID from segment_features"
    seg_ids = read_query(conn, cursor, query)
    #print(seg_ids)
    for i in range(len(seg_ids)):
        seg_ids[i] = seg_ids[i][0]
    
    #get all segment features
    query = "SELECT SEG_Features from segment_features"
    seg_features = read_query(conn, cursor, query)
    for i in range(len(seg_features)):
        seg_features[i] = seg_features[i][0]
    
    #put ids and features in dict for easy lookup
    segment_dic = dict(zip(seg_ids, seg_features))
    
    #get manufacturer info for each product
    query = "SELECT Manufacturer from products"
    manufacturers = read_query(conn, cursor, query)
    
    query = "SELECT MfrPartNum from products"
    mfrPartNums = read_query(conn, cursor, query)
    
    #put manufacturer info in dict for easy lookup
    mfr_dic = dict(zip(manufacturers, mfrPartNums))
    
    #create lookup table to get all categories for a given segement
    categories_features_lookup = pd.read_sql_query("SELECT categories.CAT_ID, categories.SEG_ID, category_features.CAT_Features FROM categories INNER JOIN category_features USING(CAT_ID)", conn)
    
    #create lookup table to get all classes for a given category
    classes_features_lookup = pd.read_sql_query("SELECT classes.CLS_ID, classes.CAT_ID, class_features.CLS_Features FROM classes INNER JOIN class_features USING(CLS_ID)", conn)
    
    #create lookup table to get all subclasses for a given class
    subclasses_features_lookup = pd.read_sql_query("SELECT subclasses.SUB_ID, subclasses.CLS_ID, subclass_features.SUB_Features FROM subclasses INNER JOIN subclass_features USING(SUB_ID)", conn)
    
    print("Queries completed")

    # open the output file in the write mode
    f = open(fileName, 'w', encoding='UTF8', newline='')
    
    # create the csv writer
    writer = csv.writer(f)
    
    # write header row to the csv file
    header = ['SKU','Manufacturer','MfrPartNum','Segment_Score','Category_Score','Class_Score','Sub_Class_Score']
    writer.writerow(header)
    
    #iterate through each product in the table
    for i in range(len(skus)):
        #keep track of how long each product takes to run by storing start time
        start_time = time.time()

        #if product has no features, fill in features with lorem ipsum
        if(features[i]== ""):
            var = "Lorem ipsum dolor sitt amet"
        #otherwise, store given features
        else:
            var = features[i]

        #get similarity scores for product compared with each segment
        segment_scores = compare_bin_level(var, seg_features, seg_ids)

        #sort all of the segment scores to see which ones are the highest
        data_seg_score = [x for x, y in segment_scores if y==segments[i][0]]
        segment_scores.sort(reverse=True)
        
        #create empty arrays for categories of top segments
        category_features = []
        category_ids = []

        #keeps track of whether the current product's semgent is included in top segments
        data_seg_bool = False

        #iterate through top 4 segments according to scores
        for j in range(min(4, len(seg_ids))):
            seg = segment_scores[j][1]
            #if the segment being checked is the segment that the product is currently in, set flag to true so we know not to add the features again
            if (seg==segments[i]):
                data_seg_bool = True

            #find all categories for current segment
            cat_temp = categories_features_lookup.loc[categories_features_lookup['SEG_ID'] == seg]
            #add the ids and features of the categories to the list to be compared against
            category_features.extend(cat_temp['CAT_Features'])
            category_ids.extend(cat_temp['CAT_ID'])
            
        #if current segment has not already been included, find current segment's categories
        if (data_seg_bool == False):
            cat_temp = categories_features_lookup.loc[categories_features_lookup['SEG_ID'] == segments[i][0]]
            #add category ids and features to the list to be compared against
            category_features.extend(cat_temp['CAT_Features'])
            category_ids.extend(cat_temp['CAT_ID'])

        #compare all categories in list with current product and store scores
        category_scores = compare_bin_level(var, category_features, category_ids)
        data_cat_score = [x for x, y in category_scores if y==categories[i][0]]
        category_scores.sort(reverse=True)
        
        #create empty arrays for classes
        class_features = []
        class_ids = []
        
        #keeps track of whether the current product's category is included in top categories
        data_cat_bool = False

        #iterate through top 4 categories according to scores
        for j in range(min(4, len(category_scores))):
            cat = category_scores[j][1]
            #if the category being checked is the cateogry that the product is currently in, set flag to true so we know not to add the features again
            if (cat==categories[i]):
                data_cat_bool = True
            #find all classes for current category
            cls_temp = classes_features_lookup.loc[classes_features_lookup['CAT_ID'] == cat]
            #add the ids and features of the classes to the list to be compared against
            class_features.extend(cls_temp['CLS_Features'])
            class_ids.extend(cls_temp['CLS_ID'])
        
        #if product's category has not already been included, find product's category's classes
        if(data_cat_bool == False):
            cls_temp = classes_features_lookup.loc[classes_features_lookup['CAT_ID'] == categories[i][0]]
            #add class ids and features to the list to be compared against
            class_features.extend(cls_temp['CLS_Features'])
            class_ids.extend(cls_temp['CLS_ID'])
        
        #compare all classes with current product and store scores
        class_scores = compare_bin_level(var, class_features, class_ids)
        data_cls_score = [x for x, y in class_scores if y==classes[i][0]]
        class_scores.sort(reverse=True)
    
        #create empty arrays for subclasses
        subclass_features = []
        subclass_ids = []

        #keeps track of whether the current product's class is included in top classes
        data_cls_bool = False

        #iterate through top 4 classes according to scores
        for j in range(min(4, len(class_scores))):
            clas = class_scores[j][1]
            #if the class being checked is the class that the product is currently in, set flag to true so we know not to add the features again
            if (clas==classes[i]):
                data_cls_bool = True
            #find all subclasses for current class
            sub_temp = subclasses_features_lookup.loc[subclasses_features_lookup['CLS_ID'] == clas]
            #add the ids and features of the subclasses to the list to be compared against
            subclass_features.extend(sub_temp['SUB_Features'])
            subclass_ids.extend(sub_temp['SUB_ID'])
            
        #if product's class has not already been included, find product's classes's subclasses
        if(data_cls_bool == False):
            sub_temp = subclasses_features_lookup.loc[subclasses_features_lookup['CLS_ID'] == classes[i][0]]
            #add subclass ids and features to the list to be compared against
            subclass_features.extend(sub_temp['SUB_Features'])
            subclass_ids.extend(sub_temp['SUB_ID'])
        
        #compare all subclasses with current product and store scores
        subclass_scores = compare_bin_level(var, subclass_features, subclass_ids)
        data_sub_score = [x for x, y in subclass_scores if y==subclasses[i][0]]
        subclass_scores.sort(reverse=True)
    
        #if product doesn't have a segment or top segment score is 0, set all scores below to 0
        if ((segments[i][0]==None) or (segment_scores[0][0] == 0)):
            segment_score = 0.0
            category_score = 0.0
            class_score = 0.0
            subclass_score = 0.0
        #otherwise, calculate scores by dividing score of the bin the product is currently in by the top score for each bin
        else:
            segment_score = (data_seg_score[0] / segment_scores[0][0])
            #if top category score is 0, or the calculated segment score is less than 0.3, assign all scores below segment to 0
            if ((category_scores[0][0]==0) or (segment_score < 0.3)):
                category_score = 0.0
                class_score = 0.0
                subclass_score = 0.0
            #if product doesn't have a category and the top category score is less than the product's current category score, set all bin scores below segment to 1
            elif ((categories[i][0]==None) and (category_scores[0][0] <= data_seg_score[0])):
                category_score = 1.0
                class_score = 1.0
                subclass_score = 1.0
            #if the product doesn't have a category and the top category score is greater than the product's current category score, set all bin scores below segment to 0
            elif ((categories[i][0]==None) and (category_scores[0][0] > data_seg_score[0])):
                category_score = 0.0
                class_score = 0.0
                subclass_score = 0.0
            else:
                #calculate category score
                category_score = (data_cat_score[0] / category_scores[0][0])
                #if top class score is 0, or the calculated category score is less than 0.3, assign all scores below category to 0
                if ((class_scores[0][0]==0) or (category_score < 0.3)):
                    class_score = 0.0
                    subclass_score = 0.0
                #if product doesn't have a class and the top class score is less than the product's current class score, set all bin scores below category to 1
                elif ((classes[i][0]==None) and (class_scores[0][0] <= data_cat_score[0])):
                    class_score = 1.0
                    subclass_score = 1.0
                #if the product doesn't have a class and the top class score is greater than the product's current class score, set all bin scores below category to 0
                elif ((classes[i][0]==None) and (class_scores[0][0] > data_cat_score[0])):
                    class_score = 0.0
                    subclass_score = 0.0
                else:
                    #calculate class score
                    class_score = (data_cls_score[0] / class_scores[0][0])
                    #if top subclass score is 0, or the calculated class score is less than 0.3, assign all scores below class to 0
                    if ((subclass_scores[0][0]==0) or (class_score < 0.3)):
                        subclass_score = 0.0
                    #if product doesn't have a subclass and the top subclass score is less than the product's current subclass score, set all bin scores below class to 1
                    elif ((subclasses[i][0]==None) and (subclass_scores[0][0] <= data_cls_score[0])):
                        subclass_score = 1.0
                    #if the product doesn't have a subclass and the top subclass score is greater than the product's current subclass score, set all bin scores below class to 0
                    elif ((subclasses[i][0]==None) and (subclass_scores[0][0] > data_cls_score[0])):
                        subclass_score = 0.0
                    else:
                        #assign subclass score 
                        subclass_score = (data_sub_score[0] / subclass_scores[0][0])
    
        #if classified right at the lower levels then propagate that up
        if (subclass_score == 1.0):
            class_score = 1.0
        if (class_score == 1.0):
            category_score = 1.0
        if (category_score == 1.0):
            segment_score = 1.0
        
        #create row with product information and scores
        row = [skus[i][0], manufacturers[i][0], mfrPartNums[i][0], segment_score, category_score, class_score, subclass_score]
        #store end time for product
        end_time = time.time()
        #print how long the product took to run
        sys.stdout.write("\r---%d: %f seconds ---" % (i,(end_time-start_time)))
        #write row to file
        writer.writerow(row)
        
        #after every 40 products, update the file
        if((i%40) == 0):
            f.flush()
    
    #close file
    f.close()
    #[ront foma; to,e]
    print(time.time())

#run main function
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_name")
    parser.add_argument("file_name")
    args = parser.parse_args()
    main(args.database_name, args.file_name)