import copy
import sqlite3
import random
import csv
import sys
import pandas as pd
from difflib import SequenceMatcher
import time
import string
# from sklearn.metrics.pairwise import cosine_similarity
# from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
# from docsim import DocSim
#import gensim.downloader as api
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import random
    
# from nltk.corpus import stopwords
# stopwords = stopwords.words('english')

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
        
def clean_string (text):
    text = "". join([word for word in text if word not in string.punctuation])
    text = "".join([word for word in text if not word.isdigit()])
    text = text.lower()
    text = " ".join([word for word in text.split() if word not in stopwords])
    return text


# def compare_features(a_features, b_features):
#     #features = clean_string(a_features)
#     #bin_features = clean_string(b_features)
#     docsim = DocSim(model)
#     similarities = docsim.similarity_query(a_features, b_features)
#     #print("Similarity: ", similarities)
#     return similarities

def compare_features(a_features, b_features):
    # print(a_features)
    # print(b_features)
    sentences = copy.deepcopy(b_features)
    sentences.append(a_features)
    # print(len(sentences))
    sentence_embeddings = model.encode(sentences)
    sentence_embeddings.shape
    return cosine_similarity([sentence_embeddings[-1]], sentence_embeddings[:-1])


# =============================================================================
# def compare_features(a_features, b_features):
#     features_a = clean_string(a_features)
#     features_b = clean_string(b_features)
#     s = SequenceMatcher(lambda x: x == " ", features_a, features_b)
#     print (s.ratio())
#     return s.ratio()
# =============================================================================
    

# def compare_bin_level(product_features, bin_features, bin_id):
#     scores = []
#     for i in range(len(bin_features)):
#         #print(product_features)
#         s = compare_features(product_features, bin_features[i])
#         #print(str(s) + " " + str(bin_id[i]))
#         scores.append((s, bin_id[i]))
#     return scores


def compare_bin_level(product_features, bin_features, bin_id):
    scores = compare_features(product_features, bin_features)
    #print(str(s) + " " + s: tr(bin_id[i]))
    #print("product features: ", product_features)
    #print("bin_features: ", bin_features)
    # print(scores[0])
    scores_id = []
    for i in range(len(scores[0])):
        scores_id.append((scores[0][i], bin_id[i]))
    # print(scores_id)
    return scores_id

# =============================================================================
# def compare_seg_level(product_features, bin_features, bin_id):
#     scores = []
#     for i in range(len(bin_features)):
#         s = compare_features(product_features, bin_features[i])
#         #print(str(s) + " " + str(bin_id[i]))
#         scores.append((s, bin_id[i][0]))
#     return scores
# =============================================================================

def main(dbName, fileName):

    model = SentenceTransformer('bert-base-nli-mean-tokens')
    
    # default_model = "glove-wiki-gigaword-50"
    # model = api.load(default_model)
    print("Default model loaded")
    
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
    
    #get product features
    query = "SELECT SKU from features"
    skus = read_query(conn, query)
    
    query = "SELECT features from features"
    features = read_query(conn, query)
    for i in range(len(features)):
        features[i] = features[i][0]
    
    #put features in dict for easy lookup
    product_dic = dict(zip(skus, features))
    
    #get subclass features
    query = "SELECT subclass from products"
    subclasses = read_query(conn, query)
    
    query = "SELECT SUB_ID from subclass_features"
    sub_ids = read_query(conn, query)
    
    query = "SELECT SUB_Features from subclass_features"
    sub_features = read_query(conn, query)
    for i in range(len(sub_features)):
        sub_features[i] = sub_features[i][0]
    
    #put features in dict for easy lookup
    subclass_dic = dict(zip(sub_ids, sub_features))
    
    #get class features
    query = "SELECT class from products"
    classes = read_query(conn, query)
    
    query = "SELECT CLS_ID from class_features"
    cls_ids = read_query(conn, query)
    
    query = "SELECT CLS_Features from class_features"
    cls_features = read_query(conn, query)
    for i in range(len(cls_features)):
        cls_features[i] = cls_features[i][0]
    
    #put features in dict for easy lookup
    class_dic = dict(zip(cls_ids, cls_features))
    
    #get category features
    query = "SELECT category from products"
    categories = read_query(conn, query)
    
    query = "SELECT CAT_ID from category_features"
    cat_ids = read_query(conn, query)
    
    query = "SELECT CAT_Features from category_features"
    cat_features = read_query(conn, query)
    for i in range(len(cat_features)):
        cat_features[i] = cat_features[i][0]
    
    category_dic = dict(zip(cat_ids, cat_features))
    
    #get segment features
    query = "SELECT segment from products"
    segments = read_query(conn, query)
    
    query = "SELECT SEG_ID from segment_features"
    seg_ids = read_query(conn, query)
    #print(seg_ids)
    for i in range(len(seg_ids)):
        seg_ids[i] = seg_ids[i][0]
    
    query = "SELECT SEG_Features from segment_features"
    seg_features = read_query(conn, query)
    for i in range(len(seg_features)):
        seg_features[i] = seg_features[i][0]
    
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
    
    print("Queries completed")
    # open the file in the write mode
    f = open(fileName, 'w', encoding='UTF8', newline='')
    
    # create the csv writer
    writer = csv.writer(f)
    
    # write a row to the csv file
    header = ['SKU','Manufacturer','MfrPartNum','Segment_Score','Category_Score','Class_Score','Sub_Class_Score']
    writer.writerow(header)
    
    #random.sample(range(20000), 2000)
    #iterate through each product in the table
    for i in random.sample(range(20000), 2000):
        start_time = time.time()
    
        if(features[i]== ""):
            var = "Lorem ipsum dolor sitt amet"
        else:
            var = features[i]
    
        segment_scores = compare_bin_level(var, seg_features, seg_ids)
    
        data_seg_score = [x for x, y in segment_scores if y==segments[i][0]]
        segment_scores.sort(reverse=True)
        
        category_features = []
        category_ids = []
    
        # category_features.extend(data_cat_features)
        # category_ids.extend(categories[i])
    
        data_seg_bool = False
    
        for j in range(min(4, len(seg_ids))):
            seg = segment_scores[j][1]
            if (seg==segments[i]):
                data_seg_bool = True
    
            cat_temp = categories_features_lookup.loc[categories_features_lookup['SEG_ID'] == seg]
            category_features.extend(cat_temp['CAT_Features'])
            category_ids.extend(cat_temp['CAT_ID'])
            #query for all the categories that belong to this segment
            #add the features of the category to the list and the ids
    
        if (data_seg_bool == False):
            cat_temp = categories_features_lookup.loc[categories_features_lookup['SEG_ID'] == segments[i][0]]
            category_features.extend(cat_temp['CAT_Features'])
            category_ids.extend(cat_temp['CAT_ID'])
    
        category_scores = compare_bin_level(var, category_features, category_ids)
        data_cat_score = [x for x, y in category_scores if y==categories[i][0]]
        category_scores.sort(reverse=True)
        
        
        class_features = []
        class_ids = []
        
        data_cat_bool = False
    
        for j in range(min(4, len(category_scores))):
            cat = category_scores[j][1]
            if (cat==categories[i]):
                data_cat_bool = True
            cls_temp = classes_features_lookup.loc[classes_features_lookup['CAT_ID'] == cat]
            #cls_temp = classes_features_lookup.query(q)
            class_features.extend(cls_temp['CLS_Features'])
            class_ids.extend(cls_temp['CLS_ID'])
            #query for all classes that belong to this category
            # add the features of the classes to the list and the ids
        
        if(data_cat_bool == False):
            cls_temp = classes_features_lookup.loc[classes_features_lookup['CAT_ID'] == categories[i][0]]
            class_features.extend(cls_temp['CLS_Features'])
            class_ids.extend(cls_temp['CLS_ID'])
        
        class_scores = compare_bin_level(var, class_features, class_ids)
        data_cls_score = [x for x, y in class_scores if y==classes[i][0]]
        class_scores.sort(reverse=True)
    
    
        subclass_features = []
        subclass_ids = []
    
        data_cls_bool = False
    
        for j in range(min(4, len(class_scores))):
            clas = class_scores[j][1]
            if (clas==classes[i]):
                data_cls_bool = True
            sub_temp = subclasses_features_lookup.loc[subclasses_features_lookup['CLS_ID'] == clas]
            #sub_temp = subclasses_features_lookup.query(q)
            subclass_features.extend(sub_temp['SUB_Features'])
            subclass_ids.extend(sub_temp['SUB_ID'])
            #query for all classes that belong to this category
            # add the features of the subclasses to the list and the ids
    
        if(data_cls_bool == False):
            sub_temp = subclasses_features_lookup.loc[subclasses_features_lookup['CLS_ID'] == classes[i][0]]
            subclass_features.extend(sub_temp['SUB_Features'])
            subclass_ids.extend(sub_temp['SUB_ID'])
                 
        subclass_scores = compare_bin_level(var, subclass_features, subclass_ids)
        data_sub_score = [x for x, y in subclass_scores if y==subclasses[i][0]]
        subclass_scores.sort(reverse=True)
    
        
        # if((segments[i][0]==None) or (segment_scores[0][0]==0)):
        #     segment_score =0.0
        # else:
        #     segment_score = (data_seg_score[0] / segment_scores[0][0])
        
        # if((categories[i][0]==None) or (category_scores[0][0]==0)):
        #     category_score = 0.0
        # else:
        #     category_score = (data_cat_score[0] / category_scores[0][0])
            
        # if((classes[i][0]==None) or (class_scores[0][0]==0)):
        #     class_score = 0.0
        # else: 
        #     class_score = (data_cls_score[0] / class_scores[0][0])
            
        # if((subclasses[i][0]==None)  or (subclass_scores[0][0]==0)):
        #     subclass_score = 0.0
        # else:
        #     subclass_score = (data_sub_score[0] / subclass_scores[0][0])
    
        
        if ((segments[i][0]==None) or (segment_scores[0][0] == 0)):
            segment_score = 0.0
            category_score = 0.0
            class_score = 0.0
            subclass_score = 0.0
        else:
            segment_score = (data_seg_score[0] / segment_scores[0][0])
            if ((category_scores[0][0]==0) or (segment_score < 0.3)):
                category_score = 0.0
                class_score = 0.0
                subclass_score = 0.0
            elif ((categories[i][0]==None) and (category_scores[0][0] <= data_seg_score[0])):
                category_score = 1.0
                class_score = 1.0
                subclass_score = 1.0
            elif ((categories[i][0]==None) and (category_scores[0][0] > data_seg_score[0])):
                category_score = 0.0
                class_score = 0.0
                subclass_score = 0.0
            else: 
                category_score = (data_cat_score[0] / category_scores[0][0])
                if ((class_scores[0][0]==0) or (category_score < 0.3)):
                    class_score = 0.0
                    subclass_score = 0.0
                elif ((classes[i][0]==None) and (class_scores[0][0] <= data_cat_score[0])):
                    class_score = 1.0
                    subclass_score = 1.0
                elif ((classes[i][0]==None) and (class_scores[0][0] > data_cat_score[0])):
                    class_score = 0.0
                    subclass_score = 0.0
                else: 
                    class_score = (data_cls_score[0] / class_scores[0][0])
                    if ((subclass_scores[0][0]==0) or (class_score < 0.3)):
                        subclass_score = 0.0
                    elif ((subclasses[i][0]==None) and (subclass_scores[0][0] <= data_cls_score[0])):
                        subclass_score = 1.0
                    elif ((subclasses[i][0]==None) and (subclass_scores[0][0] > data_cls_score[0])):
                        subclass_score = 0.0
                    else: 
                        subclass_score = (data_sub_score[0] / subclass_scores[0][0])
    
        #if classified right at the lower levels then propagate that up
        if (subclass_score == 1.0):
            class_score = 1.0
        if (class_score == 1.0):
            category_score = 1.0
        if (category_score == 1.0):
            segment_score = 1.0
        
    
        row = [skus[i][0], manufacturers[i][0], mfrPartNums[i][0], segment_score, category_score, class_score, subclass_score]
        end_time = time.time()
        print("---" + str(i) + ": " + str(end_time-start_time) + " seconds ---")
        writer.writerow(row)
        
        if((i%40) == 0):
            f.flush()
    
    f.close()
    print(time.time())