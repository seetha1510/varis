import pandas as pd
import csv

class Node:
    def __init__(self,key):
        self.key = key
        self.child = []
        
        
def new_node(key):
    n = Node(key)
    return n

if __name__=='__main__':
    
    column_list = ["Segment","Category","Class","Sub-Class"]
    df = pd.read_csv("Capstone_Dataset_1.csv", usecols = column_list)
    #print(df["Segment"][1])
    list_segment = []
    root = new_node("Taxonomy")
    for segment in df["Segment"]:
        s = new_node(segment)
        if(segment not in list_segment):
            list_segment.append(segment)
            root.child.append(s)
            #print(list_segment)
    #print(len(root.child))
    line = 0
    for segment_no in range(len(root.child)):
        list_category = []
        for category in range(len(df["Segment"])):
            if root.child[segment_no].key == df["Segment"][category]:
                    #print(root.child[segment_no].key)
                        #print(row[10])
                    cat = new_node(df["Category"][category])
                    if(cat.key not in list_category):
                        list_category.append(cat.key)
                        root.child[segment_no].child.append(cat)
                        #print("New category")
                            #print(cat.key)
                    
                        #print("Hell naw")
               # print(row)
        line+=1
        #print(line)
        #print(list_category)
            
                    
    for segment_no in range(len(root.child)):
        for category_no in range(len(root.child[segment_no].child)):
            list_class = []
                #print(root.child[segment_no].child[category].key)
            for category in range(len(df["Category"])):
                    #print(row[12])
                if root.child[segment_no].child[category_no].key == df["Category"][category]:
                    cl = new_node(df["Class"][category])
                    #print("Hello")
                    if(cl.key not in list_class):
                        root.child[segment_no].child[category_no].child.append(cl)
                        list_class.append(cl.key)
                        #print("New Class!")
            #print(list_class)
                            
         #print(root.child[segment_no].key)
            #with open('Capstone_Dataset_1.csv') as csvfile:
                
                #reader = csv.reader(csvfile, delimiter = ',')
                
                   # print(row[10])
    for segment_no in range(len(root.child)):
        for category_no in range(len(root.child[segment_no].child)):
            for class_no in range(len(root.child[segment_no].child[category_no].child)):
                list_subclass = []
                for cl in range(len(df["Class"])):
                    if root.child[segment_no].child[category_no].child[class_no].key == df["Class"][cl]:
                        sub_cl = new_node(df["Sub-Class"][cl])
                            
                        if(sub_cl.key not in list_subclass):
                            root.child[segment_no].child[category_no].child[class_no].child.append(sub_cl)
                            list_subclass.append(sub_cl.key)
                            #print("New Subclass!")
                    
                #print(list_subclass)
    
    
        
        
    def printTree(root, level=0):
        print("  " * level, root.key)
        for child in root.child:
            printTree(child, level + 1)
    
    printTree(root)
        
        
                            
            
                        
        
        
        
        
        