import pandas as pd
from functools import reduce
import xml.etree.ElementTree as ET
import os  
from collections import Counter
import datetime
import numpy 
#functions open files XML´s
def read_xml(file):
    read = ET.parse(file)
    root = read.getroot()
    return root

#Files in to chunks
def chunckify(file,chunks):
    for i in range(0,len(file), chunks):
        yield file[i:i + chunks]

# relation  between in viewcount and answerscount

#Viewcounts

def view_count(file):

    post_id_stack = file.attrib['PostTypeId']
    if post_id_stack== '1':
        try :
            views = int(file.attrib['ViewCount'])
        except:
            None
        return views
    
    
#AnswerCount

def get_post_views(data):
    # Get data views
    try:
        views = int(data.attrib['AnswerCount'])
    except Exception as ex:
        return None

    return views
        
#Mappers
#Functions Map() == Views
def map_post(data):
    map_date_post = list(map(view_count, data))
    date_count= Counter(map_date_post)
    return date_count


#Functions Map() == Answers
def mapper(data):
    views = list(map(get_post_views, data))
    date_count= Counter(views)
    return date_count

# Merge function
def merge_date(D1,D2) :
    D1.update(D2) 
    return D1 

#create function reduce for viewcount and answercount
#ViewCounts
def reduce_date(iter):
    reduce_post = reduce(merge_date,iter)
    #create dataframe to reduce_post
    df = pd.DataFrame.from_dict(reduce_post, orient='index')
    df.reset_index(inplace=True)  
     #delete NaN and convert int
    df.dropna(inplace=True)
    df['index'] = df['index'].astype(int)
    #rename index to viewcounts
    df.rename(columns={'index':'ViewCounts'}, inplace=True)
    #delete 0 counts
    df = df[df.ViewCounts != 0]
    return df

def reduce_date_2(iter):
    reduce_post = reduce(merge_date,iter)
    #create dataframe to reduce_post
    df2 = pd.DataFrame.from_dict(reduce_post, orient='index')
    df2.reset_index(inplace=True)
    # delete nan
    df2.dropna(inplace=True)
    #change name indx to counter_viwes
    df2.rename(columns={'index':'counter_views',0:'views'},inplace=True)
    #counter views as int
    df2['counter_views']=df2['counter_views'].astype(int)
    return df2

#create function union df1 and df2
def union_df(df1,df2):
    df_union = pd.merge(df1,df2,on='ViewCounts',how='outer')
    df_union.dropna(inplace=True)
    df_union['counter_views']=df_union['counter_views'].astype(int)
    return df_union

#create function main
def main ():
    read_file = read_xml("posts.xml")
    chunky_data = chunckify(read_file,50)
    Map_data = list(map(map_post, chunky_data))
    df1 = reduce_date(Map_data) 
    #answercount
    chunky_data = chunckify(read_file,50)
    body_views = list(map(mapper,chunky_data))
    df2 = reduce_date_2(body_views)
    print(df2)


#create function to create a new column with the relation between views and answers
if __name__ == '__main__': 
    main()
    