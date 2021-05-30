# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
from wikilanguages_utils import *
# time
import time
import datetime
from dateutil import relativedelta
import calendar
# system
import os
import sys
import shutil
import re
import random
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import gzip
import zipfile
import bz2
import json
import csv
import codecs
import unidecode
# requests and others
import requests
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import webbrowser
import reverse_geocoder as rg
import numpy as np
# data
import pandas as pd
# classifier
from sklearn import svm, linear_model
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import gc


# this script collects content related to lgbt topics.
# MAIN
def main():

    for languagecode in wikilanguagecodes: # wikilanguagecodes[wikilanguagecodes.index('nds'):] 
        print ('\n\n\n *** ' +languagecode+ ' '+languages.loc[languagecode]['languagename'])
        store_articles_category_crawling_keywords_biographies_links(languagecode)
        store_articles_lgbt_topic_binary_classifier(languagecode)

    update_push_lgbt_topics_wikipedia_diversity()
    # print ('done')
    # input('')

################################################################

def store_articles_category_crawling_keywords_biographies_links(languagecode):

    functionstartTime = time.time()
    function_name = 'store_articles_category_crawling_keywords_biographies_links '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    print (function_name)
    # os.remove(databases_path + lgbt_content_db); print (lgbt_content_db+' deleted.');
    
    conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + lgbt_content_db); cursor3 = conn3.cursor()

    # wikilanguages_utils.store_lgbt_label('store')

    # Create the table.
    query = ('CREATE TABLE IF NOT EXISTS '+languagecode+'wiki_lgbt ('+

    # general
    'qitem text, '+
    'page_id integer, '+
    'page_title text, '+
    'lgbt_biography, '+

    'keyword text, '+ 
    'category_crawling_level integer, '+ 

    'num_inlinks_from_lgbt integer, '+
    'num_outlinks_to_lgbt integer, '+
    'percent_inlinks_from_lgbt real, '+
    'percent_outlinks_to_lgbt real, '+


    'lgbt_binary integer, '+
    'PRIMARY KEY (qitem));')

    try:
        cursor3.execute(query)
        conn3.commit()
        print ('Table for languagecode '+languagecode+' created.')
    except:
        pass

    try:
        keyword = wikilanguages_utils.store_lgbt_label('get')[languagecode]
        keyword_unicode = unidecode.unidecode(keyword).lower().replace(' ','_')
    except:
        keyword = None
        keyword_unicode = None
        pass

    print (keyword)



    if keyword != None:
        # PRIMER: s’han d’haver agafat totes les categories. també les que contenen paraules clau.
        category_page_ids_page_titles = {}
        category_page_titles_page_ids = {}

        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
        wikilanguages_utils.check_dump(dumps_path, script_name)
        
        dump_in = gzip.open(dumps_path, 'r')

        while True:
            line = dump_in.readline()
            try: line = line.decode("utf-8")
            except UnicodeDecodeError: line = str(line)

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                for row in rows:
                    page_id = int(row[0])
                    page_namespace = int(row[1])
                    cat_title = str(row[2])

                    if page_namespace != 14: continue
                    category_page_ids_page_titles[page_id]=cat_title
                    category_page_titles_page_ids[cat_title]=page_id


        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        print (len(category_links_cat_cat))
        print ('all categories loaded')


        storing_catlinks = False
        if len(category_page_ids_page_titles) > 2000000: # if the language is over 2,000,000 categories

            storing_catlinks = True
            print ('storing category links.')

            conn = sqlite3.connect(databases_path + languagecode + 'wiki_category_links_temp.db'); cursor = conn.cursor()
            query = ('CREATE TABLE IF NOT EXISTS category_links_cat_art (category_title text, page_id integer, PRIMARY KEY (category_title, page_id));')
            cursor.execute(query); conn.commit()
            query = ('CREATE TABLE IF NOT EXISTS category_links_cat_cat (category_title text, subcategory_title text, PRIMARY KEY (category_title, subcategory_title));')
            cursor.execute(query); conn.commit()

            category_links_cat_cat = []
            category_links_cat_art = []

        else:
            category_links_cat_cat_dict = {}
            category_links_cat_art_dict = {}
            for cat_title in category_page_titles_page_ids.keys():
                category_links_cat_cat_dict[cat_title] = set()
                category_links_cat_art_dict[cat_title] = set()



        # SEGON:
        # Category links. Muntar estructura de category links amb diccionaris i sets. Un diccionari amb les relacions entre cat-page i un altre entre cat-cat.
        # https://www.mediawiki.org/wiki/Manual:Categorylinks_table
        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-categorylinks.sql.gz'
        print (dumps_path)
        wikilanguages_utils.check_dump(dumps_path, script_name)
        dump_in = gzip.open(dumps_path, 'r')

        iter = 0
        while True:
            iter+=1
            line = dump_in.readline()
            try: line = line.decode("utf-8")
            except UnicodeDecodeError: line = str(line)

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                for row in rows:

                    # print (row)
                    # input('')

                    try:
                        page_id = int(row[0])
                        # if page_id == 375668:
                        #     print (row)
                        #     input('')
                        
                    except:
                        continue

                    try:
                        cat_title = str(row[1].strip("'"))
                        # if page_id == 375668:
                        #     print (cat_title)
                        #     print ()
                        #     input('')

                    except:

                        continue


                    if cat_title not in category_page_titles_page_ids:
                        continue

                    if storing_catlinks:
                        if page_id in category_page_ids_page_titles: # is this a category
                            category_links_cat_cat.append((cat_title, category_page_ids_page_titles[page_id]))
                        else: # this is an article
                            category_links_cat_art.append((cat_title, page_id))

                    else:
                        if page_id in category_page_ids_page_titles: # is this a category
                            category_links_cat_cat_dict[cat_title].add(category_page_ids_page_titles[page_id])
                        else: # this is an article
                            category_links_cat_art_dict[cat_title].add(page_id)


                    # if page_id == 375668:
                    #     print (row)
                    #     input('')

                    # if 'LGBT' in str(row[1]):
                    #     print (row)
                    #     print (category_links_cat_cat['LGBT'])
                    #     print (category_links_cat_art['LGBT'])


            if storing_catlinks and iter % 5000 == 0:
                print (str(iter)+' categorylinks lines read.')

                cursor.executemany('INSERT OR IGNORE INTO category_links_cat_cat (category_title, subcategory_title) VALUES (?,?)', category_links_cat_cat);
                conn.commit()
                cursor.executemany('INSERT OR IGNORE INTO category_links_cat_art (category_title, page_id) VALUES (?,?)', category_links_cat_art);
                conn.commit()

                category_links_cat_cat = []
                category_links_cat_art = []

        if storing_catlinks:
            cursor.executemany('INSERT OR IGNORE INTO category_links_cat_cat (category_title, subcategory_title) VALUES (?,?)', category_links_cat_cat);
            conn.commit()
            cursor.executemany('INSERT OR IGNORE INTO category_links_cat_art (category_title, page_id) VALUES (?,?)', category_links_cat_art);
            conn.commit()

            category_links_cat_cat = []
            category_links_cat_art = []


        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        print ('all category links loaded')
        # input('')


        # print (category_links_cat_cat['LGBT'])
        # print (category_links_cat_art['LGBT'])
        # input('')


        # TERCER:
        # Iterar els nivells corresponents. crawling.
        print ('*\n')
        print ('With language '+ languagecode+ ' and category '+keyword);

        # Get categories level zero
        keyword_category = {}
        keyword_category[keyword]=set()
        for cat_title in category_page_ids_page_titles.values():
            if keyword_unicode in unidecode.unidecode(cat_title.lower().replace('_',' ')):
                keyword_category[keyword].add(cat_title)

        cattitles_total_level = {}
        for cat_title in keyword_category[keyword]:
            cattitles_total_level[cat_title] = None
        if len(cattitles_total_level) == 0: return


        # ITERATIONS
        # CATEGORIES FROM LEVELS
        level = 1
        num_levels = 25
        if languagecode=='en': num_levels = 10
        j = 0
        total_categories = dict(); total_categories.update(cattitles_total_level)
        print ('Number of categories to start: '+str(len(total_categories)))

 
        selectedarticles_level = {}

        while (level <= num_levels): # Here we choose the number of levels we prefer.
            i = 0


            newcategories = dict()
            for cat_title in cattitles_total_level.keys():
                # print (cat_title)           
                # print (category_links_cat_cat[cat_title])

                if storing_catlinks:
                    for row in cursor.execute('SELECT subcategory_title FROM category_links_cat_cat WHERE category_title = ?;', (cat_title,)):
                        cat_title2 = row[0]
                        try: total_categories[cat_title2]
                        except: newcategories[cat_title2] = None

                else:
                    for cat_title2 in category_links_cat_cat_dict[cat_title]:
                        try: total_categories[cat_title2]
                        except: newcategories[cat_title2] = None


                if storing_catlinks:
                    for row in cursor.execute('SELECT page_id FROM category_links_cat_art WHERE category_title = ?;', (cat_title,)):
                        page_id = row[0]
                        try:
                            cur_level = selectedarticles_level[page_id]
                            if cur_level > level: selectedarticles_level[page_id] = level
                        except:
                            selectedarticles_level[page_id] = level

                        if page_id in selectedarticles:
                            selectedarticles[page_id] = level
                        else:
                            selectedarticles[page_id] = level

                        i += 1

                else:
                    for page_id in category_links_cat_art_dict[cat_title]:
                        try:
                            cur_level = selectedarticles_level[page_id]
                            if cur_level > level: 
                                selectedarticles_level[page_id] = level
                        except:
                            selectedarticles_level[page_id] = level

                        if page_id in selectedarticles:
                            selectedarticles[page_id] = level
                        else:
                            selectedarticles[page_id] = level

                        i += 1

            cattitles_total_level = dict()
            cattitles_total_level.update(newcategories)
            total_categories.update(newcategories)

            print('Level: '+str(level) + ". Number of new articles is: " + str(i)+ ". Total number of articles is "+str(len(selectedarticles_level))+'. Number of new categories is: '+str(len(newcategories))+'. Total number of categories is: '+str(len(total_categories)))

            level = level + 1
            if len(newcategories) == 0: 
                print ('No new categories: break!')
                break


    # GETTING READY TO INSERT
    parameters = []
    discarded = 0
    try:
        for row in cursor.execute('SELECT qitem, page_id, page_title, sexual_orientation, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, gender FROM '+languagecode+'wiki'):

            qitem = row[0]
            page_id = row[1]
            page_title = row[2]
            sexual_orientation = row[3]
            lgbt_binary = None
            gender = row[8]

            if gender not in ('Q6581072','Q6581097') and gender != None:
                lgbt_binary = 1

            try:
                category_crawling_level = selectedarticles_level[page_id]
            except:
                category_crawling_level = None

            if sexual_orientation != "Q1035954" and sexual_orientation != "" and sexual_orientation != None:
                lgbt_binary = 1
            else:
                sexual_orientation = None
                # if category_crawling_level == None: 
                #     discarded+=1
                #     continue


            num_inlinks_from_lgbt = row[4]
            num_outlinks_to_lgbt = row[5]
            percent_inlinks_from_lgbt = row[6]
            percent_outlinks_to_lgbt = row[7]

            page_title_rect = unidecode.unidecode(page_title).lower().replace(' ','_')
            keyword_param = None
            if keyword_unicode in page_title_rect:
                keyword_param = keyword
                lgbt_binary = 1


            if num_inlinks_from_lgbt == 0 and num_outlinks_to_lgbt == 0 and category_crawling_level == None:
                discarded+=1
                continue

            parameters.append((qitem, page_id, page_title, sexual_orientation, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, keyword_param, category_crawling_level, lgbt_binary))

    except:
        pass

    cursor3.executemany('INSERT OR IGNORE INTO '+languagecode+'wiki_lgbt (qitem, page_id, page_title, lgbt_biography, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, keyword, category_crawling_level, lgbt_binary) VALUES (?,?,?,?,?,?,?,?,?,?,?)', parameters);
    conn3.commit()
    print (discarded)
    print (len(parameters))


    try:
        os.remove(databases_path + languagecode + 'wiki_category_links_temp.db'); print (languagecode + 'wiki_category_links_temp.db'+' deleted.');
    except:
        pass


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def store_articles_lgbt_topic_binary_classifier(languagecode):

    function_name = 'store_articles_lgbt_topic_binary_classifier '+languagecode
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    try:
        keyword = wikilanguages_utils.store_lgbt_label('get')[languagecode]
        print (keyword)
    except:
        print ('There is not a keyword for LGBT in this language: '+languagecode)
        return
    # input('')

    # OBTAIN THE DATA TO FIT.
    conn3 = sqlite3.connect(databases_path + lgbt_content_db); cursor3 = conn3.cursor()
    query = 'SELECT qitem, page_id, page_title, lgbt_biography, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, keyword, category_crawling_level, lgbt_binary FROM '+languagecode+'wiki_lgbt;'

    features = ['qitem']+['lgbt_binary', 'category_crawling_level', 'num_inlinks_from_lgbt','num_outlinks_to_lgbt', 'percent_inlinks_from_lgbt', 'percent_outlinks_to_lgbt', 'keyword'] # 'lgbt_biography', 'keyword', 

    lgbt_df = pd.read_sql_query(query, conn3)
    qitems_page_titles = pd.Series(lgbt_df.page_title.values,index=lgbt_df.qitem).to_dict() 

    lgbt_df = lgbt_df[features]
    lgbt_df = lgbt_df.set_index(['qitem'])

    lgbt_df = lgbt_df.fillna(0)

    lgbt_df.num_inlinks_from_lgbt = lgbt_df.num_inlinks_from_lgbt.astype(int)
    lgbt_df.num_outlinks_to_lgbt = lgbt_df.num_outlinks_to_lgbt.astype(int)
   

    if len(lgbt_df.index.tolist())==0: print ('It is not possible to classify Wikipedia Articles as there is no groundtruth.'); return (0,0,[],[]) # maxlevel,num_articles_lgbt,lgbt_df_list,binary_list


    # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
    category_crawling_absolute_level=lgbt_df['category_crawling_level'].tolist()

    try:
        maxlevel = max(category_crawling_absolute_level)
    except:
        print ('There are not articles retrieved with category crawling, therefore we cannot classify articles.')
        return

    if maxlevel == 0:
        print ('There are not articles retrieved with category crawling, therefore we cannot classify articles.')
        return

    for n, i in enumerate(category_crawling_absolute_level):
        if i > 0:
            category_crawling_absolute_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_absolute_level[n]=0
    lgbt_df = lgbt_df.assign(category_crawling_level = category_crawling_absolute_level)


    # keyword_title=lgbt_df['keyword'].tolist()
    # for n, i in enumerate(keyword_title):
    #     if i is not 0: keyword_title[n]=1
    #     else: keyword_title[n]=0
    # lgbt_df = lgbt_df.assign(keyword = keyword_title)


    # lgbt_biography=lgbt_df['lgbt_biography'].tolist()
    # for n, i in enumerate(lgbt_biography):
    #     if i is not 0: lgbt_biography[n]=1
    #     else: lgbt_biography[n]=0
    # lgbt_df = lgbt_df.assign(lgbt_biography = lgbt_biography)

    lgbt_df = lgbt_df.fillna(0)


    # SAMPLING
    print ('sampling method: negative sampling.')

    lgbt_df_yes = lgbt_df.loc[lgbt_df['keyword'] == keyword] # positives are the biographies

    lgbt_df = lgbt_df.drop(columns=['keyword'])

    # lgbt_df_yes = lgbt_df.loc[lgbt_df['lgbt_binary'] == 1] # positives are the biographies

    lgbt_df_yes = lgbt_df_yes.drop(columns=['lgbt_binary','keyword'])


    lgbt_df_list_yes = lgbt_df_yes.values.tolist()
    num_articles_lgbt = len(lgbt_df_list_yes)

    lgbt_df_list_probably_no = []
    size_sample = 6
    if languagecode == 'en': size_sample = 4 # exception for English

    for i in range(1,1+size_sample):
        lgbt_df = lgbt_df.sample(frac=1) # randomize the rows order

        lgbt_df_probably_no = lgbt_df.loc[(lgbt_df['num_outlinks_to_lgbt'] == 0) | (lgbt_df['num_inlinks_from_lgbt'] == 0)]
        lgbt_df_probably_no = lgbt_df_probably_no.sample(frac=1) # randomize the rows order

        lgbt_df_probably_no = lgbt_df_probably_no.drop(columns=['lgbt_binary'])
        lgbt_df_list_probably_no = lgbt_df_list_probably_no + lgbt_df_probably_no.values.tolist()[:num_articles_lgbt]

        num_probably_no = len(lgbt_df_list_probably_no)
        lgbt_df_list = lgbt_df_list_yes + lgbt_df_list_probably_no
        binary_list = [1]*num_articles_lgbt+[0]*num_probably_no

    print ('\nConverting the dataframe...')
    print ('These are its columns:')
    print (list(lgbt_df_yes.columns.values))

    print (num_articles_lgbt) # these are the yes (groundtruth)
    print (len(lgbt_df_list)) # these are the no (groundtruth)
    print (len(binary_list))

   
    # WE GET THE POTENTIAL CCC ARTICLES THAT HAVE NOT BEEN 1 BY ANY OTHER MEANS.
    # For the testing takes those with one of these features category crawling, els keywords i els outlinks a ethnic groups > 0.
    # query = 'SELECT * FROM lgbt_articles WHERE primary_lang = "'+languagecode+'" AND qitem_lgbt = "'+qitem_lgbt+' AND category_crawling_absolute_level IS NOT NULL AND lgbt_binary != 1 AND (num_outlinks_to_group > 0 OR num_inlinks_from_group > 0)";'
    query = 'SELECT qitem, page_id, page_title, lgbt_biography, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, keyword, category_crawling_level, lgbt_binary FROM '+languagecode+'wiki_lgbt WHERE lgbt_binary IS NULL AND category_crawling_level IS NOT NULL;'

    features = ['qitem']+['category_crawling_level', 'num_inlinks_from_lgbt','num_outlinks_to_lgbt', 'percent_inlinks_from_lgbt', 'percent_outlinks_to_lgbt'] #'lgbt_biography', 'keyword', 

    potential_lgbt_df = pd.read_sql_query(query, conn3)
    potential_lgbt_df = potential_lgbt_df[features]
    potential_lgbt_df = potential_lgbt_df.set_index(['qitem'])
    potential_lgbt_df = potential_lgbt_df.fillna(0)
    print (len(potential_lgbt_df))

    # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
    category_crawling_absolute_level=potential_lgbt_df['category_crawling_level'].tolist()

    try:
        maxlevel = max(category_crawling_absolute_level)
    except:
        print ('There are not articles retrieved with category crawling, therefore we cannot classify articles.')
        return

    for n, i in enumerate(category_crawling_absolute_level):
        if i > 0:
            category_crawling_absolute_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_absolute_level[n]=0
    potential_lgbt_df = potential_lgbt_df.assign(category_crawling_level = category_crawling_absolute_level)


    # keyword_title=lgbt_df['keyword'].tolist()
    # for n, i in enumerate(keyword_title):
    #     if i is not 0: keyword_title[n]=1
    #     else: keyword_title[n]=0
    # lgbt_df = lgbt_df.assign(keyword = keyword_title)


    # lgbt_biography=lgbt_df['lgbt_biography'].tolist()
    # for n, i in enumerate(lgbt_biography):
    #     if i is not 0: lgbt_biography[n]=1
    #     else: lgbt_biography[n]=0
    # lgbt_df = lgbt_df.assign(lgbt_biography = lgbt_biography)


    # NOT ENOUGH ARTICLES
    if len(potential_lgbt_df)==0: print ('There are not potential articles for lgbt, so it returns empty'); return
    potential_lgbt_df = potential_lgbt_df.sample(frac=1) # randomize the rows order

    print ('We selected this number of potential Ethnic articles: '+str(len(potential_lgbt_df)))


    # FIT THE SVM MODEL
    print ('We have: num_articles_lgbt,lgbt_df_list,binary_list,potential_lgbt_df')
    print ('Fitting the data into the classifier.')
    print ('The data has '+str(len(lgbt_df_list))+' samples.')
    if num_articles_lgbt == 0 or len(lgbt_df_list)<10: print ('There are not enough samples.'); return

    X = lgbt_df_list
    y = binary_list

    # print (X)
    # print (y)
    print (len(X))
    print (len(y))


    print ('The chosen classifier is RandomForest')
    clf = RandomForestClassifier(n_estimators=100)
    clf.fit(X, y)

    print ('The fit classes are: '+str(clf.classes_))
    print ('The fit has a score of: '+str(clf.score(X, y, sample_weight=None)))
    print (clf.feature_importances_.tolist())


    # TEST THE DATA
    print ('Calculating which page is IN or OUT...')
    if potential_lgbt_df is None: 
        print ('No Articles to verify.'); 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return     
    if len(potential_lgbt_df)==0: 
        print ('No Articles to verify.'); 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return

    qitems = potential_lgbt_df.index.tolist()
    potential = potential_lgbt_df.values.tolist()

    print ('This is the number of articles we have for testing '+str(len(potential)))

    # input('')

    print ('We print the results (0 for no, 1 for yes):')
    visible = 0
    print (visible)

    selected=[]

    # DO NOT PRINT THE CLASSIFIER RESULTS ARTICLE BY ARTICLE
    if visible == 0:
    #    testdict = {}
        result = clf.predict(potential)
        i = 0
        for x in result:
    #        testdict[qitems[i]]=(x,potential[i])
            if x == 1:
                qitem=qitems[i]
                selected.append((qitem,))
            i += 1
#    print (testdict)

    # PRINT THE CLASSIFIER RESULTS ARTICLE BY ARTICLE
    else:
        # provisional
#        print (potential[:15])
#        print (qitems[:15])
        count_yes=0
        count_no=0
        for n,i in enumerate(potential):
            result = clf.predict([i])
            qitem=qitems[n]
            if qitem == None: continue

            if result[0] == 1:
                count_yes+=1
                print (['category_crawling_level', 'num_inlinks_from_lgbt','num_outlinks_to_lgbt', 'percent_inlinks_from_lgbt', 'percent_outlinks_to_lgbt']) # 'lgbt_biography', 'keyword', 
                print(i)
                print(clf.predict_proba([i]).tolist())
                page_title = qitems_page_titles[qitem]
                if page_title == None: page_title = ''
                print (str(count_yes)+'\tIN\t'+qitem+':'+page_title+'\n')
                # input('')

                try: selected.append((qitem))
                except: pass
            else:
                count_no+=1
                print (['category_crawling_level', 'num_inlinks_from_lgbt','num_outlinks_to_lgbt', 'percent_inlinks_from_lgbt', 'percent_outlinks_to_lgbt']) # 
                print(i)
                print(clf.predict_proba([i]).tolist())
                page_title = qitems_page_titles[qitem]
                if page_title == None: page_title = ''
                print (str(count_no)+'\tOUT:\t'+qitem+':'+page_title+'\n')

#                input('')


    print ('There were already '+str(num_articles_lgbt)+' lgbt Articles selected as groundtruth.')

    print ('\nThis algorithm CLASSIFIED '+str(len(selected))+' Articles as lgbt_binary = 1 from a total of '+str(len(potential))+' from the testing data. This is a: '+str(round(100*len(selected)/len(potential),3))+'%.')

    # input('')

    conn3 = sqlite3.connect(databases_path + lgbt_content_db); cursor3 = conn3.cursor()
    query = 'UPDATE '+languagecode+'wiki_lgbt SET lgbt_binary = 1 WHERE qitem = ?;'
    cursor3.executemany(query,selected)
    conn3.commit()

    print ('lgbt topics in '+languagecode+' created.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def update_push_lgbt_topics_wikipedia_diversity():


    functionstartTime = time.time()
    conn2 = sqlite3.connect(databases_path + lgbt_content_db); cursor2 = conn2.cursor()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    qitems = {}
    keyword_title = {}
    for languagecode in wikilanguagecodes:
        for row in cursor2.execute('SELECT qitem, keyword FROM '+languagecode+'wiki_lgbt WHERE lgbt_binary = 1;'):
            try:
                qitems[row[0]]+=1
            except:
                qitems[row[0]]=1

            keyword = row[1]
            if keyword!='' and keyword!=None:
                keyword_title[row[0]]=row[1]




    for languagecode in wikilanguagecodes:
        print (languagecode)
        try:
            query = 'UPDATE '+languagecode+'wiki SET lgbt_topic = NULL;'
            cursor.execute(query)
            conn.commit()
        except:
            continue

        params = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        for qitem,value in qitems.items():
            try:
                keyword = keyword_title[qitem]
            except:
                keyword = None
            try:
                page_title = qitems_page_titles[qitem]
                page_id = page_titles_page_ids[page_title]
                params.append((value, keyword, qitem, page_title, page_id))
            except:
                pass

        query = 'UPDATE '+languagecode+'wiki SET lgbt_topic = ?, lgbt_keyword_title = ? WHERE qitem = ? AND page_title = ? and page_id = ?;'
        cursor.executemany(query,params)
        conn.commit()            

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)



#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("lgbt_content_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("lgbt_content_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':

    script_name = 'lgbt_content_selection.py'

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
    startTime = time.time()


    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();

    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)

    # Only those with a geographical context
    languageswithoutterritory=list(set(languages.index.tolist()) - set(list(territories.index.tolist())))
    for languagecode in languageswithoutterritory:
        try: wikilanguagecodes.remove(languagecode)
        except: pass

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=False)]

    # if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit();
    main()
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)


    wikilanguages_utils.finish_email(startTime,'lgbt_content_selection.out','lgbt CONTENT Selection')
