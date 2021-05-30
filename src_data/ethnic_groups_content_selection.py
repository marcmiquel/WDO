# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
from wikilanguages_utils import *
# time
import time
import datetime
import dateutil
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



# this script collects content related to ethnic groups (and indigenous groups) and LGBT+ culture.
# MAIN
def main():


    get_relevance_features_wikipedia_diversity()

    print ('fi')
    input('')

    # store_choosen_langs_for_ethnic_groups() # it stores the relationship between ethnic groups and the languages where to retrieve articles from (e.g. those with which the biographies of the ethnic belongs to its CCC).


    create_ethnic_groups_content_db() # it creates the database.

    store_articles_biographies() # it retrieves from wikipedia_diversity.db the biographies from every language and their membership to an ethnic group.

    store_articles_keywords() # it stores the articles which contain an ethnic group name in the title in every language.

    store_articles_category_crawling() # it stores the articles about each ethnic group after crawling the category graph using the keywords to start off.

    store_articles_links() # it stores the articles whose inlinks and outlinks point to anarticle of an ethnic group.

    store_articles_ethnic_group_binary_classifier() # it generates the final collection of articles for each ethnic group.

    get_relevance_features_wikipedia_diversity()

    print ('the end!')
    return


    # conn = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor = conn.cursor()
    # for languagecode in wikilanguagecodes:
    #     for row in cursor.execute('select qitem_ethnic_group, name_primary_lang_ethnic_group, count(*), primary_lang from ethnic_group_articles where primary_lang = "'+languagecode+'" and ethnic_group_binary = 1 group by 1 order by 3 desc limit 100;'):
    #         print (row)
    #     print ('\n\n')

    
    update_push_ethnic_group_topic_wikipedia_diversity() # it updates the binary for every ethnic group in the languages tables in wikipedia_diversity.db

    wikilanguages_utils.copy_db_for_production(ethnic_groups_content_db, 'ethnic_groups_content_selection.py', databases_path)






def store_choosen_langs_for_ethnic_groups():
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()

    function_name = 'store_choosen_langs_for_ethnic_groups'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()


    df = pd.read_sql_query('SELECT distinct qitem FROM ethnic_groups', conn2)
    qitems_ethnic_group = list(df.qitem.unique())

    try:
        df = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs', conn2)

    except:
        df = pd.DataFrame(None, columns = ['count', 'ethnic_group','lang', 'cycle_year_month', 'choosen_langs'])


    if len(df.loc[(df["cycle_year_month"] == cycle_year_month)])==0:

        df = pd.DataFrame(None, columns = ['count', 'ethnic_group','lang'])
        df_ccc= pd.DataFrame(None, columns = ['count', 'ethnic_group','lang'])

        for languagecode in wikilanguagecodes: #['ca','en','it','ar','hy']
            print (languagecode)
            # try: df = pd.concat([df, pd.read_sql_query('SELECT count(*) as count, ethnic_group, "'+languagecode+'wiki" as lang, "no" as ccc, "'+cycle_year_month+'" as cycle_year_month, "0" as "choosen_lang" FROM '+languagecode+'wiki GROUP BY ethnic_group ORDER BY ethnic_group DESC;', conn)],sort=True)
            # except: pass
            try: df_ccc = pd.concat([df_ccc, pd.read_sql_query('SELECT count(*) as count, ethnic_group, "'+languagecode+'wiki" as lang, "yes" as ccc, "'+cycle_year_month+'" as cycle_year_month, "0" as "choosen_lang" FROM '+languagecode+'wiki WHERE ccc_binary = 1 GROUP BY ethnic_group ORDER BY ethnic_group DESC;', conn)],sort=True)
            except: pass

        # df = pd.concat([df, df_ccc], sort=True)
        df = df_ccc

        df.to_sql('ethnic_groups_choosen_langs', conn2, index = False);
        conn2.commit()

        params = []
        for qitem_eg in qitems_ethnic_group:

            u = df_ccc.loc[(df_ccc["ethnic_group"] == qitem_eg)][['count','lang']].sort_values(by=['count'], ascending=False)
            # v = df.loc[(df["ethnic_group"] == qitem_eg)][['count','lang']].sort_values(by=['count'], ascending=False)

            choosen_langs = set()
            try: 
                for i in range (0,3): choosen_langs.add(u['lang'].iloc[i])
            except: pass

            # try: 
            #     for i in range (0,2): choosen_langs.add(v['lang'].iloc[i])
            # except: pass

            for cl in choosen_langs:
                params.append((cl, qitem_eg))

        query = 'UPDATE ethnic_groups_choosen_langs SET choosen_lang = 1 WHERE lang = ? AND ethnic_group = ?;'
        cursor2.executemany(query, params)
        conn2.commit()

        cursor2.execute('DELETE FROM ethnic_groups_choosen_langs WHERE choosen_lang != 1;')
        conn2.commit()
        cursor2.execute('DELETE FROM ethnic_groups_choosen_langs WHERE ethnic_group = "" OR ethnic_group IS NULL;')
        conn2.commit()

        df = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs', conn2)
        print ('choosen langs: new data.')

    else:
        print ('choosen langs: data retrieved.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)

    return df





def create_ethnic_groups_content_db():
    function_name = 'create_ethnic_groups_content_db'

#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor = conn.cursor()


    query = ('CREATE TABLE IF NOT EXISTS ethnic_group_articles ('+

    # general
    'qitem text NOT NULL, '+
    'page_title text, '+
    'page_id integer, '+

    'primary_lang text, '+
    'qitem_ethnic_group text, '+
    'name_en_ethnic_group text, '+
    'name_primary_lang_ethnic_group text, '+

    'gender text, '+ # this indicates whether it is a biography or not.
    'ccc_binary text, '+ # this indicates whether it belongs to a language CCC.

    'keyword_title text, '+
    'category_crawling_absolute_level integer, '+
    'category_crawling_relative_level integer, '+ 

    'num_inlinks integer, '+
    'num_outlinks integer, '+
    'num_inlinks_from_group integer, '+
    'num_outlinks_to_group integer, '+
    'percent_inlinks_from_group real, '+
    'percent_outlinks_to_group real, '+

    'ethnic_group_binary integer, '+

    # feature
    'date_created integer, '+

    # GENERAL TOPICS DIVERSITY
    # topics: people, organizations, things and places
    'folk text, '+
    'earth text, '+
    'monuments_and_buildings text, '+
    'music_creations_and_organizations text, '+
    'sport_and_teams text, '+
    'food text, '+
    'paintings text, '+
    'glam text,'+
    'books text,'+
    'clothing_and_fashion text,'+
    'industry text, '+
    'religion text, '+ # as a topic
    'time_interval text, '+
    'start_time integer, '+
    'end_time text, '+

    # characteristics of article relevance
    'num_inlinks integer, '+
    'num_outlinks integer, '+
    'num_bytes integer, '+
    'num_references integer, '+
    'num_edits integer, '+
    'num_edits_last_month integer, '+
    'num_editors integer, '+
    'num_discussions integer, '+
    'num_pageviews integer, '+
    'num_wdproperty integer, '+
    'num_interwiki integer, '+
    'num_images integer, '+


    # relevance features
    'PRIMARY KEY (qitem, qitem_ethnic_group, primary_lang));')

    cursor.execute(query)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def store_articles_biographies():

    function_name = 'store_articles_biographies'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    print ('\n'+function_name)

    ethnic_groups = group_labels.loc[(group_labels["group_label"] == 'ethnic_group')]
    qitems_ethnic_group = ethnic_groups.qitem.unique()


    conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        params = []
        print ('\n* '+languagecode)

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        try:
            for row in cursor.execute('SELECT qitem, gender, ccc_binary, ethnic_group, page_title FROM '+languagecode+'wiki WHERE ethnic_group IS NOT NULL;'):
                gender = row[1]

                qit = row[0]
                
                if row[1] == 'Q6581072': gender = 1
                elif row[1] == 'Q6581097': gender = 2
                ccc_binary = row[2]
        
                egroup = row[3]
                page_title = row[4]

                if ';' in egroup:
                    for eg in egroup.split(';'):
                        if eg != '' or eg == None:
                            try:
                                eg_name = qitems_page_titles[eg]
                                # eg_name = ethnic_groups.loc[(ethnic_groups["lang"] == languagecode) & (ethnic_groups["qitem"] == eg)].tolist()[0]
                            except:
                                eg_name = ''
                            params.append((1, qit, page_title, eg, gender, ccc_binary, languagecode, eg_name))

                else:
                    try:
                        # eg_name = ethnic_groups.loc[(ethnic_groups["lang"] == languagecode) & (ethnic_groups["qitem"] == egroup)].tolist()[0]
                        eg_name = qitems_page_titles[egroup]

                    except:
                        eg_name = ''
                    params.append((1, qit, page_title, egroup, gender, ccc_binary, languagecode, eg_name))
        except:
            pass

        cursor2.executemany('INSERT OR IGNORE INTO ethnic_group_articles (ethnic_group_binary, qitem, page_title, qitem_ethnic_group, gender, ccc_binary, primary_lang, name_primary_lang_ethnic_group) VALUES (?,?,?,?,?,?,?,?)', params);
        conn2.commit()


    print ('biographies stored.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def store_articles_keywords():

    function_name = 'store_articles_keywords'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    print ('\n'+function_name)


    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()
    df = pd.read_sql_query('SELECT qitem, native_language FROM ethnic_groups WHERE native_language != "";', conn2).set_index('qitem')
    ethnic_group_to_native_language = df.to_dict('dict')['native_language']


    conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()

    ethnic_groups_choosen_langs = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs;', conn2)

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()


    # iterating languages
    for languagecode in wikilanguagecodes: #['ca']:

        languageTime = time.time()

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        gender_qitems_lang = {}
        ccc_qitems_lang = {}
        try:
            for row in cursor.execute('SELECT qitem, page_title FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
                ccc_qitems_lang[row[0]]=row[1]
            for row in cursor.execute('SELECT qitem, gender FROM '+languagecode+'wiki WHERE gender IS NOT NULL;'):
                gender_qitems_lang[row[0]]=row[1]
        except:
            continue


        qs_ethnic_group_choosen_lang = ethnic_groups_choosen_langs.loc[(ethnic_groups_choosen_langs["lang"] == languagecode+'wiki')].ethnic_group.unique()


        qs_group_labels = group_labels.loc[(group_labels["group_label"] == 'ethnic_group') & (group_labels["lang"] == languagecode)].qitem.unique()

        page_titles_qitems_unicode = {}
        for page_title,qitem in page_titles_qitems.items():
            page_title_rect = unidecode.unidecode(page_title).lower().replace(' ','_')
            page_titles_qitems_unicode[page_title_rect]=qitem

        print ('\n\n* '+languagecode)
        print (len(qs_ethnic_group_choosen_lang))
        print (len(qs_group_labels))
        print (len(set(set(qs_ethnic_group_choosen_lang).intersection(set(qs_group_labels)))))

        qs_ethnic_group_choosen_lang = set(set(qs_ethnic_group_choosen_lang).intersection(set(qs_group_labels)))

        print (qs_ethnic_group_choosen_lang)


        unicode_kw = {}
        ethnic_group_keywords = {}
        # iterating ethnic groups
        for qitem_eg in qs_ethnic_group_choosen_lang: #['Q244504']: # qs_ethnic_group_choosen_lang
            keywords = set()

            try:
                native_language_name = ethnic_group_to_native_language[qitem_eg]

                if ';' in native_language_name:
                    for qt in native_language_name.split(';'):

                        if qt in wikilanguages_qitems: 
                            continue

                        kwcur = qitems_page_titles[qt]
                        keyword = unidecode.unidecode(kwcur).lower().replace(' ','_')
                        unicode_kw[keyword]=kwcur
                        keywords.add(keyword)

                elif native_language_name not in wikilanguages_qitems: 
                        kwcur = qitems_page_titles[native_language_name]
                        keyword = unidecode.unidecode(kwcur).lower().replace(' ','_')
                        unicode_kw[keyword]=kwcur
                        keywords.add(keyword)
            except:
                pass

            try:
                ethnic_group_name = qitems_page_titles[qitem_eg]
                keyword = unidecode.unidecode(ethnic_group_name).lower().replace(' ','_')
                unicode_kw[keyword]=ethnic_group_name
                keywords.add(keyword)
            except:
                pass

            if len(keywords) == 0: continue
            # print (languagecode, qitem_eg, keywords)

            ethnic_group_keywords[qitem_eg] = keywords



        print ('number of qitems with keywords: '+str(len(ethnic_group_keywords)))
        print (ethnic_group_keywords)


        print ('number of articles in this wikipedia: '+str(len(page_titles_qitems_unicode)))
        params = []
        i=0
        for qitem_eg, keywords in ethnic_group_keywords.items():
            for keyword in keywords:
                i+=1
                print (str(i)+ ' keywords checked. time: '+str(datetime.timedelta(seconds=time.time() - languageTime)))

                kw_compiled = re.compile(r'\b({0})\b'.format(keyword), flags=re.IGNORECASE).search

                for page_title_rect,qitem in page_titles_qitems_unicode.items():
                    ptr = page_title_rect.replace('_',' ')

                    # if keyword not in ptr:
                    #     continue 

                    if kw_compiled(ptr) == None: continue


                    try:
                        gdr = gender_qitems_lang[qitem]
                        if gdr != None: continue
                        gender = None
                    except:
                        gender = None
                        gdr = None

                    try:
                        ccc_qitems_lang[qitem]
                        ccc_binary = 1
                    except:
                        ccc_binary = 0

                    try:
                        name_primary_lang_ethnic_group = qitems_page_titles[qitem_eg]
                    except:
                        name_primary_lang_ethnic_group = ''

                    try:
                        page_title = qitems_page_titles[qitem]
                    except:
                        page_title = ''

                    params.append((page_title, gender, unicode_kw[keyword], ccc_binary,  name_primary_lang_ethnic_group, qitem, qitem_eg, languagecode))

        #             if qitem_eg == 'Q244504':
                    # print ((page_title, gender, unicode_kw[keyword], ccc_binary,  name_primary_lang_ethnic_group, qitem, qitem_eg, languagecode))

        cursor3.executemany('INSERT OR IGNORE INTO ethnic_group_articles (page_title, gender, keyword_title, ccc_binary, name_primary_lang_ethnic_group, qitem, qitem_ethnic_group, primary_lang) VALUES (?,?,?,?,?,?,?,?)', params);
        conn3.commit()

        cursor3.executemany('UPDATE ethnic_group_articles SET (page_title, gender, keyword_title, ccc_binary,  name_primary_lang_ethnic_group) = (?,?,?,?,?) WHERE qitem = ? AND qitem_ethnic_group = ? AND primary_lang = ?', params);
        conn3.commit()
        print ('number of articles inserted: '+str(len(params)))
        print (str(datetime.timedelta(seconds=time.time() - languageTime)))


    print ('keywords stored.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def store_articles_category_crawling():

    functionstartTime = time.time()
    function_name = 'store_articles_category_crawling'
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    print ('\n'+function_name)

    conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()

    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()
    df = pd.read_sql_query('SELECT qitem, native_language FROM ethnic_groups WHERE native_language != "";', conn2).set_index('qitem')
    ethnic_group_to_native_language = df.to_dict('dict')['native_language']

    conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()

    ethnic_groups_choosen_langs = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs;', conn2)


    # iterating languages
    for languagecode in wikilanguagecodes: # wikilanguagecodes: # ['ca']: 
        print ('\n* '+languagecode)
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}


        gender_qitems_lang = {}
        ccc_qitems_lang = {}
        try:
            for row in cursor.execute('SELECT qitem, page_title FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
                ccc_qitems_lang[row[0]]=row[1]
            for row in cursor.execute('SELECT qitem, gender FROM '+languagecode+'wiki WHERE gender IS NOT NULL;'):
                gender_qitems_lang[row[0]]=row[1]
        except:
            continue


        qs_ethnic_group_choosen_lang = ethnic_groups_choosen_langs.loc[(ethnic_groups_choosen_langs["lang"] == languagecode+'wiki')].ethnic_group.unique()
        print (qs_ethnic_group_choosen_lang)
     

        unicode_kw = {}
        ethnic_group_keywords = {}
        # iterating ethnic groups
        for qitem_eg in qs_ethnic_group_choosen_lang: # qs_ethnic_group_choosen_lang
            keywords = set()

            try:
                native_language_name = ethnic_group_to_native_language[qitem_eg]

                if ';' in native_language_name:
                    for qt in native_language_name.split(';'):

                        if qt in wikilanguages_qitems: 
                            continue

                        kwcur = qitems_page_titles[qt]
                        keyword = unidecode.unidecode(kwcur).lower().replace(' ','_')
                        unicode_kw[keyword]=kwcur
                        keywords.add(keyword)

                elif native_language_name not in wikilanguages_qitems: 
                        kwcur = qitems_page_titles[native_language_name]
                        keyword = unidecode.unidecode(kwcur).lower().replace(' ','_')
                        unicode_kw[keyword]=kwcur
                        keywords.add(keyword)
            except:
                pass

            try:
                ethnic_group_name = qitems_page_titles[qitem_eg]
                keyword = unidecode.unidecode(ethnic_group_name).lower().replace(' ','_')
                unicode_kw[keyword]=ethnic_group_name
                keywords.add(keyword)
            except:
                pass

            if len(keywords) == 0: continue
            # print (languagecode, qitem_eg, keywords)

            ethnic_group_keywords[qitem_eg] = keywords

        print (ethnic_group_keywords)





        # PRIMER: s’han d’haver agafat totes les categories. també les que contenen paraules clau.
        category_page_ids_page_titles = {}
        category_page_titles_page_ids = {}

        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
        wikilanguages_utils.check_dump(dumps_path, script_name)
        
        dump_in = gzip.open(dumps_path, 'r')

        while True:
            line = dump_in.readline()

            try: line = line.decode("utf-8", "ignore")
            except UnicodeDecodeError: 
                print ('error.')
                line = str(line)

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
        print (len(category_page_ids_page_titles))
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
        wikilanguages_utils.check_dump(dumps_path, script_name)
        dump_in = gzip.open(dumps_path, 'r')

        a = 0
        c = 0
        iter = 0
        while True:
            iter+=1

            line = dump_in.readline()
            try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/T264850#6535424
            except: 
                print ('error.')
                line = str(line)

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                for row in rows:

                    try:
                        page_id = int(row[0])
                        cat_title = str(row[1].strip("'"))
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

        print ('there is a total of '+str(len(ethnic_group_keywords))+' ethnic groups. Now we start the crawling.')



        # TERCER:
        # Iterar els nivells corresponents. crawling.
        k = 0
        for qitem_eg, keywords in ethnic_group_keywords.items():
            k+=1
            print ('*\n')
            print (str(k))
            print ('With language '+ languagecode +", ethnic group ("+qitem_eg+") and Keywords: ");
            ks = []
            for ke in keywords: ks.append(unicode_kw[ke])
            print (ks)

            selectedarticles = {}
            selectedarticles_level = {}

            keyword_category = {}
            for val in keywords:
                if val != None:
                    keyword_category[val.lower()]=set()


            for ke in keywords:
                k_unicode = unidecode.unidecode(ke.lower().replace('_',' '))

                kw_compiled = re.compile(r'\b({0})\b'.format(k_unicode), flags=re.IGNORECASE).search

                for cat_title in category_page_ids_page_titles.values():

                    cat_title_normalized = unidecode.unidecode(cat_title.lower().replace('_',' '))

                    if kw_compiled(cat_title_normalized) == None: continue

                    keyword_category[ke.lower()].add(cat_title)


            # ITERATIONS
            for keyword in keywords:
                kw = keyword.lower()
                cattitles_total_level = {}

                if kw == '' or kw == None: continue
                for cat_title in keyword_category[kw.lower()]:
                    cattitles_total_level[cat_title] = None

                if len(cattitles_total_level) == 0: continue


                # CATEGORIES FROM LEVELS
                level = 1
                num_levels = 5
                if languagecode=='en': num_levels = 5
                j = 0
                total_categories = dict(); total_categories.update(cattitles_total_level)
                print ('Number of categories to start: '+str(len(total_categories)))


                # print (total_categories)
                # input('')

                while (level <= num_levels): # Here we choose the number of levels we prefer.
                    i = 0

                    newcategories = dict()
                    for cat_title in cattitles_total_level.keys():

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
                                    selectedarticles[page_id].add(kw)
                                else:
                                    selectedarticles[page_id] = {kw}

                                i += 1

                        else:
                            for page_id in category_links_cat_art_dict[cat_title]:
                                try:
                                    cur_level = selectedarticles_level[page_id]
                                    if cur_level > level: selectedarticles_level[page_id] = level
                                except:
                                    selectedarticles_level[page_id] = level

                                if page_id in selectedarticles:
                                    selectedarticles[page_id].add(kw)
                                else:
                                    selectedarticles[page_id] = {kw}

                                i += 1

                    cattitles_total_level = dict()
                    cattitles_total_level.update(newcategories)
                    total_categories.update(newcategories)

                    print('Level: '+str(level) + ". Number of new articles is: " + str(i)+ ". Total number of articles is "+str(len(selectedarticles_level))+'. Number of new categories is: '+str(len(newcategories))+'. Total number of categories is: '+str(len(total_categories)))

                    level = level + 1
                    if len(newcategories) == 0: 
                        print ('No new categories: break!')
                        break


            ##### ------------------ #####

            # GETTING READY TO INSERT
            parameters = []
            for page_id, elements in selectedarticles.items():        
                try: 
                    page_title = page_ids_page_titles[page_id]
                except: 
                    continue
                try:
                    qitem = page_titles_qitems[page_title]
                except:
                    qitem=None
                categorycrawling = ";".join(elements)

                absolute_level = selectedarticles_level[page_id]
                relative_level = absolute_level/(level-1)


                try:
                    gdr = gender_qitems_lang[qitem]
                    if gdr == 'Q6581072': gender = 1
                    elif gdr == 'Q6581097': gender = 2
                except:
                    gender = None

                try:
                    ccc_qitems_lang[qitem]
                    ccc_binary = 1
                except:
                    ccc_binary = 0

                try:
                    name_primary_lang_ethnic_group = qitems_page_titles[qitem_eg]
                except:
                    name_primary_lang_ethnic_group = ''

                try:
                    page_title = page_ids_page_titles[page_id]
                except:
                    page_title = ''


                parameters.append((page_title, gender, ccc_binary,  name_primary_lang_ethnic_group, absolute_level, relative_level, qitem, qitem_eg, languagecode))

            #     print ((page_title, gender, keyword, ccc_binary,  name_primary_lang_ethnic_group, absolute_level, relative_level, qitem, qitem_eg, languagecode))

            cursor3.executemany('INSERT OR IGNORE INTO ethnic_group_articles (page_title, gender, ccc_binary, name_primary_lang_ethnic_group, category_crawling_absolute_level, category_crawling_relative_level, qitem, qitem_ethnic_group, primary_lang) VALUES (?,?,?,?,?,?,?,?,?)', parameters);
            conn3.commit()

            cursor3.executemany('UPDATE ethnic_group_articles SET (page_title, gender, ccc_binary,  name_primary_lang_ethnic_group, category_crawling_absolute_level, category_crawling_relative_level) = (?,?,?,?,?,?) WHERE qitem = ? AND qitem_ethnic_group = ? AND primary_lang = ?', parameters);
            conn3.commit()
            print ('Total number of articles collected: '+str(len(parameters)))


        try:
            os.remove(databases_path + languagecode + 'wiki_category_links_temp.db'); print (languagecode + 'wiki_category_links_temp.db'+' deleted.');
        except:
            pass


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print ('category crawling stored.')
    print (duration)


    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def store_articles_links():

    functionstartTime = time.time()
    function_name = 'label_potential_ethnic_groups_content_with_links'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    print ('\n'+function_name)


    for languagecode in wikilanguagecodes: #['ca']: # wikilanguagecodes[wikilanguagecodes.index('sco')+1:]

        print ('\n* '+languagecode)
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        page_ids_qitems = {v: page_titles_qitems[k] for k, v in page_titles_page_ids.items()}
        z = []
        for x,y in page_ids_qitems.items():
            if y==None: z.append(x)
        for i in z: del page_ids_qitems[i]


        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-pagelinks.sql.gz'
        try:
            dump_in = gzip.open(dumps_path, 'r')
        except:
            continue

        conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()
        conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()


        gender_qitems_lang = {}
        ccc_qitems_lang = {}
        try:
            for row in cursor.execute('SELECT qitem, page_title FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
                ccc_qitems_lang[row[0]]=row[1]
            for row in cursor.execute('SELECT qitem, gender FROM '+languagecode+'wiki WHERE gender IS NOT NULL;'):
                gender_qitems_lang[row[0]]=row[1]
        except:
            continue

        current_database_qitems = {}
        for row in cursor3.execute('SELECT qitem FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'"'):
            current_database_qitems[row[0]]=''
        current_database_qitems_list = list(current_database_qitems.keys())


        ethnic_groups_choosen_langs = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs WHERE ccc = "yes" AND count > 0;', conn2)

        qs_ethnic_group_choosen_lang = ethnic_groups_choosen_langs.loc[(ethnic_groups_choosen_langs["lang"] == languagecode+'wiki')].ethnic_group.unique()

        print ('This is the number of ethnic groups we will look for content in this language: '+ languagecode)
        print (qs_ethnic_group_choosen_lang)
        print (len(qs_ethnic_group_choosen_lang))

        label_primary_lang_ethnic_group = {}
        for qitem_ethnic_group in qs_ethnic_group_choosen_lang:
            try:
                label_primary_lang_ethnic_group[qitem_ethnic_group]=qitems_page_titles[qitem_ethnic_group]
            except:
                continue

        increment = 1000
        i = 0
        j = increment
        current = qs_ethnic_group_choosen_lang[i:j]
        current_len = len(current)


        while current_len > 0:
            print ('Iteration '+str((i/increment))+' with this number of ethnic groups: '+str((j-i)))


            page_asstring = ','.join( ['?'] * len(current) )


            query = 'SELECT page_title, qitem_ethnic_group FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'" AND qitem_ethnic_group IN ('+page_asstring+') AND ((gender IS NOT NULL AND ethnic_group_binary = 1) OR (keyword_title IS NOT NULL AND gender IS NULL) OR (category_crawling_absolute_level = 1)) ORDER BY page_title;'

#            query = 'SELECT page_id, ethnic_group FROM '+languagecode+'wiki WHERE ethnic_group IN ('+page_asstring+') AND (gender IS NOT NULL AND ethnic_group_binary = 1) ORDER BY page_id;'

            page_ids_ethnic_groups = {}
            old_page_id = ''
            current_ethnic_group = set()
            for row in cursor3.execute(query, current):
                try: page_id = page_titles_page_ids[row[0]]
                except: continue

                ethnic_group = row[1]

                # print (row)

                if page_id != old_page_id and old_page_id != '':
                    page_ids_ethnic_groups[old_page_id]=current_ethnic_group
                    current_ethnic_group = set()

                current_ethnic_group.add(ethnic_group)
                old_page_id = page_id

            page_ids_ethnic_groups[old_page_id]=current_ethnic_group
            print (len(page_ids_ethnic_groups))

            # print (page_ids_ethnic_groups)
            # input('')

            current_ethnic_group = set()


            qitems = list(page_ids_qitems.values())

            # print ('creating dataframes.')
            # num_of_outlinks_to_ethnic_group= pd.DataFrame(index = [], columns = list(current))
            # num_of_inlinks_from_ethnic_groups= pd.DataFrame(index = [], columns = list(current))

            num_of_outlinks_to_ethnic_group= {}
            num_of_inlinks_from_ethnic_groups= {}

            # num_of_outlinks_to_ethnic_group = num_of_outlinks_to_ethnic_group.fillna(0)
            # num_of_inlinks_from_ethnic_groups = num_of_inlinks_from_ethnic_groups.fillna(0)

            num_of_outlinks = {}
            num_of_inlinks = {}
            for qitem in page_ids_qitems.values():
                num_of_outlinks[qitem]=0
                num_of_inlinks[qitem]=0

            w = 0
            v = 0
            iteratingstartTime = time.time()
            print ('Iterating the dump.')
            while True:
                line = dump_in.readline()
                try: line = line.decode("utf-8")
                except UnicodeDecodeError: line = str(line)

                v+= 1
                if line == '':
                    i+=1
                    if i==3: break
                else: i=0

                if wikilanguages_utils.is_insert(line):
                    values = wikilanguages_utils.get_values(line)
                    if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                    for row in rows:
                        w+=1
        #                print(row)
                        pl_from = int(row[0])
                        pl_from_namespace = row[1]
                        pl_title = str(row[2])
                        pl_namespace = row[3]

                        try:
                            page_id = page_titles_page_ids[pl_title]
                        except:
                            pass

                        get_current_value = 0
                        if pl_from_namespace != '0' or pl_namespace != '0': continue

                        try:
                            pl_title_page_id = int(page_titles_page_ids[pl_title])
                        except:
                            pl_title_page_id = None

                        try:
                            pl_from_qitem = page_ids_qitems[pl_from]
                        except:
                            continue
                        try:
                            pl_title_qitem = page_titles_qitems[pl_title]
                        except:
                            continue



                        # basic outlinks / inlinks
                        try:
                            num_of_outlinks[pl_from_qitem]= num_of_outlinks[pl_from_qitem] + 1
                        except:
                            pass
                        try:
                            num_of_inlinks[pl_title_qitem] = num_of_inlinks[pl_title_qitem] + 1
                        except:
                            pass


                        try:
                            eth_outlinks = page_ids_ethnic_groups[pl_title_page_id]
                        except:
                            eth_outlinks = []

                        try:
                            eth_inlinks = page_ids_ethnic_groups[pl_from]
                        except:
                            eth_inlinks = []


                        for qitem in eth_outlinks:
                            try:
                                get_current_value = num_of_outlinks_to_ethnic_group[pl_from_qitem][qitem]
                                num_of_outlinks_to_ethnic_group[pl_from_qitem][qitem] = get_current_value + 1
                            except:
                                try:
                                    qitem_dict = num_of_outlinks_to_ethnic_group[pl_from_qitem][qitem] = 1
                                except:
                                    num_of_outlinks_to_ethnic_group[pl_from_qitem] = {qitem:1}


                        for qitem in eth_inlinks:
                            try:
                                get_current_value = num_of_inlinks_from_ethnic_groups[pl_title_qitem][qitem]
                                num_of_inlinks_from_ethnic_groups[pl_title_qitem][qitem] = get_current_value + 1
                            except:
                                try:
                                    num_of_inlinks_from_ethnic_groups[pl_title_qitem][qitem] = 1
                                except:
                                    num_of_inlinks_from_ethnic_groups[pl_title_qitem] = {qitem:1}


                        if w % 1000000 == 0: # 10 million
                            print (w, v)
                            print ('current time: ' + str(time.time() - iteratingstartTime)+ ' '+languagecode)
                            print ('number of lines per second: '+str(round(((w/(time.time() - iteratingstartTime))/1000),2))+ ' thousand.')

                            # print (num_of_inlinks_from_ethnic_groups);
                            # print (num_of_outlinks_to_ethnic_group);

            print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
            print ('Done with the dump.')


            parameters = []
            parameters_other = []
            for qitem_of_article in page_ids_qitems.values():
                page_title = qitems_page_titles[qitem_of_article]

                gender = None
                try:
                    gdr = gender_qitems_lang[qitem_of_article]
                    if gdr == 'Q6581072': gender = 1
                    elif gdr == 'Q6581097': gender = 2
                except:
                    gender = None

                ccc_binary = None
                try:
                    ccc_qitems_lang[qitem_of_article]
                    ccc_binary = 1

                except:
                    ccc_binary = 0

                try:
                    num_inlinks = num_of_inlinks[qitem_of_article]
                except:
                    num_inlinks = 0

                try:
                    num_outlinks = num_of_outlinks[qitem_of_article]
                except:
                    num_outlinks = 0

                parameters_other.append((num_outlinks, num_inlinks, qitem_of_article, languagecode))
                # print ((num_outlinks, num_inlinks, qitem_of_article, languagecode))


                for qitem_ethnic_group in current:
                    try:
                        name_primary_lang_ethnic_group = label_primary_lang_ethnic_group[qitem_ethnic_group]
                    except:
                        name_primary_lang_ethnic_group = None

                    try:
                        num_inlinks_from_etgrup = num_of_inlinks_from_ethnic_groups[qitem_of_article][qitem_ethnic_group]
                    except:
                        num_inlinks_from_etgrup = 0
                    try:
                        num_outlinks_to_etgrup = num_of_outlinks_to_ethnic_group[qitem_of_article][qitem_ethnic_group]
                    except:
                        num_outlinks_to_etgrup = 0

                    if num_inlinks_from_etgrup == 0 and num_outlinks_to_etgrup == 0: continue

                    if num_outlinks!= 0: percent_outlinks_to_group = float(num_outlinks_to_etgrup)/float(num_outlinks)
                    else: percent_outlinks_to_group = 0

                    if num_inlinks!= 0: percent_inlinks_from_group = float(num_inlinks_from_etgrup)/float(num_inlinks)
                    else: percent_inlinks_from_group = 0

                    parameters.append((page_title, gender, ccc_binary, name_primary_lang_ethnic_group, num_outlinks,num_outlinks_to_etgrup,percent_outlinks_to_group,num_inlinks,num_inlinks_from_etgrup,percent_inlinks_from_group,qitem_of_article,qitem_ethnic_group,languagecode))

                    # if qitem_ethnic_group == 'Q1131768':
                    #     print ((page_title, gender, ccc_binary, name_primary_lang_ethnic_group, num_outlinks,num_outlinks_to_etgrup,percent_outlinks_to_group,num_inlinks,num_inlinks_from_etgrup,percent_inlinks_from_group,qitem_of_article,qitem_ethnic_group,languagecode))

            query_insert = 'INSERT OR IGNORE INTO ethnic_group_articles (page_title, gender, ccc_binary, name_primary_lang_ethnic_group, num_outlinks, num_outlinks_to_group, percent_outlinks_to_group, num_inlinks, num_inlinks_from_group, percent_inlinks_from_group, qitem, qitem_ethnic_group, primary_lang) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);'
            cursor3.executemany(query_insert,parameters)
            conn3.commit()
            
            query_insert = 'UPDATE ethnic_group_articles SET (page_title, gender, ccc_binary, name_primary_lang_ethnic_group, num_outlinks, num_outlinks_to_group, percent_outlinks_to_group, num_inlinks, num_inlinks_from_group, percent_inlinks_from_group) = (?,?,?,?,?,?,?,?,?,?) WHERE qitem = ? AND qitem_ethnic_group = ? AND primary_lang=?;'
            cursor3.executemany(query_insert,parameters)
            conn3.commit()

            query_insert = 'UPDATE ethnic_group_articles SET (num_outlinks, num_inlinks) = (?,?) WHERE qitem = ? AND primary_lang=?;'
            cursor3.executemany(query_insert,parameters_other)
            conn3.commit()

            print ('* Done with the iteration with languagecode: '+languagecode)

            i+= increment
            j+= increment
            current = current[i:j]
            current_len = len(current)
            if current_len == 0: print ('There are no qitems left for a new iteration.')
            else: print ('These are the qitems left for the next iteration: '+str(current_len))


    print ('links stored.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def store_articles_ethnic_group_binary_classifier():

    function_name = 'store_ethnic_group_binary_classifier'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()
    ethnic_groups_choosen_langs = pd.read_sql_query('SELECT count, ethnic_group, lang, ccc, cycle_year_month, choosen_lang FROM ethnic_groups_choosen_langs WHERE ccc = "yes" AND count >= 1;', conn2)


    # iterate languages
    for languagecode in wikilanguagecodes: #['ca']: # wikilanguagecodes

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        print ('\n\n\n*** Obtaining the final Ethnic Groups for language: ' +languagecode+ ' '+languages.loc[languagecode]['languagename'])
  
        qs_ethnic_group_choosen_lang = ethnic_groups_choosen_langs.loc[(ethnic_groups_choosen_langs["lang"] == languagecode+'wiki')].ethnic_group.unique()

        print ('This is the number of ethnic groups we will look for content in this language: '+ languagecode)

        label_primary_lang_ethnic_group = {}
        for qitem_ethnic_group in qs_ethnic_group_choosen_lang:
            try:
                label_primary_lang_ethnic_group[qitem_ethnic_group]=qitems_page_titles[qitem_ethnic_group]
            except:
                continue

        print (label_primary_lang_ethnic_group)
        eg_groups_count = str(len(qs_ethnic_group_choosen_lang))
        k = 0


        # iterate ethnic groups
        for qitem_ethnic_group in qs_ethnic_group_choosen_lang: # qs_ethnic_group_choosen_lang # Q1131768 # Q244504
            k+=1

            try:
                eg_name_en = ethnic_groups.loc[(ethnic_groups["lang"] == 'en') & (ethnic_groups["qitem"] == qitem_ethnic_group)]['label'].values[0]
            except:
                eg_name_en = ''

            try:
                eg_name = label_primary_lang_ethnic_group[qitem_ethnic_group]
            except:
                eg_name = ''

            print ('\n### ' +languagecode+ ' '+languages.loc[languagecode]['languagename']+' / ethnic group: '+str(k)+'/'+str(eg_groups_count)+' '+qitem_ethnic_group+' ('+eg_name+ '). English name: '+eg_name_en)


             # OBTAIN THE DATA TO FIT.
            conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()
            query = 'SELECT * FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'" AND qitem_ethnic_group = "'+qitem_ethnic_group+'";'
            ethnic_df = pd.read_sql_query(query, conn3)


            features = ['qitem']+['gender', 'ccc_binary', 'keyword_title', 'category_crawling_absolute_level', 'category_crawling_relative_level', 'num_inlinks_from_group', 'num_outlinks_to_group', 'percent_inlinks_from_group', 'percent_outlinks_to_group', 'ethnic_group_binary']

            ethnic_df = ethnic_df[features]
            ethnic_df = ethnic_df.set_index(['qitem'])

            if len(ethnic_df.index.tolist())==0: 
                print ('It is not possible to classify Wikipedia Articles as there is no groundtruth.'); 
                break
            ethnic_df = ethnic_df.fillna(0)




            # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
            try:
                category_crawling_absolute_level=ethnic_df['category_crawling_absolute_level'].tolist()
                maxlevel = max(category_crawling_absolute_level)

                for n, i in enumerate(category_crawling_absolute_level):
                    if i > 0:
                        category_crawling_absolute_level[n]=abs(i-(maxlevel+1))
                    else:
                        category_crawling_absolute_level[n]=0
                ethnic_df = ethnic_df.assign(category_crawling_absolute_level = category_crawling_absolute_level)
            except:
                pass

            try:
                category_crawling_relative_level=ethnic_df['category_crawling_relative_level'].tolist()
                maxlevel = max(category_crawling_absolute_level)
                for n, i in enumerate(category_crawling_absolute_level):
                    if i > 0:                   
                        category_crawling_relative_level[n]=i/maxlevel
                    else:
                        category_crawling_relative_level[n]=0
                ethnic_df = ethnic_df.assign(category_crawling_relative_level = category_crawling_relative_level)
            except:
                pass


            keyword_title=ethnic_df['keyword_title'].tolist()
            for n, i in enumerate(keyword_title):
                if i != 0: keyword_title[n]=1
                else: keyword_title[n]=0
            ethnic_df = ethnic_df.assign(keyword_title = keyword_title)



            # SAMPLING
            print ('sampling method: negative sampling.')


            # # WE GET THE GROUNDTRUTH
            ethnic_group_df_yes = ethnic_df.loc[

                (ethnic_df['gender'].notnull() & ethnic_df['ethnic_group_binary']==1) 

                | 

                (
                    ethnic_df['keyword_title']==1 
                )

                |

                (
                    (ethnic_df['category_crawling_absolute_level'] == maxlevel)
                    # &
                    # ((ethnic_df['num_outlinks_to_group'] > 0) | (ethnic_df['num_inlinks_from_group']>0))
                )

                ] # positives are the biographies


            # ethnic_group_df_yes = ethnic_df.loc[(ethnic_df['gender'].notnull()) & (ethnic_df['ethnic_group_binary']==1)] 


            ethnic_group_df_yes = ethnic_group_df_yes.drop(columns=['ethnic_group_binary'])

            ethnic_group_df_yes = ethnic_group_df_yes.drop(columns=['gender'])
            ethnic_df = ethnic_df.drop(columns=['gender'])

            # ethnic_group_df_yes = ethnic_group_df_yes.drop(columns=['keyword_title'])
            # ethnic_df = ethnic_df.drop(columns=['keyword_title'])

            ethnic_df_list_yes = ethnic_group_df_yes.values.tolist()
            num_articles_ethno = len(ethnic_df_list_yes)

            print (len(ethnic_group_df_yes));

            
            ethnic_df_list_probably_no = []
            size_sample = 16
            if languagecode == 'en': size_sample = 8 # exception for English

            for i in range(1,1+size_sample):
                ethnic_df = ethnic_df.sample(frac=1) # randomize the rows order


                ethnic_df_probably_no = ethnic_df.loc[(ethnic_df['num_outlinks_to_group'] < 2) & (ethnic_df['num_inlinks_from_group'] < 2) & (ethnic_df['category_crawling_relative_level'] == 0)]



                ethnic_df_probably_no = ethnic_df_probably_no.drop(columns=['ethnic_group_binary'])
                ethnic_df_list_probably_no = ethnic_df_list_probably_no + ethnic_df_probably_no.values.tolist()[:num_articles_ethno]

                num_probably_no = len(ethnic_df_list_probably_no)
                ethnic_df_list = ethnic_df_list_yes + ethnic_df_list_probably_no
                binary_list = [1]*num_articles_ethno+[0]*num_probably_no

            print ('\nConverting the dataframe...')
            print ('These are its columns:')
            print (list(ethnic_group_df_yes.columns.values))

            print (num_articles_ethno) # these are the yes (groundtruth)
            print (len(ethnic_df_list)) # these are the no (groundtruth)
            print (len(binary_list))


            # WE GET THE POTENTIAL ETHNIC GROUP ARTICLES THAT HAVE NOT BEEN 1 BY ANY OTHER MEANS.
            # For the testing takes those with one of these features category crawling, els keywords i els outlinks a ethnic groups > 0.
            # query = 'SELECT * FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'" AND qitem_ethnic_group = "'+qitem_ethnic_group+' AND category_crawling_absolute_level IS NOT NULL AND ethnic_group_binary != 1 AND (num_outlinks_to_group > 0 OR num_inlinks_from_group > 0)";'
            query = 'SELECT * FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'" AND qitem_ethnic_group = "'+qitem_ethnic_group+'" AND ethnic_group_binary IS NULL AND category_crawling_absolute_level IS NOT NULL;'
            potential_ethnic_df = pd.read_sql_query(query, conn3)


            features = ['qitem']+['ccc_binary', 'keyword_title', 'category_crawling_absolute_level', 'category_crawling_relative_level', 'num_inlinks_from_group', 'num_outlinks_to_group', 'percent_inlinks_from_group', 'percent_outlinks_to_group']

            potential_ethnic_df = potential_ethnic_df[features]
            potential_ethnic_df = potential_ethnic_df.set_index(['qitem'])
            potential_ethnic_df = potential_ethnic_df.fillna(0)

            # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
            try:
                category_crawling_absolute_level=potential_ethnic_df['category_crawling_absolute_level'].tolist()
                maxlevel = max(category_crawling_absolute_level)
                for n, o in enumerate(category_crawling_absolute_level):
                    if o > 0:
                        category_crawling_absolute_level[n]=abs(i-(maxlevel+1))
                    else:
                        category_crawling_absolute_level[n]=0
                potential_ethnic_df = potential_ethnic_df.assign(category_crawling_absolute_level = category_crawling_absolute_level)
            except:
                pass

            try:
                category_crawling_relative_level=potential_ethnic_df['category_crawling_relative_level'].tolist()
                maxlevel = max(category_crawling_absolute_level)
                for n, p in enumerate(category_crawling_absolute_level):
                    if p > 0:                   
                        category_crawling_relative_level[n]=i/maxlevel
                    else:
                        category_crawling_relative_level[n]=0
                potential_ethnic_df = potential_ethnic_df.assign(category_crawling_relative_level = category_crawling_relative_level)
            except:
                pass


            keyword_title=potential_ethnic_df['keyword_title'].tolist()
            for n, i in enumerate(keyword_title):
                if i != 0: keyword_title[n]=1
                else: keyword_title[n]=0
            potential_ethnic_df = potential_ethnic_df.assign(keyword_title = keyword_title)



            # NOT ENOUGH ARTICLES
            if len(potential_ethnic_df)==0: 
                print ('There are not potential Ethnic articles for '+qitem_ethnic_group+', so it returns empty'); 
                continue
            potential_ethnic_df = potential_ethnic_df.sample(frac=1) # randomize the rows order

            print ('We selected this number of potential Ethnic articles: '+str(len(potential_ethnic_df)))

           

            # FIT THE SVM MODEL
            print ('Fitting the data into the classifier.')
            print ('The data has '+str(len(ethnic_df_list))+' samples.')
            if num_articles_ethno == 0 or len(ethnic_df_list)<10: 
                print ('There are not enough samples.'); 
                continue

            X = ethnic_df_list
            y = binary_list
        #    print (X)
        #    print (y)

            print ('The chosen classifier is RandomForest')
            clf = RandomForestClassifier(n_estimators=100)
            clf.fit(X, y)

            print ('The fit classes are: '+str(clf.classes_))
            print ('The fit has a score of: '+str(clf.score(X, y, sample_weight=None)))
            print (clf.feature_importances_.tolist())



            # TEST THE DATA
            print ('Calculating which page is IN or OUT...')
            if potential_ethnic_df is None: 
                print ('No Articles to verify.'); 
                continue     
            if len(potential_ethnic_df)==0: 
                print ('No Articles to verify.'); 
                continue

            qitems = potential_ethnic_df.index.tolist()
            potential = potential_ethnic_df.values.tolist()

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
                        selected.append((qitem, qitem_ethnic_group, languagecode))
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
                    if result[0] == 1:
                        count_yes+=1
                        print (['ccc_binary','keyword_title','category_crawling_absolute_level', 'category_crawling_relative_level', 'num_inlinks_from_group', 'num_outlinks_to_group', 'percent_inlinks_from_group', 'percent_outlinks_to_group'])
                        print(i)
                        print(clf.predict_proba([i]).tolist())

                        try: 
                            selected.append((qitem, qitem_ethnic_group, languagecode))
                            print (str(count_yes)+'\tIN\t'+qitem+':'+qitems_page_titles[qitem]+'.\n')
                        except: pass
                        # input('')

                    else:
                        count_no+=1
                        print (['ccc_binary','keyword_title','category_crawling_absolute_level', 'category_crawling_relative_level', 'num_inlinks_from_group', 'num_outlinks_to_group', 'percent_inlinks_from_group', 'percent_outlinks_to_group'])
                        print(i)
                        print(clf.predict_proba([i]).tolist())
                        try:
                            print (str(count_no)+'\tOUT:\t'+qitem+':'+qitems_page_titles[qitem]+'.\n')
                        except:
                            pass

                        input('')

            print ('There were already '+str(num_articles_ethno)+' articles selected as groundtruth.')

            print ('\nThis algorithm CLASSIFIED '+str(len(selected))+' Articles as ethnic_group_binary = 1 from a total of '+str(len(potential))+' from the testing data. This is a: '+str(round(100*len(selected)/len(potential),3))+'%.')

            conn = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor = conn.cursor()
            query = 'UPDATE ethnic_group_articles SET ethnic_group_binary = 1 WHERE qitem = ? AND qitem_ethnic_group = ? AND primary_lang=?;'
            cursor.executemany(query,selected)
            conn.commit()

            print ('Ethnic group topics '+qitem_ethnic_group+' '+(languagecode)+' created.')


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def update_push_ethnic_group_topic_wikipedia_diversity():

    function_name = 'update_push_ethnic_group_topic_wikipedia_diversity'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    for languagecode in wikilanguagecodes:

        conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()

        qitem_ethnic_groups = {}
        query = 'SELECT qitem, qitem_ethnic_group FROM ethnic_group_articles WHERE primary_lang = "'+languagecode+'" AND ethnic_group_binary = 1 ORDER BY qitem;'

        for row in cursor3.execute(query):

            qitem = row[0]
            qitem_ethnic_group = row[1]

            try:
                qitem_ethnic_groups[qitem]=qitem_ethnic_groups[qitem]+';'+qitem_ethnic_group
            except:
                qitem_ethnic_groups[qitem]=qitem_ethnic_group


        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

        params = []
        for q, ethnic_groups in qitem_ethnic_groups.items():
            page_title = qitems_page_titles[q]
            page_id = page_titles_page_ids[page_title]
            params.append((ethnic_groups, q, page_title, page_id))

        query = 'UPDATE '+languagecode+'wiki SET ethnic_group_topic = NULL;'
        cursor.execute(query)

        query = 'UPDATE '+languagecode+'wiki SET ethnic_group_topic = ? WHERE qitem = ? AND page_title = ? AND page_id = ?;'
        cursor.executemany(query,params)
        conn.commit()


def get_relevance_features_wikipedia_diversity():

    function_name = 'get_relevance_features_wikipedia_diversity'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    for languagecode in wikilanguagecodes:
        print (languagecode)

        conn3 = sqlite3.connect(databases_path + ethnic_groups_content_db); cursor3 = conn3.cursor()
        qitem_ethnic_groups = []
        query = 'SELECT DISTINCT qitem FROM ethnic_group_articles WHERE ethnic_group_binary = 1 AND primary_lang = "'+languagecode+'";'
        for row in cursor3.execute(query):
            qitem = row[0];
            qitem_ethnic_groups.append(qitem)


        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()      
        while (len(qitem_ethnic_groups) > 0):

            sample = qitem_ethnic_groups[:50000]

            page_asstring = ','.join( ['?'] * len(sample) );
            query = 'SELECT qitem, page_id, date_created, folk, earth, monuments_and_buildings, music_creations_and_organizations, sport_and_teams, food, paintings, glam, books, clothing_and_fashion, industry, religion, time_interval, start_time, end_time, num_bytes, num_references, num_edits, num_edits_last_month, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, num_images FROM '+languagecode+'wiki WHERE qitem IN (%s);' % page_asstring

            params = []
            k = 0
            for row in cursor.execute(query, sample):
                k+=1

                qitem = row[0]
                page_id = row[1]
                date_created = row[2]
                folk = row[3]
                earth = row[4]
                monuments_and_buildings = row[5]
                music_creations_and_organizations = row[6]
                sport_and_teams = row[7]
                food = row[8]
                paintings = row[9]
                glam = row[10]
                books = row[11]
                clothing_and_fashion = row[12]
                industry = row[13]
                religion = row[14]
                time_interval = row[15]
                start_time = row[16]
                end_time = row[17]
                num_bytes = row[18]
                num_references = row[19]
                num_edits = row[20]
                num_edits_last_month = row[21]
                num_editors = row[22]
                num_discussions = row[23]
                num_pageviews = row[24]
                num_wdproperty = row[25]
                num_interwiki = row[26]
                num_images = row[27]

                params.append((page_id, date_created, folk, earth, monuments_and_buildings, music_creations_and_organizations, sport_and_teams, food, paintings, glam, books, clothing_and_fashion, industry, religion, time_interval, start_time, end_time, num_bytes, num_references, num_edits, num_edits_last_month, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, num_images, qitem, languagecode))
        
            query = 'UPDATE ethnic_group_articles SET page_id = ?, date_created = ?, folk = ?, earth = ?, monuments_and_buildings = ?, music_creations_and_organizations = ?, sport_and_teams = ?, food = ?, paintings = ?, glam = ?, books = ?, clothing_and_fashion = ?, industry = ?, religion = ?, time_interval = ?, start_time = ?, end_time = ?, num_bytes = ?, num_references = ?, num_edits = ?, num_edits_last_month = ?, num_editors = ?, num_discussions = ?, num_pageviews = ?, num_wdproperty = ?, num_interwiki = ?, num_images = ? WHERE qitem = ? AND primary_lang = ?;'
            cursor3.executemany(query,params)
            conn3.commit()

            qitem_ethnic_groups = qitem_ethnic_groups[50000:]
            print (len(qitem_ethnic_groups))
    
        print ('inserted')



#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("ethnic_groups_content_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("ethnic_groups_content_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':

    script_name = 'ethnic_groups_content_selection.py'

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
    startTime = time.time()


    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = languages.index.tolist()
    wikilanguages_qitems = languages.Qitem.tolist()

    print (wikilanguagecodes)
    input('')
    wikilanguagecodes = wikilanguagecodes[wikilanguagecodes.index('smn')+1:]
#    wikilanguagecodes = ['en']
    # wikilanguagecodes = ['ro','ja','af','ca']
    # wikilanguagecodes = ['ro','it','fr','gl','ru','zh','is','ja','vi','oc','sw','af','ca']
    # wikilanguagecodes = ['an', 'sco', 'ko', 'et', 'jam', 'pam', 'pnb', 'ast', 'bar', 'hr', 'la', 'lad', 'ln', 'mk']

    # select qitem_ethnic_group, name_primary_lang_ethnic_group, count(*) from ethnic_group_articles where primary_lang = 'en' and ethnic_group_binary = 1 group by 1 order by 3 desc limit 100;

    print (wikilanguagecodes)


    ethnic_groups = wikilanguages_utils.get_ethnic_groups_labels()
    # group_labels = wikilanguages_utils.get_diversity_categories_labels()

    # if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit();
    main()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))
    # wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)

    wikilanguages_utils.finish_email(startTime,'ethnic_groups_content_selection.out','ETHNIC GROUPS CONTENT CONTENT Selection')
