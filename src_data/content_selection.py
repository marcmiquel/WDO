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
import urllib
import webbrowser
import reverse_geocoder as rg
import numpy as np
from random import shuffle
# data
import pandas as pd
# classifier
from sklearn import svm, linear_model
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
import gc



# MAIN
def main():

    execution_block_potential_ccc_features()
    execution_block_classifying_ccc()


################################################################

def execution_block_potential_ccc_features():

    wikilanguages_utils.send_email_toolaccount('WDO - CONTENT SELECTION', '# INTRODUCE THE ARTICLE CCC FEATURES')
    functionstartTime = time.time()
    function_name = 'execution_block_potential_ccc_features'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    for languagecode in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        label_potential_ccc_articles_category_crawling(languagecode,page_titles_page_ids,page_titles_qitems)
        label_potential_ccc_articles_language_weak_wd(languagecode,page_titles_page_ids)
        label_potential_ccc_articles_affiliation_properties_wd(languagecode,page_titles_qitems)
        label_potential_ccc_articles_has_part_properties_wd(languagecode,page_titles_page_ids)
        label_potential_ccc_articles_with_links(languagecode,page_titles_page_ids,page_titles_qitems)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def execution_block_classifying_ccc():

    wikilanguages_utils.send_email_toolaccount('WDO - CONTENT SELECTION', '# CLASSIFYING AND CREATING THE DEFINITIVE CCC')
    functionstartTime = time.time()
    function_name = 'execution_block_classifying_ccc'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    # Classifying and creating the definitive CCC
    for languagecode in wikilanguagecodes: groundtruth_reaffirmation(languagecode)

    biggest = wikilanguagecodes_by_size[:20]
    smallest = wikilanguagecodes_by_size[20:]

    for languagecode in biggest:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        calculate_articles_ccc_binary_classifier(languagecode,'RandomForest',page_titles_page_ids,page_titles_qitems);

    for languagecode in smallest:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        calculate_articles_ccc_binary_classifier(languagecode,'RandomForest',page_titles_page_ids,page_titles_qitems);

    for languagecode in wikilanguagecodes: 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        calculate_articles_ccc_main_territory(languagecode)
        calculate_articles_ccc_retrieval_strategies(languagecode)


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



################################################################

# DIVERSITY FEATURES
####################
# Obtain the Articles contained in the Wikipedia categories with a keyword in title (recursively). This is considered potential CCC.
def label_potential_ccc_articles_category_crawling(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'label_potential_ccc_articles_category_crawling '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    # CREATING KEYWORDS DICTIONARY
    keywordsdictionary = {}
    if languagecode not in languageswithoutterritory:
        try: qitems=territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems=[];qitems.append(territories.loc[languagecode]['QitemTerritory'])
        for qitem in qitems:
            keywords = []
            # territory in Native language
            territorynameNative = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territorynameNative']
            # demonym in Native language
            try: 
                demonymsNative = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['demonymNative'].split(';')
                # print (demonymsNative)
                for demonym in demonymsNative:
                    if demonym!='':keywords.append(demonym.strip())
            except: pass
            keywords.append(territorynameNative)
            keywordsdictionary[qitem]=keywords

    # language name
    languagenames = languages.loc[languagecode]['nativeLabel'].split(';')
    qitemresult = languages.loc[languagecode]['Qitem']
    keywordsdictionary[qitemresult]=languagenames

    keyword_category = {}
    keywords = []
    for values in keywordsdictionary.values():
        for val in values:
            if val != None:
                keyword_category[val.lower()]=set()
                keywords.append(val.lower())


    # PRIMER: s’han d’haver agafat totes les categories. també les que contenen paraules clau.
    category_page_ids_page_titles = {}
    category_links_cat_cat = {}
    category_links_cat_art = {}

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
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
                page_id = int(row[0])
                page_namespace = int(row[1])
                cat_title = str(row[2])

                if page_namespace != 14: continue
                category_page_ids_page_titles[page_id]=cat_title

                category_links_cat_cat[cat_title]=set()
                category_links_cat_art[cat_title]=set()

        if iter % 10000 == 0:
            print (str(iter)+' categories loaded.')


    for k in keywords:
        k_normalized = unidecode.unidecode(k)
        cat_title_normalized = unidecode.unidecode(cat_title.lower().replace('_',' '))

        kw_compiled = re.compile(r'\b({0})\b'.format(k_normalized), flags=re.IGNORECASE).search

        for cat_title in category_page_ids_page_titles.values():

            cat_title_normalized = unidecode.unidecode(cat_title.lower().replace('_',' '))

            if kw_compiled(cat_title_normalized) == None: continue

            # if k_unicode not in cat_title_normalized:
            #     continue
            # if unidecode.unidecode(k) in unidecode.unidecode(cat_title.lower().replace('_',' ')):

            keyword_category[k.lower()].add(cat_title)
            # print ('* ', k, cat_title, 'PREMI!')





    # for x,y in keyword_category.items():
    #     print(x,len(y))
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (len(category_links_cat_cat))
    print ('all categories loaded')


    # SEGON:
    # Category links. Muntar estructura de category links amb diccionaris i sets. Un diccionari amb les relacions entre cat-page i un altre entre cat-cat.
    # https://www.mediawiki.org/wiki/Manual:Categorylinks_table
    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-categorylinks.sql.gz'
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

                try:
                    page_id = int(row[0])
                    cat_title = str(row[1].strip("'"))
                except:
                    continue

                if cat_title not in category_links_cat_cat:
                    # print (row,'cat title does not exist.')
                    continue

                if page_id in category_page_ids_page_titles: # is this a category
                    category_links_cat_cat[cat_title].add(category_page_ids_page_titles[page_id])
                else: # this is an article
                    category_links_cat_art[cat_title].add(page_id)


        if iter % 100000 == 0:
            print (str(iter)+' categorylinks loaded.')

    # for x,y in category_links_cat_cat.items():
    #     print(x,len(y))
    #     print(x,len(category_links_cat_art[x]))

    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (len(category_links_cat_art))
    print ('all category links loaded')
    # input('')


    # TERCER:
    # Iterar els nivells corresponents. crawling.
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    print ('\n'+languagecode+' Starting selection of raw CCC.'); 
    print ('With language '+ languagecode +" and Keywords: ");
    print (keywordsdictionary)

    selectedarticles = {}
    selectedarticles_level = {}

    # QITEMS
    for item in keywordsdictionary:
        wordlist = keywordsdictionary[item]
#        wordlist = keywordsdictionary['Q1008']
        print ('\n'+(item))
        print (wordlist)

        cattitles_total_level = {}

        for keyword in wordlist:
            if keyword == '' or keyword == None: continue
            for cat_title in keyword_category[keyword.lower()]:
                cattitles_total_level[cat_title] = None

        if len(cattitles_total_level) == 0: continue


        # CATEGORIES FROM LEVELS
        level = 1
        num_levels = 25
        if languagecode=='en': num_levels = 10     
        j = 0
        total_categories = dict(); total_categories.update(cattitles_total_level)
        print ('Number of categories to start: '+str(len(total_categories)))

        while (level <= num_levels): # Here we choose the number of levels we prefer.
            i = 0

            newcategories = dict()
            for cat_title in cattitles_total_level.keys():           
                for cat_title2 in category_links_cat_cat[cat_title]:
                    try:
                        total_categories[cat_title2]
                    except:
                        newcategories[cat_title2] = None

                for page_id in category_links_cat_art[cat_title]:

                    try:
                        cur_level = selectedarticles_level[page_id]
                        if cur_level > level: selectedarticles_level[page_id] = level
                    except:
                        selectedarticles_level[page_id] = level

                    if page_id in selectedarticles:
                        selectedarticles[page_id].add(item)
                    else:
                        selectedarticles[page_id] = {item}

                    i += 1

            cattitles_total_level = dict()
            cattitles_total_level.update(newcategories)
            total_categories.update(newcategories)

            print('Level: '+str(level) + ". Number of new articles is: " + str(i)+ ". Total number of articles is "+str(len(selectedarticles_level))+'. Number of new categories is: '+str(len(newcategories))+'. Total number of categories is: '+str(len(total_categories)))

            level = level + 1
            if len(newcategories) == 0: break


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
        parameters.append((categorycrawling,selectedarticles_level[page_id],page_title,page_id,qitem))
        # print((categorycrawling,selectedarticles_level[page_id],page_title,page_id,qitem))


    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'UPDATE '+languagecode+'wiki SET (category_crawling_territories,category_crawling_territories_level) = (?,?) WHERE page_title = ? AND page_id = ? AND qitem=?;'
    cursor.executemany(query,parameters)
    conn.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(parameters)/num_art


    # ALL ARTICLES
    wp_number_articles = wikipedialanguage_numberarticles[languagecode]
    string = "The total number of category crawling selected Articles is: " + str(len(parameters)); print (string)
    string = "The total number of Articles in this Wikipedia is: "+str(wp_number_articles)+"\n"; print (string)
    string = "The percentage of category crawling related Articles in this Wikipedia is: "+str(percent)+"\n"; print (string)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def label_potential_ccc_articles_with_links(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'label_potential_ccc_articles_with_links '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    try: cursor.execute('SELECT 1 FROM '+languagecode+'wiki;')
    except: return

    content_selection_page_title = {}
    content_selection_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE ccc_binary=1;'
    for row in cursor.execute(query):
        content_selection_page_id[row[0]]=row[1]
        content_selection_page_title[row[1]]=row[0]

    other_content_selection_page_title = {}
    other_content_selection_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE ccc_binary=0;'
    for row in cursor.execute(query):
        other_content_selection_page_id[row[0]]=row[1]
        other_content_selection_page_title[row[1]]=row[0]


    female_page_title = {}
    female_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE gender = "Q6581072";'
    for row in cursor.execute(query):
        female_page_id[row[0]]=row[1]
        female_page_title[row[1]]=row[0]


    male_page_title = {}
    male_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE gender = "Q6581097";'
    for row in cursor.execute(query):
        male_page_id[row[0]]=row[1]
        male_page_title[row[1]]=row[0]


    lgbt_page_title = {}
    lgbt_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE sexual_orientation != "Q1035954";'
    for row in cursor.execute(query):
        lgbt_page_id[row[0]]=row[1]
        lgbt_page_title[row[1]]=row[0]


#    print (len(page_titles_page_ids),len(content_selection_page_id),len(other_content_selection_page_id))
    num_of_outlinks = {}
    num_of_inlinks = {}

    num_outlinks_ccc = {}
    num_outlinks_other_ccc = {}
    num_outlinks_women = {}
    num_outlinks_men = {}
    num_outlinks_lgbt = {}

    num_inlinks_ccc = {}
    num_inlinks_other_ccc = {}
    num_inlinks_women = {}
    num_inlinks_men = {}
    num_inlinks_lgbt = {}


    for page_id in page_titles_page_ids.values():
        num_of_outlinks[page_id]=0
        num_of_inlinks[page_id]=0

        num_outlinks_ccc[page_id]=0
        num_outlinks_other_ccc[page_id]=0

        num_inlinks_ccc[page_id]=0
        num_inlinks_other_ccc[page_id]=0

        num_inlinks_women[page_id]=0
        num_outlinks_women[page_id]=0

        num_inlinks_men[page_id]=0
        num_outlinks_men[page_id]=0

        num_inlinks_lgbt[page_id]=0
        num_outlinks_lgbt[page_id]=0



#    dumps_path = 'gnwiki-20190720-pagelinks.sql.gz' # read_dump = '/public/dumps/public/wikidatawiki/latest-all.json.gz'

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-pagelinks.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)
    try:
        dump_in = gzip.open(dumps_path, 'r')
    except:
        print ('error. the file pagelinks is not working.')

    w = 0
    iteratingstartTime = time.time()
    print ('Iterating the dump.')
    while True:
        line = dump_in.readline()
        try: line = line.decode("utf-8")
        except UnicodeDecodeError: line = str(line)

        if line == '':
            i+=1
            if i==3: break
        else: i=0

        if wikilanguages_utils.is_insert(line):
            # table_name = wikilanguages_utils.get_table_name(line)
            # columns = wikilanguages_utils.get_columns(line)
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                w+=1
#                print(row)
                pl_from = int(row[0])
                pl_from_namespace = row[1]
                pl_title = str(row[2])
                pl_namespace = row[3]

#                if pl_from == 893:
#                    print(row)

                try:
                    pl_title_page_id = page_titles_page_ids[pl_title]
                except:
                    pl_title_page_id = None


                if pl_from_namespace != '0' or pl_namespace != '0': continue

                ########
                # OUTLINKS

                try:
                    num_of_outlinks[pl_from]= num_of_outlinks[pl_from] + 1
#                    print('num_outlinks')
#                    print (num_of_outlinks[pl_from])
                except:
                    pass

                try:
                    ccc=content_selection_page_id[pl_title_page_id]
                    num_outlinks_ccc[pl_from] = num_outlinks_ccc[pl_from] + 1
#                    print (num_outlinks_ccc[pl_from])
                except:
                    pass

                try:
                    abroad=other_content_selection_page_id[pl_title_page_id]
                    num_outlinks_other_ccc[pl_from] = num_outlinks_other_ccc[pl_from] + 1
#                    print('num_outlinks_other_ccc')
#                    print (num_outlinks_other_ccc[pl_from])
                except:
                    pass

                ########

                try:
                    female=female_page_id[pl_title_page_id]
                    num_outlinks_women[pl_from] = num_outlinks_women[pl_from] + 1
                except:
                    pass

                try:
                    male=male_page_id[pl_title_page_id]
                    num_outlinks_men[pl_from] = num_outlinks_men[pl_from] + 1
                except:
                    pass

                try:
                    lgbt=lgbt_page_id[pl_title_page_id]
                    num_outlinks_lgbt[pl_from] = num_outlinks_lgbt[pl_from] + 1
                except:
                    pass


                ########

                # INLINKS

                try:
                    num_of_inlinks[pl_title_page_id] = num_of_inlinks[pl_title_page_id] + 1
                except:
                    pass

                try:
                    ccc=content_selection_page_id[pl_from]
                    num_inlinks_ccc[pl_title_page_id] = num_inlinks_ccc[pl_title_page_id] + 1
                except:
                    pass

                try:
                    abroad=other_content_selection_page_id[pl_from]
                    num_inlinks_other_ccc[pl_title_page_id] = num_inlinks_other_ccc[pl_title_page_id] + 1
#                    print('num_inlinks_other_ccc')                    
#                    print (num_inlinks_other_ccc[page_titles_page_ids[pl_title]])
                except:
                    pass



                ########

                try:
                    female=female_page_id[pl_from]
                    num_inlinks_women[pl_title_page_id] = num_inlinks_women[pl_title_page_id] + 1
                except:
                    pass

                try:
                    male=male_page_id[pl_from]
                    num_inlinks_men[pl_title_page_id] = num_inlinks_men[pl_title_page_id] + 1
                except:
                    pass

                try:
                    lgbt=other_content_selection_page_id[pl_from]
                    num_inlinks_lgbt[pl_title_page_id] = num_inlinks_lgbt[pl_title_page_id] + 1
                except:
                    pass


                if w % 1000000 == 0: # 10 million
                    print (w)
                    print ('current time: ' + str(time.time() - iteratingstartTime)+ ' '+languagecode)
                    print ('number of lines per second: '+str(round(((w/(time.time() - iteratingstartTime))/1000),2))+ ' thousand.')

                    # print (num_of_inlinks_from_ethnic_groups);
                    # print (num_of_outlinks_to_ethnic_group);


#    input('')
    print ('Done with the dump.')

    n_outlinks=0
    n_outlinks_ccc =0
    n_outlinks_other_ccc =0
    n_outlinks_women = 0
    n_outlinks_men = 0
    n_outlinks_lgbt = 0


    n_inlinks =0
    n_inlinks_ccc =0
    n_inlinks_other_ccc =0
    n_inlinks_women =0
    n_inlinks_men =0
    n_inlinks_lgbt =0


    parameters = []
    for page_title, page_id in page_titles_page_ids.items():
        qitem = page_titles_qitems[page_title]

        num_outlinks = 0
        num_outlinks_to_CCC = 0
        num_outlinks_to_geolocated_abroad = 0
        num_outlinks_to_women = 0
        num_outlinks_to_men = 0
        num_outlinks_to_lgbt = 0

        num_inlinks = 0
        num_inlinks_from_CCC = 0
        num_inlinks_from_geolocated_abroad = 0
        num_inlinks_from_women = 0
        num_inlinks_from_men = 0
        num_inlinks_from_lgbt = 0



        num_outlinks = num_of_outlinks[page_id]

        num_outlinks_to_CCC = num_outlinks_ccc[page_id]
        if num_outlinks!= 0: percent_outlinks_to_CCC = float(num_outlinks_to_CCC)/float(num_outlinks)
        else: percent_outlinks_to_CCC = 0

        num_outlinks_to_geolocated_abroad = num_outlinks_other_ccc[page_id]
        if num_outlinks!= 0: percent_outlinks_to_geolocated_abroad = float(num_outlinks_to_geolocated_abroad)/float(num_outlinks)
        else: percent_outlinks_to_geolocated_abroad = 0

        num_outlinks_to_women = num_outlinks_women[page_id]
        if num_outlinks!= 0: percent_outlinks_to_women = float(num_outlinks_to_women)/float(num_outlinks)
        else: percent_outlinks_to_women = 0

        num_outlinks_to_men = num_outlinks_men[page_id]
        if num_outlinks!= 0: percent_outlinks_to_men = float(num_outlinks_to_men)/float(num_outlinks)
        else: percent_outlinks_to_men = 0

        num_outlinks_to_lgbt = num_outlinks_lgbt[page_id]
        if num_outlinks!= 0: percent_outlinks_to_lgbt = float(num_outlinks_to_lgbt)/float(num_outlinks)
        else: percent_outlinks_to_lgbt = 0



        num_inlinks = num_of_inlinks[page_id]

        num_inlinks_from_CCC = num_inlinks_ccc[page_id]
        if num_inlinks!= 0: percent_inlinks_from_CCC = float(num_inlinks_from_CCC)/float(num_inlinks)
        else: percent_inlinks_from_CCC = 0

        num_inlinks_from_geolocated_abroad = num_inlinks_other_ccc[page_id]
        if num_inlinks!= 0: percent_inlinks_from_geolocated_abroad = float(num_inlinks_from_geolocated_abroad)/float(num_inlinks)
        else: percent_inlinks_from_geolocated_abroad = 0

        num_inlinks_from_women = num_inlinks_women[page_id]
        if num_inlinks!= 0: percent_inlinks_from_women = float(num_inlinks_from_women)/float(num_inlinks)
        else: percent_inlinks_from_women = 0

        num_inlinks_from_men = num_inlinks_men[page_id]
        if num_inlinks!= 0: percent_inlinks_from_men = float(num_inlinks_from_men)/float(num_inlinks)
        else: percent_inlinks_from_men = 0

        num_inlinks_from_lgbt = num_inlinks_lgbt[page_id]
        if num_inlinks!= 0: percent_inlinks_from_lgbt = float(num_inlinks_from_lgbt)/float(num_inlinks)
        else: percent_inlinks_from_lgbt = 0


        parameters.append((num_outlinks,num_outlinks_to_CCC,percent_outlinks_to_CCC,num_outlinks_to_geolocated_abroad,percent_outlinks_to_geolocated_abroad,num_inlinks,num_inlinks_from_CCC,percent_inlinks_from_CCC,num_inlinks_from_geolocated_abroad,percent_inlinks_from_geolocated_abroad,num_inlinks_from_women, num_outlinks_to_women, percent_inlinks_from_women, percent_outlinks_to_women, num_inlinks_from_men, num_outlinks_to_men, percent_inlinks_from_men, percent_outlinks_to_men, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt,page_id,qitem,page_title))

        # print ((num_outlinks,num_outlinks_to_CCC,percent_outlinks_to_CCC,num_outlinks_to_geolocated_abroad,percent_outlinks_to_geolocated_abroad,num_inlinks,num_inlinks_from_CCC,percent_inlinks_from_CCC,num_inlinks_from_geolocated_abroad,percent_inlinks_from_geolocated_abroad,num_inlinks_from_women, num_outlinks_to_women, percent_inlinks_from_women, percent_outlinks_to_women, num_inlinks_from_men, num_outlinks_to_men, percent_inlinks_from_men, percent_outlinks_to_men, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt,page_id,qitem,page_title))


    query = 'UPDATE '+languagecode+'wiki SET (num_outlinks,num_outlinks_to_CCC,percent_outlinks_to_CCC,num_outlinks_to_geolocated_abroad,percent_outlinks_to_geolocated_abroad,num_inlinks,num_inlinks_from_CCC,percent_inlinks_from_CCC,num_inlinks_from_geolocated_abroad,percent_inlinks_from_geolocated_abroad,num_inlinks_from_women, num_outlinks_to_women, percent_inlinks_from_women, percent_outlinks_to_women, num_inlinks_from_men, num_outlinks_to_men, percent_inlinks_from_men, percent_outlinks_to_men, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt)=(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        
    cursor.executemany(query,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# Obtain the Articles with a "weak" language property that is associated the language. This is considered potential CCC.
def label_potential_ccc_articles_language_weak_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_potential_ccc_articles_language_weak_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()

    # language qitems
    qitemresult = languages.loc[languagecode]['Qitem']
    if ';' in qitemresult: qitemresult = qitemresult.split(';')
    else: qitemresult = [qitemresult];

    # get Articles
    qitem_properties = {}
    qitem_page_titles = {}
    other_ccc_language = {}
    query = 'SELECT language_weak_properties.qitem, language_weak_properties.property, language_weak_properties.qitem2, sitelinks.page_title FROM language_weak_properties INNER JOIN sitelinks ON sitelinks.qitem = language_weak_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 not in qitemresult: 
            if qitem not in other_ccc_language: other_ccc_language[qitem]=1
            else: other_ccc_language[qitem]=other_ccc_language[qitem]+1

        else:
    #        print ((qitem, wdproperty, language_properties_weak[wdproperty], page_title))
            # Put the items into a dictionary
            value = wdproperty+':'+qitem2
            if qitem not in qitem_properties: qitem_properties[qitem]=value
            else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value

        qitem_page_titles[qitem]=page_title


    # Get the tuple ready to insert.
    ccc_language_items = []
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_language_items.append((values,qitem_page_titles[qitem],qitem,page_id))
        except: continue

    # Get the tuple ready to insert for other language strong CCC.
    other_ccc_language_items = []
    for qitem, values in other_ccc_language.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_language_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass


    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    query = 'UPDATE '+languagecode+'wiki SET language_weak_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()


   # Insert to the corresponding CCC database.
    query = 'UPDATE '+languagecode+'wiki SET other_ccc_language_weak_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,other_ccc_language_items)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# Get the Articles with the number of affiliation-like properties to other Articles already retrieved as CCC.
def label_potential_ccc_articles_affiliation_properties_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()

    function_name = 'label_potential_ccc_articles_affiliation_properties_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikidata_db); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

    potential_ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki;'):
        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    other_ccc_articles = {}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=0;'):
        other_ccc_articles[row[1]]=row[0].replace(' ','_')

#    print (affiliation_properties)
#    input('')

    
    other_ccc_affiliation_qitems = {}
    qitem_page_titles = {}
    selected_qitems = {}
    query = 'SELECT affiliation_properties.qitem, affiliation_properties.property, affiliation_properties.qitem2, sitelinks.page_title FROM affiliation_properties INNER JOIN sitelinks ON sitelinks.qitem = affiliation_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if (qitem2 in ccc_articles):
#            if (qitem in ccc_articles): continue
#                print ((qitem, page_title, wdproperty, affiliation_properties[wdproperty],ccc_articles[qitem2],'ALREADY IN!'))           
#            elif (qitem in potential_ccc_articles): continue
#                print ((qitem, page_title, wdproperty, affiliation_properties[wdproperty],ccc_articles[qitem2],'POTENTIAL.'))
#            else:
#                print ((qitem, page_title, wdproperty, affiliation_properties[wdproperty],ccc_articles[qitem2],'NEW NEW NEW NEW NEW!'))
            if qitem not in selected_qitems:
                selected_qitems[qitem]=[page_title,wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]
#    print (len(selected_qitems))
#    for keys,values in selected_qitems.items(): print (keys,values)


        elif (qitem2 in other_ccc_articles):
            if qitem not in other_ccc_affiliation_qitems: other_ccc_affiliation_qitems[qitem]=1
            else: other_ccc_affiliation_qitems[qitem]=other_ccc_affiliation_qitems[qitem]+1

        qitem_page_titles[qitem]=page_title


    ccc_affiliation_items = []
    for qitem, values in selected_qitems.items():
        page_title=values[0]
        try: page_id=page_titles_page_ids[page_title]
        except: continue
        value = ''
        for x in range(0,int((len(values)-1)/2)):
            if x >= 1: value = value + ';'
            value = value + values[x*2+1]+':'+values[x*2+2]
#        print ((value,page_title,qitem,page_id))
        ccc_affiliation_items.append((value,page_title,qitem,page_id))
#    print (len(ccc_affiliation_items))


    other_ccc_affiliation_items = []
    for qitem, values in other_ccc_affiliation_qitems.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_affiliation_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass


    # INSERT INTO CCC DATABASE
    query = 'UPDATE '+languagecode+'wiki SET affiliation_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_affiliation_items)
    conn.commit()

    query = 'UPDATE '+languagecode+'wiki SET other_ccc_affiliation_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,other_ccc_affiliation_items)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Get the Articles with the properties that state that has Articles already retrieved as CCC as part of them.
def label_potential_ccc_articles_has_part_properties_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()

    function_name = 'label_potential_ccc_articles_has_part_properties_wd '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikidata_db); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

    potential_ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE (ccc_binary=1 OR category_crawling_territories IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL);'):

    # for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki;'):
        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    print (len(ccc_articles))

    qitem_page_titles = {}
    other_ccc_has_part_properties = {}
    selected_qitems={}
    query = 'SELECT has_part_properties.qitem, has_part_properties.property, has_part_properties.qitem2, sitelinks.page_title FROM has_part_properties INNER JOIN sitelinks ON sitelinks.qitem = has_part_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if (qitem2 in ccc_articles) and (qitem in potential_ccc_articles):
            # print ((qitem, page_title, wdproperty, has_part_properties[wdproperty],ccc_articles[qitem2]))
            if qitem not in selected_qitems:
                selected_qitems[qitem]=[page_title,wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]


        if (qitem2 not in ccc_articles) and (qitem not in potential_ccc_articles) and (qitem not in ccc_articles):
            if qitem not in other_ccc_has_part_properties: 
                other_ccc_has_part_properties[qitem]=1
            else: 
                other_ccc_has_part_properties[qitem]=other_ccc_has_part_properties[qitem]+1

        qitem_page_titles[qitem] = page_title


    ccc_has_part_items = []
    for qitem, values in selected_qitems.items():
        page_title=values[0]
        try: page_id=page_titles_page_ids[page_title]
        except: continue
        value = ''
#        print (values)
        for x in range(0,int((len(values)-1)/2)):
            if x >= 1: value = value + ';'
            value = value + values[x*2+1]+':'+values[x*2+2]
#        print ((value,page_title,qitem,page_id))
        ccc_has_part_items.append((value,page_title,qitem,page_id))

    other_ccc_has_part_items = []
    for qitem, values in other_ccc_has_part_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_has_part_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass


    # INSERT INTO CCC DATABASE
    query = 'UPDATE '+languagecode+'wiki SET has_part_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_has_part_items)
    conn.commit()

    query = 'UPDATE '+languagecode+'wiki SET other_ccc_has_part_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,other_ccc_has_part_items)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# obtains the blacklisted/whitelist articles from the previous CCC, stores them in the new database and uses them to assign them ccc_binary = 0.
def retrieving_ccc_surelist_list():

    function_name = 'retrieving_ccc_surelist_list '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS ccc_surelist (languagecode text, qitem text, ccc_binary int, PRIMARY KEY (languagecode, qitem));")

    conn2 = sqlite3.connect(databases_path + 'ccc_old.db'); cursor2 = conn2.cursor()
    query = 'SELECT languagecode, qitem FROM ccc_surelist WHERE page_namespace=0;'

    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

    lang_articles = {}
    for languagecode in wikilanguagecodes:
        lang_articles[languagecode]=[]

    qitem = ''
    parameters = []
    for row in cursor.execute(query):
        languagecode = row[0]
        if languagecode != old_languagecode and qitem!='':
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        qitem = row[1]
        ccc_binary = row[2]
        page_id = page_titles_page_ids[qitems_page_titles[qitem]]

        parameters.append((languagecode,qitem,ccc_binary))
        lang_articles[languagecode]=lang_articles[languagecode].append((ccc_binary,page_id,qitem))

        old_languagecode = languagecode

    query = "INSERT INTO ccc_surelist (languagecode, qitem, ccc_binary) VALUES (?,?,?);"
    cursor.executemany(query,parameters)
    conn.commit()

    for languagecode in wikilanguagecodes:
        query = 'UPDATE '+languagecode+'wiki SET ccc_binary=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,lang_articles[languagecode])
        conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# ARTICLE CCC CLASSIFYING / SCORING FUNCTIONS
#############################################

def get_ccc_training_data(languagecode):

    # OBTAIN THE DATA TO FIT.
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
    ccc_df = pd.read_sql_query(query, conn)


    positive_features = ['category_crawling_territories','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']

    negative_features = ['other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd']  #'other_ccc_country_wd','other_ccc_location_wd' are out because now are considered totally negative groundtruth (25.9.18)

#    negative_features_2 = ['other_ccc_keyword_title','other_ccc_category_crawling_relative_level', 'num_inlinks_from_geolocated_abroad', 'num_outlinks_to_geolocated_abroad', 'percent_inlinks_from_geolocated_abroad', 'percent_outlinks_to_geolocated_abroad']

    features = ['qitem']+positive_features+negative_features
#    features = positive_features

    ccc_df = ccc_df[features]
    ccc_df = ccc_df.set_index(['qitem'])
#    print (ccc_df.head())
    if len(ccc_df.index.tolist())==0: print ('It is not possible to classify Wikipedia Articles as there is no groundtruth.'); return (0,0,[],[]) # maxlevel,num_articles_ccc,ccc_df_list,binary_list
    ccc_df = ccc_df.fillna(0)


    # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
    category_crawling_paths=ccc_df['category_crawling_territories'].tolist()
    for n, i in enumerate(category_crawling_paths):
        if i is not 0:
            category_crawling_paths[n]=1
            if i.count(';')>=1: category_crawling_paths[n]=i.count(';')+1
        else: category_crawling_paths[n]=0
    ccc_df = ccc_df.assign(category_crawling_territories = category_crawling_paths)

    category_crawling_territories_level=ccc_df['category_crawling_territories_level'].tolist()
    maxlevel = max(category_crawling_territories_level)
    for n, i in enumerate(category_crawling_territories_level):
        if i > 0:
            category_crawling_territories_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_territories_level[n]=0
    ccc_df = ccc_df.assign(category_crawling_territories_level = category_crawling_territories_level)

    language_weak_wd=ccc_df['language_weak_wd'].tolist()
    for n, i in enumerate(language_weak_wd):
        if i is not 0: language_weak_wd[n]=1
        else: language_weak_wd[n]=0
    ccc_df = ccc_df.assign(language_weak_wd = language_weak_wd)

    affiliation_wd=ccc_df['affiliation_wd'].tolist()
    for n, i in enumerate(affiliation_wd):
        if i is not 0: 
            affiliation_wd[n]=1
            if i.count(';')>=1: affiliation_wd[n]=i.count(';')+1
        else: affiliation_wd[n]=0
    ccc_df = ccc_df.assign(affiliation_wd = affiliation_wd)

    has_part_wd=ccc_df['has_part_wd'].tolist()
    for n, i in enumerate(has_part_wd):
        if i is not 0: 
            has_part_wd[n]=1
            if i.count(';')>=1: has_part_wd[n]=i.count(';')+1
        else: has_part_wd[n]=0
    ccc_df = ccc_df.assign(has_part_wd = has_part_wd)
#    print (ccc_df.head())

    
    # SAMPLING
    sampling_method = 'negative_sampling'
    print ('sampling method: '+sampling_method)

    if sampling_method == 'negative_sampling':
        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]
        ccc_df_yes = ccc_df_yes.drop(columns=['ccc_binary'])
        ccc_df_list_yes = ccc_df_yes.values.tolist()
        num_articles_ccc = len(ccc_df_list_yes)

        ccc_df_list_probably_no = []
        size_sample = 6
        if languagecode == 'en': size_sample = 4 # exception for English
        for i in range(1,1+size_sample):
            ccc_df = ccc_df.sample(frac=1) # randomize the rows order
            ccc_df_probably_no = ccc_df.loc[ccc_df['ccc_binary'] != 1]
            ccc_df_probably_no = ccc_df_probably_no.drop(columns=['ccc_binary'])
            ccc_df_list_probably_no = ccc_df_list_probably_no + ccc_df_probably_no.values.tolist()[:num_articles_ccc]

        num_probably_no = len(ccc_df_list_probably_no)
        ccc_df_list = ccc_df_list_yes + ccc_df_list_probably_no
        binary_list = [1]*num_articles_ccc+[0]*num_probably_no

    if sampling_method == 'balanced_sampling':
        ccc_df = ccc_df.sample(frac=1) # randomize the rows order
        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]
        ccc_df_yes = ccc_df_yes.drop(columns=['ccc_binary'])
    #    print (ccc_df_yes.head())

        ccc_df_no = ccc_df.loc[ccc_df['ccc_binary'] == 0]
        ccc_df_no = ccc_df_no.drop(columns=['ccc_binary'])
    #    print (ccc_df_no.head())

        sample = 10000 # the number samples per class
        sample = min(sample,len(ccc_df_yes),len(ccc_df_no))
        ccc_df_list_yes = ccc_df_yes.values.tolist()[:sample]
        ccc_df_list_no = ccc_df_no.values.tolist()[:sample]

        ccc_df_list = ccc_df_list_yes+ccc_df_list_no
        binary_list = [1]*sample+[0]*sample
        num_articles_ccc = len(ccc_df_yes)

    print ('\nConverting the dataframe...')
    print ('These are its columns:')
    print (list(ccc_df_yes.columns.values))

#    print (maxlevel)
#    print (len(num_articles_ccc))
#    print (len(ccc_df_list))
#    print (len(binary_list))

    return maxlevel,num_articles_ccc,ccc_df_list,binary_list


def get_ccc_testing_data(languagecode,maxlevel):

    # OBTAIN THE DATA TO TEST
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
#    query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NULL;' # ALL
    
    # WE GET THE POTENTIAL CCC ARTICLES THAT HAVE NOT BEEN 1 BY ANY OTHER MEANS.
    # For the testing takes those with one of these features not null (category crawling, language weak, affiliation or has part).
    query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NULL AND (category_crawling_territories IS NOT NULL OR category_crawling_territories_level IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL OR has_part_wd IS NOT NULL);'

    # For the testing takes those with one of these features not null (category crawling, language weak, affiliation or has part), and those with keywords on title.
#    query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NULL AND (category_crawling_territories IS NOT NULL OR category_crawling_territories_level IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL OR has_part_wd IS NOT NULL) OR keyword_title IS NOT NULL;' # POTENTIAL

    potential_ccc_df = pd.read_sql_query(query, conn)

    positive_features = ['category_crawling_territories','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC']

    negative_features = ['other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd']

#    negative_features_2 = ['other_ccc_keyword_title','other_ccc_category_crawling_relative_level', 'num_inlinks_from_geolocated_abroad', 'num_outlinks_to_geolocated_abroad', 'percent_inlinks_from_geolocated_abroad', 'percent_outlinks_to_geolocated_abroad']
    features = ['page_title'] + positive_features + negative_features
#    features = positive_features

    potential_ccc_df = potential_ccc_df[features]
    potential_ccc_df = potential_ccc_df.set_index(['page_title'])
    potential_ccc_df = potential_ccc_df.fillna(0)

    # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
    category_crawling_paths=potential_ccc_df['category_crawling_territories'].tolist()
    for n, i in enumerate(category_crawling_paths):
        if i is not 0:
            category_crawling_paths[n]=1
            if i.count(';')>=1: category_crawling_paths[n]=i.count(';')+1
        else: category_crawling_paths[n]=0
    potential_ccc_df = potential_ccc_df.assign(category_crawling_territories = category_crawling_paths)

    category_crawling_territories_level=potential_ccc_df['category_crawling_territories_level'].tolist()
#    print (maxlevel)
#    print (max(category_crawling_territories_level))
    for n, i in enumerate(category_crawling_territories_level):
        if i > 0:
            category_crawling_territories_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_territories_level[n]=0
    potential_ccc_df = potential_ccc_df.assign(category_crawling_territories_level = category_crawling_territories_level)

    language_weak_wd=potential_ccc_df['language_weak_wd'].tolist()
    for n, i in enumerate(language_weak_wd):
        if i is not 0:
            language_weak_wd[n]=1
        else: language_weak_wd[n]=0
    potential_ccc_df = potential_ccc_df.assign(language_weak_wd = language_weak_wd)

    affiliation_wd=potential_ccc_df['affiliation_wd'].tolist()
    for n, i in enumerate(affiliation_wd):
        if i is not 0:
            affiliation_wd[n]=1
            if i.count(';')>=1: affiliation_wd[n]=i.count(';')+1
        else: affiliation_wd[n]=0
    potential_ccc_df = potential_ccc_df.assign(affiliation_wd = affiliation_wd)

    has_part_wd=potential_ccc_df['has_part_wd'].tolist()
    for n, i in enumerate(has_part_wd):
        if i is not 0:
            has_part_wd[n]=1
            if i.count(';')>=1: has_part_wd[n]=i.count(';')+1
        else: has_part_wd[n]=0
    potential_ccc_df = potential_ccc_df.assign(has_part_wd = has_part_wd)


    # NOT ENOUGH ARTICLES
    if len(potential_ccc_df)==0: print ('There are not potential CCC Articles, so it returns empty'); return
    potential_ccc_df = potential_ccc_df.sample(frac=1) # randomize the rows order

    print ('We selected this number of potential CCC Articles: '+str(len(potential_ccc_df)))

    return potential_ccc_df


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# Takes the ccc_score and decides whether it must be in ccc or not.
def calculate_articles_ccc_binary_classifier(languagecode,classifier,page_titles_page_ids,page_titles_qitems):

    function_name = 'calculate_articles_ccc_binary_classifier '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    print ('\nObtaining the final CCC for language: '+languagecode)

    # FIT THE SVM MODEL
    maxlevel,num_articles_ccc,ccc_df_list,binary_list = get_ccc_training_data(languagecode)
    print ('Fitting the data into the classifier.')
    print ('The data has '+str(len(ccc_df_list))+' samples.')
    if num_articles_ccc == 0 or len(ccc_df_list)<10: print ('There are not enough samples.'); return

    X = ccc_df_list
    y = binary_list
#    print (X)
#    print (y)

    print ('The chosen classifier is: '+classifier)
    if classifier=='SVM':
        clf = svm.SVC()
        clf.fit(X, y)
    if classifier=='RandomForest':
        clf = RandomForestClassifier(n_estimators=100)
        clf.fit(X, y)
    if classifier=='LogisticRegression':
        clf = linear_model.LogisticRegression(solver='liblinear')
        clf.fit(X, y)
    if classifier=='GradientBoosting':
        clf = GradientBoostingClassifier()
        clf.fit(X, y)

    print ('The fit classes are: '+str(clf.classes_))
    print ('The fit has a score of: '+str(clf.score(X, y, sample_weight=None)))
    print (clf.feature_importances_.tolist())
#    input('')

    # TEST THE DATA
    print ('Calculating which page is IN or OUT...')
    potential_ccc_df = get_ccc_testing_data(languagecode,maxlevel)



    if potential_ccc_df is None: 
        print ('No Articles to verify.'); 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return     
    if len(potential_ccc_df)==0: 
        print ('No Articles to verify.'); 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return

    page_titles = potential_ccc_df.index.tolist()
    potential = potential_ccc_df.values.tolist()

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
    #        testdict[page_titles[i]]=(x,potential[i])
            if x == 1:
                page_title=page_titles[i]
                selected.append((page_titles_page_ids[page_title],page_titles_qitems[page_title]))
            i += 1
#    print (testdict)

    # PRINT THE CLASSIFIER RESULTS ARTICLE BY ARTICLE
    else:
        # provisional
#        print (potential[:15])
#        print (page_titles[:15])
        count_yes=0
        count_no=0
        for n,i in enumerate(potential):
            result = clf.predict([i])
            page_title=page_titles[n]
            if result[0] == 1:
                count_yes+=1
                print (['category_crawling_paths','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd'])
                print(i)
                print(clf.predict_proba([i]).tolist())
                print (str(count_yes)+'\tIN\t'+page_title+'.\n')

                try: selected.append((page_titles_page_ids[page_title],page_titles_qitems[page_title]))
                except: pass
            else:
                count_no+=1
                print (['category_crawling_paths','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd'])
                print(i)
                print(clf.predict_proba([i]).tolist())
                print (str(count_no)+'\tOUT:\t'+page_title+'.\n')
#                input('')

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: 
        percent = 0
        percent_selected = '0'
    else: 
        percent = round(100*num_articles_ccc/num_art,3)
        percent_selected = str(round(100*(num_articles_ccc+len(selected))/num_art,3))


    print ('\nThis Wikipedia ('+languagecode+'wiki) has a total of '+str(wikipedialanguage_numberarticles[languagecode])+' Articles.')
    print ('There were already '+str(num_articles_ccc)+' CCC Articles selected as groundtruth. This is a: '+str(percent)+'% of the WP language edition.')

    print ('\nThis algorithm CLASSIFIED '+str(len(selected))+' Articles as ccc_binary = 1 from a total of '+str(len(potential))+' from the testing data. This is a: '+str(round(100*len(selected)/len(potential),3))+'%.')
    print ('With '+str(num_articles_ccc+len(selected))+' Articles, the current and updated percentage of CCC is: '+percent_selected+'% of the WP language edition.\n')

#    evaluate_content_selection_manual_assessment(languagecode,selected,page_titles_page_ids)
#    input('')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,selected)
    conn.commit()

    print ('Language CCC '+(languagecode)+' created.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def calculate_articles_ccc_main_territory(languagecode):

    functionstartTime = time.time()
    function_name = 'calculate_articles_ccc_main_territory '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    if languagecode in languageswithoutterritory: print ('This language has no territories: '+languagecode); return

    number_iterations = 3
    print ('number of iterations: '+str(number_iterations))
    for i in range(1,number_iterations+1):
        print ('* iteration nº: '+str(i))
        # this function verifies the keywords associated territories, category crawling associated territories, and the wikidata associated territories in order to choose one.
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

        if languagecode not in languageswithoutterritory:
            try: qitems=territories.loc[languagecode]['QitemTerritory'].tolist()
            except: qitems=[];qitems.append(territories.loc[languagecode]['QitemTerritory'])

        main_territory_list = []
        main_territory_in = {}
        query = 'SELECT qitem, main_territory FROM '+languagecode+'wiki WHERE main_territory IS NOT NULL';
        for row in cursor.execute(query):
            main_territory_in[row[0]]=row[1]
    #    print (len(main_territory_in))

        query = 'SELECT qitem, page_id, main_territory, country_wd, location_wd, part_of_wd, has_part_wd, keyword_title, category_crawling_territories, created_by_wd, affiliation_wd FROM '+languagecode+'wiki'+' WHERE main_territory IS NULL AND ccc_binary=1'
#        print (query)
        for row in cursor.execute(query):
#            print (row)

            qitem = str(row[0])
            page_id = row[1]
            main_territory = row[2]
    #        print ('* row:')
    #        print (row)
            
            # check the rest of parameters to assign the main territory.
            main_territory_dict={}

            for x in range(3,len(row)):
                parts = row[x]
    #            print (x)

                if parts != None:
                    if ';' in parts:
                        parts = row[x].split(';')

                        if x==7: # exception: it is in keywords and there is only one Qitem that is not language. IN.
                            in_part=[]
                            for part in parts:
                                if part in qitems: in_part.append(part)
                            if len(in_part) >0:
                                if in_part[0] in qitems:
                                    main_territory_list.append((main_territory, qitem, page_id))
                                    main_territory_in[qitem]=main_territory
#                                    print ('number 7.1')
#                                    print ((main_territory, qitem, page_id))
                                    continue

                        for part in parts:
                            if ':' in part:
                                subparts = part.split(':')

                                if len(subparts) == 3:
                                    subpart = subparts[2]

                                if len(subparts) == 2: # we are giving it the main territory of the subpart. this is valid for: part_of_wd, has_part_wd, created_by_wd, affiliation_wd.
                                    subpart = subparts[1]
                                    try:
    #                                        print ('número 2 this: '+subpart+' is: '+ main_territory_in[subpart])
                                        subpart = main_territory_in[subpart]
                                    except: pass

                                if subpart in qitems:
                                    if subpart not in main_territory_dict:
                                        main_territory_dict[subpart]=1
                                    else:
                                        main_territory_dict[subpart]=main_territory_dict[subpart]+1
                            else:
    #                                print ('número 1 per part.')
                                if part in qitems:
                                    if part not in main_territory_dict:
                                        main_territory_dict[part]=1
                                    else:
                                        main_territory_dict[part]=main_territory_dict[part]+1
                    else:
    #                        print ('un de sol.')

                        if ':' in parts:
                            subparts = parts.split(':')

                            if len(subparts) == 3:
                                subpart = subparts[2]

                            if len(subparts) == 2: # we are giving it the main territory of the subpart. this is valid for: part_of_wd, has_part_wd, created_by_wd, affiliation_wd.
                                subpart = subparts[1]
                                try:
    #                                    print ('número 2 this: '+subpart+' is: '+ main_territory_in[subpart])
                                    subpart = main_territory_in[subpart]
                                except: pass

                            if subpart in qitems:
                                if subpart not in main_territory_dict:
                                    main_territory_dict[subpart]=1
                                else:
                                    main_territory_dict[subpart]=main_territory_dict[subpart]+1

                        else:
                            # exception: it is in keywords and there is only one Qitem. IN.

                            if parts in qitems:
                                if x == 7:
                                    main_territory_list.append((main_territory, qitem, page_id))
                                    main_territory_in[qitem]=main_territory
#                                    print ('number 7.2')
#                                    print ((main_territory, qitem, page_id))
                                    continue     

                                if parts not in main_territory_dict:
                                    main_territory_dict[parts]=1
                                else:
                                    main_territory_dict[parts]=main_territory_dict[parts]+1
                else:
    #                    print ('None')
                    pass

    #        print ('here is the selection:')
    #        print (main_territory_dict)

            # choose the territory with more occurences
            if len(main_territory_dict)>1: 
                if sorted(main_territory_dict.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][1] == sorted(main_territory_dict.items(), key=lambda item: (item[1], item[0]), reverse=True)[1][1]:
                    main_territory=None
    #                    print ('NO EN TENIM.')
                    continue
                else:
                    main_territory=sorted(main_territory_dict.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0] 
            elif len(main_territory_dict)>0:
                main_territory=list(main_territory_dict.keys())[0]

            # put it into a list
            main_territory_list.append((main_territory, qitem, page_id))
#            print ('this is in:')
#            print ((main_territory, qitem, page_id))

        query = 'UPDATE '+languagecode+'wiki SET main_territory = ? WHERE qitem = ? AND page_id = ? AND ccc_binary = 1;'
        cursor.executemany(query,main_territory_list)
        conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# Calculates the number of strategies used to retrieve and introduce them into the database.
def calculate_articles_ccc_retrieval_strategies(languagecode):

    function_name = 'calculate_articles_ccc_retrieval_strategies '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn.cursor()

    strategies = []
    query = 'SELECT qitem, page_id, geocoordinates, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC FROM '+languagecode+'wiki'+';'
    for row in cursor.execute(query):
        num_retrieval_strategies = sum(x is not None for x in row)-2
        qitem = str(row[0])
        page_id = row[1]
        strategies.append((num_retrieval_strategies, qitem, page_id))
    query = 'UPDATE '+languagecode+'wiki SET num_retrieval_strategies = ? WHERE qitem = ? AND page_id = ?;'
    cursor.executemany(query,strategies)
    conn.commit()

    print ('CCC number of retrieval strategies for each Article assigned.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# CCC VERIFICATION TOOLS FUNCTIONS
#############################################

# Filter: Deletes all the CCC selected qitems from a language which are geolocated but not among the geolocated Articles to the territories associated to that language.
def groundtruth_reaffirmation(languagecode):

    function_name = 'groundtruth_reaffirmation '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

#    print ('cleant. NOW WE STaRT.')
#    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = NULL;'
#    cursor2.execute(query);
#    conn2.commit()

    # POSITIVE GROUNDTRUTH
    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE ccc_geolocated=1;'
    cursor2.execute(query);
    conn2.commit()
    print ('geolocated in, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE country_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('country_wd, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE location_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('location_wd, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE language_strong_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('language_strong_wd, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE created_by_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('created_by_wd, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE part_of_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('part_of_wd, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 1 WHERE keyword_title IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('keyword_title, done.')


    # NEGATIVE GROUNDTRUTH
    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 0 WHERE ccc_geolocated=-1;'
    cursor2.execute(query);
    conn2.commit()
    print ('geolocated abroad, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 0 WHERE other_ccc_location_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('location wikidata property abroad, done.')

    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = 0 WHERE other_ccc_country_wd IS NOT NULL AND country_wd IS NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('country wikidata property abroad, done.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def check_current_groundtruth(languagecode):

    functionstartTime = time.time()
    print ('\n* Check the ccc_binary from all the Articles from language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    print ('These are the ccc_binary null, zero and one: ')
    query = 'SELECT ccc_binary, count(*) FROM '+languagecode+'wiki GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    ## BINARY 0
    print ('\nFor those that are ZERO:')
    print ('- geolocated:')
    query = 'SELECT ccc_geolocated, count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 0 GROUP BY ccc_geolocated;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- other_ccc_location_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 0 AND other_ccc_location_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- other_ccc_country_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 0 AND other_ccc_country_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    ## BINARY 1
    print ('\nFor those that are ONE:')
    print ('- geolocated:')
    query = 'SELECT ccc_geolocated, count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 GROUP BY ccc_geolocated;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- country_wd:')
    query = 'SELECT country_wd, count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND country_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- location_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND location_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- language_strong_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND language_strong_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- created_by_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND created_by_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- part_of_wd:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND part_of_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- keyword_title:')
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary = 1 AND keyword_title IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('\nFor those that are POTENTIAL ONE, we check the distribution of features in ccc_binary:')
    print ('- category_crawling:')
    query = 'SELECT ccc_binary, count(*) FROM '+languagecode+'wiki WHERE category_crawling_territories_level IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- language_weak_wd:')
    query = 'SELECT ccc_binary, count(*) FROM '+languagecode+'wiki WHERE language_weak_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- affiliation_wd:')
    query = 'SELECT ccc_binary, count(*) FROM '+languagecode+'wiki WHERE affiliation_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- has_part_wd:')
    query = 'SELECT ccc_binary, count(*) FROM '+languagecode+'wiki WHERE has_part_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])


def evaluate_content_selection_manual_assessment(languagecode, selected, page_titles_page_ids):

    print("start the CONTENT selection manual assessment ")

    if selected is None:
        print ('Retrieving the CCC Articles from the .db')
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
        ccc_df = pd.read_sql_query(query, conn)
        ccc_df = ccc_df[['page_title','category_crawling_territories','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']]
        ccc_df = ccc_df.set_index(['page_title'])
        ccc_df = ccc_df.sample(frac=1) # randomize the rows order

        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]
        ccc_df_no = ccc_df.loc[ccc_df['ccc_binary'] == 0]

        sample = 100
        ccc_df_list_yes = ccc_df_yes.index.tolist()[:sample]
        ccc_df_list_no = ccc_df_no.index.tolist()[:sample]

        """
        output_file_name = 'ccc_assessment.txt'
        output_file_name_general1 = open(output_file_name, 'w')
        output_file_name_general1.write(', '.join('"{0}"'.format(w) for w in ccc_df_list_yes))
        output_file_name_general1.write(', '.join('"{0}"'.format(w) for w in ccc_df_list_no))
        """

        print ('ccc_df_list_yes=')
        print (ccc_df_list_yes)

        print ('ccc_df_list_no=')
        print (ccc_df_list_no)

        return # we return because this is actually run in another file: ccc_manual_assessment.py as it is not possible to open browsers via ssh.

        binary_list = sample*['c']+sample*['w']

        ccc_df_list = ccc_df_list_yes + ccc_df_list_no
        samplearticles=dict(zip(ccc_df_list,binary_list))

    else:
        page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

        print ('Using the CCC Articles that have just been classified.')
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
        ccc_df = pd.read_sql_query(query, conn)
        ccc_df = ccc_df[['page_title','category_crawling_territories','category_crawling_territories_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']]
        ccc_df = ccc_df.set_index(['page_title'])
#        ccc_df = ccc_df.sample(frac=1) # randomize the rows order

        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]

        new = []
        for x in selected: new.append(page_ids_page_titles[x[0]])
        ccc_df_list_yes = ccc_df_yes.index.tolist() + new
        ass = random.sample(ccc_df_list_yes, len(ccc_df_list_yes))
        ass = random.sample(ass, len(ass)); ass = random.sample(ass, len(ass))
        ccc_df_list_yes = ass
#        print (len(ccc_df_list_yes))

        ccc_df_no = page_titles_page_ids
        for x in ccc_df_list_yes: del ccc_df_no[x]
        ccc_df_list_no = list(ccc_df_no.keys())
        ass = random.sample(ccc_df_list_no, len(ccc_df_list_no))
        ass = random.sample(ass, len(ass)); ass = random.sample(ass, len(ass))
        ccc_df_list_no = ass
#        print (len(ccc_df_list_no))

        sample = 50
        ccc_df_list_yes = ccc_df_list_yes[:sample]
        ccc_df_list_no = ccc_df_list_no[:sample]

        print (ccc_df_list_yes)
        print (ccc_df_list_no)

        return # we return because this is actually run in another file: ccc_manual_assessment.py as it is not possible to open browsers via ssh.

        binary_list = sample*['c']+sample*['w']
        ccc_df_list = ccc_df_list_yes + ccc_df_list_no
        samplearticles=dict(zip(ccc_df_list,binary_list))

#        print (ccc_df_list)
#        print (samplearticles)
#        print (len(samplearticles))

    print ('The Articles are ready for the manual assessment.')
    ccc_df_list = random.shuffle(ccc_df_list)
    testsize = 200
    CCC_falsepositive = 0
    WP_falsenegative = 0

    counter = 1
    for title in samplearticles.keys():

        page_title = title
        wiki_url = urllib.parse.urljoin(
            'https://%s.wikipedia.org/wiki/' % (languagecode,),
            urllib.parse.quote_plus(page_title.encode('utf-8')))
        translate_url = urllib.parse.urljoin(
            'https://translate.google.com/translate',
            '?' + urllib.parse.urlencode({
                'hl': 'en',
                'sl': 'auto',
                'u': wiki_url,
            }))
        print (str(counter)+'/'+str(testsize)+' '+translate_url)
    #    webbrowser.open_new(wiki_url)
        webbrowser.open_new(translate_url)

        answer = input()

        if (answer != samplearticles[title]) & (samplearticles[title]=='c'): # c de correct
            CCC_falsepositive = CCC_falsepositive + 1
    #        print 'CIRA fals positiu'
        if (answer != samplearticles[title]) & (samplearticles[title]=='w'):  # w de wrong
            WP_falsenegative = WP_falsenegative + 1
    #        print 'WP fals negatiu'

        counter=counter+1

    result = 'WP '+languagecode+'wiki, has these false negatives: '+str(WP_falsenegative)+', a ratio of: '+str((float(WP_falsenegative)/100)*100)+'%.'+'\n'
    result = result+'CCC from '+languagecode+'wiki, has these false positives: '+str(CCC_falsepositive)+', a ratio of: '+str((float(CCC_falsepositive)/100)*100)+'%.'+'\n'
    print (result)




def create_update_qitems_single_ccc_table():

    function_name = 'create_update_qitems_single_ccc_table'
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    query = 'DROP TABLE IF EXISTS qitems_lang_ccc;'
    cursor.execute(query)
    conn.commit()

    query = ('CREATE table if not exists qitems_lang_ccc ('+
    'qitem text primary key,'+
    'langs text'+')')
    cursor.execute(query)
    conn.commit()

    qitems_langs = {} 
    for languagecode in wikilanguagecodes:
        print(languagecode)
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ccc_binary = 1;'
        i=0
        for row in cursor.execute(query): 
            i+=1
            qitem = row[0]
            try:
                langs = qitems_langs[qitem]
                qitems_langs[qitem] = langs + '\t' + languagecode
            except:
                qitems_langs[qitem] = languagecode
        print(i)
    params = []
    for qitem, langs in qitems_langs.items():
        params.append((qitem, langs))

    query = 'INSERT OR IGNORE INTO qitems_lang_ccc (qitem, langs) values (?,?)'
    cursor.executemany(query, params); # to top_diversity_articles.db
    conn.commit()

    for languagecode in wikilanguagecodes:
        query = "SELECT COUNT(*) FROM "+languagecode+"wiki INNER JOIN qitems_lang_ccc on "+languagecode+"wiki.qitem = qitems_lang_ccc.qitem WHERE "+languagecode+"wiki.ccc_binary=0;"
        cursor.execute(query)
        value = cursor.fetchone()
        print (languagecode)
        if value != None: 
            print(value[0])

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



#######################################################################################


def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WDO - CONTENT SELECTION ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/content_selection.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WDO - CONTENT SELECTION ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue


######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("content_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("content_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'content_selection.py'
    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
#    check_time_for_script_run(script_name, cycle_year_month)
    startTime = time.time()

    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();


    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)
    print (len(wikilanguagecodes))

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')
#    print (wikilanguagecodes)
    
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)]
    biggest = wikilanguagecodes_by_size[:20]; smallest = wikilanguagecodes_by_size[20:]


    # if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)


    wikilanguages_utils.finish_email(startTime,'content_selection.out','Content Selection')

