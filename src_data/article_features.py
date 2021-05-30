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
import io
import os
import sys
import shutil
import re
import random
import operator
from statistics import median
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
# requests and others
import requests
import urllib
import webbrowser
import reverse_geocoder as rg
import numpy as np
from random import shuffle
# data
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# Twice the same table in a short period of time not ok.
# Load all page_titles from all languages is not ok.
import gc

import xml.etree.ElementTree as etree


# MAIN
def main():

    extend_articles_pageviews()
    extend_articles_images()
    extend_articles_wikirank()
    

    for languagecode in wikilanguagecodes: 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids)
        extend_articles_featured(languagecode, page_titles_qitems)
        extend_articles_interwiki_qitem_properties_identifiers_sister_projects(languagecode, page_titles_page_ids)


    for languagecode in wikilanguagecodes: 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        extend_articles_history_features(languagecode, page_titles_qitems, page_titles_page_ids)
        # print (languagecode)
        # print('.\n')

    extend_articles_first_timestamp_lang()


################################################################


def extend_articles_pageviews():
    function_name = 'extend_articles_pageviews'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    pageviews_dict = {}

    no_dump = True
    increment = 31
    while (no_dump):
        lastMonth = datetime.date.today() - datetime.timedelta(days=increment)
        month_day = lastMonth.strftime("%Y-%m")
        filename = 'pagecounts-'+month_day+'-views-ge-5-totals.bz2'
        read_dump = '/public/dumps/public/other/pagecounts-ez/merged/'+filename
        print (read_dump)

        try:
            dump_in = bz2.open(read_dump, 'r')
            no_dump = False
        except:
            print (filename+ ' does not exist. We try with the previous month.')
            increment = increment + 30


    line = dump_in.readline()
    line = line.rstrip().decode('utf-8')[:-1]
    values=line.split(' ')
    last_language = values[0].split('.')[0]
    print ('there we go.')

    iter = 0
    while line != '':
        iter += 1
        if iter % 10000000 == 0: print (str(iter/10000000)+' million lines.')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values=line.split(' ')


        if len(values)<3: continue
        language = values[0].split('.')[0]
        page_title = values[1]
        pageviews_count = values[2]

        if language!=last_language:
            if last_language in wikilanguagecodes:
                (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,last_language)
                # print (last_language)
                # print (len(pageviews_dict))
                pageviews = []
                for key,value in pageviews_dict.items():
                    try:
    #                    if last_language=='ca':
    #                        print ((key[0], key[1], pageviews_dict[(key[0],key[1])]))
#                        print((value, page_titles_page_ids[key[1]],page_titles_qitems[key[1]]),key[1],last_language)
                        pageviews.append((value, page_titles_page_ids[key[1]],page_titles_qitems[key[1]]))
                    except:
                        pass
    #            query = "INSERT INTO pageviews (languagecode, page_title, num_pageviews) VALUES (?,?,?);"                    
                query = 'UPDATE '+last_language+'wiki SET num_pageviews=? WHERE page_id = ? AND qitem = ?;'
                cursor.executemany(query,pageviews)
                conn.commit()
                # print (len(pageviews))
            pageviews_dict={}
#            input('')

        if pageviews_count == '': continue
#            print (line)
        if (language,page_title) in pageviews_dict: 
            pageviews_dict[(language,page_title)]=pageviews_dict[(language,page_title)]+int(pageviews_count)
        else:
            pageviews_dict[(language,page_title)]=int(pageviews_count)

#        if page_title == 'Berga' and language == 'ca':
#            print (line)
#            print ((language,page_title))
#            print (pageviews_dict[(language,page_title)])
#            input('')

        last_language=language

    # last iteration
    if last_language in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,last_language)
        # print (last_language)
        # print (len(pageviews_dict))
        pageviews = []
        for key,value in pageviews_dict.items():
            try:
#                    if last_language=='ca':
#                        print ((key[0], key[1], pageviews_dict[(key[0],key[1])]))
#                        print((value, page_titles_page_ids[key[1]],page_titles_qitems[key[1]]),key[1],last_language)
                pageviews.append((value, page_titles_page_ids[key[1]],page_titles_qitems[key[1]]))
            except:
                pass
#            query = "INSERT INTO pageviews (languagecode, page_title, num_pageviews) VALUES (?,?,?);"                    
        query = 'UPDATE '+last_language+'wiki SET num_pageviews=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,pageviews)
        conn.commit()
        # print (len(pageviews))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)

# Extends the Articles table with the number of references per Article.
def extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_references '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-externallinks.sql.gz'
    wikilanguages_utils.check_dump(dumps_path, script_name)

    dump_in = gzip.open(dumps_path, 'r')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    num_references = {}
    for page_id,page_title in page_ids_page_titles.items():
        num_references[page_id]=0

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
                try:
                    el_from = int(row[1])
                    num_references[el_from]+=1
                except:
                    pass
    print ('Done with the dump.')

    parameters = []
    for page_title, page_id in page_titles_page_ids.items():
#        print((num_references[page_id],page_id,page_title,page_titles_qitems[page_title]))
        parameters.append((num_references[page_id],page_id,page_titles_qitems[page_title]))

    query = 'UPDATE '+languagecode+'wiki SET num_references=? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Extends the Articles table with the number of interwiki links.
def extend_articles_interwiki_qitem_properties_identifiers_sister_projects(languagecode, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_interwiki_qitem_properties_identifiers_sister_projects '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    updated = []
    query = "SELECT metadata.qitem, metadata.sitelinks, metadata.properties, metadata.wd_identifiers, metadata.sisterprojects_sitelinks, sitelinks.page_title FROM metadata INNER JOIN sitelinks ON sitelinks.qitem = metadata.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):
    

        try:
            qitem=row[0]
            iw_count=row[1]
            num_wdproperties=row[2]
            wd_identifiers = row[3]
            sisterprojects_sitelinks = row[4]
            page_id=page_titles_page_ids[row[5].replace(' ','_')]
            updated.append((iw_count,num_wdproperties,wd_identifiers,sisterprojects_sitelinks,page_id,qitem))
#            print ((iw_count,num_wdproperties,wd_identifiers,sisterprojects_sitelinks,page_id,qitem))

        except:
            pass
    query = 'UPDATE '+languagecode+'wiki SET num_interwiki = ?, num_wdproperty = ?, num_wdidentifiers = ?, num_multilingual_sisterprojects = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()


    updated = []
    query = "SELECT qitem, sisterprojects, page_title FROM sitelinks WHERE langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):

        try:
            qitem=row[0]
            sisterprojects=row[1]
            page_id=page_titles_page_ids[row[2].replace(' ','_')]
            updated.append((sisterprojects,page_id,qitem))
    #        print ((sisterprojects,page_id,qitem))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki SET sister_projects = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Extends the Articles table with the featured Articles.
def extend_articles_featured(languagecode, page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'extend_articles_featured '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    featuredarticleslanguages = {}
    featuredarticleslanguages['enwiki']="Featured_articles"
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read('en'); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute('SELECT ll_lang,ll_title FROM langlinks WHERE ll_from = 8966941;')
    rows = mysql_cur_read.fetchall()
    for row in rows:
        language = str(row[0].decode('utf-8'))+'wiki'
        language = language.replace('-', '_')
        title = row[1].decode('utf-8')
#        print (title)
        title = title.replace(' ', '_')
        hyphen = title.index(':')
        title = title[(hyphen+1):len(title)]
        # print (title,language)
        featuredarticleslanguages[language] = title
        if language == 'itwiki': featuredarticleslanguages[language] = 'Voci_in_vetrina_su_it.wiki'
        if language == 'ruwiki': featuredarticleslanguages[language] = 'Википедия:Избранные_статьи_по_алфавиту'
    # print ('These are the featured Articles categories in the different languages.')
    # print (featuredarticleslanguages)
    # input('')

    if languagecode+'wiki' in featuredarticleslanguages: featuredtitle=featuredarticleslanguages[languagecode+'wiki']
    else: 
        print ('No featured Articles for language: '+languagecode);
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return

    featuredarticles=[]
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute('SELECT page_title, page_id FROM categorylinks INNER JOIN page on cl_from=page_id WHERE cl_to = %s', (featuredtitle,)) # Extreure
    rows = mysql_cur_read.fetchall()
    for row in rows: 
        page_title=str(row[0].decode('utf-8'))
        page_id=row[1]

        #print (page_title)
        try:
            featuredarticles.append((page_id,page_titles_qitems[page_title]))
            # print ((page_id,page_title,page_titles_qitems[page_title]))
        except:
            pass
#            print ('This Article does not exist: '+page_title)

    query = 'UPDATE '+languagecode+'wiki SET featured_article=1 WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,featuredarticles)
    conn.commit();

    print ('We obtained '+str(len(featuredarticles))+' featured Articles')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
#    print (featuredtitle,languagecode)



# Extends the Articles table with the number of images.
def extend_articles_images():

    functionstartTime = time.time()
    function_name = 'extend_articles_images'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return




    ####
    # create imagelinks.db
    conn = sqlite3.connect(databases_path + imageslinks_db); cursor = conn.cursor()
    for languagecode in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

        page_ids_qitems = {}
        for page_title in page_titles_qitems.keys():
            page_id = page_titles_page_ids[page_title]
            qitem = page_titles_qitems[page_title]
            page_ids_qitems[page_id]=qitem

        cursor.execute("CREATE TABLE IF NOT EXISTS imagelinks (langcode text, qitem text, image_title text, PRIMARY KEY (langcode, qitem, image_title));")
        conn.commit()

        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-imagelinks.sql.gz'
        wikilanguages_utils.check_dump(dumps_path, script_name)

        dump_in = gzip.open(dumps_path, 'r')
        images_count = {}
        images_from = {}

        print ('Iterating the imagelinks dump: '+dumps_path)

        start_dict_true = {}
        k = 0
        j = 0
        parameters = []
        while True:
            line = dump_in.readline()
            try: line = line.decode("utf-8")
            except UnicodeDecodeError: line = str(line)

            if len(start_dict_true)<3:
                if '`il_from` ' in line: 
                    start_dict_true['il_from']=k
                    k+=1
                if '`il_to` ' in line: 
                    start_dict_true['il_to']=k
                    k+=1
                if '`il_from_namespace` ' in line: 
                    start_dict_true['il_from_namespace']=k
                    k+=1

            if line == '':
                i+=1
                if i==3: break
            else: i=0

            if wikilanguages_utils.is_insert(line):
                values = wikilanguages_utils.get_values(line)
                if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)
                for row in rows:

                    j+=1
                    try:
                        il_from_namespace = int(row[start_dict_true['il_from_namespace']])
                    except:
                        continue
                    if il_from_namespace != 0: continue


                    try:
                        il_from = int(row[start_dict_true['il_from']])
                        qitem = page_ids_qitems[il_from]
                    except:
                        continue

                    il_to = row[start_dict_true['il_to']]

                    parameters.append((languagecode, qitem, il_to))

                    if j % 500000 == 0:
                        print (languagecode+' imagelinks row: '+str(j))
                        query = 'INSERT OR IGNORE INTO imagelinks (langcode, qitem, image_title) VALUES (?,?,?);'
                        cursor.executemany(query,parameters)
                        conn.commit()
                        parameters = []

        query = 'INSERT OR IGNORE INTO imagelinks (langcode, qitem, image_title) VALUES (?,?,?);'
        cursor.executemany(query,parameters)
        conn.commit()

        print ('Done with the dump imagelinks for language: '+languagecode)
        print ('Lines: '+str(j))
        print ('* number of parameters to introduce: '+str(len(parameters))+'\n')




    """

    ####
    # create images.db
    print ('creating images.db')
    try:
        os.remove(databases_path+images_db)
    except:
        pass

    conn = sqlite3.connect(databases_path + images_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + imageslinks_db); cursor2 = conn2.cursor()

    print ('start counting.')
    cursor.execute("CREATE TABLE IF NOT EXISTS all_images_count (image text, count integer, PRIMARY KEY (image));")
    conn.commit()

    print ('counting')
    params = []
    query = 'SELECT image_title, count(qitem) FROM imagelinks GROUP BY image_title ORDER BY 2 DESC LIMIT 10000;'
    for row in cursor2.execute(query):
        image_title = row[0]
        num = row[1]
        params.append((image_title,num))
    print (len(params))


    query = 'INSERT OR IGNORE INTO all_images_count (image, count) VALUES (?,?);'
    cursor.executemany(query,params)
    conn.commit()
    print ('counts done.')

    undesired = {}
    query = 'SELECT image FROM all_images_count;'
    for row in cursor.execute(query):
        undesired[row[0]]=None


    print ('start qitems - images')
    cursor.execute("CREATE TABLE IF NOT EXISTS all_qitems_images (qitem text, images text, PRIMARY KEY (qitem));")
    conn.commit()

    query = 'SELECT qitem, image_title FROM imagelinks ORDER BY qitem;'

    image_count = {}
    params = []
    i = 0
    old_qitem = ''
    for row in cursor2.execute(query):
        qitem = row[0]

        if qitem != old_qitem and i != 0:
            listofTuples = sorted(image_count.items(), key=operator.itemgetter(1), reverse=True)

            j = 0
            images = ''
            for tup in listofTuples:
                # if j>100: break
                image_title = tup[0]

                try:
                    undesired[image_title]
                except:
                    if images != '': images+= ';'
                    images += image_title+':'+str(tup[1])

                j+=1

            params.append((old_qitem, images))
            image_count = {}


        image_title = row[1]
        try:
            image_count[image_title]=image_count[image_title]+1
        except:
            image_count[image_title]=1

        old_qitem = qitem
        if i % 2400000 == 0:
            print (i)
            query = 'INSERT OR IGNORE INTO all_qitems_images (qitem, images) VALUES (?,?);'
            cursor.executemany(query,params)
            conn.commit()
            params = []

        i+= 1

    query = 'INSERT OR IGNORE INTO all_qitems_images (qitem, images) VALUES (?,?);'
    cursor.executemany(query,params)
    conn.commit()
    print (i)
    print ('qitems - images done.')

    query = 'DROP TABLE all_images_count;'
    cursor.execute(query)
    conn.commit()

    wikilanguages_utils.copy_db_for_production(images_db, 'article_features.py', databases_path)
    os.remove(databases_path+images_db)

    """




    ####
    # extend num_images in wikipedia_diversity.db
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + imageslinks_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

        qitems_page_ids = {v: page_titles_page_ids[k] for k, v in page_titles_qitems.items()}

        query = "SELECT qitem, count(image_title) FROM imagelinks WHERE langcode = ? GROUP BY image_title;"

        params = []
        for row in cursor2.execute(query, (languagecode,)):
            qitem = row[0]
            params.append((row[1],qitems_page_ids[qitem],qitem))

        query = 'UPDATE '+languagecode+'wiki SET num_images = ? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,params)
        conn.commit()

    os.remove(databases_path+imageslinks_db)



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def extend_articles_wikirank():

    functionstartTime = time.time()
    function_name = 'extend_articles_wikirank'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    # Locate the dump
    read_dump = 'wikipediaquality2018.zip' # read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'

    parameters = []
    languagecode = ''

    with zipfile.ZipFile(dumps_path + read_dump) as dumpzip:
        with dumpzip.open('wikirank_scores_201807.tsv') as myfile:

            line = myfile.readline()
            iter = 0

            print ('Iterating the dump.')
            while line != '':
                line = myfile.readline()
                line = line.rstrip().decode('utf-8')[:-1]
                page_data = line.split('\t')

                if page_data[0] == 'language': continue

                try:
                    cur_languagecode = page_data[0]
                    cur_page_id = int(page_data[1])
                    cur_wikirank_quality = page_data[4]
                except:
#                    print (page_data)
                    continue

                if cur_languagecode != languagecode:
#                    print ('loading articles ids.')
                    print (cur_languagecode)
#                    print (page_data)
#                    print (line)
                    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,cur_languagecode.replace('-','_'))
                    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

                try:
                    cur_page_title = page_ids_page_titles[cur_page_id]
                    cur_qitem = page_titles_qitems[cur_page_title]
                    parameters.append((cur_wikirank_quality,cur_page_id,cur_qitem))
                except:
                    continue

                if cur_languagecode != languagecode and iter!=0:
#                    print ('\n* language: ')
#                    print (languagecode)
                    query = 'UPDATE '+languagecode.replace('-','_')+'wiki SET wikirank=? WHERE page_id = ? AND qitem = ?;'
                    cursor2.executemany(query,parameters)
                    conn2.commit()
                    print ('this number of articles has been updated: '+str(len(parameters)))
                    print ('current time: ' + str(time.time() - startTime))
                    parameters = []
                    print ('*\n')

                languagecode = cur_languagecode
                iter += 1

            print ('\n* language: ')
            print (languagecode)
            query = 'UPDATE '+languagecode.replace('-','_')+'wiki SET wikirank=? WHERE page_id = ? AND qitem = ?;'
            cursor2.executemany(query,parameters)
            conn2.commit()
            print ('this number of articles has been updated: '+str(len(parameters)))
            print ('current time: ' + str(time.time() - startTime))
            parameters = []
            print ('*\n')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def extend_articles_history_features(languagecode,page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_history_features '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    page_ids_qitems = {page_titles_page_ids[k]: v for k, v in page_titles_qitems.items()}


    # last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
    last_month_date = datetime.datetime.strptime(cycle_year_month,'%Y-%m')
    next_month_date = datetime.datetime.strptime(cycle_year_month,'%Y-%m') + relativedelta.relativedelta(months=1)

    first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day = int(next_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))

    # last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))
    print (first_day, last_day)


    # IF IT GETS TOO BIG, WE COULD CLEAN THE TABLE WITH THE PAGES ALREADY INSERTED.
    try:
        os.remove(databases_path+'editors_pages.db')
    except:
        pass

    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    try:
        query = 'CREATE TABLE editors (page_id integer, editor integer, PRIMARY KEY (page_id, editor));'
        cursor2.execute(query)
        conn2.commit()
        query = 'CREATE TABLE editors_history (editor integer, edit_count integer, year_first_edit integer, flag text, PRIMARY KEY (editor));'

        cursor2.execute(query)
        conn2.commit()
        print ('editors table created.')
    except:
        print ('editors table could not be created.')
        pass

    article_completed = {}

    first_edit_timestamp = {}
    last_edit_timestamp = {}
    last_discussion_timestamp = {}

    num_edits = {}
    num_edits_last_month = {}
    num_discussions = {}

    num_anonymous_edits = {}
    num_bots_edits = {}
    num_reverts = {}

    editor_history = {}


    for page_title in page_titles_qitems.keys():
        num_edits[page_title]=0
        num_edits_last_month[page_title]=0
        num_discussions[page_title]=0

        first_edit_timestamp[page_title]=0
        last_edit_timestamp[page_title]=0
        last_discussion_timestamp[page_title]=0

        num_anonymous_edits[page_title]=0
        num_bots_edits[page_title]=0
        num_reverts[page_title]=0



    def get_mediawiki_paths(languagecode):

        cym = cycle_year_month
        d_paths = []

        print ('/public/dumps/public/other/mediawiki_history/'+cym)
        if os.path.isdir('/public/dumps/public/other/mediawiki_history/'+cym)==False:
            cym = datetime.datetime.strptime(cym,'%Y-%m')-dateutil.relativedelta.relativedelta(months=1)
            cym = cym.strftime('%Y-%m')
            print ('/public/dumps/public/other/mediawiki_history/'+cym)

        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.all-time.tsv.bz2'

        if os.path.isfile(dumps_path):
            print ('one all-time file.')
            d_paths.append(dumps_path)
        else:
            print ('multiple files.')
            for year in range (2025, 1999, -1):
                dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'.tsv.bz2'
                if os.path.isfile(dumps_path): 
                    d_paths.append(dumps_path)

            if len(d_paths) == 0:
                for year in range(2025, 1999, -1): # months
                    for month in range(13, 0, -1):
                        if month > 9:
                            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-'+str(month)+'.tsv.bz2'
                        else:
                            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-0'+str(month)+'.tsv.bz2'

                        if os.path.isfile(dumps_path) == True:
                            d_paths.append(dumps_path)

        print(len(d_paths))
        print (d_paths)

        return d_paths


    d_paths = get_mediawiki_paths(languagecode)
    print ('Total number of articles: '+str(len(num_edits)))


    if (len(d_paths)==0):
        print ('dump error at script '+script_name+'. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values=line.split(' ')

        parameters = []
        editors_params = []
        iter = 0
        while line != '':
            # iter += 1
            # if iter % 1000000 == 0: print (str(iter/1000000)+' million lines.')
            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values=line.split('\t')

            if len(values)==1: continue

            # page_title_historical = values[24]
            page_id = values[23]
            page_title = values[25]


            if page_title not in first_edit_timestamp: continue

            try: page_namespace = int(values[28])
            except: continue

            try: edit_count = values[34]
            except: continue

            if edit_count == 'null': edit_count = 1
            else: edit_count = int(edit_count)

            # seconds_last_edit = values[35]

            last_timestamp = values[3]
            if last_timestamp != 'null': last_timestamp = int(datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'))
            else: last_timestamp = 0

            if page_namespace == 0:
                if last_edit_timestamp[page_title] <= last_timestamp:
                    last_edit_timestamp[page_title] = last_timestamp

                    if edit_count > num_edits[page_title]: num_edits[page_title] = edit_count
                    # num_edits[page_title]=edit_count

                first_timestamp = values[33]
                if first_timestamp != 'null' and first_timestamp != '': first_timestamp = int(datetime.datetime.strptime(first_timestamp[:len(first_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'))
                else: first_timestamp = 0

                if last_timestamp == first_timestamp:
                    article_completed[page_title] = None
                    first_edit_timestamp[page_title]=first_timestamp

                user_id = values[5]
                # user_text = values[38]
                # print (user_id, values[38])


                # bots
                event_is_bot_by = values[13]
                if event_is_bot_by != '':
                    try: num_bots_edits[page_title]+=1
                    except: pass


                if user_id != "":

                    if event_is_bot_by != '':
                        try:
                            editors_params.append((user_id,page_id))
                            # num_editors[page_title].add(int(user_anon))

                            try: 
                                editor_history[user_id]
                            except: 

                                editor_first_edit_timestamp = values[20]
                                year_first_edit = ''
                                if editor_first_edit_timestamp != None and editor_first_edit_timestamp != '':  
                                    year_first_edit = datetime.datetime.strptime(editor_first_edit_timestamp[:len(editor_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')

                                user_flag = values[11]
                                edit_count = values[21]

                                editor_history[user_id] = (user_id, edit_count, year_first_edit, user_flag)


                        except:
                            pass

                else:
                    num_anonymous_edits[page_title]+=1





                revision_is_identity_reverted = values[64]
                if revision_is_identity_reverted == 'true':
                    try: num_reverts[page_title]+=1
                    except: pass


                if last_timestamp > first_day and last_timestamp < last_day:
                    # print (page_title, first_day, last_day, last_edit_timestamp)
                    try: num_edits_last_month[page_title]+=1
                    except: pass



            if page_namespace == 1:
                if last_discussion_timestamp[page_title] < last_timestamp:
                    last_discussion_timestamp[page_title] = last_timestamp
                    num_discussions[page_title] = edit_count



        query = 'INSERT OR IGNORE INTO editors (editor, page_id) VALUES (?,?);'
        cursor2.executemany(query,editors_params)
        conn2.commit()
        editors_params = []


#        print (editor_history)
        editor_history_params = []
        for user_id, data in editor_history.items():
            editor_history_params.append(data)

        query = 'INSERT OR IGNORE INTO editors_history (editor, edit_count, year_first_edit, flag) VALUES (?,?,?,?);'
        cursor2.executemany(query,editor_history_params)
        conn2.commit()
        editor_history_params = []
        editor_history = {}



        for page_title in article_completed.keys():

            if num_edits_last_month[page_title] > num_edits[page_title]: num_edits[page_title] = num_edits_last_month[page_title]

            try:
                parameters.append((num_edits[page_title], num_discussions[page_title], num_edits_last_month[page_title], first_edit_timestamp[page_title], last_edit_timestamp[page_title], last_discussion_timestamp[page_title], num_anonymous_edits[page_title], num_bots_edits[page_title], num_reverts[page_title], page_titles_page_ids[page_title], page_titles_qitems[page_title], page_title))

                # del num_editors[page_title]
                del num_edits[page_title]
                del num_discussions[page_title]
                del num_edits_last_month[page_title]


                del first_edit_timestamp[page_title]
                del last_edit_timestamp[page_title]
                del last_discussion_timestamp[page_title]

                del num_anonymous_edits[page_title]
                del num_bots_edits[page_title]
                del num_reverts[page_title]

            except:
                pass

        article_completed = {}

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'UPDATE '+languagecode+'wiki SET num_edits = ?, num_discussions = ?, num_edits_last_month = ?, date_created = ?, date_last_edit = ?, date_last_discussion = ?, num_anonymous_edits = ?, num_bot_edits = ?, num_reverts = ? WHERE page_id = ? AND qitem = ? AND page_title = ?;'

        # print (query)
        cursor.executemany(query,parameters)
        conn.commit()
        print ('Articles introduced: '+str(len(parameters))+'. Articles left for next files or not in the database: '+str(len(num_edits))+'.')
        print ('This file took: '+str(datetime.timedelta(seconds=time.time() - iterTime)))
        parameters = []
        conn.commit()



    # IF IT GETS TOO BIG, WE COULD CLEAN THE TABLE WITH THE PAGES ALREADY INSERTED.
    parameters = []   
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT count(*), page_id FROM editors GROUP BY page_id ORDER BY page_id;'
    for row in cursor2.execute(query):
        page_id = row[1]
        try:
            parameters.append((row[0],page_id,page_ids_qitems[page_id]))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki SET num_editors = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters)
    conn.commit()


    # median human editor edits
    parameters = []   
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT editors_history.edit_count, page_id FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor ORDER BY page_id;'

    count_values = []
    old_page_id = ''
    for row in cursor2.execute(query):
        page_id = row[1]

        if page_id != old_page_id and old_page_id != '':

            try:
                parameters.append((median(count_values),page_id,page_ids_qitems[page_id]))
            except:
                pass
            count_values = []

        try:
            count_values.append(int(row[0]))
        except:
            pass

        old_page_id = page_id


    try:
        parameters.append((median[count_values],page_id,page_ids_qitems[page_id]))
    except:
        pass
    count_values = []

    query = 'UPDATE '+languagecode+'wiki SET median_editors_edits = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters)
    conn.commit()


    # median year first edit
    parameters = []   
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT editors_history.year_first_edit, page_id FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor ORDER BY page_id;'

    year_values = []
    old_page_id = ''
    for row in cursor2.execute(query):
        page_id = row[1]

        if page_id != old_page_id and old_page_id != '':

            try:
                parameters.append((median(year_values),page_id,page_ids_qitems[page_id]))
            except:
                pass
            year_values = []

        try:
            year_values.append(int(row[0]))
        except:
            pass
        old_page_id = page_id

    try:
        parameters.append((median(year_values),page_id,page_ids_qitems[page_id]))
    except:
        pass
    year_values = []

    query = 'UPDATE '+languagecode+'wiki SET median_year_first_edit = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters)
    conn.commit()


    # number of admin editors
    parameters = []   
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    query = 'SELECT count(*), page_id, flag FROM editors INNER JOIN editors_history ON editors.editor = editors_history.editor WHERE editors_history.flag != "" GROUP BY page_id ORDER BY page_id;'
    for row in cursor2.execute(query):
        page_id = row[1]
        flag = row[2]

        for x in ["sysop","bureaucrat","oversight","checkuser","steward"]:
            if x in flag:
                try:
                    parameters.append((row[0],page_id,page_ids_qitems[page_id]))
                except:
                    pass   
        # ("sysop","bureaucrat","oversight","checkuser","steward") 

    query = 'UPDATE '+languagecode+'wiki SET num_admin_editors = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query, parameters)
    conn.commit()
    os.remove(databases_path+'editors_pages.db')

    print(languagecode+' history parsed and stored')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def extend_articles_first_timestamp_lang():
    function_name = 'extend_articles_first_timestamp_lang'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    
    lang_qitems_is_first_timestamp = {}
    qitems_timestamp_lang = {}
    for languagecode in wikilanguagecodes:
        print (languagecode)
        lang_qitems_is_first_timestamp[languagecode]=[]

        query = 'SELECT qitem, date_created FROM '+languagecode+'wiki;'
        for row in cursor.execute(query):
            qitem = row[0]
            first_timestamp = row[1]

            try:
                lang_timestamp = qitems_timestamp_lang[qitem]

                stored_timestamp = lang_timestamp[0]
                stored_lang = lang_timestamp[1]

                if stored_timestamp > first_timestamp:
                    qitems_timestamp_lang[qitem] = [first_timestamp,languagecode]

            except:
                qitems_timestamp_lang[qitem] = [first_timestamp,languagecode]

    parameters = []
    for qitem,lang_timestamp in qitems_timestamp_lang.items():
        parameters.append((str(lang_timestamp[1]),str(qitem)))
        stored_lang = lang_timestamp[1]

    print (len(parameters))
    print ('now introducing...')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        query = 'UPDATE '+languagecode+'wiki SET first_timestamp_lang = ? WHERE qitem = ?;'
        cursor.executemany(query,parameters)
        conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





#######################################################################################


def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WCDO - ARTICLE FEATURES ERROR: '+ cycle_year_month, 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/article_features.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WCDO - ARTICLE FEATURES ERROR: '+ cycle_year_month, 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue


#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("article_features"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("article_features"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'article_features.py'
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

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')   
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=False)]
    biggest = wikilanguagecodes_by_size[20:]; smallest = wikilanguagecodes_by_size[:50]


    if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)

    wikilanguages_utils.finish_email(startTime,'article_features.out', 'WIKIPEDIA DIVERSITY ARTICLE features')
