# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
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


# EXECUTION THE 7TH DAY OF EVERY MONTH!
# MAIN
def main():

    for languagecode in ['en']: 
    # for languagecode in sorted(wikilanguagecodes,reverse=True): 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids)

    return


    # FETS
    extend_articles_pageviews()
    extend_articles_wikirank()

    for languagecode in wikilanguagecodes: 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

        extend_articles_interwiki(languagecode, page_titles_page_ids)
        extend_articles_qitems_properties(languagecode, page_titles_page_ids)
        extend_articles_images(languagecode,page_titles_qitems,page_titles_page_ids)
        extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids)
        extend_articles_featured(languagecode, page_titles_page_ids)


    # UPDATING THE ARTICLES
    # i = 0
    # with ThreadPoolExecutor(max_workers=2) as executor:
    #     for languagecode in wikilanguagecodes: 
    #         (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

    #         executor.submit(extend_articles_references, languagecode, page_titles_qitems, page_titles_page_ids)
    #         executor.submit(extend_articles_bytes, languagecode, page_titles_qitems)
    #         executor.submit(extend_articles_qitems_properties, languagecode, page_titles_page_ids)
    #         executor.submit(extend_articles_featured, languagecode, page_titles_page_ids)
    #         executor.submit(extend_articles_interwiki, languagecode, page_titles_page_ids)
    #         executor.submit(extend_articles_images, languagecode, page_titles_qitems, page_titles_page_ids)
    #         extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids)

    #     del (page_titles_qitems, page_titles_page_ids); gc.collect()
#    backup_db()
 

################################################################


# def extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids):

#     functionstartTime = time.time()
#     function_name = 'extend_articles_revision_features '+languagecode
#     if create_function_account_db(function_name, 'check','')==1: return

#     num_editors = {}
#     num_edits = {}
#     num_edits_last_month = {}
#     first_edit_timestamp = {}
#     num_discussions = {}


#     page_ids_qitems = {}
#     for page_title,page_id in page_titles_page_ids.items():
#         page_ids_qitems[page_id]=page_titles_qitems[page_title]

#     for page_id in page_titles_page_ids.values():
#         num_discussions[page_id]=0
#         num_editors[page_id]=set()
#         num_edits[page_id]=0
#         num_edits_last_month[page_id]=0
#         first_edit_timestamp[page_id]=''

#     last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
#     first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
#     last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))

#     d_paths = []
#     i = 0
#     loop = True
#     while (loop):
#         i+=1
#         dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history'+str(i)+'.xml.gz'
#         loop = os.path.isfile(dumps_path)
#         dumps_path = '/srv/wcdo/dumps/enwiki-latest-stub-meta-history'+str(i)+'.xml'#.gz'
#         if loop == True: 
#             d_paths.append(dumps_path)
#         if i==1 and loop == False:#True:
#             d_paths.append('/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history.xml.gz')

#     print(len(d_paths))

#     page_title_page_id = {}
#     for dump_path in d_paths:

#         print(dump_path)

#         page_title = None
#         cur_page_title = None
#         ns = None
#         page_id = None

#         cur_time = time.time()
#         i=0

#         n_discussions = 0
#         n_edits = 0
#         n_editors = set()


# #        with gzip.open(dump_path, 'rb') as xml_file:
#         pages = etree.iterparse(dump_path, events=("start", "end"))
#         for event, elem in pages:

#             if event == 'start':

#                 if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}mediawiki':
#                     root = elem

#             elif event == 'end':
#                 # SECTION PAGETITLE, PAGEID, NS

#                 taggy = elem.tag.replace('{http://www.mediawiki.org/xml/export-0.10/}','')
#                 text =  elem.text
# #                print (taggy)
# #                print (text)

#                 if taggy == 'title': 
#                     page_title = text.replace(' ','_')

#                 # if elem.attrib != None:
#                 #     try:
#                 #         page_title = elem.attrib['title'].replace(' ','_')
#                 #     except:
#                 #         pass

#                 if page_title != cur_page_title and cur_page_title != None:
#                     num_edits[page_id] = n_edits
#                     n_edits = 0
#                     num_editors[page_id]=len(n_editors)
#                     n_editors=set()
#                     num_discussions[page_id]=n_discussions
#                     page_id = None

#                     i+=1
#                     if i%100==0: 
#                         last_time=cur_time
#                         cur_time=time.time()
#                         print('\t'+str(i)+' '+str(datetime.timedelta(seconds=cur_time - last_time)))
#                         print(str(round(100/(cur_time - last_time),3))+' '+'pages per second.')

#                 cur_page_title = page_title




#                 if taggy == 'id' and page_id == None:
#                     page_id = int(text)

#                     if ns == 0:
#                         try:
# 	                        qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
# 	                        page_title_page_id[cur_page_title]=page_id
#                         except:
#                             page_id = None

#                     if ns == 1:
#                         title = cur_page_title.split(':')[1]
#                         try:
#                             page_id = page_title_page_id[title]
#                             qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
#                         except:
#                             page_id = None

#                 if taggy == 'ns':
#                     ns = int(text)

#                 if (taggy == 'username' or taggy == 'ip') and page_id != None:
#                     username = text

#                     if ns == 0:
#                         n_editors.add(username)
#                         n_edits+=1

#                     if ns == 1 and page_id!= None:
#                         n_discussions+=1
# #                        print(page_id,ns,cur_page_title)

#                 if taggy == 'timestamp' and ns == 0 and page_id != None: 

#                     timestamp = datetime.datetime.strptime(text,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H%M%S')
#                     if first_edit_timestamp[page_id]=='': first_edit_timestamp[page_id] = timestamp
#                     timestamp = int(timestamp)
#                     if timestamp > first_day and timestamp < last_day:
#                         num_edits_last_month[page_id]+=1

#                 elem.clear()

#     print('parsed')

#     page_ids_page_titles = {v: k for k, v in page_title_page_id.items()}
#     parameters = []
#     for page_id in num_editors:
#         # print((len(num_editors[page_id]), 
#         #     num_edits[page_id], 
#         #     num_edits_last_month[page_id], 
#         #     first_edit_timestamp[page_id], 
#         #     num_discussions[page_id], 
#         #     page_id, 
#         #     page_ids_qitems[page_id], 
#         #     page_ids_page_titles[page_id]))

#         try:
#             parameters.append((num_editors[page_id], num_edits[page_id], num_edits_last_month[page_id], first_edit_timestamp[page_id], num_discussions[page_id], page_id, page_ids_qitems[page_id], page_ids_page_titles[page_id]))
#         except:
#             pass
# #    print (len(parameters),len(page_titles_qitems))

#     conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

#     query = 'UPDATE '+languagecode+'wiki SET (num_editors, num_edits, num_edits_last_month, date_created,num_discussions)=(?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
#     cursor.executemany(query,parameters)
#     conn.commit()

#     duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#     create_function_account_db(function_name, 'mark', duration)





# def extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids):

#     functionstartTime = time.time()
#     # function_name = 'extend_articles_revision_features '+languagecode
#     # if create_function_account_db(function_name, 'check','')==1: return

#     num_editors = {}
#     num_edits = {}
#     num_edits_last_month = {}
#     first_edit_timestamp = {}
#     num_discussions = {}


#     page_ids_qitems = {}
#     for page_title,page_id in page_titles_page_ids.items():
#         page_ids_qitems[page_id]=page_titles_qitems[page_title]

#     for page_id in page_titles_page_ids.values():
#         num_discussions[page_id]=0
#         num_editors[page_id]=set()
#         num_edits[page_id]=0
#         num_edits_last_month[page_id]=0
#         first_edit_timestamp[page_id]=''

#     last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
#     first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
#     last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))


#     dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history.xml.gz'
# #    print(dumps_path)

#     page_title = None
#     cur_page_title = None
#     ns = None
#     page_id = None

#     cur_time = time.time()
#     i=0

#     n_discussions = 0
#     n_edits = 0
#     n_editors = set()

#     parameters = []

#     page_title_page_id = {}

#     cur_time = time.time()
#     i=0

#     with gzip.open(dumps_path, 'rb') as xml_file:

#         f = io.BufferedReader(xml_file)

#         pages = etree.iterparse(f, events=("start", "end"))
#         for event, elem in pages:

#             if event == 'start':

#                 if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}mediawiki':
#                     root = elem

#             elif event == 'end':
#                 # SECTION PAGETITLE, PAGEID, NS

#                 taggy = elem.tag.replace('{http://www.mediawiki.org/xml/export-0.10/}','')
#                 text =  elem.text
# #                print (taggy)
# #                print (text)

#                 if taggy == 'title': 
#                     page_title = text.replace(' ','_')

#                 # if elem.attrib != None:
#                 #     try:
#                 #         page_title = elem.attrib['title'].replace(' ','_')
#                 #     except:
#                 #         pass

#                 if page_title != cur_page_title and cur_page_title != None:
#                     num_edits[page_id] = n_edits
#                     n_edits = 0
#                     num_editors[page_id]=len(n_editors)
#                     n_editors=set()
#                     num_discussions[page_id]=n_discussions
#                     page_id = None

#                     i+=1
#                     if i%1000==0: 
#                         last_time=cur_time
#                         cur_time=time.time()
#                         print('\t'+str(i)+' '+str(datetime.timedelta(seconds=cur_time - last_time)))
#                         print(str(round(1000/(cur_time - last_time),3))+' '+'pages per second.')

#                 cur_page_title = page_title




#                 if taggy == 'id' and page_id == None:
#                     page_id = int(text)

#                     if ns == 0:
#                         try:
#                             qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
#                             page_title_page_id[cur_page_title]=page_id
#                         except:
#                             page_id = None

#                     if ns == 1:
#                         title = cur_page_title.split(':')[1]
#                         try:
#                             page_id = page_title_page_id[title]
#                             qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
#                         except:
#                             page_id = None

#                 if taggy == 'ns':
#                     ns = int(text)

#                 if (taggy == 'username' or taggy == 'ip') and page_id != None:
#                     username = text

#                     if ns == 0:
#                         n_editors.add(username)
#                         n_edits+=1

#                     if ns == 1 and page_id!= None:
#                         n_discussions+=1
# #                        print(page_id,ns,cur_page_title)

#                 if taggy == 'timestamp' and ns == 0 and page_id != None: 

#                     timestamp = datetime.datetime.strptime(text,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H%M%S')
#                     if first_edit_timestamp[page_id]=='': first_edit_timestamp[page_id] = timestamp
#                     timestamp = int(timestamp)
#                     if timestamp > first_day and timestamp < last_day:
#                         num_edits_last_month[page_id]+=1

#                 elem.clear()

#     print('parsed')
#     input('')

#     return

#     page_ids_page_titles = {v: k for k, v in page_title_page_id.items()}

#     for page_id in num_editors:
#         # print((len(num_editors[page_id]), 
#         #     num_edits[page_id], 
#         #     num_edits_last_month[page_id], 
#         #     first_edit_timestamp[page_id], 
#         #     num_discussions[page_id], 
#         #     page_id, 
#         #     page_ids_qitems[page_id], 
#         #     page_ids_page_titles[page_id]))

#         try:
#             parameters.append((len(num_editors[page_id]), num_edits[page_id], num_edits_last_month[page_id], first_edit_timestamp[page_id], num_discussions[page_id], page_id, page_ids_qitems[page_id], page_ids_page_titles[page_id]))
#         except:
#             pass
# #    print (len(parameters),len(page_titles_qitems))

#     conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

#     query = 'UPDATE '+languagecode+'wiki SET (num_editors, num_edits, num_edits_last_month, date_created,num_discussions)=(?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
#     cursor.executemany(query,parameters)
#     conn.commit()

# #     duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
# #     create_function_account_db(function_name, 'mark', duration)



def extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    # function_name = 'extend_articles_revision_features '+languagecode
    # if create_function_account_db(function_name, 'check','')==1: return

    num_editors = {}
    num_edits = {}
    num_edits_last_month = {}
    first_edit_timestamp = {}
    num_discussions = {}


    page_ids_qitems = {}
    for page_title,page_id in page_titles_page_ids.items():
        page_ids_qitems[page_id]=page_titles_qitems[page_title]

    for page_id in page_titles_page_ids.values():
        num_discussions[page_id]=0
        num_editors[page_id]=set()
        num_edits[page_id]=0
        num_edits_last_month[page_id]=0
        first_edit_timestamp[page_id]=''

    last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
    first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))


    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history.xml.gz'
#    print(dumps_path)

    page_title = None
    cur_page_title = None
    ns = None
    page_id = None

    cur_time = time.time()
    i=0

    n_discussions = 0
    n_edits = 0
    n_editors = set()

    parameters = []

    page_title_page_id = {}

    cur_time = time.time()
    i=0
    
    p=re.compile('<(.*)>(.*)</(.*)>')

    with gzip.open(dumps_path, 'rb') as xml_file:

        f = io.BufferedReader(xml_file)

#         for line in f:
#             line=line.decode('utf-8')
#             if line=='\n': continue
#             line = line.strip()
# #            print (line)
#             groups = p.match(line)
#             if groups != None:
#                 groups = groups.groups()
#                 taggy = groups[0]
#                 text = groups[1]

#                 if taggy == 'title':
#                     page_title = text.replace(' ','_')


#                     if page_title != cur_page_title and cur_page_title != None:
#                         num_edits[page_id] = n_edits
#                         n_edits = 0
#                         num_editors[page_id]=len(n_editors)
#                         n_editors=set()
#                         num_discussions[page_id]=n_discussions
#                         page_id = None

#                         i+=1
#                         if i%1000==0: 
#                             last_time=cur_time
#                             cur_time=time.time()
#                             print('\t'+str(i)+' '+str(datetime.timedelta(seconds=cur_time - last_time)))
#                             print(str(round(1000/(cur_time - last_time),3))+' '+'pages per second.')

#                     cur_page_title = page_title

#                 if taggy == 'id' and page_id == None:
#                     page_id = int(text)

#                     if ns == 0:
#                         try:
#                             qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
#                             page_title_page_id[cur_page_title]=page_id
#                         except:
#                             page_id = None

#                     if ns == 1:
#                         title = cur_page_title.split(':')[1]
#                         try:
#                             page_id = page_title_page_id[title]
#                             qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
#                         except:
#                             page_id = None

#                 if taggy == 'ns':
#                     ns = int(text)

#                 if (taggy == 'username' or taggy == 'ip') and page_id != None:
#                     username = text

#                     if ns == 0:
#                         n_editors.add(username)
#                         n_edits+=1

#                     if ns == 1 and page_id!= None:
#                         n_discussions+=1
# #                        print(page_id,ns,cur_page_title)

#                 if taggy == 'timestamp' and ns == 0 and page_id != None: 

#                     timestamp = datetime.datetime.strptime(text,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H%M%S')
#                     if first_edit_timestamp[page_id]=='': first_edit_timestamp[page_id] = timestamp
#                     timestamp = int(timestamp)
#                     if timestamp > first_day and timestamp < last_day:
#                         num_edits_last_month[page_id]+=1



#         input('')


        pages = cElementTree.iterparse(f, events=("start", "end"))
        for event, elem in pages:

            if event == 'start':

                if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}mediawiki':
                    root = elem

            elif event == 'end':
                # SECTION PAGETITLE, PAGEID, NS

                taggy = elem.tag.replace('{http://www.mediawiki.org/xml/export-0.10/}','')
                text =  elem.text
#                print (taggy)
#                print (text)

                if taggy == 'title': 
                    page_title = text.replace(' ','_')

                # if elem.attrib != None:
                #     try:
                #         page_title = elem.attrib['title'].replace(' ','_')
                #     except:
                #         pass

                if page_title != cur_page_title and cur_page_title != None:
                    num_edits[page_id] = n_edits
                    n_edits = 0
                    num_editors[page_id]=len(n_editors)
                    n_editors=set()
                    num_discussions[page_id]=n_discussions
                    page_id = None

                    i+=1
                    if i%1000==0: 
                        last_time=cur_time
                        cur_time=time.time()
                        print('\t'+str(i)+' '+str(datetime.timedelta(seconds=cur_time - last_time)))
                        print(str(round(1000/(cur_time - last_time),3))+' '+'pages per second.')

                cur_page_title = page_title




                if taggy == 'id' and page_id == None:
                    page_id = int(text)

                    if ns == 0:
                        try:
                            qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
                            page_title_page_id[cur_page_title]=page_id
                        except:
                            page_id = None

                    if ns == 1:
                        title = cur_page_title.split(':')[1]
                        try:
                            page_id = page_title_page_id[title]
                            qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
                        except:
                            page_id = None

                if taggy == 'ns':
                    ns = int(text)

                if (taggy == 'username' or taggy == 'ip') and page_id != None:
                    username = text

                    if ns == 0:
                        n_editors.add(username)
                        n_edits+=1

                    if ns == 1 and page_id!= None:
                        n_discussions+=1
#                        print(page_id,ns,cur_page_title)

                if taggy == 'timestamp' and ns == 0 and page_id != None: 

                    timestamp = datetime.datetime.strptime(text,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H%M%S')
                    if first_edit_timestamp[page_id]=='': first_edit_timestamp[page_id] = timestamp
                    timestamp = int(timestamp)
                    if timestamp > first_day and timestamp < last_day:
                        num_edits_last_month[page_id]+=1

                elem.clear()

    print('parsed')

    page_ids_page_titles = {v: k for k, v in page_title_page_id.items()}

    for page_id in num_editors:
        # print((len(num_editors[page_id]), 
        #     num_edits[page_id], 
        #     num_edits_last_month[page_id], 
        #     first_edit_timestamp[page_id], 
        #     num_discussions[page_id], 
        #     page_id, 
        #     page_ids_qitems[page_id], 
        #     page_ids_page_titles[page_id]))

        try:
            parameters.append((len(num_editors[page_id]), num_edits[page_id], num_edits_last_month[page_id], first_edit_timestamp[page_id], num_discussions[page_id], page_id, page_ids_qitems[page_id], page_ids_page_titles[page_id]))
        except:
            pass
#    print (len(parameters),len(page_titles_qitems))

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    query = 'UPDATE '+languagecode+'wiki SET (num_editors, num_edits, num_edits_last_month, date_created,num_discussions)=(?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
    cursor.executemany(query,parameters)
    conn.commit()

#     duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#     create_function_account_db(function_name, 'mark', duration)


def extend_articles_first_timestamp_lang():
    function_name = 'extend_articles_first_timestamp_lang'
    # if create_function_account_db(function_name, 'check','')==1: return
    # functionstartTime = time.time()

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
    create_function_account_db(function_name, 'mark', duration)



def extend_articles_pageviews():
    function_name = 'extend_articles_pageviews'
    if create_function_account_db(function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    increment = 30
    lastMonth = datetime.date.today() - datetime.timedelta(days=increment)
    month_day = lastMonth.strftime("%Y-%m")
    filename = 'pagecounts-'+month_day+'-views-ge-5-totals.bz2'
    read_dump = '/public/dumps/public/other/pagecounts-ez/merged/'+filename


    pageviews_dict = {}
    dump_in = bz2.open(read_dump, 'r')
    line = dump_in.readline()
    line = line.rstrip().decode('utf-8')[:-1]
    values=line.split(' ')
    last_language = values[0].split('.')[0]

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
                print (last_language)
                print (len(pageviews_dict))
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
                print (len(pageviews))
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
            print (last_language)
            print (len(pageviews_dict))
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
            print (len(pageviews))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)

# Extends the Articles table with the number of references per Article.
def extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_references '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-externallinks.sql.gz'

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
            table_name = wikilanguages_utils.get_table_name(line)
            columns = wikilanguages_utils.get_columns(line)
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
    create_function_account_db(function_name, 'mark', duration)


# Extends the Articles table with the number of interwiki links.
def extend_articles_interwiki(languagecode, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_interwiki '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    updated = []
    query = "SELECT metadata.qitem, metadata.sitelinks, sitelinks.page_title FROM metadata INNER JOIN sitelinks ON sitelinks.qitem = metadata.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):
        try:
            page_id=page_titles_page_ids[row[2].replace(' ','_')]
            qitem=row[0]
            iw_count=row[1]
            updated.append((iw_count,page_id,qitem))
        except:
            pass
    query = 'UPDATE '+languagecode+'wiki SET num_interwiki = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print ('CCC interwiki updated.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


# Extends the Articles table with the number of qitem properties.
def extend_articles_qitems_properties(languagecode, page_titles_page_ids):

    functionstartTime = time.time()

    function_name = 'extend_articles_qitems_properties '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    updated = []
    query = "SELECT metadata.qitem, metadata.properties, sitelinks.page_title FROM metadata INNER JOIN sitelinks ON sitelinks.qitem = metadata.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki'"
    for row in cursor.execute(query):
        try:
            page_id=page_titles_page_ids[row[2].replace(' ','_')]
            qitem=row[0]
            num_wdproperty=row[1]
            updated.append((num_wdproperty,page_id,qitem))
#            print (page_id,qitem,iw_count)
        except:
            pass
    query = 'UPDATE '+languagecode+'wiki SET num_wdproperty=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print ('CCC qitems properties updated.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



# Extends the Articles table with the featured Articles.
def extend_articles_featured(languagecode, page_titles_qitems):

#    print('\n* '+languagecode)

    functionstartTime = time.time()
    function_name = 'extend_articles_featured '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

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
#        print (title,language)
        featuredarticleslanguages[language] = title
        if language == 'itwiki': featuredarticleslanguages[language] = 'Voci_in_vetrina_su_it.wiki'
        if language == 'ruwiki': featuredarticleslanguages[language] = 'Википедия:Избранные_статьи_по_алфавиту'
#    print ('These are the featured Articles categories in the different languages.')
#    print (featuredarticleslanguages)

    if languagecode+'wiki' in featuredarticleslanguages: featuredtitle=featuredarticleslanguages[languagecode+'wiki']
    else: 
        print ('No featured Articles for language: '+languagecode);
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        create_function_account_db(function_name, 'mark', duration)
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
#            print ((page_id,page_title,page_titles_qitems[page_title]))
        except:
            pass
#            print ('This Article does not exist: '+page_title)

    query = 'UPDATE '+languagecode+'wiki SET featured_article=1 WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,featuredarticles)
    conn.commit();

    print ('We obtained '+str(len(featuredarticles))+' featured Articles')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)

#    print (featuredtitle,languagecode)


# Extends the Articles table with the number of images.
def extend_articles_images(languagecode,page_titles_qitems,page_titles_page_ids):

    functionstartTime = time.time()
    # function_name = 'extend_articles_images '+languagecode
    # if create_function_account_db(function_name, 'check','')==1: return

    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-imagelinks.sql.gz'
    dump_in = gzip.open(dumps_path, 'r')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    num_images = {}
    for page_id,page_title in page_ids_page_titles.items():
        num_images[page_id]=0

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
            table_name = wikilanguages_utils.get_table_name(line)
            columns = wikilanguages_utils.get_columns(line)
            values = wikilanguages_utils.get_values(line)
            if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

            for row in rows:
                el_from = int(row[0])
                try:
                    num_images[el_from]=num_images[el_from]+1
                except:
                    pass
    print ('Done with the dump.')

    parameters = []
    for page_title, page_id in page_titles_page_ids.items():
        # print((num_images[page_id],page_id,page_title,page_titles_qitems[page_title]))
        parameters.append((num_images[page_id],page_id,page_titles_qitems[page_title]))

    print (len(parameters))

    query = 'UPDATE '+languagecode+'wiki SET num_images = ? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,parameters)
    conn.commit()

    # duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # create_function_account_db(function_name, 'mark', duration)



def extend_articles_wikirank():

    functionstartTime = time.time()
    function_name = 'extend_articles_wikirank'
    if create_function_account_db(function_name, 'check','')==1: return

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
    create_function_account_db(function_name, 'mark', duration)


def check_language_relevance_features():

    features = ['num_inlinks','num_outlinks','num_bytes','num_references','num_edits','num_editors','num_discussions','num_pageviews','num_wdproperty','num_interwiki','num_images','num_edits_last_month','featured_article','wikirank']

    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print(languagecode)

        for feature in features:
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+feature+' IS NOT NULL;'
            cursor2.execute(query)
            value = cursor2.fetchone()
            if value != None: 
                count = value[0]
            if count != 0: 
                print(languagecode+' '+feature+' '+str(count))
            if count == 0 or count== None:
                print(languagecode+' '+feature+' MISSING!')

        print ('\n')


def delete_wikidata_db():
    function_name = 'delete_wikidata_db'
    functionstartTime = time.time()

    if create_function_account_db(function_name, 'check','')==1: return
    os.remove(databases_path + "wikidata.db")

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


#######################################################################################


### SYNCHRONISATION AND SAFETY FUNCTIONS ###
def backup_db():
    function_name = 'backup_db relevance_features'
    if create_function_account_db(function_name, 'check','')==1: return
    functionstartTime = time.time()

    try:
        shutil.copyfile(databases_path + 'wikipedia_diversity.db', databases_path + "wikipedia_diversity_backup.db")
        print ('File wikipedia_diversity.db copied as wikipedia_diversity_backup.db at the end of the relevance_features.py script.')
    except:
        print ('Not possible to create the backup.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)




def create_function_account_db(function_name, action, duration):
    function_name_string = function_name

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS function_account (function_name text, year_month text, finish_time text, duration text, PRIMARY KEY (function_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM function_account WHERE function_name = ? AND year_month = ?;'
        cursor.execute(query,(function_name,cycle_year_month))
        function_name = cursor.fetchone()
        if function_name != None:
            print ('= Process Accountant: The function "'+function_name_string+'" has already been run. It lasted: '+function_name[0])
            return 1
        else:
        	print ('- Process Accountant: The function "'+function_name_string+'" has not run yet. Do it! Now: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S")))
        	return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S");
        query = "INSERT INTO function_account (function_name, year_month, finish_time, duration) VALUES (?,?,?,?);"
        cursor.execute(query,(function_name,cycle_year_month,finish_time,duration))
        conn.commit()
        print ('+ Process Accountant: '+function_name+' DONE! After '+duration+'.\n')


def clean_failed_function_account():
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    function_parameters = {'extend_articles_timestamp':'date_created', 'extend_articles_editors':'num_editors', 'extend_articles_discussions':'num_discussions','extend_article_edits':'num_edits','extend_articles_edits_last_month':'num_edits_last_month','extend_articles_references':'num_references','extend_articles_bytes':'num_bytes','extend_articles_interwiki':'num_interwiki','extend_articles_qitems_properties':'num_wdproperty','extend_articles_featured':'featured_article','extend_articles_images':'num_images','extend_articles_pageviews':'num_pageviews'}


    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode)

        for function, parameter in function_parameters.items():
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+parameter+' IS NOT NULL;'
            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num = row[0]

            query = 'SELECT count(*) FROM function_account WHERE function_name = "'+function+ ' ' + languagecode
            if function == 'extend_articles_wikidata_topics': query+= ' gender"'
            else: query+='"'

            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num2 = row[0]

#            if num2 != 0 and num != 0:
#                print(function+' exists and the database is full. nothing to do, everything is fine.')
            if num2 != 0 and num == 0:
                print(function+' '+languagecode+' exists and the database is empty. we delete function account so the function runs again next time.')
                query = 'DELETE FROM function_account WHERE function_name = "'+function+' '+languagecode
                if function == 'extend_articles_wikidata_topics': query+= ' gender"'
                else: query+='"'

                cursor.execute(query)
            if num != 0 and num2 == 0:
                print(parameter+' '+languagecode+' is in the database but the function '+function+' '+languagecode+' has not been accounted.')
        conn.commit()


def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WCDO - RELEVANCE FEATURES ERROR: '+ cycle_year_month, 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/relevance_features.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WCDO - RELEVANCE FEATURES ERROR: '+ cycle_year_month, 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue


def verify_time_for_iteration():
    print ("Let's check it is the right time for Stats generation iteration...")

    # CONDITION 1: CCC created this month.
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "set_production_wikipedia_diversity_db" AND year_month = ?;'
    cursor.execute(query,cycle_year_month)
    function_name1 = cursor.fetchone()

    # CONDITION 2: TOP CCC created this month.
    conn = sqlite3.connect(databases_path + top_wikipedia_diversity_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "update_top_ccc_articles_titles translations" AND year_month = ?;'
    cursor.execute(query,cycle_year_month)
    function_name2 = cursor.fetchone()

    if function_name1 != None and function_name2 != None: return True


#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("relevance_features"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("relevance_features"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    dumps_path = '/srv/wcdo/dumps/'
    databases_path = '/srv/wcdo/databases/'
    wikipedia_diversity_db = 'wikipedia_diversity.db'

#    while True:
#    time.sleep(84600) # every ninety days
#    print ("Good morning. It is: "+time.today()+". Let's see if today is the day to update the articles features...")

#        # CHAINED TO CCC CREATION (ONCE A MONTH)
#        if verify_time_for_iteration():


    startTime = time.time()
    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()

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

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')   
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=False)]
    biggest = wikilanguagecodes_by_size[20:]; smallest = wikilanguagecodes_by_size[:50]

    print ('\n* Starting the FEATURES OF RELEVANCE CYCLE '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now()))

    main()
#    main_with_exception_email()
#    main_loop_retry()

    finishTime = time.time()
    print ('* Done with the FEATURES OF RELEVANCE CYCLE. Completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'relevance_features.out', 'WIKIPEDIA DIVERSITY relevance features')
