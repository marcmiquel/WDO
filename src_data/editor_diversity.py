# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
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
import operator
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
# classifier
from sklearn import svm, linear_model
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# Twice the same table in a short period of time not ok.
# Load all page_titles from all languages is not ok.
import gc



# MAIN
def main():














def extend_editors_retention_metrics(languagecode,page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_history_features '+languagecode

    last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
    first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))

    conn2 = sqlite3.connect(databases_path + 'editors_pages.db'); cursor2 = conn2.cursor()
    try:
        os.remove(databases_path+'editors_pages.db')
    except:
        pass

    try:
        query = 'CREATE TABLE editors (page_id integer, editor integer, PRIMARY KEY (page_id, editor));'
        cursor2.execute(query)
        conn2.commit()
    except:
        pass

    article_completed = {}

    first_edit_timestamp = {}
    last_edit_timestamp = {}
    last_discussion_timestamp = {}

    num_edits = {}
    num_edits_last_month = {}

    num_discussions = {}
    for page_title in page_titles_qitems.keys():
        num_edits[page_title]=0
        num_edits_last_month[page_title]=0
        num_discussions[page_title]=0

        first_edit_timestamp[page_title]=0
        last_edit_timestamp[page_title]=0
        last_discussion_timestamp[page_title]=0

    d_paths = []

    cym = cycle_year_month
    print ('/public/dumps/public/other/mediawiki_history/'+cym)
    if os.path.isdir('/public/dumps/public/other/mediawiki_history/'+cym)==False:
        cym = datetime.datetime.strptime(cym,'%Y-%m')-dateutil.relativedelta.relativedelta(months=1)
        cym = cym.strftime('%Y-%m')
        print ('/public/dumps/public/other/mediawiki_history/'+cym)

    dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+languagecode+'wiki.all-time.tsv.bz2'
    if os.path.isfile(dumps_path):
        print ('one all-time file.')
        d_paths.append(dumps_path)

    else:
        print ('multiple files.')
        for year in range (2025, 1999, -1):
            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+languagecode+'wiki.'+str(year)+'.tsv.bz2'
            if os.path.isfile(dumps_path): 
                d_paths.append(dumps_path)

        if len(d_paths) == 0:
            for year in range(2025, 1999, -1): # months
                for month in range(13, 0, -1):
                    if month > 9:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+languagecode+'wiki.'+str(year)+'-'+str(month)+'.tsv.bz2'
                    else:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+languagecode+'wiki.'+str(year)+'-0'+str(month)+'.tsv.bz2'

                    if os.path.isfile(dumps_path) == True:
                        d_paths.append(dumps_path)

    print(len(d_paths))
    print (d_paths)
    print ('Total number of articles: '+str(len(num_edits)))

    if (len(d_paths)==0):
        print ('dump error at script '+script_name)
        send_email_toolaccount('dump error at script '+script_name, dumps_path)
        quit()

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

            try:
                page_namespace = int(values[28])
            except:
                continue

            edit_count = values[34]
            # seconds_last_edit = values[35]

            last_timestamp = values[3]
            if last_timestamp != 'null': last_timestamp = int(datetime.datetime.strptime(last_timestamp[:len(last_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'))
            else: last_timestamp = 0

            if page_namespace == 0:
                if last_edit_timestamp[page_title] < last_timestamp:
                    last_edit_timestamp[page_title] = last_timestamp
                    num_edits[page_title]=edit_count

                first_timestamp = values[33]
                if first_timestamp != 'null': first_timestamp = int(datetime.datetime.strptime(first_timestamp[:len(first_timestamp)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S'))
                else: first_timestamp = 0

                if last_timestamp == first_timestamp:
                    article_completed[page_title] = None
                    first_edit_timestamp[page_title]=first_timestamp

                user_anon = values[5]
                # user_text = values[38]
                if user_anon != "null":
                    try:
                        editors_params.append((user_anon,page_id))
                        # num_editors[page_title].add(int(user_anon))
                    except:
                        pass

                if last_timestamp > first_day and last_timestamp < last_day:
                    # print (page_title, first_day, last_day, last_edit_timestamp)
                    try:
                        num_edits_last_month[page_title]+=1
                    except:
                        pass

            if page_namespace == 1:
                if last_discussion_timestamp[page_title] < last_timestamp:
                    last_discussion_timestamp[page_title] = last_timestamp
                    num_discussions[page_title] = edit_count


        query = 'INSERT OR IGNORE INTO editors (editor, page_id) VALUES (?,?);'
        cursor2.executemany(query,editors_params)
        conn2.commit()
        editors_params = []

        for page_title in article_completed.keys():

            try:
                parameters.append((num_edits[page_title], num_discussions[page_title], num_edits_last_month[page_title], first_edit_timestamp[page_title], last_edit_timestamp[page_title], last_discussion_timestamp[page_title], page_titles_page_ids[page_title], page_titles_qitems[page_title], page_title))

                # del num_editors[page_title]
                del num_edits[page_title]
                del num_discussions[page_title]
                del num_edits_last_month[page_title]
                del first_edit_timestamp[page_title]
                del last_edit_timestamp[page_title]
                del last_discussion_timestamp[page_title]

            except:
                pass
        article_completed = {}

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'UPDATE '+languagecode+'wiki SET (num_edits, num_discussions, num_edits_last_month, date_created, date_last_edit, date_last_discussion)=(?,?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title = ?;'
        cursor.executemany(query,parameters)
        conn.commit()
        print ('Articles introduced: '+str(len(parameters))+'. Articles left for next files or not in the database: '+str(len(num_edits))+'.')
        print ('This file took: '+str(datetime.timedelta(seconds=time.time() - iterTime)))
        parameters = []

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
    os.remove(databases_path+'editors_pages.db')

    print(languagecode+' history parsed and stored')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))














def old_main():


    # editors
    create_editor_diversity_db('create')
    insert_editors_flags()
    for languagecode in wikilanguagecodes:
        insert_editors_edits(languagecode)
        insert_editors_multilingualism(languagecode)

    for languagecode in wikilanguagecodes:
        store_all_history_table(languagecode)
        store_all_history_table_monthly(languagecode,'last month')
        revisions_extend_editor_iterator(languagecode)


################################################################


def create_editor_diversity_db(create_update_table):

    # create_editor_characteristics_metrics_table
    conn = sqlite3.connect(databases_path + editor_diversity_db)
    cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        table_name = languagecode+'wiki_editor_characteristics_aggregated'
        create_update_table = 'create'
        if create_update_table == 'create':
            try:
                cursor.execute("DROP TABLE "+table_name+";")
                conn.commit()
                print ('table dropped.')
            except:
                print ('table not dropped.')

        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, "+
            "namespace_0_edits integer, namespace_1_edits integer, namespace_2_edits integer, namespace_3_edits integer, namespace_4_edits integer, namespace_5_edits integer, namespace_6_edits integer, namespace_7_edits integer, namespace_8_edits integer, namespace_9_edits integer, namespace_10_edits integer, namespace_11_edits integer, namespace_12_edits integer, namespace_13_edits integer, namespace_14_edits integer, namespace_15_edits integer,"
                "userpage_edits integer, userpage_talk_edits integer,"
            "PRIMARY KEY (user_name, user_id));")

        cursor.execute(query)
    conn.commit()

    # create_editor_engagement_metrics_table
    for languagecode in wikilanguagecodes:
        table_name = languagecode+'wiki_editor_engagement_aggregated'
        create_update_table = 'create'
        if create_update_table == 'create':
            try:
                cursor.execute("DROP TABLE "+table_name+";")
                conn.commit()
                print ('table dropped.')
            except:
                print ('table not dropped.')


        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, "+
            "first_edit_timestamp integer, last_edit_timestamp integer, " # period
            "registered_within_month integer, registered_within_year integer," # new editor
            "active_editor_last_month integer," # active editor status
            "edit_count integer, onemonthago_edit_count integer, twomonthago_edit_count integer, threemonthago_edit_count integer integer," # edit count
            "edit_count_24h integer, edit_count_7d, edit_count_30d integer, edit_count_60d integer, edit_count_after_60d integer," # retention period
            "active_months integer, total_months integer, max_months_row integer," # time
            "lifetime_days integer, drop_off_days integer," # drop off

            "PRIMARY KEY (user_name, user_id));")

        cursor.execute(query)
    conn.commit()

    # create_ccc_distributed_table
    for languagecode in wikilanguagecodes:
        table_name = languagecode+'wiki_ccc_distributed'
        create_update_table = 'create'
        if create_update_table == 'create':
            try:
                cursor.execute("DROP TABLE "+table_name+";")
                conn.commit()
                print ('table dropped.')
            except:
                print ('table not dropped.')

            cursor.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, "
                "edit_count, lang_ccc text,"
                "PRIMARY KEY (user_id, lang_ccc));")

    conn.commit()

    # create_multilingualism_aggregated
    table_name = languagecode+'wiki_multilingualism_aggregated'

    if create_update_table == 'create':
        try:
            cursor.execute("DROP TABLE "+table_name+";")
            conn.commit()
            print ('table dropped.')

        except:
            print ('table not dropped.')

        cursor.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, primarybinary integer, primarylang text, primarybinary_ecount, total_ecount, numberlangs integer, PRIMARY KEY (user_name, user_id));")
        conn.commit()

    # create_editors_flags
    query = ("CREATE TABLE IF NOT EXISTS flags (languagecode text, user_id integer, user_name text, flag text, PRIMARY KEY (languagecode, user_id, flag));")
    cursor.execute(query)
    conn.commit()


def insert_editors_flags():

    conn = sqlite3.connect(databases_path + editor_diversity_db)
    cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode)

        parameters = []

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); 
        mysql_cur_read = mysql_con_read.cursor()

        query = 'SELECT user_id, user_name, ug_group FROM user_groups INNER JOIN user ON ug_user=user_id order by ug_group, user_name;'

        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()

        for row in rows:
            user_id = row[0]
            user_name = row[1]
            ug_group = row[2]
            parameters.append((languagecode, user_id,user_name,ug_group))

        query = 'SELECT user_id, user_name FROM user WHERE CONVERT(user_name USING utf8mb4) COLLATE utf8mb4_general_ci LIKE "%bot%" AND user_id NOT IN (SELECT user_id FROM user_groups INNER JOIN user ON ug_user=user_id WHERE ug_group != "bot" AND ug_group != "flow-bot") AND user_editcount > 1000;'

        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()

        for row in rows:
            user_id = row[0]
            user_name = row[1]
            parameters.append((languagecode, user_id,user_name,'bot'))

        query = 'INSERT OR IGNORE INTO flags (languagecode, user_id, user_name, flag) VALUES (?,?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()

    print ('All the editors flags from all languages were introduced in flags database.')


# BASIC (USER TABLE) # https://www.mediawiki.org/wiki/Manual:User_table
def insert_editors_edits(languagecode):

    function_name = 'insert_editors_edits '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime


    # https://www.mediawiki.org/wiki/Manual:Database_layout
    table_name = languagecode+'wiki_editors'

    conn = sqlite3.connect(databases_path + editor_diversity_db)
    cursor = conn.cursor()

    if create_update_table == 'create':
        try:
            cursor.execute("DROP TABLE "+table_name+";")
            print ('table dropped.')
            conn.commit()
        except:
            print ('table not dropped.')

        cursor.execute("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, edit_count integer, registration text, PRIMARY KEY (user_name, user_id));")
        conn.commit()


    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    parameters = []


    mysql_cur_read.execute("SELECT max(user_id) FROM user;")
    maxval = int(mysql_cur_read.fetchone()[0])
    print (maxval)

    if maxval < 10000000:
        print ('Trying the big query.')
        query = 'SELECT user_name, user_id, user_registration, user_editcount FROM user;'
        mysql_cur_read.execute(query)

        rows = mysql_cur_read.fetchall()
        for row in rows:
            username = str(row[0].decode('utf-8'))
            user_id = str(row[1])
            user_registration = row[2]
            user_editcount = str(row[3])
            parameters.append((username,user_id,user_registration,user_editcount))
    else:
        print ('Trying to run the query with batches.')

        increment = 500000
        range_values = 0
        while (range_values < maxval):
            val1 = range_values
            range_values = range_values + increment
            if range_values > maxval: range_values = maxval
            val2 = range_values

            pastparameterslength = len(parameters)

            interval = 'user_id BETWEEN '+str(val1)+' AND '+str(val2)
            query = "SELECT user_name, user_id, user_registration, user_editcount FROM user WHERE "+interval+";"
            print (query)

            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                try: 
                    username = str(row[0].decode('utf-8'))
                    user_id = str(row[1])
                    user_registration = row[2]
                    user_editcount = str(row[3])
                    parameters.append((username,user_id,user_registration,user_editcount))

                except: continue
            print (len(parameters))

            print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')

            seconds = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            if seconds == 0: seconds = 1
            print (str((len(parameters)-pastparameterslength)/seconds)+' rows per second.')
            last_period_time = time.time()


    print ('In this language '+languagecode+' there is this number of editors: '+str(len(parameters)))

    if create_update_table == 'create':
        query = 'INSERT OR IGNORE INTO '+table_name+' (user_name, user_id, registration, edit_count) VALUES (?,?,?,?);';
        print ('insert')
    else:
        query = 'UPDATE '+table_name+' (user_name, user_id, registration, edit_count) VALUES (?,?,?,?);'
        print ('update')

    cursor.executemany(query,parameters)
    conn.commit()
    conn.close()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



def insert_editors_multilingualism(languagecode):
    # QUEDA EL DUBTE SI AQUESTA FUNCIÓ TAMBÉ HAURIA DE FER UN INSERT A LA DE MULTILINGUALISM DISTRIBUTED PEL QUÈ FA A WP.

    # FIRST WE OBTAIN THE MAIN TABLE DATA
    # fem-ho tot en base a joins i deixem-nos de diccionaris.
    # PRIMER: carregar en memòria els editors de la llengua principal. diccionari i paràmeters.
    # SEGON: fer el inner join i treure aquells que en la llengua estrangera superen la principal en edits i modificar l'array.

    function_name = 'insert_editors_multilingualism '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + editor_diversity_db)
    cursor = conn.cursor()

    languagecode_editors = {}
    languagecode_editors_edit_count = {}

    query = "SELECT user_name, edit_count, user_id FROM "+languagecode+'wiki_editors;'
    cursor.execute(query)
    for row in cursor.execute(query):
        user_name=row[0]
        edit_count=row[1]
        user_id=row[2]

        primarylang=languagecode
        primarybinary=1
        numberlangs=1

        array = [user_id,primarylang,primarybinary,numberlangs]
        languagecode_editors_edit_count[user_name] = edit_count
        languagecode_editors[user_name]=array


    # THEN WE CONSULT EVERY OTHER TABLE DATA
    for langcode in wikilanguagecodes:
        if languagecode == langcode: continue
       # BONA QUERY 'SELECT ca.user_name, en.edit_count FROM cawiki_editors ca INNER JOIN enwiki_editors en ON ca.user_name = en.user_name WHERE en.edit_count > 0 AND en.edit_count > ca.edit_count;'

        query = 'SELECT '+languagecode+'wiki'+'.user_name, '+langcode+'wiki'+'.edit_count FROM '+languagecode+'wiki_editors '+languagecode+'wiki'+' INNER JOIN '+langcode+'wiki_editors '+langcode+'wiki'+' ON '+languagecode+'wiki'+'.user_name = '+langcode+'wiki'+'.user_name WHERE '+langcode+'wiki'+'.edit_count > 0;'

#        query = 'SELECT '+languagecode+'wiki'+'.user_name, '+langcode+'wiki'+'.edit_count FROM '+languagecode+'wiki_editors '+languagecode+'wiki'+' INNER JOIN '+langcode+'wiki_editors '+langcode+'wiki'+' ON '+languagecode+'wiki'+'.user_name = '+langcode+'wiki'+'.user_name WHERE '+langcode+'wiki'+'.edit_count > 0 AND '+langcode+'wiki'+'.edit_count > '+languagecode+'wiki'+'.edit_count;'

        print (query)
        i = 0
        for row in cursor.execute(query):
            i+=1
            user_name=row[0]
            edit_count_new=row[1]

            try:
                array=languagecode_editors[user_name]
                user_id=array[0]
                primarylang = array[1]
                primarybinary = array[2]
                numberlangs = array[3]+1

                edit_count = languagecode_editors_edit_count[user_name]
                if edit_count_new>edit_count:
                    primarylang = langcode
                    primarybinary = 0
                    languagecode_editors_edit_count[user_name] = edit_count_new

                languagecode_editors[user_name]=[user_id,primarylang,primarybinary,numberlangs]
#                print ([user_id,primarylang,primarybinary,numberlangs])
            except:
                pass
        print (langcode+'\t'+str(i))

    parameters = []
    for user_name, array in languagecode_editors.items():
        parameters.append((array[0],user_name,array[1],array[2],array[3]))

    # FINALLY WE INTRODUCE THE DATA TO THE FINAL TABLE
    if create_update_table == 'create':
        query = 'INSERT OR IGNORE INTO '+table_name+' (user_id, user_name, primarylang, primarybinary, numberlangs) VALUES (?,?,?,?,?);';
        print ('insert')
    else:
        query = 'UPDATE '+table_name+' (user_id, user_name, primarylang, primarybinary, numberlangs) VALUES (?,?,?,?,?);'
        print ('update')

    cursor.executemany(query,parameters)
    conn.commit()
    conn.close()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)






"""
USER DATA LEGEND:

source data:
* revision.db
wiki_revisions (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp)
wiki_revisions_monthly (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp)

target data:
* editor_diversity.db
wiki_editor_engagement_aggregated
wiki_editor_characteristics_aggregated
wiki_editors (user_id integer, user_name text, primarybinary integer, primarylang text, primarybinary_ecount, total_ecount, numberlangs integer, PRIMARY KEY (user_name, user_id

wiki_multilingualism_aggregated (user_id integer, user_name text, primarybinary integer, primarylang text, numberlangs integer, PRIMARY KEY (user_name, user_id)

wiki_ccc_distributed (user_id integer, user_name text, content_type text, target_lang text PRIMARY KEY (user_name, user_id))
-> els editors de cawiki i el què han editat de jawikiccc a cawiki.
"""


def store_all_history_table(languagecode):

    function_name = 'store_all_history_table '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime


    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS "+languagecode+"wiki_revisions (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp));")
    conn.commit()


    cursor.execute('SELECT max(rev_id) FROM '+languagecode+'wiki_revisions')
    max_rev_id=cursor.fetchone()[0]


    print ('Trying to run the query with batches.')
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute("SELECT max(rev_id) FROM revision;")
    maxval = mysql_cur_read.fetchone()[0]
    print (maxval)

#    filename = languagecode+'wiki_'+complete+'_'+last_month+'.csv'

    k=0
    i=0

    increment = 500000
#    increment = 250000
#    increment = 50000
#    increment = 25000

    iterations = int(maxval/increment)
    print ('There are '+str(iterations)+' intervals.')

    if max_rev_id != None:
        range_values = max_rev_id
    else:
        range_values = 0
    
    while (range_values < maxval):

        val1 = range_values
        range_values = range_values + increment
        if range_values > maxval: range_values = maxval
        val2 = range_values

        i+=1

        interval = 'AND rev_id BETWEEN '+str(val1)+' AND '+str(val2)
        query = 'SELECT rev_page, rev_user, rev_user_text, rev_timestamp, rev_id FROM revision WHERE rev_user != 0 '+interval

        print (query)
        parameters = []
        try:
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
    #        file = open(revisions_path+filename,'a') 

            j=0
            for row in rows:
                j+=1
    #            file.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+'\n')
    #            file.write(','.join(row))
                parameters.append((row[0],row[1],row[2].decode('utf-8'),row[3],row[4]))
            k+=j
        except:
            print ('the query timed out.')

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_revisions (rev_page, rev_user, rev_user_text, rev_timestamp, rev_id) VALUES (?,?,?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()

        print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')

        try:
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(j/tempo)+' rows per second.')
            print (str(int((iterations-i)*tempo))+' seconds as estimated remaining time.')
        except:
            print ('too fast.')

        print ('total of '+str(k)+' rows.')
        print ('* '+str(i)+' queries have been run out of '+str(iterations))

#        file.close()

        last_period_time = time.time()

    conn.close()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


def store_all_history_table_monthly(languagecode, time_range):

    function_name = 'store_all_history_table_monthly '+languagecode + ' '+time_range
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime


    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS "+languagecode+"wiki_revisions_monthly (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_user_text,rev_timestamp));")

    conn.commit()


    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute("SELECT max(rev_id) FROM revision;")
    maxval = mysql_cur_read.fetchone()[0]
    print (maxval)
    print (time_range)


    if time_range == 'last month':
        period = list(sorted(periods_monthly.keys()))[len(periods_monthly-1)]
        interval = periods_monthly[period]
        periods_monthly = {}
        periods_monthly[period] = interval


    iterations = len(periods_monthly)

#    print (periods_monthly.keys())
    k=0
    i=0
    for period,interval in periods_monthly.items():
        i+=1

        interval = 'AND ' + interval
        query = 'SELECT rev_page, rev_user, rev_user_text, rev_timestamp, rev_id FROM revision WHERE rev_user != 0 '+interval

        print (query)

        try:
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
    #        file = open(revisions_path+filename,'a') 

            parameters = []

            j=0
            for row in rows:
                j+=1
    #            file.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+'\n')
    #            file.write(','.join(row))
                parameters.append((row[0],row[1],row[2],row[3],row[4]))
            k+=j
        except:
            print ('the query timed out.')

        query = 'INSERT OR IGNORE INTO '+languagecode+'_revisions_monthly (rev_page, rev_user, rev_user_text, rev_timestamp, rev_id) VALUES (?,?,?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()

        print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')

        try:
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(j/tempo)+' rows per second.')
            print (str(int(iterations*tempo))+' seconds as estimated remaining time.')
        except:
            print ('too fast.')

        print ('total of '+str(k)+' rows.')
        print ('* '+str(i)+' queries have been run out of '+str(iterations))

#        file.close()
        last_period_time = time.time()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



# EXTENDED (REVISION TABLE) # https://www.mediawiki.org/wiki/Manual:Revision_table
def revisions_extend_editor_iterator(languagecode):

    function_name = 'revisions_extend_editor_iterator '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime

    conn = sqlite3.connect(databases_path + editor_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + revision_db); cursor2 = conn2.cursor()

    query = 'SELECT count(user_id) FROM '+languagecode+'wiki_editors;'
    cursor.execute(query)
    max_users = cursor.fetchone()[0]

    edits_page_ids = []; edits_timestamps = []

    # page_ids to intersect
    """
    page_ids_language_ccc = get_language_ccc_page_ids(languagecode)
    page_ids_namespaces = get_language_namespaces_page_ids(languagecode)
    user_id_userpage_page_id, user_id_userpage_talk_page_id = get_language_editors_user_page_ids(languagecode)
    """

    ccc_count = {}; 
    for languagecode2 in wikilanguagecodes: ccc_count[languagecode2]=0

    month_periods = []
    for day in datespan(datetime.date(2001, 1, 16), datetime.date.today(),delta=relativedelta.relativedelta(months=1)):
        month_period.append(day.strftime('%Y-%m'))


    ccc_params = []; 
    characteristics_params = []
    engagement_params = []

    rev_user = ''
    i = 0
    j = 0
    k = 0
    query = 'SELECT * FROM '+languagecode+'wiki_revisions ORDER BY rev_user, rev_timestamp ASC;'
    for row in cursor2.execute(query):

        cur_rev_user = row[1]
        cur_rev_page = row[0]
        cur_rev_user_text = row[2]
        cur_rev_timestamp = datetime.datetime.strptime(str(row[3].decode('utf-8')),'%Y%m%d%H%M%S')

        if cur_rev_user != rev_user and i!=0:
            j+=1

            # check the revisions
            """
            ccc_params += extend_editors_ccc_distributed(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_language_ccc, ccc_count)
            
            characteristics_params += extend_editors_namespaces_edits(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_namespaces, user_id_userpage_page_id, user_id_userpage_talk_page_id)
            """

            engagement_params += extend_editors_engagement_metrics(languagecode, rev_user, rev_user_text, edits_timestamps, month_period)

            edits_page_ids = []; edits_timestamps = []


        edits_page_ids.append(cur_rev_page)
        edits_timestamps.append(cur_rev_timestamp)

        rev_user=cur_rev_user
        rev_user_text=cur_rev_user_text
        i+=1

        # COUNT
        if i%1000000 == 0:

            print ('\nAccum. revision lines: '+str(i))
            print ('Accum. editors: '+str(j))


            # store the editors
            """
            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_ccc_distributed (user_id, user_name, edit_count, lang_ccc) VALUES (?,?,?,?);';
            cursor.executemany(query,ccc_params)
            conn.commit()

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_characteristics_aggregated (user_id, user_name, namespace_0_edits, namespace_1_edits, namespace_2_edits, namespace_3_edits, namespace_4_edits, namespace_5_edits, namespace_6_edits, namespace_7_edits, namespace_8_edits, namespace_8_edits, namespace_10_edits, namespace_11_edits, namespace_12_edits, namespace_13_edits, namespace_14_edits, namespace_15_edits, userpage_edits, userpage_talk_edits) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
            cursor.executemany(query,characteristics_params)
            conn.commit()
            """

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement_aggregated (user_id, user_name, first_edit_timestamp, last_edit_timestamp, registered_within_month, registered_within_year, active_editor_last_month, edit_count, onemonthago_edit_count, twomonthago_edit_count, threemonthago_edit_count, edit_count_24h, edit_count_7d, edit_count_30d, edit_count_60d, edit_count_after_60d, active_months, total_months, max_months_row, lifetime_days, drop_off_days) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
            cursor.executemany(query,engagement_params)
            conn.commit()

            total = int(datetime.timedelta(seconds=time.time() - functionstartTime).total_seconds())
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(round(1000000/tempo,3))+' rows per second in this iteration.')
            print (str(round(j/total,3))+' editors per second in total.')
            print (str(int(max_users-j/round(j/total,3)))+' seconds as estimated remaining time.')
            print ('Time for group of iterations: '+str(tempo)+' sec.')
            print ('Total time: ' + str(total))+' sec.'
            last_period_time = time.time()


    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_ccc_distributed (user_id, user_name, edit_count, lang_ccc) VALUES (?,?,?,?);';
    cursor.executemany(query,ccc_params)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


# * EDITOR CHARACTERISTICS METRICS
# e.g. cawiki_editors_characteristic

def extend_editors_namespaces_edits(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_namespaces, user_id_userpage_page_id, user_id_userpage_talk_page_id):

    try: userpage_page_id = user_id_userpage_page_id[rev_user]
    except: userpage_page_id = None
    try: userpage_talk_page_id = user_id_userpage_talk_page_id[rev_user]
    except: userpage_talk_page_id = None

    
    namespace_0_edits = 0; namespace_1_edits = 0; namespace_2_edits = 0; namespace_3_edits = 0; namespace_4_edits = 0; namespace_5_edits = 0; namespace_6_edits = 0; namespace_7_edits = 0; namespace_8_edits = 0; namespace_8_edits = 0; namespace_10_edits = 0; namespace_11_edits = 0; namespace_12_edits = 0; namespace_13_edits = 0; namespace_14_edits = 0; namespace_15_edits = 0
    userpage_page_id_edits = 0; userpage_talk_page_id_edits = 0

    for rev in edits_page_ids:
        if rev == userpage_page_id: userpage_page_id_edits += 1
        elif rev == userpage_talk_page_id: userpage_talk_page_id_edits += 1
        try:
            if page_ids_namespaces[rev] == 0: namespace_0_edits += 1
            elif page_ids_namespaces[rev] == 1: namespace_1_edits += 1
            elif page_ids_namespaces[rev] == 2: namespace_2_edits += 1
            elif page_ids_namespaces[rev] == 3: namespace_3_edits += 1
            elif page_ids_namespaces[rev] == 4: namespace_4_edits += 1
            elif page_ids_namespaces[rev] == 5: namespace_5_edits += 1
            elif page_ids_namespaces[rev] == 6: namespace_6_edits += 1
            elif page_ids_namespaces[rev] == 7: namespace_7_edits += 1
            elif page_ids_namespaces[rev] == 8: namespace_8_edits += 1
            elif page_ids_namespaces[rev] == 9: namespace_9_edits += 1
            elif page_ids_namespaces[rev] == 10: namespace_10_edits += 1
            elif page_ids_namespaces[rev] == 11: namespace_11_edits += 1
            elif page_ids_namespaces[rev] == 12: namespace_12_edits += 1
            elif page_ids_namespaces[rev] == 13: namespace_13_edits += 1
            elif page_ids_namespaces[rev] == 14: namespace_14_edits += 1
            elif page_ids_namespaces[rev] == 15: namespace_15_edits += 1
        except:
            continue

    characteristics_params = [(rev_user, rev_user_text, 
        namespace_0_edits, namespace_1_edits, namespace_2_edits, namespace_3_edits, namespace_4_edits, namespace_5_edits, namespace_6_edits, namespace_7_edits, namespace_8_edits, namespace_8_edits, namespace_10_edits, namespace_11_edits, namespace_12_edits, namespace_13_edits, namespace_14_edits, namespace_15_edits, userpage_page_id_edits, userpage_talk_page_id_edits)]

    return characteristics_params


# general
def extend_editors_engagement_metrics(languagecode, rev_user, rev_user_text, edits_timestamps, month_period):
    # aquí faríem tots els càlculs amb els timestamps passats a datetime.

    # INITIAL DATA
    user_id = rev_user
    user_name = rev_user_text

    first = edits_timestamps[0]
    first_edit_timestamp = first.strftime('%Y%m%d%H%M%S')
    last = edits_timestamps[len(edits_timestamps)-1]; 
    last_edit_timestamp = last.strftime('%Y%m%d%H%M%S')

    lifetime_days = last - first
    drop_off_days = today - last

    edit_count = len(edits_timestamps)

    edit_count_24h = ''
    edit_count_7d = ''
    edit_count_30d = ''
    edit_count_60d = ''
    onemonthago_edit_count = ''
    twomonthago_edit_count = ''
    threemonthago_edit_count = ''

    # THRESHOLDS
    one_month_ago = datetime.date.today() - relativedelta.relativedelta(months=1)
    two_month_ago = datetime.date.today() - relativedelta.relativedelta(months=2)
    three_month_ago = datetime.date.today() - relativedelta.relativedelta(months=3)
    one_year_ago = datetime.date.today() - relativedelta.relativedelta(year=1)

#    24h_after_firstedit = first + relativedelta.relativedelta(hours=24)
#    7d_after_firstedit = first + relativedelta.relativedelta(days=7)
#    60d_after_firstedit = first + relativedelta.relativedelta(days=60)


    # total_months = r.relativedelta.relativedelta(last,first).months


    # # ITERATION
    # months_active = []
    # i = 0
    # for cur_edit_date in edits_timestamps:
    #     i+=1
    #     if cur_edit_date > 24h_after_firstedit and edit_count_24h == '': edit_count_24h = i
    #     if cur_edit_date > 7d_after_firstedit and edit_count_7d == '': edit_count_7d = i
    #     if cur_edit_date > 30d_after_firstedit and edit_count_30d == '': edit_count_30d = i
    #     if cur_edit_date > 60d_after_firstedit and edit_count_60d == '': edit_count_60d = i
    #     if cur_edit_date > three_month_ago and threemonthago_edit_count == '': threemonthago_edit_count = i
    #     if cur_edit_date > two_month_ago and twomonthago_edit_count == '': twomonthago_edit_count = i
    #     if cur_edit_date > one_month_ago and onemonthago_edit_count == '': onemonthago_edit_count = i

    #     cur_month = cur_edit_date.strftime('%Y-%m')
    #     if cur_month not in months_active: months_active.append(cur_month)

    # active_months = len(months_active)


    # j = 0
    # for month in months_active:

    #     cur_month = datetime.datetime.strptime(month,'%Y-%m')
    #     expected_next_month = prev_month + relativedelta.relativedelta(months=1)

    #     if cur_month == expected_next_month:
    #         j+= 1
    #     else:
    #         j=0

    #     prev_month = cur_month

    # max_months_row = str(j)

    # edit_count_after_60d = edit_count - edit_count_60d

    # if first < one_month_ago: registered_within_month = 1
    # else: registered_within_month = 0

    # if last > one_month_ago: active_editor_last_month = 1
    # else: active_editor_last_month = 0

    # engagement_params = []

    # return engagement_params




#### --- ########################################################################

def extend_editors_ccc_distributed(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_language_ccc, ccc_count):

    ccc_count = dict(ccc_count)
    edit_count = len(edits_page_ids)
#    if edit_count <= 100: return ccc_params

    for rev in edits_page_ids:
        try:
            for lang in page_ids_language_ccc[rev]:
                ccc_count[lang]+=1
        except:
            pass

    ccc_params = []
    for lang, count in ccc_count.items():
        if count != 0: ccc_params.append((rev_user, rev_user_text, count, lang))

    return ccc_params


def delete_all_history_tables(monthly):

    function_name = 'delete_all_history_tables'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime

    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    if _monthly != None:
        query = "DROP TABLE "+languagecode+"wiki_revisions;"
    else:
        query = "DROP TABLE "+languagecode+"wiki_revisions_monthly"
    cursor.execute(query)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)




#### --- ####
# OTHER UTILITIES


def get_language_ccc_page_ids(languagecode):
    
    page_ids_language_ccc = {}
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    for languagecode2 in wikilanguagecodes:
        if languagecode2 == languagecode: continue
        page_ids = {}
        query = 'SELECT page_id FROM '+languagecode+'wiki WHERE qitem IN (SELECT qitem FROM '+languagecode2+'wiki WHERE ccc_binary=1);'

        c = 0
        try:
            for row in cursor.execute(query):
                c+=1
                page_id = row[0]
                if page_id not in page_ids_language_ccc:
                    page_ids_language_ccc[page_id]=[languagecode2]
                else:
                    page_ids_language_ccc[page_id].append(languagecode2)
        except:
            pass
#        print (languagecode2,c)

    for row in cursor.execute('SELECT page_id FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
        page_id = row[0]
        if page_id not in page_ids_language_ccc:
            page_ids_language_ccc[page_id]=[languagecode]
        else:
            page_ids_language_ccc[page_id].append(languagecode)

    print ("We've got the languages' CCC contained in this language: "+languagecode)

    return page_ids_language_ccc



def get_language_namespaces_page_ids(languagecode):

    page_ids_namespaces = {}
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute('SELECT page_id, page_namespace FROM page ORDER BY page_namespace;')
    rows = mysql_cur_read.fetchall()
    for row in rows: page_ids_namespaces[row[0]]=row[1]


    print ("We've got the page_ids for all the page_namespaces used in this language: "+languagecode)

    return page_ids_namespaces


"""

    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id GROUP BY rev_user_text') # EDITS TOTAL
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY rev_user_text') # EDITS ARTICLES
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace!=0 AND page_is_redirect=0 GROUP BY rev_user_text') # EDITS NO ARTICLES
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (6, 7, 14, 100, 10) GROUP BY rev_user_text') # EDITS DATASPACES: Files, categories, portals and templates
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (1, 2, 3) GROUP BY rev_user_text') # EDITS SN: user, user talks and article talks
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (4, 5, 8, 9, 11, 12, 13, 15) GROUP BY rev_user_text') # EDITS META: about Wikipedia, guidelines and help

"""

def get_language_editors_user_pages_page_ids(languagecode):

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); 
    mysql_cur_read = mysql_con_read.cursor()

    user_id_userpage_page_id = {}
    query = 'SELECT user_id, page_id FROM page INNER JOIN user ON user_name=page_title WHERE page.page_namespace = 2' # userpage
    mysql_cur_read.execute(query)
    rows = mysql_cur_read.fetchall()
    for row in rows: user_id_userpage_page_id[row[0]]=row[1]

    user_id_userpage_talk_page_id = {}
    query = 'SELECT user_id, page_id FROM page INNER JOIN user ON user_name=page_title WHERE page.page_namespace = 3' # userpage talk
    mysql_cur_read.execute(query)
    rows = mysql_cur_read.fetchall()
    for row in rows: user_id_userpage_talk_page_id[row[0]]=row[1]

    print ("We've got the user_pages for all the users in this language: "+languagecode)

    return user_id_userpage_page_id, user_id_userpage_talk_page_id





#######################################################################################


def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WDO - WIKIPEDIA DIVERSITY OBSERVATORY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/editor_diversity.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WDO - WIKIPEDIA DIVERSITY OBSERVATORY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue



#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("editor_diversity"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("editor_diversity"+".err", "w")
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
    wikidata_db = 'wikidata.db'

    editor_diversity_db = 'editor_diversity.db'
    revision_db = 'revision.db'
    imageslinks_db = 'imagelinks.db'
    images_db = 'images.db'


    # WIKIDATA DIVERSITY PROPERTIES
#    while True:
#    time.sleep(84600)
#    print ("Good morning. It is: "+time.today()+". Let's see if today is the day to download the Wikipedia Diversity OBSERVATORY data and start another entire cycle...")

    startTime = time.time()

    # The new cycle is always the last completed month
    cycle_year_month = wikilanguages_utils.get_new_cycle_year_month()

    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();

    # Verify there is a new language
    wikilanguages_utils.extract_check_new_wiki_projects();

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
#    print (wikilanguagecodes)
    
    # wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)]
    # biggest = wikilanguagecodes_by_size[:20]; smallest = wikilanguagecodes_by_size[20:]

    allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties, industry_properties, instance_of_subclasses_of_properties,sexual_orientation_properties,religion_properties,race_and_ethnia_properties,time_properties = wd_properties()

    print ('* Starting the EDITOR DIVERSITY '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))

    main()
#    main_with_exception_email()
#    main_loop_retry()

    finishTime = time.time()
    print ('* Done with the EDITOR DIVERSITY completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'editor_diversity.out', 'WIKIPEDIA DIVERSITY OBSERVATORY')
