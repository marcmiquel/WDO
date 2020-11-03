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
import gc



# https://stats.wikimedia.org/#/all-projects
# https://meta.wikimedia.org/wiki/List_of_Wikipedias/ca
# https://meta.wikimedia.org/wiki/Research:Metrics#Volume_of_contribution
# https://meta.wikimedia.org/wiki/Research:Wikistats_metrics/Active_editors



# MAIN
def main():

    # community_metrics('ca')
    # print ('community metrics with rel value!!')
    # input('')


    for languagecode in wikilanguagecodes:

        ###
        create_community_health_metrics_db()
        participation_metrics(languagecode) # it fills the database cawiki_editors, cawiki_editor_metrics
        time_based_metrics(languagecode) # it fills the database cawiki_editor_metrics
        community_metrics(languagecode) # it fills the database cawiki_community_metrics


    print ('done')
    ###


    # export_community_health_metrics_csv(languagecode) # it fills the database cawiki_editor_metrics

    # content_diversity_metrics(languagecode)    
    # multilingual_metrics(languagecode)



################################################################




# FUNCTIONS

def create_community_health_metrics_db():

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    
    for languagecode in wikilanguagecodes:


        table_name = languagecode+'wiki_editors'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, bot text, user_flag text, primarybinary integer, primarylang text, primarybinary_ecount integer, totallangs_ecount integer, numberlangs integer, registration_date, year_month_registration, first_edit_timestamp text, year_month_first_edit text, survived60d text, last_edit_timestamp text, lifetime_days integer, days_since_last_edit integer, seconds_between_last_two_edits integer, PRIMARY KEY (user_name, user_id))")
        cursor.execute(query)



        table_name = languagecode+'wiki_editor_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, abs_value real, rel_value real, metric_name text, year_month text, timestamp text, PRIMARY KEY (user_id, metric_name, year_month))")
        cursor.execute(query)


        table_name = languagecode+'wiki_community_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (abs_value real, rel_value real, metric_name text, year_month text, PRIMARY KEY (metric_name, year_month))")
        cursor.execute(query)


        table_name = languagecode+'wiki_article_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (qitem text, page_id integer, page_title text, abs_value real, rel_value, metric_name text, year_month text, PRIMARY KEY (metric_name, year_month))")
        cursor.execute(query)


        ### ---- ###

        table_name = languagecode+'wiki_editor_content_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, content_type text, abs_value real, rel_value real, year_month text, PRIMARY KEY (user_name, user_id, content_type))")
        cursor.execute(query)


    conn.commit()




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
        for year in range (1999, 2025):
            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'.tsv.bz2'
            if os.path.isfile(dumps_path): 
                d_paths.append(dumps_path)

        if len(d_paths) == 0:
            for year in range(1999, 2025): # months
                for month in range(0, 13):
                    if month > 9:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-'+str(month)+'.tsv.bz2'
                    else:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-0'+str(month)+'.tsv.bz2'

                    if os.path.isfile(dumps_path) == True:
                        d_paths.append(dumps_path)

    print(len(d_paths))
    print (d_paths)

    return d_paths




def participation_metrics(languagecode):

    functionstartTime = time.time()
    function_name = 'participation_metrics '+languagecode
    print (function_name)

    d_paths = get_mediawiki_paths(languagecode)


    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()


    user_id_user_name_dict = {}

    user_id_bot_dict = {}
    user_id_user_groups_dict = {}

    editor_first_edit_timestamp = {}
    editor_registration_date = {}

    editor_last_edit_timestamp = {}
    editor_seconds_since_last_edit = {}


    # for the survival part
    survived_dict = {}
    survival_measures = []
    user_id_edit_count = {}
    editor_user_page_edit_count = {}
    editor_user_page_talk_page_edit_count = {}


    # for the monthly part
    editor_monthly_namespace0_edits = {}
    editor_monthly_namespace1_edits = {}
    editor_monthly_namespace2_edits = {}
    editor_monthly_namespace3_edits = {}
    editor_monthly_namespace4_edits = {}
    editor_monthly_namespace5_edits = {}
    editor_monthly_namespace6_edits = {}
    editor_monthly_namespace7_edits = {}
    editor_monthly_namespace8_edits = {}
    editor_monthly_namespace9_edits = {}
    editor_monthly_namespace10_edits = {}
    editor_monthly_namespace11_edits = {}
    editor_monthly_namespace12_edits = {}
    editor_monthly_namespace13_edits = {}
    editor_monthly_namespace14_edits = {}
    editor_monthly_namespace15_edits = {}
    editor_monthly_user_page_edit_count = {}
    editor_monthly_user_page_talk_page_edit_count = {}
    editor_monthly_edits = {}
    editor_monthly_seconds_between_edits = {}



    last_year_month = 0
    first_date = datetime.datetime.strptime('2001-01-01 01:15:15','%Y-%m-%d %H:%M:%S')

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = 'something'
        line = dump_in.readline()


        while line != '':

            # print ('*')
            # print (line)
            # print (seconds_since_last_edit)
            # print ('*')
            # input('')            

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')
            if len(values)==1: continue

            event_entity = values[1]
            event_type = values[2]


            event_user_id = values[5]
            try: int(event_user_id)
            except: 
                continue

            event_user_text = values[7]
            if event_user_text != '': user_id_user_name_dict[event_user_id] = event_user_text
            else: 
                continue


            event_timestamp = values[3]
            event_timestamp_dt = datetime.datetime.strptime(event_timestamp[:len(event_timestamp)-2],'%Y-%m-%d %H:%M:%S')
            editor_last_edit_timestamp[event_user_id] = event_timestamp



            event_user_groups = values[11]
            if event_user_groups != '':
                user_id_user_groups_dict[event_user_id] = event_user_groups
                # print (event_user_text, event_user_groups)

            event_is_bot_by = values[13]
            if event_is_bot_by != '':
                user_id_bot_dict[event_user_id] = event_is_bot_by
                # print (event_user_text, event_is_bot_by)

            event_user_is_anonymous = values[17]
            if event_user_is_anonymous == True or event_user_id == '': continue

            event_user_registration_date = values[18]
            if event_user_id not in editor_registration_date: 
                if event_user_registration_date != '':
                    editor_registration_date[event_user_id] = event_user_registration_date


            ####### ---------

            # MONTHLY EDITS COUNTER
            try: editor_monthly_edits[event_user_id] = editor_monthly_edits[event_user_id]+1
            except: editor_monthly_edits[event_user_id] = 1


            # MONTHLY NAMESPACES EDIT COUNTER
            page_namespace = values[28]
            if page_namespace == '0':
                try: editor_monthly_namespace0_edits[event_user_id] = editor_monthly_namespace0_edits[event_user_id]+1
                except: editor_monthly_namespace0_edits[event_user_id] = 1
            elif page_namespace == '1':
                try: editor_monthly_namespace1_edits[event_user_id] = editor_monthly_namespace1_edits[event_user_id]+1
                except: editor_monthly_namespace1_edits[event_user_id] = 1
            elif page_namespace == '2':
                try: editor_monthly_namespace2_edits[event_user_id] = editor_monthly_namespace2_edits[event_user_id]+1
                except: editor_monthly_namespace2_edits[event_user_id] = 1
            elif page_namespace == '3':
                try: editor_monthly_namespace3_edits[event_user_id] = editor_monthly_namespace3_edits[event_user_id]+1
                except: editor_monthly_namespace3_edits[event_user_id] = 1
            elif page_namespace == '4':
                try: editor_monthly_namespace4_edits[event_user_id] = editor_monthly_namespace4_edits[event_user_id]+1
                except: editor_monthly_namespace4_edits[event_user_id] = 1
            elif page_namespace == '5':
                try: editor_monthly_namespace5_edits[event_user_id] = editor_monthly_namespace5_edits[event_user_id]+1
                except: editor_monthly_namespace5_edits[event_user_id] = 1
            elif page_namespace == '6':
                try: editor_monthly_namespace6_edits[event_user_id] = editor_monthly_namespace6_edits[event_user_id]+1
                except: editor_monthly_namespace6_edits[event_user_id] = 1
            elif page_namespace == '7':
                try: editor_monthly_namespace7_edits[event_user_id] = editor_monthly_namespace7_edits[event_user_id]+1
                except: editor_monthly_namespace7_edits[event_user_id] = 1
            elif page_namespace == '8':
                try: editor_monthly_namespace8_edits[event_user_id] = editor_monthly_namespace8_edits[event_user_id]+1
                except: editor_monthly_namespace8_edits[event_user_id] = 1
            elif page_namespace == '9':
                try: editor_monthly_namespace9_edits[event_user_id] = editor_monthly_namespace9_edits[event_user_id]+1
                except: editor_monthly_namespace9_edits[event_user_id] = 1
            elif page_namespace == '10':
                try: editor_monthly_namespace10_edits[event_user_id] = editor_monthly_namespace10_edits[event_user_id]+1
                except: editor_monthly_namespace10_edits[event_user_id] = 1
            elif page_namespace == '11':
                try: editor_monthly_namespace11_edits[event_user_id] = editor_monthly_namespace11_edits[event_user_id]+1
                except: editor_monthly_namespace11_edits[event_user_id] = 1
            elif page_namespace == '12':
                try: editor_monthly_namespace12_edits[event_user_id] = editor_monthly_namespace12_edits[event_user_id]+1
                except: editor_monthly_namespace12_edits[event_user_id] = 1
            elif page_namespace == '13':
                try: editor_monthly_namespace13_edits[event_user_id] = editor_monthly_namespace13_edits[event_user_id]+1
                except: editor_monthly_namespace13_edits[event_user_id] = 1
            elif page_namespace == '14':
                try: editor_monthly_namespace14_edits[event_user_id] = editor_monthly_namespace14_edits[event_user_id]+1
                except: editor_monthly_namespace14_edits[event_user_id] = 1
            elif page_namespace == '15':
                try: editor_monthly_namespace15_edits[event_user_id] = editor_monthly_namespace15_edits[event_user_id]+1
                except: editor_monthly_namespace15_edits[event_user_id] = 1


            # MONTHLY USER PAGE/USER PAGE TALK PAGE EDIT COUNTER
            page_title = values[25]
            if event_user_text == page_title and page_namespace == '2':
                try: 
                    editor_monthly_user_page_edit_count[event_user_id] = editor_monthly_user_page_edit_count[event_user_id]+1
                except: 
                    editor_monthly_user_page_edit_count[event_user_id] = 1

            if event_user_text == page_title and page_namespace == '3':
                try:
                    editor_monthly_user_page_talk_page_edit_count[event_user_id] = editor_monthly_user_page_talk_page_edit_count[event_user_id]+1
                except:
                    editor_monthly_user_page_talk_page_edit_count[event_user_id] = 1

            # MONTHLY AVERAGE SECONDS BETWEEN EDITS COUNTER
            seconds_since_last_edit = values[22]
            if seconds_since_last_edit != None and seconds_since_last_edit != '':
                seconds_since_last_edit = int(seconds_since_last_edit)
                editor_seconds_since_last_edit[event_user_id] = seconds_since_last_edit
            if seconds_since_last_edit != None and seconds_since_last_edit != '':
                if event_user_id != '' and event_user_id != 0:
                    try:
                        editor_monthly_seconds_between_edits[event_user_id].append(seconds_since_last_edit)
                    except:
                        editor_monthly_seconds_between_edits[event_user_id] = [seconds_since_last_edit]


            ####### ---------
            

            # CHECK MONTH CHANGE AND INSERT MONTHLY EDITS/NAMESPACES EDITS/SECONDS
            current_year_month = datetime.datetime.strptime(event_timestamp_dt.strftime('%Y-%m'),'%Y-%m')
            if last_year_month != current_year_month and last_year_month != 0:

                lym = last_year_month.strftime('%Y-%m')
                monthly_edits = []
                monthly_seconds = []
                namespaces = []

                for user_id, edits in editor_monthly_edits.items():
                    monthly_edits.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits', lym, ''))

                for user_id, seconds_list in editor_monthly_seconds_between_edits.items():
                    if seconds_list == None: continue
                    elif len(seconds_list) > 1:
                        average_seconds = np.mean(seconds_list)
                        monthly_seconds.append((user_id, user_id_user_name_dict[user_id], average_seconds, None, 'monthly_average_seconds_between_edits', lym, ''))

                for user_id, user_name in editor_monthly_namespace0_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace0_edits[user_id], None, 'monthly_edits_ns0_main', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace1_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace1_edits[user_id], None, 'monthly_edits_ns1_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace2_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace2_edits[user_id], None, 'monthly_edits_ns2_user', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace3_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace3_edits[user_id], None, 'monthly_edits_ns3_user_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace4_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace4_edits[user_id], None, 'monthly_edits_ns4_project', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace5_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace5_edits[user_id], None, 'monthly_edits_ns5_project_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace6_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace6_edits[user_id], None, 'monthly_edits_ns6_file', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace7_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace7_edits[user_id], None, 'monthly_edits_ns7_file_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace8_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace8_edits[user_id], None, 'monthly_edits_ns8_mediawiki', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace9_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace9_edits[user_id], None, 'monthly_edits_ns9_mediawiki_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace10_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace10_edits[user_id], None, 'monthly_edits_ns10_template', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace11_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace11_edits[user_id], None, 'monthly_edits_ns11_template_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace12_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace12_edits[user_id], None, 'monthly_edits_ns12_help', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace13_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace13_edits[user_id], None, 'monthly_edits_ns13_help_talk', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace14_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace14_edits[user_id], None, 'monthly_edits_ns14_category', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_namespace15_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_namespace15_edits[user_id], None, 'monthly_edits_ns15_category_talk', lym, ''))
                    except: pass


                for user_id, user_name in editor_monthly_user_page_edit_count.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_user_page_edit_count[user_id], None, 'monthly_user_page_edit_count', lym, ''))
                    except: pass

                for user_id, user_name in editor_monthly_user_page_talk_page_edit_count.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], editor_monthly_user_page_talk_page_edit_count[user_id], None, 'monthly_user_page_talk_page_edit_count', lym, ''))
                    except: pass



                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query,monthly_edits)
                cursor.executemany(query,namespaces)
                cursor.executemany(query,monthly_seconds)
                conn.commit()

                monthly_edits = []
                monthly_seconds = []
                namespaces = []

                editor_monthly_namespace0_edits = {}
                editor_monthly_namespace1_edits = {}
                editor_monthly_namespace2_edits = {}
                editor_monthly_namespace3_edits = {}
                editor_monthly_namespace4_edits = {}
                editor_monthly_namespace5_edits = {}
                editor_monthly_namespace6_edits = {}
                editor_monthly_namespace7_edits = {}
                editor_monthly_namespace8_edits = {}
                editor_monthly_namespace9_edits = {}
                editor_monthly_namespace10_edits = {}
                editor_monthly_namespace11_edits = {}
                editor_monthly_namespace12_edits = {}
                editor_monthly_namespace13_edits = {}
                editor_monthly_namespace14_edits = {}
                editor_monthly_namespace15_edits = {}
                editor_monthly_edits = {}
                editor_monthly_seconds_between_edits = {}
                editor_monthly_user_page_edit_count = {}
                editor_monthly_user_page_talk_page_edit_count = {}


            last_year_month = current_year_month


            ####### ---------

            # SURVIVAL MEASURES

            event_user_first_edit_timestamp = values[20]
            if event_user_id not in editor_first_edit_timestamp:
                editor_first_edit_timestamp[event_user_id] = event_user_first_edit_timestamp

            if event_user_first_edit_timestamp == '' or event_user_first_edit_timestamp == None:
                event_user_first_edit_timestamp = editor_first_edit_timestamp[event_user_id]

            if event_user_first_edit_timestamp != '' and event_user_id not in survived_dict:
                event_user_first_edit_timestamp_dt = datetime.datetime.strptime(event_user_first_edit_timestamp[:len(event_user_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S')


                # thresholds
                first_edit_timestamp_1day_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=1))
                first_edit_timestamp_7days_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=7))
                first_edit_timestamp_1months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=1))
                first_edit_timestamp_2months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=2))

                try: ec = user_id_edit_count[event_user_id]
                except: ec = 1


                # at 1 day
                if event_timestamp_dt >= first_edit_timestamp_1day_dt:

                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edit_count[event_user_id], None, 'user_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edit_count[event_user_id], None, 'user_page_talk_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))


                # at 7 days
                if event_timestamp_dt >= first_edit_timestamp_7days_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_7d', first_edit_timestamp_7days_dt.strftime('%Y-%m'),first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 1 month
                if event_timestamp_dt >= first_edit_timestamp_1months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_30d', first_edit_timestamp_1months_dt.strftime('%Y-%m'),first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edit_count[event_user_id], None, 'user_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edit_count[event_user_id], None, 'user_page_talk_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))


                # at 2 months
                if event_timestamp_dt >= first_edit_timestamp_2months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_60d', first_edit_timestamp_2months_dt.strftime('%Y-%m'),first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')))
                    survived_dict[event_user_id]=event_user_text

                    try: del user_id_edit_count[event_user_id]
                    except: pass

                    try: del editor_user_page_talk_page_edit_count[event_user_id]
                    except: pass

                    try: del editor_user_page_edit_count[event_user_id]
                    except: pass


            # USER PAGE EDIT COUNT, ADD ONE MORE EDIT.
            if event_user_id not in survived_dict:

                if event_user_text == page_title and page_namespace == '2':
                    try: 
                        editor_user_page_edit_count[event_user_id] = editor_user_page_edit_count[event_user_id]+1
                    except: 
                        editor_user_page_edit_count[event_user_id] = 1

                if event_user_text == page_title and page_namespace == '3':
                    try:
                        editor_user_page_talk_page_edit_count[event_user_id] = editor_user_page_talk_page_edit_count[event_user_id]+1
                    except:
                        editor_user_page_talk_page_edit_count[event_user_id] = 1

                # EDIT COUNT, ADD ONE MORE EDIT.
                event_user_revision_count = values[21]
                if event_user_revision_count != '':
                    user_id_edit_count[event_user_id] = event_user_revision_count
                elif event_user_id in user_id_edit_count:
                    user_id_edit_count[event_user_id] = int(user_id_edit_count[event_user_id]) + 1
                else:
                    user_id_edit_count[event_user_id] = 1

            ####### ---------



        # SURVIVAL MEASURES INSERT
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query,survival_measures)
        conn.commit()
        survival_measures = []


        # MONTHLY EDITS/SECONDS INSERT (LAST ROUND)
        lym = last_year_month.strftime('%Y-%m')
        monthly_edits = []
        for event_user_id, edits in editor_monthly_edits.items():
            monthly_edits.append((event_user_id, user_id_user_name_dict[event_user_id], edits, None, 'monthly_edits', lym, ''))

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query,monthly_edits)
        conn.commit()
        editor_monthly_edits = {}
        monthly_edits = []


        monthly_seconds = []
        for event_user_id, seconds_list in editor_monthly_seconds_between_edits.items():
            if seconds_list == None: continue
            elif len(seconds_list) > 1:
                average_seconds = np.mean(seconds_list)
                monthly_seconds.append((event_user_id, user_id_user_name_dict[event_user_id], average_seconds, None, 'monthly_average_seconds_between_edits', lym, ''))

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query,monthly_seconds)
        conn.commit()
        editor_monthly_seconds_between_edits = {}
        monthly_seconds = []


        # USER CHARACTERISTICS INSERT
        user_characteristics1 = []
        user_characteristics2 = []
        for user_id, user_name in user_id_user_name_dict.items():
            
            try: user_flag = user_id_user_groups_dict[user_id]
            except: user_flag = ''

            try: bot = user_id_bot_dict[user_id]
            except: bot = 'editor'

            if user_id in survived_dict: survived60d = '1'
            else: survived60d = '0'


            try: registration_date = editor_registration_date[user_id]
            except: registration_date = ''
            
            if registration_date == '': # THIS IS SOMETHING WE "ASSUME" BECAUSE THERE ARE MANY ACCOUNTS WITHOUT A REGISTRATION DATE.
                try: registration_date = editor_first_edit_timestamp[user_id]
                except: registration_date = ''

            if registration_date != '': year_month_registration = datetime.datetime.strptime(registration_date[:len(registration_date)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            else: year_month_registration = ''

            try: fe = editor_first_edit_timestamp[user_id]
            except: fe = ''

            try: le = editor_last_edit_timestamp[user_id]
            except: le = ''

            if fe != '':  
                year_month = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
                fe_d = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S')
            else:
                year_month = ''
                fe_d = ''

            if le != '':
                le_d = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S')
                days_since_last_edit = (event_timestamp_dt - le_d).days
            else:
                le_d = ''
                days_since_last_edit = ''


            if fe != '' and le != '': lifetime_days =  (le_d - fe_d).days
            else: lifetime_days = 0
        
            try: se = editor_seconds_since_last_edit[user_id]
            except: se = ''

            user_characteristics1.append((user_id, user_name, registration_date, year_month_registration,  fe, year_month, survived60d))

            user_characteristics2.append((bot, user_flag, le, lifetime_days, days_since_last_edit, se, user_id, user_name))


        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (user_id, user_name, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, survived60d) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics1)

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (bot, user_flag, last_edit_timestamp, lifetime_days, days_since_last_edit, seconds_between_last_two_edits, user_id, user_name) VALUES (?,?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics2)

        query = 'UPDATE '+languagecode+'wiki_editors SET bot = ?, user_flag = ?, last_edit_timestamp = ?, lifetime_days = ?, days_since_last_edit = ?, seconds_between_last_two_edits = ? WHERE user_id = ? AND user_name = ?;'
        cursor.executemany(query,user_characteristics2)
        conn.commit()

        user_characteristics1 = []
        user_characteristics2 = []

        # insert or ignore + update
        user_id_bot_dict = {}
        user_id_user_groups_dict = {}
        editor_last_edit_timestamp = {}
        editor_seconds_since_last_edit = {}

        # insert or ignore
        editor_first_edit_timestamp = {}
        editor_registration_date = {}

        user_id_user_name_dict = {}


        # END OF THE DUMP!!!!
        print ('*')
        print (str(datetime.timedelta(seconds=time.time() - iterTime)))






    # CALCULATING AGGREGATED METRICS (EDIT COUNTS)
    monthly_aggregated_metrics = {'monthly_edits':'edit_count', 'monthly_user_page_edit_count': 'edit_count_editor_user_page', 'monthly_user_page_talk_page_edit_count': 'edit_count_editor_user_page_talk_page', 'monthly_edits_ns0_main':'edit_count_ns0_main', 'monthly_edits_ns1_talk':'edit_count_ns1_talk', 'monthly_edits_ns2_user':'edit_count_ns2_user', 'monthly_edits_ns3_user_talk': 'edit_count_ns3_user_talk', 'monthly_edits_ns4_project':'edit_count_ns4_project', 'monthly_edits_ns5_project_talk': 'edit_count_ns5_project_talk', 'monthly_edits_ns6_file': 'edit_count_edits_ns6_file', 'monthly_edits_ns7_file_talk':'edit_count_ns7_file_talk', 'monthly_edits_ns8_mediawiki': 'edit_count_ns8_mediawiki', 'monthly_edits_ns9_mediawiki_talk': 'edit_count_ns9_mediawiki_talk', 'monthly_edits_ns10_template':'edit_count_ns10_template', 'monthly_edits_ns11_template_talk':'edit_count_ns11_template_talk', 'monthly_edits_ns12_help':'edit_count_ns12_help','monthly_edits_ns13_help_talk':'edit_count_ns13_help_talk','monthly_edits_ns14_category':'edit_count_ns14_category','monthly_edits_ns15_category_talk':'edit_count_ns15_category_talk'}


    conn2 = sqlite3.connect(databases_path + community_health_metrics_db); cursor2 = conn2.cursor()
    for monthly_metric_name, metric_name in monthly_aggregated_metrics.items():
        edit_counts = []
        query = 'SELECT user_id, user_name, SUM(abs_value) FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "'+monthly_metric_name+'" GROUP BY 2;'
        for row in cursor.execute(query):
            edit_counts.append((row[0],row[1],row[2],metric_name,lym))

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, metric_name, year_month) VALUES (?,?,?,?,?);';
        cursor2.executemany(query,edit_counts)
        conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)






def time_based_metrics(languagecode):

    functionstartTime = time.time()
    function_name = 'time_based_metrics '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()
    # query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND user_name IN ("MuRe","TaronjaSatsuma") ORDER BY user_name, year_month;'

    # query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND user_name IN ("Gomà","Josepnogue","Cdani","Paucabot","KRLS","Leptictidium","Vriullop","Pallares","Toniher","Mzamora2","RR","Barcelona","Enric","MuRe","Papapep","Góngora","SMP","Amadalvarez","Simonjoan","Imartin6","Aries","Xtv","Beusson","Castor","Dvdgmz","Arnaugir","Lilaroja","Pacopac","Jsalescabre","Pitxiquin","Mgclapé","Kippelboy","Ferrangb","Marcmiquel","Davidpar","Lluis tgn","Al Lemos","Laurita","Catgirl","Esenabre","MALLUS","F3RaN","Joan Subirats","Auró","Galazan","Coet","Antoni Salvà","Joancreus","Oriol Dubreuil","El Caro","ESM","Marionaaragay","QuimGil","Vàngelisvillar","Ivan bea","Tituscat","Docosong","Flamenc","Jey","Julià Minguillón","Pau Colominas","Unapersona","Anskar","Laura.Girona","Planvi","Medol","Marcoil","M.Angels Massip","Carles Riba","Dnogue","Guillem Nogué","Mikicat","FranSisPac","TaronjaSatsuma","Xavier Dengra","Gerardduenas","Tiputini","Quelet","Alibey","Townie","Eaibar","Llumeureka","Tsdgeos","Kette~cawiki","Mariusmm","19Tarrestnom65","B25es","Paputx","Nenagamba","Bgasco","Vallue","Dorieo","Amper2","Pere Orga","Xavier sistach","Julio Meneses","Joan Bover","Sorenike","Brinerustle","Ponscor","Jove","Ignacio.torres","Jordi G","Jlamadorjr","Josep Gibert","Departament de Matemàtiques UAB","Albertdg","AlbertRA","Xaviaranda","Galderich","Aniol","Voraviu","Unapeça") ORDER BY user_name, year_month;'

    query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month'

    # print (query)
    user_count = 0

    old_user_id = ''
    expected_year_month_dt = ''

    parameters = []

    active_months = 0
    active_months_row = 0
    total_months = 0
    max_active_months_row = 0

    inactivity_periods = 0
    inactive_months = 0
    max_inactive_months_row = 0


    for row in cursor.execute(query):
        edits=row[0]
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]

        if cur_user_id != old_user_id and old_user_id != '':
            user_count += 1

            cycle_year_month_dt = datetime.datetime.strptime(cycle_year_month,'%Y-%m')

            months_since_last_edit = (cycle_year_month_dt.year - current_year_month_dt.year) * 12 + cycle_year_month_dt.month - current_year_month_dt.month
            if months_since_last_edit < 0: months_since_last_edit = 0

            if months_since_last_edit > 0:
                parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'months_since_last_edit', old_year_month,''))

            if months_since_last_edit > max_inactive_months_row:
                parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'max_inactive_months_row', old_year_month,''))

                parameters.append((old_user_id, old_user_name, 1, None, 'personal_drop_off_threshold', cycle_year_month,''))
            else:
                parameters.append((old_user_id, old_user_name, max_inactive_months_row, None, 'max_inactive_months_row', old_year_month,''))                

            parameters.append((old_user_id, old_user_name, inactivity_periods, None, 'inactivity_periods', old_year_month,''))
            parameters.append((old_user_id, old_user_name, active_months, None, 'active_months', old_year_month,''))
            parameters.append((old_user_id, old_user_name, max_active_months_row, None, 'max_active_months_row', old_year_month,''))
            parameters.append((old_user_id, old_user_name, total_months, None, 'total_months', old_year_month,''))



            active_months = 0
            total_months = 0
            active_months_row = 0
            max_active_months_row = 0
            inactivity_periods = 0
            inactive_months = 0
            max_inactive_months_row = 0


            if user_count % 10000 == 0:
                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'

                cursor.executemany(query,parameters)
                conn.commit()

                parameters = []



        current_year_month_dt = datetime.datetime.strptime(current_year_month,'%Y-%m')

        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:

            inactivity_periods += 1
            while expected_year_month_dt < current_year_month_dt:
                # print (expected_year_month_dt, current_year_month_dt)
                inactive_months = inactive_months + 1

                expected_year_month_dt = (expected_year_month_dt + relativedelta.relativedelta(months=1))
                total_months = total_months + 1

            if inactive_months > max_inactive_months_row:
                max_inactive_months_row = inactive_months

            if active_months_row > max_active_months_row:
                max_active_months_row = active_months_row

            active_months_row = 1
            inactive_months = 0
        else:
            active_months_row = active_months_row + 1
            if active_months_row > max_active_months_row:
                max_active_months_row = active_months_row




        total_months = total_months + 1
        active_months = active_months + 1

        old_year_month = current_year_month
        expected_year_month_dt = (datetime.datetime.strptime(old_year_month,'%Y-%m') + relativedelta.relativedelta(months=1))

        old_user_id = cur_user_id
        old_user_name = cur_user_name
        # print ('# update: ',old_user_id, old_user_name, active_months, max_active_months_row, max_inactive_months_row, total_months)
        # input('')



    # COMPUTER METRICS FOR THE LAST EDITOR
    cycle_year_month_dt = datetime.datetime.strptime(cycle_year_month,'%Y-%m')

    months_since_last_edit = (cycle_year_month_dt.year - current_year_month_dt.year) * 12 + cycle_year_month_dt.month - current_year_month_dt.month
    if months_since_last_edit < 0: months_since_last_edit = 0

    if months_since_last_edit > 0:
        parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'months_since_last_edit', old_year_month,''))

    if months_since_last_edit > max_inactive_months_row:
        parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'max_inactive_months_row', old_year_month,''))
        parameters.append((old_user_id, old_user_name, 1, None, 'over_personal_personal_drop_off_threshold', cycle_year_month,''))
    else:
        parameters.append((old_user_id, old_user_name, max_inactive_months_row, None, 'max_inactive_months_row', old_year_month,''))                

    parameters.append((old_user_id, old_user_name, inactivity_periods, None, 'inactivity_periods', old_year_month,''))
    parameters.append((old_user_id, old_user_name, active_months, None, 'active_months', old_year_month,''))
    parameters.append((old_user_id, old_user_name, max_active_months_row, None, 'max_active_months_row', old_year_month,''))
    parameters.append((old_user_id, old_user_name, total_months, None, 'total_months', old_year_month,''))


    # INSERT THE LAST EDITORS
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'

    cursor.executemany(query,parameters)
    conn.commit()

    print ('done with the months.')



    """***
    For the English Wikipedia it may not be possible to keep in memory these two metrics for all the editors.
    max_inactive_months_row
    months_since_last_edit

    In this case, it would be necessary to recode the following lines

    ****"""





    # CALCULATING EDIT COUNT BINS AND DROP-OFF THRESHOLDS
    max_inactive_months_row_dict = {}
    query = 'SELECT user_id, abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "max_inactive_months_row";'
    for row in cursor.execute(query):
        max_inactive_months_row_dict[row[0],row[1]]



    months_since_last_edit_dict = {}
    query = 'SELECT user_id, abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "months_since_last_edit";'
    for row in cursor.execute(query):
        months_since_last_edit_dict[row[0],row[1]]



    # CALCULATE DROP-OFF THRESHOLDS
    query = 'SELECT user_id, user_name, abs_value as edits FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "edit_count";'

    # query = 'SELECT ee.user_id, ee.user_name, ee.abs_value as edits, ec.year_month_registration, ec.lifetime_days, ec.last_edit_timestamp FROM '+languagecode+'wiki_editor_metrics ee INNER JOIN '+languagecode+'wiki_editors ec ON ee.user_id = ec.user_id WHERE metric_name = "edit_count" AND ee.user_name IN ("Gomà","Josepnogue","Cdani","Paucabot","KRLS","Leptictidium","Vriullop","Pallares","Toniher","Mzamora2","RR","Barcelona","Enric","MuRe","Papapep","Góngora","SMP","Amadalvarez","Simonjoan","Imartin6","Aries","Xtv","Beusson","Castor","Dvdgmz","Arnaugir","Lilaroja","Pacopac","Jsalescabre","Pitxiquin","Mgclapé","Kippelboy","Ferrangb","Marcmiquel","Davidpar","Lluis tgn","Al Lemos","Laurita","Catgirl","Esenabre","MALLUS","F3RaN","Joan Subirats","Auró","Galazan","Coet","Antoni Salvà","Joancreus","Oriol Dubreuil","El Caro","ESM","Marionaaragay","QuimGil","Vàngelisvillar","Ivan bea","Tituscat","Docosong","Flamenc","Jey","Julià Minguillón","Pau Colominas","Unapersona","Anskar","Laura.Girona","Planvi","Medol","Marcoil","M.Angels Massip","Carles Riba","Dnogue","Guillem Nogué","Mikicat","FranSisPac","TaronjaSatsuma","Xavier Dengra","Gerardduenas","Tiputini","Quelet","Alibey","Townie","Eaibar","Llumeureka","Tsdgeos","Kette~cawiki","Mariusmm","19Tarrestnom65","B25es","Paputx","Nenagamba","Bgasco","Vallue","Dorieo","Amper2","Pere Orga","Xavier sistach","Julio Meneses","Joan Bover","Sorenike","Brinerustle","Ponscor","Jove","Ignacio.torres","Jordi G","Jlamadorjr","Josep Gibert","Departament de Matemàtiques UAB","Albertdg","AlbertRA","Xaviaranda","Galderich","Aniol","Voraviu","Unapeça");'

    df = pd.read_sql_query(query, conn)
    # print (df.head(10))

    # df = df.set_index('user_id')
    df["edits"] = pd.to_numeric(df["edits"])

    df["edits"] = df['edits'].values.astype(int)

    df['max_inactive_months_row'] = df['user_id'].map(max_inactive_months_row_dict)
    df['months_since_last_edit'] = df['user_id'].map(months_since_last_edit_dict).fillna(0)

    bins = [0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 1000000000000] # in my phd I used, [1, 100, 1000, 5000, 10000, +)

    labels = []
    for x in range(0,len(bins)-1):
        if x < len(bins):
            labels.append(str(bins[x])+'_'+str(bins[x+1]))

    df['edit_count_bin'] = pd.cut(x = df['edits'], bins = bins, labels = labels)


    df['drop_off_bin_threshold'] = df['max_inactive_months_row'].groupby(df['edit_count_bin']).transform('mean')
    df['over_edit_count_bin_personal_drop_off_threshold'] = df['drop_off_bin_threshold'] - df['months_since_last_edit']
    df['over_personal_drop_off_thresold'] = df['max_inactive_months_row'] - df['months_since_last_edit']


    # df.to_csv('e.csv')

    # users = df.user_name.tolist()
    # allusers = ["Gomà","Josepnogue","Cdani","Paucabot","KRLS","Leptictidium","Vriullop","Pallares","Toniher","Mzamora2","RR","Barcelona","Enric","MuRe","Papapep","Góngora","SMP","Amadalvarez","Simonjoan","Imartin6","Aries","Xtv","Beusson","Castor","Dvdgmz","Arnaugir","Lilaroja","Pacopac","Jsalescabre","Pitxiquin","Mgclapé","Kippelboy","Ferrangb","Marcmiquel","Davidpar","Lluis tgn","Al Lemos","Laurita","Catgirl","Esenabre","MALLUS","F3RaN","Joan Subirats","Auró","Galazan","Coet","Antoni Salvà","Joancreus","Oriol Dubreuil","El Caro","ESM","Marionaaragay","QuimGil","Vàngelisvillar","Ivan bea","Tituscat","Docosong","Flamenc","Jey","Julià Minguillón","Pau Colominas","Unapersona","Anskar","Laura.Girona","Planvi","Medol","Marcoil","M.Angels Massip","Carles Riba","Dnogue","Guillem Nogué","Mikicat","FranSisPac","TaronjaSatsuma","Xavier Dengra","Gerardduenas","Tiputini","Quelet","Alibey","Townie","Eaibar","Llumeureka","Tsdgeos","Kette~cawiki","Mariusmm","19Tarrestnom65","B25es","Paputx","Nenagamba","Bgasco","Vallue","Dorieo","Amper2","Pere Orga","Xavier sistach","Julio Meneses","Joan Bover","Sorenike","Brinerustle","Ponscor","Jove","Ignacio.torres","Jordi G","Jlamadorjr","Josep Gibert","Departament de Matemàtiques UAB","Albertdg","AlbertRA","Xaviaranda","Galderich","Aniol","Voraviu","Unapeça"]
    # for user in users:
    #     allusers.remove(user)
    # print (allusers)
    # input('')


    # COMMUNITY HEALTH METRICS
    parameters = []
    for index, rows in df.iterrows():
        user_id = rows['user_id']
        user_name = rows['user_name']

        over_edit_count_bin_personal_drop_off_threshold = rows['over_edit_count_bin_personal_drop_off_threshold']
        edit_count_bin = str(rows['edit_count_bin'])
        # edit_count_bin = str(rows['edit_count_bin']).replace(' ','_').replace(',','')#[1:]
        # edit_count_bin = edit_count_bin[:len(edit_count_bin)-2]

        if over_edit_count_bin_personal_drop_off_threshold < 0:
            parameters.append((user_id, user_name, 1, None, 'over_edit_count_bin_personal_drop_off_threshold', cycle_year_month,''))
        parameters.append((user_id, user_name, edit_count_bin, None, 'edit_count_bin', cycle_year_month,''))
        # print ((user_id, user_name, edit_count_bin, 'edit_count_bin', cycle_year_month,''))


    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
    cursor.executemany(query,parameters)
    conn.commit()


    # COMMUNITY ENGAGEMENT METRICS
    parameters = []
    df.edit_count_bin = df.edit_count_bin.astype(str)
    editors_bin_count = df.groupby(df['edit_count_bin']).size().to_dict()
    for bin, count in editors_bin_count.items():
        # bin = bin.replace(' ','_').replace(',','')
        # bin = bin[1:][:len(bin)-2]
        parameters.append((count, None, 'edit_count_bin_'+bin+'_count', cycle_year_month))
        # print ((count, 'edit_count_bin_'+bin+'_count', cycle_year_month))


    df = df.loc[(df["over_edit_count_bin_personal_drop_off_threshold"] < 0)]
    editors_bin_count = df.groupby(df['edit_count_bin']).size().to_dict()
    for bin, count in editors_bin_count.items():
        # bin = bin.replace(' ','_').replace(',','')
        # bin = bin[1:][:len(bin)-2]
        parameters.append((count, None, 'edit_count_bin_'+bin+'_drop_off_count', cycle_year_month))
        # print ((count, 'edit_count_bin_'+bin+'_drop_off_count', cycle_year_month))


    query_cm = 'INSERT OR IGNORE INTO '+languagecode+'wiki_community_metrics (abs_value, rel_value, metric_name, year_month) VALUES (?,?,?,?);'
    cursor.executemany(query_cm,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)

    # select cawiki_editor_metrics.user_id, cawiki_editor_metrics.user_name, value, last_edit_timestamp, days_since_last_edit from cawiki_editor_metrics inner join cawiki_editors on cawiki_editor_metrics.user_id = cawiki_editors.user_id where metric_name = 'edit_count' and cawiki_editor_metrics.user_id in (select user_id from cawiki_editor_metrics where metric_name = 'edit_count' and value > 1000) and cawiki_editor_metrics.user_id in (select user_id from cawiki_editor_metrics where metric_name = 'personal_drop_off_threshold') and cawiki_editors.bot = 'editor' order by value desc limit 50;




def editors_social_metrics(languagecode):

    pass

"""
Iteració sencera a MediaWiki history
Mètriques mensuals.

Iterar pel mes que anem. 
Consulta a cada mes als registrats d'aquell mes o dos abans.
Comprovar quants d'aquests s'hi interactua I fer els comptadors

Cal guardar les últimes edicions de tots els usuaris per comprovar si hi ha interacció. 


Fetes 
Interactions newcomers_user_page_talk_page_edits
Interactions newcomers_article_talk_page_edits
Interactions newcomer_count
Interactions survivors_count (aquesta sempre hi haurà un decalatge de dos mesos) -> quants dels newcomers amb qui has interactuat sobreviuen.

Interaccions rebudes
User talk pages
Article talk pages


Hipòtesi. Quan els editors estan a punt de fer drop off... Deixen abans d'interactuar amb newcomers. 

Hipòtesi. Quan els editors deixen d'interactuar amb ells... Estan més a prop del drop off. 
"""




def community_metrics(languagecode):

    functionstartTime = time.time()
    function_name = 'community_metrics '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    query_cm = 'INSERT OR IGNORE INTO '+languagecode+'wiki_community_metrics (abs_value, rel_value, metric_name, year_month) VALUES (?,?,?,?);'

    # ACTIVE CONTRIBUTORS
    # active_editors, active_editors_5, active_editors_10, active_editors_50, active_editors_100, active_editors_500, active_editors_1000
    values = [1,5,10,50,100,500,1000]
    parameters = []
    year_months = set()
    for v in values:
        query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND abs_value >= '+str(v)+' GROUP BY year_month ORDER BY year_month'
        for row in cursor.execute(query):
            # print (row)
            value=row[0];
            year_month=row[1]
            if year_month == '': continue
            year_months.add(year_month)
            metric_name='active_editors_'+str(v)
            parameters.append((value, None, metric_name, year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()


    retention_baseline = {}

    # MONTHLY REGISTERED
    parameters = []
    query = 'SELECT count(distinct user_id), year_month_registration FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
    for row in cursor.execute(query):
        value=row[0];
        year_month=row[1]
        if year_month == '': continue
        metric_name='editors_registered'
        try: retention_baseline[year_month] = int(value)
        except: pass
        parameters.append((value, None, metric_name, year_month))


    query = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
    for row in cursor.execute(query):
        value=row[0];
        year_month=row[1]
        if year_month == '': continue
        metric_name='editors_first_edit'
        parameters.append((value, None, metric_name, year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()



    # SURVIVAL / RETENTION
    parameters = []
    queries_retention_dict = {}

    # number of editors who edited at least once 24h after the first edit
    queries_retention_dict['editors_edited_24h_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once 7 days after the first edit
    queries_retention_dict['editors_edited_7d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_7d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once 30 days after the first edit
    queries_retention_dict['editors_edited_30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_30d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once 60 days after the first edit
    queries_retention_dict['editors_edited_60d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_60d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once 365 days after the first edit
    queries_retention_dict['editors_edited_365d_afe'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 365 GROUP BY 2 ORDER BY 1;'

    # number of editors who edited at least once 730 days after the first edit
    queries_retention_dict['editors_edited_730d_afe'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 730 GROUP BY 2 ORDER BY 1;'



    # number of editors who edited their user_page at least once during the first 24h after their first edit
    queries_retention_dict['editors_edited_user_page_d24h_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page at least once during the first 30 days after their first edit
    queries_retention_dict['editors_edited_user_page_d30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_1month" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page at least once
    queries_retention_dict['editors_edited_user_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits_ns2_user" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'



    # number of editors who edited their user_page_talk_page at least once during the first  24h after their first edit
    queries_retention_dict['editors_edited_user_page_talk_page_d24h_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page_talk_page at least once during the first  30 daysafter their first edit
    queries_retention_dict['editors_edited_user_page_talk_page_d30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_1month" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page_talk_page at least once after the first edit
    queries_retention_dict['editors_edited_user_page_talk_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits_ns3_user_talk" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    for metric_name, query in queries_retention_dict.items():
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '': continue
            if metric_name == '': continue
            
            try: base = retention_baseline[year_month]
            except: pass

            if base == None or base == 0: rel_value = 0
            else: rel_value = 100*value/base

            parameters.append((value, rel_value, metric_name, year_month))


    cursor.executemany(query_cm,parameters)
    conn.commit()


    # GINI
    def gini(x):
        # (Warning: This is a concise implementation, but it is O(n**2)
        # in time and memory, where n = len(x).  *Don't* pass in huge
        # samples!)

        # Mean absolute difference
        mad = np.abs(np.subtract.outer(x, x)).mean()
        # Relative mean absolute difference
        rmad = mad/np.mean(x)
        # Gini coefficient
        g = 0.5 * rmad
        return g

    parameters = []
    ym = sorted(list(year_months))
    for year_month in ym:
        query = 'SELECT ce.abs_value FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits" AND year_month="'+year_month+'" AND ch.bot = "editor" AND ce.abs_value > 0;'

        query = 'SELECT abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND year_month="'+year_month+'"'
        values = []
        for row in cursor.execute(query): values.append(row[0]);
        v = gini(values)
        # print ((v, 'gini_monthly', year_month))
        parameters.append((v, None, 'gini_monthly_edits', year_month))


    # query = 'SELECT ce.abs_value FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count" AND ch.bot = "editor" AND ce.abs_value > 0;'
    # values = []
    # for row in cursor.execute(query): values.append(row[0]);
    # v = gini(values)
    # print (v)
    # parameters.append((v, 'gini_edits', year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)





def multilingual_metrics(languagecode):
    print('')

# * wiki_editors
# (user_id integer, user_name text, bot text, user_flag text, primarybinary, primarylang text, primarybinary_ecount, totallangs_ecount, numberlangs integer)

# FUNCTION
# multilingualism: això cal una funció que passi per les diferents bases de dades i creï aquesta



def content_diversity_metrics(languagecode):
    print('')


#    https://stackoverflow.com/questions/28816330/sqlite-insert-if-not-exist-else-increase-integer-value

#    PER NO GUARDAR-HO TOT EN MEMÒRIA. FER L'INSERT DELS CCC EDITATS A CADA ARXIU.


# * wiki_editor_content_metrics

# (user_id integer, user_name text, content_type text, value real)

# FUNCTION
# això cal una funció que corri el mediawiki history amb aquest objectiu havent preseleccionat editors també.

    functionstartTime = time.time()
    function_name = 'content_diversity_metrics '+languagecode
    print (function_name)
    print (languagecode)

    d_paths = get_mediawiki_paths(languagecode)

    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values = line.split(' ')

        parameters = []
        editors_params = []

        iter = 0
        while line != '':
            # iter += 1
            # if iter % 1000000 == 0: print (str(iter/1000000)+' million lines.')

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')

            if len(values)==1: continue

            page_id = values[23]
            page_title = values[25]
            page_namespace = int(values[28])
            edit_count = values[34]

"""

Pel tema Edits A Ccc

Diccionari de diccionaris amb el què va editant cada editor cada mes. Més ràpid pel hash.

dict_editors {}
dict_CCC_per_editor {}


Els Edits mensuals a cada CCC? els anem col·locant a una bbdd, que pot ser la mateixa o una altra.
Després sumar l'acumulat final i ja està.
S'esborren els mensuals... Ja que és massa contingut. 



"""



def export_community_health_metrics_csv(languagecode):

    functionstartTime = time.time()
    function_name = 'export_community_health_metrics_csv '+languagecode
    print (function_name)

    cursor = datetime.datetime.strptime('2001-01','%Y-%m')
    final = datetime.datetime.strptime('2020-12','%Y-%m')

    list_months = []
    list_numerals = []
    months_numeral = {}
    numeral = 0
    while cursor < final:
        numeral += 1
        cursor_text = cursor.strftime('%Y-%m')
        months_numeral[cursor_text] = numeral
        cursor = (cursor + relativedelta.relativedelta(months=1))
        list_numerals.append(numeral)
        list_months.append(cursor_text)

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()
    query = 'SELECT ee.user_id, ee.user_name, ee.value as edits, ec.year_month_registration, ec.lifetime_days, ec.last_edit_timestamp, ec.bot, ec.user_flag FROM '+languagecode+'wiki_editor_metrics ee INNER JOIN '+languagecode+'wiki_editors ec ON ee.user_id = ec.user_id WHERE metric_name = "edit_count";'
    df = pd.read_sql_query(query, conn)
    df = df.set_index('user_id')

    df1 = pd.concat([pd.DataFrame(columns=list_months),df], sort = True)
    df2 = pd.concat([pd.DataFrame(columns=list_numerals),df], sort = True)

    query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month;'

    for row in cursor.execute(query):
        edits=row[0]
        current_year_month = row[1]
        current_year_month_numeral = months_numeral[current_year_month]
        cur_user_id = row[2]
        df1.at[cur_user_id, current_year_month] = edits
        df2.at[cur_user_id, current_year_month_numeral] = edits

    df1 = df1.fillna(0)
    df2 = df2.fillna(0)

    df1.to_csv('/srv/wcdo/datasets/ca_editors_monthly_participation_month.csv')
    df2.to_csv('/srv/wcdo/datasets/ca_editors_monthly_participation_numeral.csv')

    print (df1.head(10))
    print (df2.head(10))

    df3 = pd.concat([pd.DataFrame(columns=list_months),df], sort = True)
    df4 = pd.concat([pd.DataFrame(columns=list_numerals),df], sort = True)

    query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_average_seconds_between_edits" ORDER BY user_name, year_month;'

    for row in cursor.execute(query):
        seconds=row[0]
        current_year_month = row[1]
        current_year_month_numeral = months_numeral[current_year_month]
        cur_user_id = row[2]
        df3.at[cur_user_id, current_year_month] = seconds
        df4.at[cur_user_id, current_year_month_numeral] = seconds

    df3 = df3.fillna(0)
    df4 = df4.fillna(0)


    df3.to_csv('/srv/wcdo/datasets/ca_editors_monthly_idle_time_month.csv')
    df4.to_csv('/srv/wcdo/datasets/ca_editors_monthly_idle_time_numeral.csv')

    print (df3.head(10))
    print (df4.head(10))






#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("community_health_metrics2.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("community_health_metrics.err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    startTime = time.time()

    cycle_year_month = (datetime.date.today() - relativedelta.relativedelta(months=1)).strftime('%Y-%m')

    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();

    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)


    wikilanguagecodes = ['ca']

    
    print ('* Starting the COMMUNITY HEALTH METRICS '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
    main()

    finishTime = time.time()
    print ('* Done with the COMMUNITY HEALTH METRICS completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'community_health_metrics2.out', 'WIKIPEDIA DIVERSITY OBSERVATORY')
