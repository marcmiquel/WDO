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



    languagecode = 'ca'
    create_editor_engagement_db()
    participation_metrics(languagecode)
    time_based_metrics(languagecode)
    community_metrics(languagecode)
    print ('done')
    input('')





    # content_diversity_metrics(languagecode)    
    # multilingual_metrics(languagecode)



################################################################




# FUNCTIONS

def create_editor_engagement_db():

    conn = sqlite3.connect(databases_path + editor_engagement_db); cursor = conn.cursor()

    wikilanguagecodes = ['ca']
    for languagecode in wikilanguagecodes:

        table_name = languagecode+'wiki_community_engagement'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (value real, metric text, year_month text, PRIMARY KEY (metric, year_month))")
        cursor.execute(query)



        table_name = languagecode+'wiki_editor_characteristics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, bot text, user_flag text, primarybinary integer, primarylang text, primarybinary_ecount integer, totallangs_ecount integer, numberlangs integer, registration_date, year_month_registration, first_edit_timestamp text, year_month_first_edit text, survived60d text, last_edit_timestamp text, lifetime_days integer, drop_off_days integer, PRIMARY KEY (user_name, user_id))")
        cursor.execute(query)



        table_name = languagecode+'wiki_editor_engagement'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, value real, metric_name text, year_month text, timestamp text, PRIMARY KEY (user_id, metric_name, year_month))")
        cursor.execute(query)



        table_name = languagecode+'wiki_editor_content_diversity'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, content_type text, value real, PRIMARY KEY (user_name, user_id, content_type))")
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

    # d_paths2 = []
    # for d in d_paths:
    #     if '2016' in d:
    #         d_paths2 = [d]
    # d_paths = d_paths2

    # print (d_paths)
    # input('')

    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    conn = sqlite3.connect(databases_path + editor_engagement_db); cursor = conn.cursor()


    user_id_user_name_dict = {}

    user_id_bot_dict = {}
    user_id_user_groups_dict = {}

    user_id_edit_count = {}
    survived_dict = {}
    survival_measures = []

    editor_user_page_edits = {}
    editor_user_page_talk_page_edits = {}
    editor_namespace0_edits = {}
    editor_namespace1_edits = {}
    editor_namespace2_edits = {}
    editor_namespace3_edits = {}
    editor_namespace4_edits = {}
    editor_namespace5_edits = {}
    editor_namespace6_edits = {}
    editor_namespace7_edits = {}
    editor_namespace8_edits = {}
    editor_namespace9_edits = {}
    editor_namespace10_edits = {}
    editor_namespace11_edits = {}
    editor_namespace12_edits = {}
    editor_namespace13_edits = {}
    editor_namespace14_edits = {}
    editor_namespace15_edits = {}

    editor_monthly_edits = {}

    editor_first_edit_timestamp = {}
    editor_last_edit_timestamp = {}
    editor_registration_date = {}

    last_year_month = 0
    last_date = datetime.datetime.strptime('2001-01-01 01:15:15','%Y-%m-%d %H:%M:%S')

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = 'something'
        line = dump_in.readline()


        while line != '':

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')
            if len(values)==1: continue

            event_entity = values[1]
            event_type = values[2]
            event_timestamp = values[3]
            event_timestamp_dt = datetime.datetime.strptime(event_timestamp[:len(event_timestamp)-2],'%Y-%m-%d %H:%M:%S')

            # print (event_timestamp_dt, line)

            event_user_id = values[5]
            event_user_text = values[7]

            try: user_text = values[38]
            except: pass

            if event_user_text != '':
                user_id_user_name_dict[event_user_id] = event_user_text
            elif user_text != '':
                user_id_user_name_dict[event_user_id] = user_text
            else:
                continue

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

            event_user_revision_count = values[21]

            page_title = values[25]
            page_namespace = values[28]


            # USER PAGE / USER TALK PAGE
            if event_user_text == page_title and page_namespace == '2':
                try:
                    editor_user_page_edits[event_user_id] = editor_user_page_edits[event_user_id]+1
                except:
                    editor_user_page_edits[event_user_id] = 1

            if event_user_text == page_title and page_namespace == '3':
                try:
                    editor_user_page_talk_page_edits[event_user_id] = editor_user_page_talk_page_edits[event_user_id]+1
                except:
                    editor_user_page_talk_page_edits[event_user_id] = 1


            # NAMESPACES
            if page_namespace == '0':
                try: editor_namespace0_edits[event_user_id] = editor_namespace0_edits[event_user_id]+1
                except: editor_namespace0_edits[event_user_id] = 1
            elif page_namespace == '1':
                try: editor_namespace1_edits[event_user_id] = editor_namespace1_edits[event_user_id]+1
                except: editor_namespace1_edits[event_user_id] = 1
            elif page_namespace == '2':
                try: editor_namespace2_edits[event_user_id] = editor_namespace2_edits[event_user_id]+1
                except: editor_namespace2_edits[event_user_id] = 1
            elif page_namespace == '3':
                try: editor_namespace3_edits[event_user_id] = editor_namespace3_edits[event_user_id]+1
                except: editor_namespace3_edits[event_user_id] = 1
            elif page_namespace == '4':
                try: editor_namespace4_edits[event_user_id] = editor_namespace4_edits[event_user_id]+1
                except: editor_namespace4_edits[event_user_id] = 1
            elif page_namespace == '5':
                try: editor_namespace5_edits[event_user_id] = editor_namespace5_edits[event_user_id]+1
                except: editor_namespace5_edits[event_user_id] = 1
            elif page_namespace == '6':
                try: editor_namespace6_edits[event_user_id] = editor_namespace6_edits[event_user_id]+1
                except: editor_namespace6_edits[event_user_id] = 1
            elif page_namespace == '7':
                try: editor_namespace7_edits[event_user_id] = editor_namespace7_edits[event_user_id]+1
                except: editor_namespace7_edits[event_user_id] = 1
            elif page_namespace == '8':
                try: editor_namespace8_edits[event_user_id] = editor_namespace8_edits[event_user_id]+1
                except: editor_namespace8_edits[event_user_id] = 1
            elif page_namespace == '9':
                try: editor_namespace9_edits[event_user_id] = editor_namespace9_edits[event_user_id]+1
                except: editor_namespace9_edits[event_user_id] = 1
            elif page_namespace == '10':
                try: editor_namespace10_edits[event_user_id] = editor_namespace10_edits[event_user_id]+1
                except: editor_namespace10_edits[event_user_id] = 1
            elif page_namespace == '11':
                try: editor_namespace11_edits[event_user_id] = editor_namespace11_edits[event_user_id]+1
                except: editor_namespace11_edits[event_user_id] = 1
            elif page_namespace == '12':
                try: editor_namespace12_edits[event_user_id] = editor_namespace12_edits[event_user_id]+1
                except: editor_namespace12_edits[event_user_id] = 1
            elif page_namespace == '13':
                try: editor_namespace13_edits[event_user_id] = editor_namespace13_edits[event_user_id]+1
                except: editor_namespace13_edits[event_user_id] = 1
            elif page_namespace == '14':
                try: editor_namespace14_edits[event_user_id] = editor_namespace14_edits[event_user_id]+1
                except: editor_namespace14_edits[event_user_id] = 1
            elif page_namespace == '15':
                try: editor_namespace15_edits[event_user_id] = editor_namespace15_edits[event_user_id]+1
                except: editor_namespace15_edits[event_user_id] = 1


            # MONTHLY EDITS
            if event_user_id != '' and event_user_id != 0:
                try:
                    editor_monthly_edits[event_user_id] = editor_monthly_edits[event_user_id]+1
                except:
                    editor_monthly_edits[event_user_id] = 1

            current_year_month = datetime.datetime.strptime(event_timestamp_dt.strftime('%Y-%m'),'%Y-%m')
            if last_year_month != current_year_month and last_year_month != 0:

                lym = last_year_month.strftime('%Y-%m')
                monthly_edits = []
                for event_user_id, edits in editor_monthly_edits.items():
                    monthly_edits.append((event_user_id, user_id_user_name_dict[event_user_id], edits, 'monthly_edits', lym, ''))

                    # if user_id_user_name_dict[event_user_id] == 'Toniher':
                    #     print (event_user_id, user_id_user_name_dict[event_user_id], edits, 'monthly_edits', lym, '')

                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
                cursor.executemany(query,monthly_edits)
                conn.commit()
                # print (editor_namespace2_edits)
                # input('')

                editor_monthly_edits = {}
            last_year_month = current_year_month


            # SURVIVAL MEASURES
            event_user_first_edit_timestamp = values[20]
            editor_first_edit_timestamp[event_user_id] = event_user_first_edit_timestamp
            editor_last_edit_timestamp[event_user_id] = event_timestamp

            event_user_registration_date = values[18]
            try: user_registration_timestamp = values[49]
            except: user_creation_timestamp = ''
            try: user_creation_timestamp = values[50]
            except: user_creation_timestamp = ''
            if event_user_registration_date != '':
                editor_registration_date[event_user_id] = event_user_registration_date
            elif user_registration_timestamp != '':
                editor_registration_date[event_user_id] = user_registration_timestamp
            elif user_creation_timestamp != '':
                editor_registration_date[event_user_id] = user_creation_timestamp


            if event_user_first_edit_timestamp != '' and event_user_id not in survived_dict:
                event_user_first_edit_timestamp_dt = datetime.datetime.strptime(event_user_first_edit_timestamp[:len(event_user_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S')

                # thresholds
                first_edit_timestamp_1day_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=1))
                first_edit_timestamp_7days_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=7))
                first_edit_timestamp_1months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=1))
                first_edit_timestamp_2months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=2))

                try:
                    ec = user_id_edit_count[event_user_id]
                except:
                    ec = 1


                # at 1 day
                if event_timestamp_dt >= first_edit_timestamp_1day_dt and event_timestamp_dt <= first_edit_timestamp_7days_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, 'edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edits:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edits[event_user_id], 'user_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edits:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edits[event_user_id], 'user_page_talk_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 7 days
                if event_timestamp_dt >= first_edit_timestamp_7days_dt and event_timestamp_dt <= first_edit_timestamp_1months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, 'edit_count_7d', first_edit_timestamp_7days_dt.strftime('%Y-%m'),first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 1 month
                if event_timestamp_dt >= first_edit_timestamp_1months_dt and event_timestamp_dt <= first_edit_timestamp_2months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, 'edit_count_30d', first_edit_timestamp_1months_dt.strftime('%Y-%m'),first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edits:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edits[event_user_id], 'user_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edits:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edits[event_user_id], 'user_page_talk_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 2 months
                if event_timestamp_dt >= first_edit_timestamp_2months_dt:

                    survival_measures.append((event_user_id, event_user_text, ec, 'edit_count_60d', first_edit_timestamp_2months_dt.strftime('%Y-%m'),first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')))
                    survived_dict[event_user_id]=''


            if event_user_revision_count != '':
                user_id_edit_count[event_user_id] = event_user_revision_count
            elif event_user_id in user_id_edit_count:
                user_id_edit_count[event_user_id] = int(user_id_edit_count[event_user_id]) + 1
            else:
                user_id_edit_count[event_user_id] = 1


        # SURVIVAL MEASURES INSERT
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
        cursor.executemany(query,survival_measures)
        conn.commit()
        survival_measures = []


    # MONTHLY EDITS INSERT (LAST ROUND)
    lym = last_year_month.strftime('%Y-%m')
    monthly_edits = []
    for event_user_id, edits in editor_monthly_edits.items():
        monthly_edits.append((event_user_id, user_id_user_name_dict[event_user_id], edits, 'monthly_edits', lym, ''))

    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,monthly_edits)
    conn.commit()
    editor_monthly_edits = {}
    monthly_edits = []


    # NAMESPACES EDITS
    parameters = []
    for user_id, user_name in user_id_user_name_dict.items():
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace0_edits[user_id], 'edit_count_ns0_main', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace1_edits[user_id], 'edit_count_ns1_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace2_edits[user_id], 'edit_count_ns2_user', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace3_edits[user_id], 'edit_count_ns3_user_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace4_edits[user_id], 'edit_count_ns4_project', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace5_edits[user_id], 'edit_count_ns5_project_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace6_edits[user_id], 'edit_count_ns6_file', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace7_edits[user_id], 'edit_count_ns7_file_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace8_edits[user_id], 'edit_count_ns8_mediawiki', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace9_edits[user_id], 'edit_count_ns9_mediawiki_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace10_edits[user_id], 'edit_count_ns10_template', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace11_edits[user_id], 'edit_count_ns11_template_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace12_edits[user_id], 'edit_count_ns12_help', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace13_edits[user_id], 'edit_count_ns13_help_talk', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace14_edits[user_id], 'edit_count_ns14_category', lym, ''))
        except: pass
        try: parameters.append((user_id, user_id_user_name_dict[user_id], editor_namespace15_edits[user_id], 'edit_count_ns15_category_talk', lym, ''))
        except: pass

    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,parameters)
    conn.commit()
    parameters = []

    # USER CHARACTERISTICS INSERT
    user_characteristics = []
    for user_id, user_name in user_id_user_name_dict.items():
        
        try:
            user_flag = user_id_user_groups_dict[user_id]
        except:
            user_flag = ''

        try:
            bot = user_id_bot_dict[user_id]
        except:
            bot = 'editor'

        if user_id in survived_dict:
            survived60d = '1'
        else:
            survived60d = '0'

        try:
            registration_date = editor_registration_date[user_id]
        except:
            registration_date = ''


        # THIS IS SOMETHING WE "ASSUME" BECAUSE THERE ARE MANY ACCOUNTS WITHOUT A REGISTRATION DATE.
        if registration_date == '':
            try:
                registration_date = editor_first_edit_timestamp[user_id]
            except:
                registration_date = ''

        if registration_date != '':
            year_month_registration = datetime.datetime.strptime(registration_date[:len(registration_date)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
        else:
            year_month_registration = ''

        try:
            fe = editor_first_edit_timestamp[user_id]
        except:
            fe = ''

        try:
            le = editor_last_edit_timestamp[user_id]
        except:
            le = ''

        if fe != '':
            year_month = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            fe_d = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S')
        else:
            year_month = ''
            fe_d = ''


        if le != '':
            le_d = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S')
            drop_off_days = (event_timestamp_dt - le_d).days
        else:
            le_d = ''
            drop_off_days = ''


        if fe != '' and le != '':
            lifetime_days =  (le_d - fe_d).days
        else:
            lifetime_days = 0
    

        user_characteristics.append((user_id, user_name, bot, user_flag, registration_date, year_month_registration,  fe, year_month, survived60d, le, lifetime_days, drop_off_days))

        # if user_name == 'Bff':
        #     print ((user_id, user_name, bot, user_flag, registration_date, year_month_registration, fe, year_month, survived60d, le, lifetime_days, drop_off_days))
        #     print (event_timestamp_dt)
        #     input('')


    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_characteristics (user_id, user_name, bot, user_flag, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, survived60d, last_edit_timestamp, lifetime_days, drop_off_days) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);'
    cursor.executemany(query,user_characteristics)
    conn.commit()


    # EDIT COUNTS INSERT
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
    edit_counts = []
    for event_user_id, edit_count in user_id_edit_count.items():
        edit_counts.append(((event_user_id, user_id_user_name_dict[event_user_id], edit_count, 'edit_count', lym, '')))
    cursor.executemany(query,edit_counts)
    conn.commit()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)



def time_based_metrics(languagecode):

    functionstartTime = time.time()
    function_name = 'time_based_metrics '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + editor_engagement_db); cursor = conn.cursor()
    # query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_engagement WHERE metric_name = "monthly_edits" AND user_name IN (#"Marcmiquel","Toniher") ORDER BY user_name, year_month'

    query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_engagement WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month'
    # print (query)

    old_user_id = ''
    expected_year_month_dt = ''

    parameters = []
    active_months = 0
    active_months_row = 0
    total_months = 0
    max_active_months_row = 0

    for row in cursor.execute(query):
        # print (row)
        edits=row[0]
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]

        if cur_user_id != old_user_id and old_user_id != '':

            # print ('****')
            # print (old_user_id, old_user_name, active_months, max_active_months_row, total_months)
            parameters.append((old_user_id, old_user_name, active_months, 'active_months', old_year_month,''))
            parameters.append((old_user_id, old_user_name, max_active_months_row, 'max_active_months_row', old_year_month,''))
            parameters.append((old_user_id, old_user_name, total_months, 'total_months', old_year_month,''))

            active_months = 0
            total_months = 0
            active_months_row = 0
            max_active_months_row = 0

        current_year_month_dt = datetime.datetime.strptime(current_year_month,'%Y-%m')

        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:

            while expected_year_month_dt < current_year_month_dt:
                # print (expected_year_month_dt, current_year_month_dt)

                expected_year_month_dt = (expected_year_month_dt + relativedelta.relativedelta(months=1))
                total_months = total_months + 1

            if active_months_row > max_active_months_row:
                max_active_months_row = active_months_row
            active_months_row = 1
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
        # print ('# update: ',old_user_id, old_user_name, active_months, max_active_months_row, total_months)
        # input('')

    # print ('****')
    # print (old_user_id, old_user_name, active_months, max_active_months_row, total_months)

    parameters.append((old_user_id, old_user_name, active_months, 'active_months', old_year_month,''))
    parameters.append((old_user_id, old_user_name, max_active_months_row, 'max_active_months_row', old_year_month,''))
    parameters.append((old_user_id, old_user_name, total_months, 'total_months', old_year_month,''))

    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement (user_id, user_name, value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)



def community_metrics(languagecode):

    functionstartTime = time.time()
    function_name = 'community_metrics '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + editor_engagement_db); cursor = conn.cursor()

    query_cm = 'INSERT OR IGNORE INTO '+languagecode+'wiki_community_engagement (value, metric, year_month) VALUES (?,?,?);'

    # ACTIVE CONTRIBUTORS
    # active_editors, active_editors_5, active_editors_10, active_editors_50, active_editors_100, active_editors_500, active_editors_1000
    values = [1,5,10,50,100,500,1000]
    parameters = []
    year_months = set()
    for v in values:
        query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_engagement WHERE metric_name = "monthly_edits" AND value >= '+str(v)+' GROUP BY year_month ORDER BY year_month'
        for row in cursor.execute(query):
            # print (row)
            value=row[0];
            year_month=row[1]
            if year_month == '': continue
            year_months.add(year_month)
            metric='active_editors_'+str(v)
            parameters.append((value, metric, year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()


    # MONTHLY REGISTERED
    parameters = []
    query = 'SELECT count(distinct user_id), year_month_registration FROM '+languagecode+'wiki_editor_characteristics GROUP BY 2 ORDER BY 2 ASC;'
    for row in cursor.execute(query):
        value=row[0];
        year_month=row[1]
        if year_month == '': continue
        metric='editors_registered'
        parameters.append((value, metric, year_month))

    query = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics GROUP BY 2 ORDER BY 2 ASC;'
    for row in cursor.execute(query):
        value=row[0];
        year_month=row[1]
        if year_month == '': continue
        metric='editors_first_edit'
        parameters.append((value, metric, year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()



    # SURVIVAL / RETENTION
    parameters = []
    queries_retention_dict = {}

    # number of editors who edited at least once between 24h and 7 days after the first edit
    queries_retention_dict['editors_edited_24h_7d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_24h" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once between 7 days and 30 days after the first edit
    queries_retention_dict['editors_edited_7d_30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_7d" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once between 30 days and 60 days after the first edit
    queries_retention_dict['editors_edited_30d_60d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_30d" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once 60 days after the first edit
    queries_retention_dict['editors_edited_60d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_60d" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited at least once after 365 days after the first edit
    queries_retention_dict['editors_edited_365d_afe'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics WHERE lifetime_days >= 365 GROUP BY 2 ORDER BY 1;'

    # number of editors who edited at least once after 730 days after the first edit
    queries_retention_dict['editors_edited_730d_afe'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics WHERE lifetime_days >= 730 GROUP BY 2 ORDER BY 1;'

    # number of editors who edited their user_page at least once between 24h and 7 days after their first edit
    queries_retention_dict['editors_edited_user_page_24h_7d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_24h" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page at least once between 30 days and 60 days after their first edit
    queries_retention_dict['editors_edited_user_page_30d_60d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_1month" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page at least once
    queries_retention_dict['editors_edited_user_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_ns2_user" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page_talk_page at least once between 24h and 7 days after their first edit
    queries_retention_dict['editors_edited_user_page_talk_page_24h_7d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_24h" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page_talk_page at least once between 30 days and 60 days after their first edit
    queries_retention_dict['editors_edited_user_page_talk_page_30d_60d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_1month" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    # number of editors who edited their user_page_talk_page at least once after the first edit
    queries_retention_dict['editors_edited_user_page_talk_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_ns3_user_talk" AND ce.value > 0 GROUP BY 2 ORDER BY 2 ASC;'

    for metric, query in queries_retention_dict.items():
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '': continue
            parameters.append((value, metric, year_month))

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
        query = 'SELECT ce.value FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits" AND year_month="'+year_month+'" AND ch.bot = "editor" AND ce.value > 0;'

        query = 'SELECT value FROM '+languagecode+'wiki_editor_engagement WHERE metric_name = "monthly_edits" AND year_month="'+year_month+'"'
        values = []
        for row in cursor.execute(query): values.append(row[0]);
        v = gini(values)
        # print ((v, 'gini_monthly', year_month))
        parameters.append((v, 'gini_monthly', year_month))


    # query = 'SELECT ce.value FROM '+languagecode+'wiki_editor_characteristics ch INNER JOIN '+languagecode+'wiki_editor_engagement ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count" AND ch.bot = "editor" AND ce.value > 0;'
    # values = []
    # for row in cursor.execute(query): values.append(row[0]);
    # v = gini(values)
    # print (v)
    # parameters.append((v, 'gini', year_month))

    cursor.executemany(query_cm,parameters)
    conn.commit()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)





def multilingual_metrics(languagecode):
    print('')

# * wiki_editor_characteristics
# (user_id integer, user_name text, bot text, user_flag text, primarybinary, primarylang text, primarybinary_ecount, totallangs_ecount, numberlangs integer)

# FUNCTION
# multilingualism: això cal una funció que passi per les diferents bases de dades i creï aquesta



def content_diversity_metrics(languagecode):
    print('')


#    https://stackoverflow.com/questions/28816330/sqlite-insert-if-not-exist-else-increase-integer-value

#    PER NO GUARDAR-HO TOT EN MEMÒRIA. FER L'INSERT DELS CCC EDITATS A CADA ARXIU.


# * wiki_editor_content_diversity

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





#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("editor_engagment"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("editor_engagment"+".err", "w")
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

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    
    print ('* Starting the EDITOR ENGAGEMENT '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
    main()

    finishTime = time.time()
    print ('* Done with the EDITOR ENGAGEMENT completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'editor_engagement.out', 'WIKIPEDIA DIVERSITY OBSERVATORY')
