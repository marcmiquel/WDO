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


    # # editors
    # create_editor_diversity_db('create')
    # insert_editors_flags()
    # for languagecode in wikilanguagecodes:
    #     insert_editors_edits(languagecode)
    #     insert_editors_multilingualism(languagecode)


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


def wd_properties():

    # Properties to be used in wp_dump_iterator
    allproperties={}
    # a) strong
    geolocated_property = {'P625':'geolocation'}; allproperties.update(geolocated_property);  # obtain places
    language_strong_properties = {'P37':'official language', 'P364':'original language of work', 'P103':'native language'}; allproperties.update(language_strong_properties); # obtain works, people and places 
    country_properties = {'P17':'country' , 'P27':'country of citizenship', 'P495':'country of origin', 'P1532':'country for sport}'}; allproperties.update(country_properties);  # obtain works, people, organizations and places
    location_properties = {'P276':'location','P131':'located in the administrative territorial entity','P1376':'capital of','P669':'located on street','P2825':'via','P609':'terminus location','P1001':'applies to jurisdiction','P3842':'located in present-day administrative territorial entity','P3018':'located in protected area','P115':'home venue','P485':'archives at','P291':'place of publication','P840':'narrative location','P1444':'destination point','P1071':'location of final assembly','P740':'location of formation','P159':'headquarters location','P2541':'operating area'}; allproperties.update(location_properties); # obtain organizations, places and things
    created_by_properties = {'P19':'place of birth','P112':'founded by','P170':'creator','P84':'architect','P50':'author','P178':'developer','P943':'programmer','P676':'lyrics by','P86':'composer'}; allproperties.update(created_by_properties);  # obtain people and things
    part_of_properties = {'P361':'part of','P1269':'facet of'}; allproperties.update(part_of_properties);  # obtain groups and places
    # b) weak
    language_weak_properties = {'P407':'language of work or name', 'P1412':'language spoken','P2936':'language used'}; allproperties.update(language_weak_properties); # obtain people and groups
    has_part_properties = {'P527':'has part','P150':'contains administrative territorial entity'}; allproperties.update(has_part_properties); # obtain organizations, things and places
    affiliation_properties = {'P463':'member of','P102':'member of political party','P54':'member of sport_and_teams team','P69':'educated at', 'P108':'employer','P39':'position held','P937':'work location','P1027':'conferred by','P166':'award received', 'P118':'league','P611':'religious order','P1416':'affiliation','P551':'residence'}; allproperties.update(affiliation_properties); # obtain people and groups
    industry_properties = {'P452':'industry'}; allproperties.update(industry_properties);

    # other types of diversity
    people_properties = {'P31':'instance of','P21':'sex or gender'}; allproperties.update(people_properties); # obtain people and groups
    sexual_orientation_properties = {'P26':'spouse', 'P91':'sexual orientation', 'P451': 'partner'}; allproperties.update(sexual_orientation_properties);
    religion_properties = {'P140': 'religion', 'P708': 'diocese'}; allproperties.update(religion_properties);
    race_and_ethnia_properties = {'P172': 'ethnic group'}; allproperties.update(race_and_ethnia_properties);
    time_properties = {'P569': 'date of birth', 'P570': 'date of death', 'P580':'start time', 'P582': 'end time', 'P577': 'publication date', 'P571': 'inception', 'P1619': 'date of official opening', 'P576':'dissolved, abolished or demolished', 'P1191': 'date of first performance', 'P813': 'retrieved', 'P1249': 'time of earliest written record', 'P575':'time of discovery or invention'}; allproperties.update(time_properties);

    # all instances of and subclasses of
    instance_of_subclasses_of_properties = {'P31':'instance of','P279':'subclass of'}; allproperties.update(instance_of_subclasses_of_properties); # obtain people and groups

    return allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties, industry_properties, instance_of_subclasses_of_properties,sexual_orientation_properties,religion_properties,race_and_ethnia_properties,time_properties


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





#######################################################################################

### SYNCHRONISATION AND SAFETY FUNCTIONS ###
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



def store_lines_per_second(duration, lines, function_name, file, period): # fer-ho amb les dades de l'última vegada que entra a la condició

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS lines_per_second (linespersecond real, lines integer, duration integer, function_name text, file text, year_month text, PRIMARY KEY (function_name, year_month));")

    linespersecond = lines/duration

    query = "INSERT OR IGNORE INTO lines_per_second (linespersecond, lines, duration, function_name, file, year_month) VALUES (?,?,?,?,?,?);"
    cursor.execute(query,(linespersecond, lines, duration, period, function_name, file, year_month))
    conn.commit()

    print ('in function '+function_name+' reading the dump '+file+', the speed is '+str(linespersecond)+' lines/second, at this period: '+period)



def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WCDO - WIKIPEDIA DIVERSITY OBSERVATORY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


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
            wikilanguages_utils.send_email_toolaccount('WCDO - WIKIPEDIA DIVERSITY OBSERVATORY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
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
