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


# this script collects content related to ethnic groups (and indigenous groups), religious groups or sexual orientation-based groups.


# MAIN
def main():



    create_group_identities_content_db()

    obtain_group_identities()
    # agafar el CCC actual i obtenir group identities (sexual_identities, ethnic_group). posar-ho a diversity_groups i posar-ho directament a cada llengua amb l'estructura de la base de dades, on també tenim per cada llengua si és ccc, etc. que sigui ccc serà necessari per identificar 

    for languagecode in wikilanguagecodes:
        copy_group_identities_articles_groundtruth(languagecode)
        # articles de cada group_identity (sexual orientation, ethnic groups, religious groups) de cada llengua wikipedia_diversity.db passen a ser groundtruth per cada llengua aquí. passar-los d’una bbdd a l’altra.

        label_group_identities_content_for_keywords(languagecode)
        # per les keyword (títol de cada group identity) obtenir articles i posar-ho a les taules de la base de dades.

        label_group_identities_category_crawling(languagecode)
        # search categories using keyword) + category crawling.

        label_potential_group_identities_with_links(languagecode)

        classifier_group_identities(languagecode)
        # random forest. classificació i neteja.


################################################################


def create_group_identities_content_db():
    function_name = 'create_group_identities_content_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + group_identities_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        # qitem, page_title, page_id, group_identity_en, group_identity_lang, sexual_group, ethnic_group, supra_ethnic_group - primary key, qitem, group_identity.

        query = ('CREATE TABLE IF NOT EXISTS '+languagecode+'wiki ('+

        # general
        'qitem text NOT NULL, '+
        'page_id integer NOT NULL, '+
        'page_title text, '+

        'ccc_binary text, '+

        'group_identity_name_en text, '+
        'group_identity_name_lang text, '+
        'sexual_group text, '+
        'ethnic_group text, '+
        'supra_ethnic_group text, '+

        # relevance features

        'PRIMARY KEY (qitem,page_id,languagecode));')

        try:
            cursor.execute(query)
            print ('Created the Group Identities table for language: '+languagecode)
        except:
            print (languagecode+' already has a table.')


    conn.commit()
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def obtain_group_identities(): # això és candidat a que vagi a wikipedia_diversity.py
    function_name = 'obtain_group_identities '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    # posar a una taula de grups de diversity_groups.db
    # posar dades a les taules de cada llengua de group_identities.db

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def copy_group_identities_articles_groundtruth(languagecode):
    function_name = 'label_group_identities_groundtruth '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def label_group_identities_content_for_keywords(languagecode):

    function_name = 'label_group_identities_groundtruth '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def label_group_identities_category_crawling(languagecode):

    # el category crawling es podria fer amb aquest search de categories amb keyword o es podria fer un “up and down” a partir dels articles que ja tenim com a groundtruth.

    # search categories using keyword) + category crawling.

    function_name = 'label_group_identities_category_crawling '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def label_potential_group_identities_with_links(group_identity):

    function_name = 'label_potential_group_identities_with_links '+group_identity
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    # no matter it is female, male, lgtb, romani people, etc.
    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-pagelinks.sql.gz'
#    dumps_path = 'gnwiki-20190720-pagelinks.sql.gz' # read_dump = '/public/dumps/public/wikidatawiki/latest-all.json.gz'
    dump_in = gzip.open(dumps_path, 'r')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    content_selection_page_title = {}
    content_selection_page_id = {}
    query = 'SELECT page_id, page_title FROM '+languagecode+'wiki WHERE group_identities_binary="'+group_identity+'"'
    for row in cursor.execute(query):
        content_selection_page_id[row[0]]=row[1]
        content_selection_page_title[row[1]]=row[0]

    num_of_outlinks = {}
    num_outlinks_group_identities = {}

    num_of_inlinks = {}
    num_inlinks_group_identities = {}

    for page_id in page_titles_page_ids.values():
        num_of_outlinks[page_id]=0
        num_outlinks_group_identities[page_id]=0

        num_of_inlinks[page_id]=0
        num_inlinks_group_identities[page_id]=0

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

                try:
                    num_of_outlinks[pl_from]= num_of_outlinks[pl_from] + 1
#                    print('num_outlinks')
#                    print (num_of_outlinks[pl_from])
                except:
                    pass

                try:
                    group_identities=content_selection_page_id[pl_title_page_id]
                    num_outlinks_group_identities[pl_from] = num_outlinks_group_identities[pl_from] + 1
                    

#                    print (num_outlinks_group_identities[pl_from])
                except:
                    pass

                try:
                    abroad=other_content_selection_page_id[pl_title_page_id]
                    num_outlinks_other_group_identities[pl_from] = num_outlinks_other_group_identities[pl_from] + 1
#                    print('num_outlinks_other_group_identities')
#                    print (num_outlinks_other_group_identities[pl_from])
                except:
                    pass


                try:
                    page_id = page_titles_page_ids[pl_title]
                    num_of_inlinks[page_id] = num_of_inlinks[page_id] + 1
#                    print('num_inlinks')                    
#                    print (num_of_inlinks[page_titles_page_ids[pl_title]])
                except:
                    pass

                try:
                    group_identities=content_selection_page_id[pl_from]
                    num_inlinks_group_identities[pl_title_page_id] = num_inlinks_group_identities[pl_title_page_id] + 1
#                    print('num_inlinks_group_identities')                    
#                    print (num_inlinks_group_identities[page_titles_page_ids[pl_title]])
                except:
                    pass

                try:
                    abroad=other_content_selection_page_id[pl_from]
                    num_inlinks_other_group_identities[pl_title_page_id] = num_inlinks_other_group_identities[pl_title_page_id] + 1
#                    print('num_inlinks_other_group_identities')                    
#                    print (num_inlinks_other_group_identities[page_titles_page_ids[pl_title]])
                except:
                    pass

#    input('')
    print ('Done with the dump.')

    n_outlinks=0
    n_outlinks_group_identities =0

    n_inlinks =0
    n_inlinks_group_identities =0

    parameters = []
    for page_title, page_id in page_titles_page_ids.items():
        qitem = page_titles_qitems[page_title]

        num_outlinks = 0
        num_outlinks_to_group_identities = 0
        num_inlinks = 0
        num_inlinks_from_group_identities = 0

        num_outlinks = num_of_outlinks[page_id]
        num_outlinks_to_group_identities = num_outlinks_group_identities[page_id]
        if num_outlinks!= 0: percent_outlinks_to_group_identities = float(num_outlinks_to_group_identities)/float(num_outlinks)
        else: percent_outlinks_to_group_identities = 0

        num_inlinks = num_of_inlinks[page_id]
        num_inlinks_from_group_identities = num_inlinks_group_identities[page_id]
        if num_inlinks!= 0: percent_inlinks_from_group_identities = float(num_inlinks_from_group_identities)/float(num_inlinks)
        else: percent_inlinks_from_group_identities = 0

        parameters.append((num_outlinks,num_outlinks_to_group_identities,percent_outlinks_to_group_identities,num_inlinks,num_inlinks_from_group_identities,percent_inlinks_from_group_identities,page_id,qitem,page_title))

        if num_outlinks != 0: n_outlinks=n_outlinks+1
        if num_outlinks_to_group_identities != 0: n_outlinks_group_identities =n_outlinks_group_identities+1

        if num_inlinks != 0: n_inlinks =n_inlinks+1
        if num_inlinks_from_group_identities != 0: n_inlinks_group_identities =n_inlinks_group_identities+1

    # print ((n_outlinks,n_outlinks_group_identities ,n_outlinks_other_group_identities , n_inlinks ,n_inlinks_group_identities ,n_inlinks_other_group_identities))
    # print ('(n_outlinks,n_outlinks_group_identities ,n_outlinks_other_group_identities , n_inlinks ,n_inlinks_group_identities ,n_inlinks_other_group_identities)')
    
    query = 'UPDATE '+languagecode+'wiki SET (num_outlinks,num_outlinks_to_group_identities,percent_outlinks_to_group_identities,num_inlinks_from_group_identities,percent_inlinks_from_group_identities)=(?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        
    cursor.executemany(query,parameters)
    conn.commit()
    
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def classifier_group_identities(languagecode):

    function_name = 'classifier_group_identities '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    # random forest. classificació i neteja.

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# culture gap > on és el coneixement relatiu al coneixement local en llengües que no tenen viquipèdia (e.g. indigenous languages)?

# com podem identificar el local content per crear-lo… més enllà del nom del grup humà i el nom de la llengua?
# em calen exemples!

# s’hauria de córrer en viquipèdies però emmagatzemar a-lingüísticament, és a dir, amb els Q de wikidata i llestos. 
# perquè sinó no es poden fer tantes columnes, una per cadascuna…
# fer-ho només amb l’anglesa o colonials?




################################################################

### SAFETY FUNCTIONS ###

def main_with_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('GROUP IDENTITIES CONTENT SELECTION: '+ cycle_year_month, 'ERROR.')

def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()        #          main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/ccc_selection.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('GROUP IDENTITIES CONTENT SELECTION: '+ cycle_year_month, 'ERROR.' + lines); print("Now let's try it again...")
            continue





#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("group_identities_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("group_identities_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':

    script_name = 'group_identities_selection.py'

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
#    check_time_for_script_run(script_name, cycle_year_month)
    startTime = time.time()

   
    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();
    pairs = wikilanguages_utils.load_language_pairs_territory_status()


    # Verify there is a new language
    wikilanguages_utils.extract_check_new_wiki_projects();

    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
    # Only those with a geographical context
    for languagecode in languageswithoutterritory: wikilanguagecodes.remove(languagecode)

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=False)]


    if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)


    wikilanguages_utils.finish_email(startTime,'group_identities_selection.out','GROUP IDENTITIES CONTENT Selection')
