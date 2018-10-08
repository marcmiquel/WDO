# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
# time
import time
import datetime
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
# data
import pandas as pd
# classifier
from sklearn import svm, linear_model
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("ccc_selection"+''+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass


# MAIN
def main():  


"""
# MAIN
######################## CCC SELECTION SCRIPT ######################## 

    wikilanguages_utils.send_email_toolaccount('WCDO', '(A) -> RAW DATA PHASE # CREATE THE WIKIDATA DB')
    # (A) -> RAW DATA PHASE
    # CREATE THE WIKIDATA DB
    download_latest_wikidata_dump()
    wd_dump_iterator()
    wd_geolocated_update_db()
    wd_labels_update_db()

    wikilanguages_utils.send_email_toolaccount('WCDO', '# CREATE THE PAGEVIEWS DB')
    # CREATE THE PAGEVIEWS DB
    download_latest_pageviews_dump()
    pageviews_dump_iterator()

    wikilanguages_utils.send_email_toolaccount('WCDO', '# CREATE THE WIKIPEDIA CCC DB')
    # * CREATE THE WIKIPEDIA CCC DB
    create_wikipedia_ccc_db()
    insert_page_ids_page_titles_qitems_ccc_db()
    wd_check_and_introduce_wikipedia_missing_qitems()

    wikilanguages_utils.send_email_toolaccount('WCDO', '# INTRODUCE THE ARTICLE FEATURES')
    # * INTRODUCE THE ARTICLE FEATURES
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)
        extend_articles_discussions(languagecode, page_titles_qitems, page_titles_page_ids)
        extend_articles_timestamp(languagecode,page_titles_qitems)
        extend_articles_edits(languagecode, page_titles_qitems)
        extend_articles_editors(languagecode,page_titles_qitems,page_titles_page_ids)
        extend_articles_references(languagecode,page_titles_qitems,page_titles_page_ids)
        extend_articles_bytes(languagecode, page_titles_qitems)

        extend_articles_interwiki(languagecode,page_titles_page_ids)
        extend_articles_qitems_properties(languagecode,page_titles_page_ids)
        extend_articles_featured(languagecode, page_titles_qitems)
        extend_articles_gender(languagecode, page_titles_page_ids,page_titles_qitems)
        extend_articles_pageviews(languagecode,page_titles_qitems,page_titles_page_ids)
    print ('ready for the Wikipedia CCC Data Phase.')

    wikilanguages_utils.send_email_toolaccount('WCDO', '# (B) -> WIKIPEDIA CCC DATA PHASE # INTRODUCE THE ARTICLE FEATURES # Obtaining CCC features for all WP')
    # Obtaining CCC features for all WP
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        # DATA STRATEGIES:
        # B1. RETRIEVE AND SET ARTICLES AS CCC:
        get_ccc_articles_geolocation_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_geolocation_wd');
        get_ccc_articles_geolocated_reverse_geocoding(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_geolocated_reverse_geocoding');
        get_ccc_articles_country_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_country_wd');
        get_ccc_articles_location_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_location_wd');
        get_ccc_articles_language_strong_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_language_strong_wd');
        get_ccc_articles_keywords(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_keywords');
        # * retrieve indirect
        get_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_created_by_properties_wd');
        get_ccc_articles_part_of_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_ccc_articles_part_of_properties_wd');

        # B2. RETRIEVE (POTENTIAL) CCC ARTICLES THAT RELATE TO CCC:
        get_articles_category_crawling(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_category_crawling');
        get_articles_language_weak_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_language_weak_wd');
        get_articles_affiliation_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_affiliation_properties_wd');
        get_articles_has_part_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_has_part_properties_wd');

        get_articles_with_inlinks(languagecode,page_titles_page_ids,page_titles_qitems,'ccc'); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_with_inlinks');
        get_articles_with_outlinks(languagecode,page_titles_page_ids,page_titles_qitems,'ccc'); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_with_outlinks');


    wikilanguages_utils.send_email_toolaccount('WCDO', '# Retrieve features from other languages CCC for all WP')
    # Obtain non CCC features for all WP
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        # DATA STRATEGIES:
        # C1. RETRIEVE ARTICLES THAT RELATE TO OTHER CCC:
        get_other_ccc_country_properties(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_other_ccc_country_properties');
        get_other_ccc_location_properties(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_other_ccc_location_properties');
        get_other_ccc_wikidata_properties(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_other_ccc_wikidata_properties');

        # C2. RETRIEVE ARTICLES THAT (POTENTIAL) RELATE TO OTHER CCC:
#        get_other_ccc_category_crawling(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_other_ccc_category_crawling');

        get_articles_with_inlinks(languagecode,page_titles_page_ids,page_titles_qitems,'no ccc'); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_with_inlinks');
        get_articles_with_outlinks(languagecode,page_titles_page_ids,page_titles_qitems,'no ccc'); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' get_articles_with_outlinks');


    wikilanguages_utils.send_email_toolaccount('WCDO', '# Classifying and creating the definitive CCC')
    # Classifying and creating the definitive CCC
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        groundtruth_reaffirmation(languagecode)
        calculate_articles_ccc_binary_classifier(languagecode,'RandomForest',page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_current.txt', languagecode+' calculate_articles_ccc_binary_classifier');
        calculate_articles_ccc_main_territory(languagecode)
        calculate_articles_ccc_retrieval_strategies(languagecode)

    wikilanguages_utils.send_email_toolaccount('WCDO', '# EXTRACT CCC DATASETS INTO CSV AND CLEAN OLD DATABASES')
    # EXTRACT CCC DATASETS INTO CSV AND CLEAN OLD DATABASES
    extract_ccc_tables_to_files()
    delete_latest_wikidata_dump()
    delete_latest_pageviews_dump()
    backup_ccc_current_db()

"""

################################################################

# Creates a CCC database for a list of languages.
def create_wikipedia_ccc_db():
    functionstartTime = time.time()
    print ('* create_wikipedia_ccc_db')

    # Removes current CCC database (just for code debugging)
    try:
        os.remove(databases_path + "ccc_current.db"); print ('ccc_current.db deleted.');
    except:
        pass

    # Creates the current CCC database.
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor(); print ('ccc_current.db created.');

    # Creates a table for each Wikipedia language edition CCC.
    nonexistingwp = []
    for languagecode in wikilanguagecodes:

        # Checks whether the Wikipedia currently exists.
        try:
            wikilanguages_utils.establish_mysql_connection_read(languagecode)
        except:
            print ('Not created. The '+languages.loc[languagecode]['Wikipedia']+' with code '+languagecode+' is not active (closed or in incubator). Therefore, we do not create a CCC dataset.')
            nonexistingwp.append(languagecode)
            continue

        # Create the table.
        query = ('CREATE TABLE ccc_'+languagecode+'wiki ('+

        # general
        'qitem text, '+
        'page_id integer, '+
        'page_title text, '+
        'date_created integer, '+
        'geocoordinates text, '+ # coordinate1,coordinate2
        'iso3166 text, '+ # code
        'iso31662 text, '+ # code
        'gender text, '+

        # calculations:
        'ccc_binary integer, '+
        'main_territory text, '+ # Q from the main territory with which is associated.
        'num_retrieval_strategies integer, '+ # this is a number

        # set as CCC
        'ccc_geolocated integer,'+ # 1, -1 o null.
        'country_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'location_wd text, '+ # 'P1:QX1:Q; P2:QX2:Q' Q is the main territory
        'language_strong_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'created_by_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'part_of_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'keyword_title text, '+ # 'QX1;QX2'

        # retrieved as potential CCC:
        'category_crawling_territories text, '+ # 'QX1;QX2'
        'category_crawling_level integer, '+ # 'level'
        'language_weak_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'affiliation_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'has_part_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'num_inlinks_from_CCC integer, '+
        'num_outlinks_to_CCC integer, '+
        'percent_inlinks_from_CCC real, '+
        'percent_outlinks_to_CCC real, '+

        # retrieved as potential other CCC (part of other CCC)
            # from wikidata properties
        'other_ccc_country_wd integer, '+
        'other_ccc_location_wd integer, '+
        'other_ccc_language_strong_wd integer, '+
        'other_ccc_created_by_wd integer, '+
        'other_ccc_part_of_wd integer, '+
        'other_ccc_language_weak_wd integer, '+
        'other_ccc_affiliation_wd integer, '+
        'other_ccc_has_part_wd integer, '+
            # from other wikipedia ccc database.
        'other_ccc_keyword_title integer, '+
        'other_ccc_category_crawling_relative_level real, '+
            # from links to/from non-ccc geolocated articles.
        'num_inlinks_from_geolocated_abroad integer, '+
        'num_outlinks_to_geolocated_abroad integer, '+
        'percent_inlinks_from_geolocated_abroad real, '+
        'percent_outlinks_to_geolocated_abroad real, '+

        # characteristics of rellevance
        'num_inlinks integer, '+
        'num_outlinks integer, '+
        'num_bytes integer, '+
        'num_references integer, '+
        'num_edits integer, '+
        'num_editors integer, '+
        'num_discussions integer, '+
        'num_pageviews integer, '+
        'num_wdproperty integer, '+
        'num_interwiki integer, '+
        'featured_article integer, '+

        'PRIMARY KEY (qitem,page_id));')

        try:
            cursor.execute(query)
            print ('Created the Wikipedia CCC table for language: '+languagecode)
        except:
            print (languagecode+' already has a Wikipedia CCC table.')

    # Deletes the WP that don't exist from the list.
    for x in nonexistingwp: wikilanguagecodes.remove(x)
    print ('* create_wikipedia_ccc_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Drop the CCC database.
def backup_ccc_current_db():
    shutil.copyfile(databases_path + "ccc_current.db", databases_path + "ccc_old.db")
    print ('File ccc_current.db copied as ccc_old.db')


# Creates a dataset from the CCC database for a list of languages.
# COMMAND LINE: sqlite3 -header -csv ccc_current.db "SELECT * FROM ccc_cawiki;" > ccc_cawiki.csv
def extract_ccc_tables_to_files():

    for languagecode in wikilanguagecodes:
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

        # These are the folders.
        superfolder = datasets_path+year_month
        languagefolder = superfolder+'/'+languagecode+'wiki/'
        latestfolder = datasets_path+'/latest/'
        if not os.path.exists(languagefolder): os.makedirs(languagefolder)
        if not os.path.exists(latestfolder): os.makedirs(latestfolder)

        # These are the files.
        ccc_filename_archived = languagecode + 'wiki_' + str(datetime.date.today()).replace('-','')+'_ccc.csv' # (e.g. 'cawiki_20180215_ccc.csv')
        ccc_filename_latest = languagecode + 'wiki_latest_ccc.csv.bz2' # (e.g. cawiki_latest_ccc.csv)

        # These are the final paths and files.
        path_latest = latestfolder + ccc_filename_latest
        path_language = languagefolder + ccc_filename_archived
        print ('Extracting the CCC from language '+languagecode+' into the file: '+path_language)
        print ('This is the path for the latest files altogether: '+path_latest)

        # Here we prepare the streams.
        path_language_file = codecs.open(path_language, 'w', 'UTF-8')
        c = csv.writer(open(path_language,'w'), lineterminator = '\n', delimiter='\t')

        # Extract database into a dataset file. Only the rows marked with CCC.
#        cursor.execute("SELECT * FROM ccc_"+languagecode+"wiki WHERE ccc_binary = 1;") # 
        cursor.execute("SELECT * FROM ccc_"+languagecode+"wiki;") # ->>>>>>> canviar * per les columnes. les de rellevància potser no cal.

        i = 0
        c.writerow([d[0] for d in cursor.description])
        for result in cursor:
            i+=1
            c.writerow(result)

        compressionLevel = 9
        source_file = path_language
        destination_file = source_file+'.bz2'

        tarbz2contents = bz2.compress(open(source_file, 'rb').read(), compressionLevel)
        fh = open(destination_file, "wb")
        fh.write(tarbz2contents)
        fh.close()

        print (languagecode+' language CCC has this number of rows: '+str(i))
        # Delete the old 'latest' file and copy the new language file as a latest file.

        try:
            os.remove(path_language);
            os.remove(path_latest); 
        except: pass
        cursor.close()

        shutil.copyfile(destination_file,path_latest)
        print ('Creating the latest_file for: '+languagecode+' with name: '+path_latest)

        # Count the number of files in the language folders and in case they are more than X, we delete them.
#        filenamelist = sorted(os.listdir(languagefolder), key = os.path.getctime)

        # Reference Datasets:
        # http://whgi.wmflabs.org/snapshot_data/
        # https://dumps.wikimedia.org/wikidatawiki/entities/
        # http://ftp.acc.umu.se/mirror/wikimedia.org/dumps/cawiki/20180201/


def extract_ccc_count(languagecode, filename, message):
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row1 = str(row[0]);

    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki;'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row2 = str(row[0]);

    languagename = languages.loc[languagecode]['languagename']

    with open(filename, 'a') as f:
        f.write(languagename+'\t'+message+'\t'+row1+'\t'+row2+'\n')

def download_latest_pageviews_dump():
    functionstartTime = time.time()
    print ('* Downloading the latest pageviews dump.')

    increment = 1
    exists = False
    while exists==False:
        lastMonth = datetime.date.today() - datetime.timedelta(days=increment)
        month_day = lastMonth.strftime("%Y-%m")
        filename = 'pagecounts-'+month_day+'-views-ge-5-totals.bz2'
        url = 'https://dumps.wikimedia.org/other/pagecounts-ez/merged/'+filename
        exists = (requests.head(url).status_code == 200)
        increment = increment + 30

    print (url)
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=10240): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

    os.rename(filename,'latest_pageviews.bz2')
    print ('* download_latest_pageviews_dump Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_pageviews_db():
    try: os.remove(databases_path + "pageviews.db");
    except: pass;
    conn = sqlite3.connect(databases_path + 'pageviews.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS pageviews (languagecode text,  page_title text, num_pageviews int, PRIMARY KEY (languagecode, page_title))")
    return conn


def pageviews_dump_iterator():
    print ('Iterating the pageviews dump.')
    conn = create_pageviews_db(); cursor = conn.cursor()

    read_dump = 'latest_pageviews.bz2'
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
            print (last_language)
            print (len(pageviews_dict))
            pageviews = []
            for key in pageviews_dict:
                try:
#                    if last_language=='ca':
#                        print ((key[0], key[1], pageviews_dict[(key[0],key[1])]))
                    pageviews.append((key[0], key[1], pageviews_dict[(key[0],key[1])]))
                except:
                    pass

            query = "INSERT INTO pageviews (languagecode, page_title, num_pageviews) VALUES (?,?,?);"
            cursor.executemany(query,pageviews)
            conn.commit()
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

    print ('Pageviews have been introduced into the database.')


def delete_latest_pageviews_dump():
    os.remove(databases_path + "latest_pageviews.bz2")

def delete_pageviews_db():
    os.remove(databases_path + "pageviews.db")


def download_latest_wikidata_dump():
    functionstartTime = time.time()
    print ('* Downloading the latest Wikidata dump.')
    url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz" # download the dump: https://dumps.wikimedia.org/wikidatawiki/entities/20180212/
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(databases_path + local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=10240): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    print ('* download_latest_wikidata_dump Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_wikidata_db():
    try: os.remove(databases_path + "wikidata.db");
    except: pass;
    conn = sqlite3.connect(databases_path + 'wikidata.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS sitelinks (qitem text, langcode text, page_title text, PRIMARY KEY (qitem, langcode));")

    cursor.execute("CREATE TABLE IF NOT EXISTS geolocated_property (qitem text, property text, coordinates text, admin1 text, iso3166 text, PRIMARY KEY (qitem));")
    cursor.execute("CREATE TABLE IF NOT EXISTS language_strong_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS country_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS location_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS created_by_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS part_of_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS language_weak_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS has_part_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS affiliation_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    cursor.execute("CREATE TABLE IF NOT EXISTS people_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (qitem text, properties integer, sitelinks integer, PRIMARY KEY (qitem));")

    cursor.execute("CREATE TABLE IF NOT EXISTS labels (qitem text, langcode text, label text, PRIMARY KEY (qitem, langcode));")

    print ('Created the Wikidata sqlite3 file and tables.')
    return conn


def wd_dump_iterator():
    functionstartTime = time.time()
    print ('* Starting the Wikidata iterator.')

    # Set List of WikiData properties we will take into account
    allproperties={}
    # a) strong
    geolocated_property = {'P625':'geolocation'}; allproperties.update(geolocated_property);  # obtain places
    language_strong_properties = {'P37':'official language', 'P364':'original language of work', 'P103':'native language'}; allproperties.update(language_strong_properties); # obtain works, people and places 
    country_properties = {'P17':'country' , 'P27':'country of citizenship', 'P495':'country of origin', 'P1532':'country for sport}'}; allproperties.update(country_properties);  # obtain works, people, organizations and places
    location_properties = {'P276':'location','P131':'located in the administrative territorial entity','P1376':'capital of','P669':'located on street','P2825':'via','P609':'terminus location','P1001':'applies to jurisdiction','P3842':'located in present-day administrative territorial entity','P3018':'located in protected area','P115':'home venue','P485':'archives at','P291':'place of publication','P840':'narrative location','P1444':'destination point','P1071':'location of final assembly','P740':'location of formation','P159':'headquarters location','P2541':'operating area'}; allproperties.update(location_properties); # obtain organizations, places and things
    created_by_properties = {'P19':'place of birth','P112':'founded by','P170':'creator','P84':'architect','P50':'author','P178':'developer','P943':'programmer','P676':'lyrics by','P86':'composer'}; allproperties.update(created_by_properties);  # obtain people and things
    part_of_properties = {'P361':'part of'}; allproperties.update(part_of_properties);  # obtain groups and places
    # b) weak
    language_weak_properties = {'P407':'language of work or name', 'P1412':'language spoken','P2936':'language used'}; allproperties.update(language_weak_properties); # obtain people and groups
    has_part_properties = {'P527':'has part','P150':'contains administrative territorial entity'}; allproperties.update(has_part_properties); # obtain organizations, things and places
    affiliation_properties = {'P463':'member of','P102':'member of political party','P54':'member of sports team','P69':'educated at', 'P108':'employer','P39':'position held','P937':'work location','P1027':'conferred by','P166':'award received', 'P118':'league','P611':'religious order','P1416':'affiliation','P551':'residence'}; allproperties.update(affiliation_properties); # obtain people and groups
    people_properties = {'P31':'instance of','P21':'sex or gender'}; allproperties.update(people_properties); # obtain people and groups

    # Locate the dump
    read_dump = 'latest-all.json.gz' # read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
    dump_in = gzip.open(databases_path + read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    conn = create_wikidata_db(); cursor = conn.cursor()
    conn.commit()
    wd_all_qitems(cursor); conn.commit() # getting all the qitems

    print ('Iterating the dump.')
    while line != '':
        iter += 1
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]

        try:
            entity = json.loads(line)
            qitem = entity['id']
            if not wd_check_qitem(cursor,qitem)=='1': continue
            if not qitem.startswith('Q'): continue
        except:
            print ('JSON error.')

        wd_sitelinks_insert_db(cursor, qitem, entity['sitelinks'])
        wd_entity_claims_insert_db(cursor, entity, allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties)

        if iter % 500000 == 0:
#            print (iter)
            print (100*iter/48138856)
            print ('current time: ' + str(time.time() - startTime))
#            break


    cursor.execute("DROP TABLE qitems;")
    conn.commit()
    conn.close()
    print ('DONE with the JSON.')
    print ('It has this number of lines: '+str(iter))
    print ('* wd_dump_iterator Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def wd_all_qitems(cursor):
    cursor.execute("CREATE TABLE qitems (qitem text PRIMARY KEY);")
    mysql_con_read = mdb.connect(host='wikidatawiki.analytics.db.svc.eqiad.wmflabs',db='wikidatawiki_p', read_default_file='./my.cnf', cursorclass=mdb_cursors.SSCursor); mysql_cur_read = mysql_con_read.cursor()
    query = 'SELECT page_title FROM page WHERE page_namespace=0;'
    mysql_cur_read.execute(query)
    while True:
        row = mysql_cur_read.fetchone()
        if row == None: break
        qitem = row[0].decode('utf-8')
        query = "INSERT INTO qitems (qitem) VALUES ('"+qitem+"');"
        cursor.execute(query)
    print ('All Qitems obtained and in wikidata.db.')


def wd_check_qitem(cursor,qitem):
    query='SELECT 1 FROM qitems WHERE qitem = "'+qitem+'"'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row = str(row[0]);
    return row


def wd_entity_claims_insert_db(cursor, entity, allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties):
#   print (entity['claims'])
    qitem = entity['id']
    claims = entity['claims']
#    print ([qitem,len(claims),len(entity['sitelinks'])])
    # meta info
    cursor.execute("INSERT OR IGNORE INTO metadata (qitem, properties, sitelinks) VALUES (?,?,?)",[qitem,len(claims),len(entity['sitelinks'])-1])

    # properties
    for claim in claims:
        wdproperty = claim
        if wdproperty not in allproperties: continue
        claimlist = claims[claim]
        for snak in claimlist:
            mainsnak = snak['mainsnak']

            if wdproperty in geolocated_property:
                try:
                    coordinates = str(mainsnak['datavalue']['value']['latitude'])+','+str(mainsnak['datavalue']['value']['longitude'])
                except:
                    continue
                values = [qitem,wdproperty,coordinates]
                cursor.execute("INSERT OR IGNORE INTO geolocated_property (qitem, property, coordinates) VALUES (?,?,?)",values)
                continue

            # the rest of properties
            try:
                qitem2 = 'Q{}'.format(mainsnak['datavalue']['value']['numeric-id'])
            except:
                continue

            if wdproperty in language_strong_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('language properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO language_strong_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in language_weak_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('language properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO language_weak_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in country_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('country properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO country_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in location_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('location properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO location_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in has_part_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('has part properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO has_part_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in affiliation_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('has part properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO affiliation_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in created_by_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('created by properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO created_by_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in part_of_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('part of properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO part_of_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in people_properties:
                if wdproperty == 'P31' and qitem2 != 'Q5': continue # if not human, continue
                values = [qitem,wdproperty,qitem2]
#                print ('people properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO people_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue


def wd_sitelinks_insert_db(cursor, qitem, wd_sitelinks):
#    print (wd_sitelinks)
    for code, title in wd_sitelinks.items():

        # in case of extension to wikibooks or other sister projects (e.g. cawikitionary) it would be necessary to introduce another 'if code in wikilanguaagescodeswikitionary'.
        if code in wikilanguagecodeswiki:
            values=[qitem,code,title['title']]
#            print (values)
            try: cursor.execute("INSERT INTO sitelinks (qitem, langcode, page_title) VALUES (?,?,?)",values)
            except: print ('This Q is already in: '+qitem)


def wd_check_and_introduce_wikipedia_missing_qitems(languagecode):

    langcodes = []
    if languagecode != '': langcodes.append(languagecode)
    else: langcodes = wikilanguagecodes

    for languagecode in langcodes:
        print ('\n* '+languagecode)

        page_titles=list()
        page_ids=list()
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
        query = 'SELECT page_title, page_id FROM ccc_'+languagecode+'wiki;'
        for row in cursor.execute(query):
            page_title=str(row[0])
            page_id = row[1]
            page_titles.append(str(row[0]))
            page_ids.append(row[1])

        parameters = []
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = 'SELECT page_title, page_id FROM page WHERE page_namespace=0 AND page_is_redirect=0;'
        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()
        articles_namespace_zero = len(rows)
        for row in rows: 
            page_title = row[0].decode('utf-8')
            page_id = row[1]
            if page_id not in page_ids: 
                print (page_title, page_id)
                qitem=page_titles_qitems[page_title]
                parameters.append((page_title,page_id,''))

        print ('* '+languagecode+' language edition has '+str(articles_namespace_zero)+' articles non redirect with namespace 0.')
        print ('* '+languagecode+' language edition has '+str(len(parameters))+' articles that have no qitem in Wikidata and therefore are not in the CCC database.\n')

        conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
        query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki (page_title,page_id,qitem) VALUES (?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()
        print ('page ids for this language are in: '+languagecode+'\n')


# Runs the reverse geocoder and update the database. It needs 5000m.
def wd_geolocated_update_db():
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    print ('* Updating the Wikidata database with the geolocation coordinates.')
    geolocated_qitems = {}
    query = "SELECT qitem, coordinates FROM geolocated_property;"   # (qitem text, property text, coordinates text, admin1 text, iso3166 text)
    print ('Querying the qitems with coordinates.')
    for row in cursor.execute(query):
        qitem=row[0]
        coordinates=str(row[1]).split(',')
        geolocated_qitems[qitem]=(coordinates[0],coordinates[1])

    print ('Retrieved all the geolocated qitems from the database.')
    results = rg.search(list(geolocated_qitems.values()))
    print ('Obtained all the data from the reverse geolocation process. They are '+str(len(results))+' items.')

    qitems = list(geolocated_qitems.keys())
    geolocated_qitemsdata = []
    qitems_selected = {}

    for x in range(0,len(results)-1):
        qitem=qitems[x]
        admin1=results[x]['admin1']
        iso3166=results[x]['cc']

        if qitem not in qitems_selected:
            qitems_selected[qitem]='in'
            geolocated_qitemsdata.append((admin1,iso3166,qitem))

    print ('Selected and now updating the db.')
    cursor.executemany("UPDATE geolocated_property SET admin1 = ?, iso3166 = ? WHERE qitem = ?", geolocated_qitemsdata)
    conn.commit()
    print ('WD geolocated table updated with '+str(len(qitems_selected))+' selected.')
    print ('* wd_geolocated_update_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# It updates the database with the labels for those items whose language does not have title in sitelink.
def wd_labels_update_db():
    # Locate the dump
    read_dump = 'latest-all.json.gz' # read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
    dump_in = gzip.open(databases_path + read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    print ('Iterating the dump to update the labels.')
    labels = 0
    while line != '':
        iter += 1
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]

        try:
            entity = json.loads(line)
            qitem = entity['id']
            if not qitem.startswith('Q'): continue
        except:
            print ('JSON error.')
        wd_labels = entity['labels']

        query = 'SELECT langcode FROM sitelinks WHERE qitem ="'+qitem+'"'
        langs = set()
        for row in cursor.execute(query): langs.add(str(row[0]))
        if len(langs)==0: continue

#        print (qitem)
#        print ('these are the languages sitelinks:')
#        print (langs)
#        print ('these are the languages labels:')
#        print (wd_labels)   
        for code, title in wd_labels.items(): # bucle de llengües
            code = code + 'wiki'
            if code in wikilanguagecodeswiki and code not in langs:
                values=[qitem,code,title['value']]
                try: 
                    cursor.execute("INSERT INTO labels (qitem, langcode, label) VALUES (?,?,?)",values)
                    labels+=1
 #                   print ('This is new:')
 #                   print (values)
                except: 
                    pass
#                    print ('This Q and language are already in: ')
 #                  print (values)

        if iter % 500000 == 0:
            conn.commit()

#            print (iter)
            print (100*iter/48138856)
            print ('current number of stored labels: '+str(labels))
            print ('current time: ' + str(time.time() - startTime))
#            break
    conn.commit()
    conn.close()
    print ('DONE with the JSON for the labels iteration.')


# Checks all the databses and updates the database.
def insert_page_ids_page_titles_qitems_ccc_db():
    functionstartTime = time.time()

    print ('* insert_page_ids_page_titles_qitems_ccc_db')
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode)

        page_titles_qitems={}
        query = 'SELECT page_title, qitem FROM sitelinks WHERE langcode = "'+languagecode+'wiki";'
        for row in cursor.execute(query):
            page_title=row[0].replace(' ','_')
            page_titles_qitems[page_title]=row[1]
        print (len(page_titles_qitems))
        print ('qitems obtained.')
        # IMPORTANT: not all articles (page_namespace=0 and page_is_redirect=0) from every Wikipedia have a Qitem related, as sometimes the link is not created. This is relevant for small Wikipedias.

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = 'SELECT page_title, page_id FROM page WHERE page_namespace=0 AND page_is_redirect=0;'
        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()
        parameters=[]
        print (len(rows))
        for row in rows:
            page_title=str(row[0].decode('utf-8'))
#            print (page_title)
            try: 
                qitem=page_titles_qitems[page_title]
                parameters.append((page_title,row[1],qitem))
            except: pass
        print (len(parameters))
        print('in')

        query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki (page_title,page_id,qitem) VALUES (?,?,?);';
        cursor2.executemany(query,parameters)
        conn2.commit()
        print ('page ids for this language are in: '+languagecode+'\n')

    print ('all articles introduced into the ccc databases.')
    print ('* insert_page_ids_page_titles_qitems_ccc_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




# Extends the articles table with the first timestamp.
def extend_articles_timestamp(languagecode, page_titles_qitems):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_timestamp')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    page_ids_timestamps = []

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute("SELECT MIN(rev_timestamp),rev_page,page_title FROM revision INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by rev_page")
        rows = mysql_cur_read.fetchall()
        for row in rows: 
            try: page_ids_timestamps.append((str(row[0].decode('utf-8')),row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
            except: continue
    except:
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = cursor.fetchone()[0]
        print (maxval)

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 1000000
        while (maxval > 0):
            page_ids_timestamps = []
            val2 = maxval
            maxval = maxval - increment
            if maxval < 0: maxval = 0
            val1 = maxval
            interval = 'AND rev_page BETWEEN '+str(val1)+' AND '+str(val2)
            query = 'SELECT MIN(rev_timestamp),rev_page,page_title FROM revision INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 '+interval+' GROUP BY rev_page'
            print (query)
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                try: page_ids_timestamps.append((str(row[0].decode('utf-8')),row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
                except: continue
            print (len(page_ids_timestamps))
            print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')
            print (str(len(page_ids_timestamps)/int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds()))+' rows per second.')
            last_period_time = time.time()

    query = 'UPDATE ccc_'+languagecode+'wiki SET date_created=? WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,page_ids_timestamps)
    conn.commit()

    print ('CCC timestamp updated.')
    print ('* extend_articles_timestamp Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



# Extends the articles table with the number of editors per article.
def extend_articles_editors(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_editors')

    page_titles_editors = []
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute('SELECT COUNT(DISTINCT rev_user_text),page_id,page_title FROM revision INNER JOIN page ON rev_page = page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_id')
        rows = mysql_cur_read.fetchall()
        for row in rows: 
            try: page_titles_editors.append((row[0],row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
            except: continue

        query = 'UPDATE ccc_'+languagecode+'wiki SET num_editors=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,page_titles_editors)
        conn.commit()


    except:       
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = cursor.fetchone()[0]
        print (maxval)

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 100000
        range_values = 0
        while (range_values < maxval):
            page_titles_editors = []
            val1 = range_values
            range_values = range_values + increment
            if range_values > maxval: range_values = maxval
            val2 = range_values
            interval = 'AND rev_page BETWEEN '+str(val1)+' AND '+str(val2)

            query = 'SELECT COUNT(DISTINCT rev_user_text),rev_page,page_title FROM revision INNER JOIN page ON rev_page = page_id WHERE page_namespace=0 AND page_is_redirect=0 '+interval+' GROUP BY rev_page'
            print (query)
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                try: page_titles_editors.append((row[0],row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
                except: continue
            print (len(page_titles_editors))
            print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')
            print (str(len(page_titles_editors)/int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds()))+' rows per second.')
            last_period_time = time.time()

            query = 'UPDATE ccc_'+languagecode+'wiki SET num_editors=? WHERE page_id = ? AND qitem = ?;'
            cursor.executemany(query,page_titles_editors)
            conn.commit()

    print ('Editors updated.')
    print ('* extend_articles_editors Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of discussions per article.
def extend_articles_discussions(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_discussions')

    updated = []
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute("SELECT page_title, COUNT(*) FROM revision r, page p WHERE r.rev_page=p.page_id AND p.page_namespace=1 GROUP by page_title;")
        rows = mysql_cur_read.fetchall()
        for row in rows:
            page_title=str(row[0].decode('utf-8'))
            try: updated.append((row[1],page_titles_page_ids[str(row[0].decode('utf-8'))],page_titles_qitems[page_title]))
            except: continue

        query = 'UPDATE ccc_'+languagecode+'wiki SET num_discussions=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,updated)
        conn.commit()

    except:
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = cursor.fetchone()[0]
        print (maxval)

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 10000000
        while (maxval > 0):
            updated = []
            val2 = maxval
            maxval = maxval - increment
            if maxval < 0: maxval = 0
            val1 = maxval
            interval = 'AND page_id BETWEEN '+str(val1)+' AND '+str(val2)
            query = "SELECT COUNT(*), page_title FROM revision r, page p WHERE r.rev_page=p.page_id AND p.page_namespace=1 "+interval+" GROUP by page_title;"
            print (query)
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                try: updated.append((row[0],page_titles_page_ids[str(row[1].decode('utf-8'))],page_titles_qitems[str(row[1].decode('utf-8'))]))
                except: continue
            print (len(updated))
            print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')
            print (str(len(updated)/int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds()))+' rows per second.')
            last_period_time = time.time()

            query = 'UPDATE ccc_'+languagecode+'wiki SET num_discussions=? WHERE page_id = ? AND qitem = ?;'
            cursor.executemany(query,updated)
            conn.commit()

    print ('Discussions updated.')
    print ('* extend_articles_discussions Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of edits per article.
def extend_articles_edits(languagecode, page_titles_qitems):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_edits')

    page_ids_edits = []
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = "SELECT page_title, page_id, COUNT(*) FROM revision INNER JOIN page on rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by rev_page;"
        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()
        for row in rows:
            page_title=str(row[0].decode('utf-8'))
            page_id=row[1]
            count=row[2]
            try: page_ids_edits.append((count,page_id,page_titles_qitems[page_title]))
            except: pass

        query = 'UPDATE ccc_'+languagecode+'wiki SET num_edits=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,page_ids_edits)
        conn.commit()

    except:
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = int(cursor.fetchone()[0])
        print (maxval)

        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 1000000
        range_values = 0
        while (range_values < maxval):
            page_ids_edits = []
            val1 = range_values
            range_values = range_values + increment
            if range_values > maxval: range_values = maxval
            val2 = range_values

            interval = 'AND rev_page BETWEEN '+str(val1)+' AND '+str(val2)
            query = "SELECT COUNT(*), page_id, page_title FROM revision INNER JOIN page on rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 "+interval+" GROUP by rev_page;"
            print (query)
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                try: page_ids_edits.append((row[0],row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
                except: continue
            print (len(page_ids_edits))
            print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')
            print (str(len(page_ids_edits)/int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds()))+' rows per second.')
            last_period_time = time.time()

            query = 'UPDATE ccc_'+languagecode+'wiki SET num_edits=? WHERE page_id = ? AND qitem = ?;'
            cursor.executemany(query,page_ids_edits)
            conn.commit()

    print ('Edits updated.')
    print ('* extend_articles_edits Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of references per article.
def extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_references')

    page_ids_references = []
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}
    print (len(page_titles_page_ids))

    try:
        i = 0
        print ('Trying to run the entire query.')
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute("SELECT el_from, COUNT(*) FROM externallinks INNER JOIN page ON el_from=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by el_from;")
        rows = mysql_cur_read.fetchall()
        for row in rows:
            try: 
                page_id=row[0]
                count=row[1]
                qitem = page_titles_qitems[page_ids_page_titles[page_id]]
                parameters = (count,page_id,qitem)
                query = 'UPDATE ccc_'+languagecode+'wiki SET num_references=? WHERE page_id = ? AND qitem = ?;'
                cursor.execute(query,parameters)
                i += 1
            except: 
                pass

        conn.commit()
        print (str(i)+' references updated.')
    except:
        try:
            print ('Trying to run the query with batches.')
            cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
            maxval = int(cursor.fetchone()[0])
            print (maxval)

            mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
            increment = 1000000
            range_values = 0
            while (range_values < maxval):
                val1 = range_values
                range_values = range_values + increment
                if range_values > maxval: range_values = maxval
                val2 = range_values

                interval = 'AND el_from BETWEEN '+str(val1)+' AND '+str(val2)
                query = "SELECT COUNT(*), el_from FROM externallinks INNER JOIN page ON el_from=page_id WHERE page_namespace=0 AND page_is_redirect=0 "+interval+" GROUP by el_from;"

                print (query)
                mysql_cur_read.execute(query)
                rows = mysql_cur_read.fetchall()
                for row in rows:
                    try:
                        page_title=page_ids_page_titles[row[1]]
                        page_ids_references.append((row[0],row[1],page_title))
                    except: continue
                print (len(page_ids_references))
                print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')
                print (str(len(page_ids_references)/int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds()))+' rows per second.')
                last_period_time = time.time()

                query = 'UPDATE ccc_'+languagecode+'wiki SET num_references=? WHERE page_id = ? AND qitem = ?;'
                cursor.executemany(query,page_ids_references)
                conn.commit()
            print ('References updated.')
        except:
            print ('MySQL permissions error.')

    print ('* extend_articles_references Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of bytes per article.
def extend_articles_bytes(languagecode, page_titles_qitems):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_bytes')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute("SELECT page_title, page_id, page_len FROM page WHERE page_namespace=0 AND page_is_redirect=0;")
    rows = mysql_cur_read.fetchall()
    for row in rows:
        page_title=str(row[0].decode('utf-8'))
        page_id=row[1]
        count=row[2]
        try:
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_bytes=? WHERE page_id = ? AND qitem = ?;'
            cursor.execute(query,(count,page_id,page_titles_qitems[page_title]))
        except:
            pass
    conn.commit()

    print ('Bytes updated.')
    print ('* extend_articles_bytes Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of interwiki links.
def extend_articles_interwiki(languagecode, page_titles_page_ids):
    functionstartTime = time.time()
    print ('* extend_articles_interwiki')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

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
    query = 'UPDATE ccc_'+languagecode+'wiki SET num_interwiki = ? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print ('CCC interwiki updated.')
    print ('* extend_articles_interwiki Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of qitem properties.
def extend_articles_qitems_properties(languagecode, page_titles_page_ids):
    functionstartTime = time.time()
    print ('* extend_articles_qitems_properties')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

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
    query = 'UPDATE ccc_'+languagecode+'wiki SET num_wdproperty=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print ('CCC qitems properties updated.')
    print ('* extend_articles_qitems_properties Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the gender property from wikidata.
def extend_articles_gender(languagecode, page_titles_page_ids, page_titles_qitems):
    functionstartTime = time.time()
    print ('* extend_articles_gender property from wikidata')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

    updated = []
#    query = "SELECT people_properties.qitem, people_properties.qitem2, sitelinks.page_title FROM people_properties INNER JOIN sitelinks ON sitelinks.qitem = people_properties.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki' AND people_properties.property = 'P21'"
    query = "SELECT people_properties.qitem, people_properties.qitem2 FROM people_properties WHERE people_properties.property = 'P21' AND people_properties.qitem IN (SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON sitelinks.qitem = people_properties.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki' AND people_properties.qitem2 = 'Q5');"

    for row in cursor.execute(query):
        try:
            qitem=row[0]
            gender=row[1]
            page_id=page_titles_page_ids[qitems_page_titles[qitem]]
            updated.append((gender,page_id,qitem))
#            print (page_id,qitem,iw_count)
        except:
            pass
    query = 'UPDATE ccc_'+languagecode+'wiki SET gender=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print (len(updated))
    print ('CCC gender properties updated.')
    print ('* extend_articles_gender Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of pageviews.
def extend_articles_pageviews(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    print ('* extend_articles_pageviews')

    conn = sqlite3.connect(databases_path + 'pageviews.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    query = "SELECT page_title, num_pageviews FROM pageviews WHERE languagecode = '"+languagecode+"';"
    updated = []
    for row in cursor.execute(query):
        try:
            page_title=row[0]
            pageviews=row[1]
            page_id = page_titles_page_ids[page_title]
            qitem = page_titles_qitems[page_title]
#            print (page_title,pageviews,page_id,qitem)
            updated.append((pageviews,page_id,qitem))
        except: 
            pass

    query = 'UPDATE ccc_'+languagecode+'wiki SET num_pageviews=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()
    print (str(len(updated))+' articles with pageviews updated.')
    print ('* extend_articles_pageviews Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the featured articles.
def extend_articles_featured(languagecode, page_titles_qitems):
    functionstartTime = time.time()
    print ('* extend_articles_featured')
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

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
#    print ('These are the featured articles categories in the different languages.')
#    print (featuredarticleslanguages)

    if languagecode+'wiki' in featuredarticleslanguages: featuredtitle=featuredarticleslanguages[languagecode+'wiki']
    else: print ('no featured articles for language: '+languagecode); return

    featuredarticles=[]
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute('SELECT page_title, page_id FROM categorylinks INNER JOIN page on cl_from=page_id WHERE CONVERT(cl_to USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s', (featuredtitle,)) # Extreure
    rows = mysql_cur_read.fetchall()
    for row in rows: 
        page_title=str(row[0].decode('utf-8'))
        page_id=row[1]

        #print (page_title)
        query = 'UPDATE ccc_'+languagecode+'wiki SET featured_article=1 WHERE page_id = ? AND qitem = ?;'
        try:
#            print ((page_id,page_title,page_titles_qitems[page_title]))
            cursor.execute(query,(page_id,page_titles_qitems[page_title]))
            featuredarticles.append(page_title)
            conn.commit();
        except:
            pass
#            print ('This article does not exist: '+page_title)

    print ('We obtained '+str(len(featuredarticles))+' featured articles from this language: '+languagecode)
    print ('* extend_articles_featured Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def delete_wikidata_db():
    os.remove(databases_path + "wikidata.db")

def delete_latest_wikidata_dump():
    os.remove(databases_path + "latest-all.json.gz")


# CCC STRATEGIES
#################

# Obtain the articles with coordinates gelocated in the territories associated to that language by reverse geocoding. Label them as CCC.
def get_ccc_articles_geolocated_reverse_geocoding(languagecode,page_titles_qitems):
    functionstartTime = time.time()
    print ('\n* Getting geolocated articles with reverse geocoding for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    # CREATING THE DICTIONARIES TO OBTAIN TERRITORY QITEMS
    # with a territory name in Native you get a Qitem
    # with a territory name in English you get a Qitem
    # with a ISO3166 code you get a Qitem
    # with a subdivision name you get a ISO 31662 (without the ISO3166 part)
    ISO31662codes={}
    territorynamesNative={}
    territorynames={}
    ISO3166codes={} 
    try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
    except: qitems = [territories.loc[languagecode]['QitemTerritory']]
    print (qitems)
    for qitem in qitems:
#        print (qitem)
        territorynameNative = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territorynameNative']
#        print (territorynameNative)
        territoryname = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territoryname']
#        print (territoryname)
        territorynamesNative[territorynameNative]=qitem
        territorynames[territoryname]=qitem
        if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='no':
            ISO3166 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166']
            ISO3166codes[ISO3166]=qitem
        if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='yes':
            ISO31662 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO31662']
            ISO31662codes[ISO31662]=qitem
    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    input_subdivisions_filename = databases_path + 'world_subdivisions.csv'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = {}
    for line in input_subdivisions_file: 
        info = line.strip('\n').split(',');
        subdivisions[info[0]] = info[1]

    geolocated_pageids_titles = {}
    geolocated_pageids_coords = {}
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    # It gets all the articles with coordinates from a language. It stores them into an adhoc database.
    query = 'SELECT page_title, gt_page_id, gt_lat, gt_lon FROM '+languagecode+'wiki_p.page INNER JOIN '+languagecode+'wiki_p.geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_title' # if there is an article with more than one geolocation it takes the first!
    mysql_cur_read.execute(query)
    result = mysql_cur_read.fetchall()
    for row in result:
        page_title = str(row[0].decode('utf-8'))
        page_id = row[1]
        lat = row[2]
        lon = row[3]
        geolocated_pageids_coords[page_id]=(lat,lon)
        geolocated_pageids_titles[page_id]=page_title

    # It calculates the reverse geocoding data and updates the database.
    if len(geolocated_pageids_coords)==0: 
        print ('No article geolocation in this language.'); return;
    results = rg.search(list(geolocated_pageids_coords.values()))
    print (str(len(results))+' coordinates reversed.')

    ccc_geolocated_items = []
    page_ids = list(geolocated_pageids_coords.keys())
    for x in range(0,len(results)-1):
        page_id=page_ids[x]
        page_title = geolocated_pageids_titles[page_id]
        admin1=results[x]['admin1']
        iso3166=results[x]['cc']
        try: iso31662=iso3166+'-'+subdivisions[admin1]
        except: pass
        lat = str(geolocated_pageids_coords[page_id][0])
        lon = str(geolocated_pageids_coords[page_id][1])
#        print (page_title,page_id,admin1,iso3166,iso31662)

        qitem=''
        # try both country code and admin1, at the same time, just in case there is desambiguation ('Punjab' in India (IN) and in Pakistan (PK) for 'pa' language)
        try: 
            qitem=territories[(territories.ISO3166 == iso3166) & (territories.territoryname == admin1)].loc[languagecode]['QitemTerritory']
#            print (qitem); print ('name and country')
        except: 
            pass
        try:
            # try to get qitem from country code.        
            if qitem == '' and iso3166 in ISO3166codes: 
                qitem = ISO3166codes[iso3166]
#                print (qitem); print ('country')
            # try to get qitem from admin1: in territorynames, territorynamesNative and subdivisions.
            else:
                if admin1 in territorynames: 
                    qitem=territorynames[admin1]
#                    print (qitem); print ('territorynames in English.')
                else: 
                    if admin1 in territorynamesNative: 
                        qitem=territorynamesNative[admin1]
#                        print (qitem); print ('territorynames in Native.')
                    else: 
                        if admin1 in subdivisions: 
                            iso31662=iso3166+'-'+subdivisions[admin1]
                            if (iso31662 in ISO31662codes): 
                                qitem=ISO31662codes[iso31662]
#                                print (qitem); print ('subdivisions')
        except:
            pass

        coordinates = lat+','+lon
        if qitem!='': 
            ccc_geolocated = 1
            ccc_binary = 1
#            print ((page_id,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
        else: 
            ccc_geolocated = -1
            ccc_binary = 0
#            print ('### NO!\n')

        try: 
            qitem_specific=page_titles_qitems[page_title]            
        except: 
            qitem_specific=None

        ccc_geolocated_items.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))

        if x%20000 == 0: print (x)

    if len(ccc_geolocated_items)==0: print ('No geolocated articles in Wikidata for this language edition.');return
    # It inserts the right articles into the corresponding CCC database.
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,main_territory) = (?,?,?,?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
    cursor.executemany(query,ccc_geolocated_items)
    conn.commit()

    print ('All geolocated articles with geolocation and validated through reverse geocoding are in. They are: '+str(len(ccc_geolocated_items))+'.')
    print ('They account for a '+str(100*len(ccc_geolocated_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('Geolocated articles from Wikipedia language '+(languagecode)+' coordinates have been inserted.');
    print ('* get_ccc_articles_geolocated_reverse_geocoding Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles whose WikiData items have properties linked to territories and language names (groundtruth). Label them as CCC.
# There is margin for optimization: articles could be updated more regularly to the database, so in every loop it is not necessary to go through all the items.
def get_ccc_articles_geolocation_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    print ('\n* Getting articles with Wikidata from items with "geolocation" property and reverse geocoding for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    # CREATING THE DICTIONARIES TO OBTAIN TERRITORY QITEMS
    # with a territory name in Native you get a Qitem
    # with a territory name in English you get a Qitem
    # with a ISO3166 code you get a Qitem
    # with a subdivision name you get a ISO 31662 (without the ISO3166 part)
    ISO31662codes={}
    territorynamesNative={}
    territorynames={}
    ISO3166codes={}
    allISO3166=[]

    try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
    except: qitems = [territories.loc[languagecode]['QitemTerritory']]
    for qitem in qitems:
        territorynameNative = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territorynameNative']
        territoryname = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territoryname']

        territorynamesNative[territorynameNative]=qitem
        territorynames[territoryname]=qitem
        if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='no':
            ISO3166 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166']
            ISO3166codes[ISO3166]=qitem
        if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='yes':
            ISO31662 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO31662']
            ISO31662codes[ISO31662]=qitem
        allISO3166.append(territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166'])
    allISO3166 = list(set(allISO3166))
    print (allISO3166)

    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    input_subdivisions_filename = databases_path + 'world_subdivisions.csv'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = {}
    for line in input_subdivisions_file: 
        info = line.strip('\n').split(',');
        subdivisions[info[0]] = info[1]

    # Get the articles, evaluate them and insert the good ones.   
    ccc_geolocated_items = []
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
#    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON sitelinks.qitem=geolocated_property.qitem WHERE sitelinks.langcode="'+languagecode+'wiki";'

    page_asstring = ','.join( ['?'] * len(allISO3166) )
    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON sitelinks.qitem=geolocated_property.qitem WHERE sitelinks.langcode="'+languagecode+'wiki" AND geolocated_property.iso3166 IN (%s) ORDER BY geolocated_property.iso3166, geolocated_property.admin1;' % page_asstring

    x = 0
    for row in cursor.execute(query,allISO3166):
#        print (row)
#        input('')
        qitem_specific=str(row[0])
        coordinates=str(row[1])
        admin1=str(row[2]) # it's the Territory Name according to: https://github.com/thampiman/reverse-geocoder
        iso3166=str(row[3])
        iso31662=''; 
        try: iso31662=iso3166+'-'+subdivisions[admin1]
        except: pass
        page_title=str(row[4]).replace(' ','_')
        try: page_id = page_titles_page_ids[page_title]
        except: continue

#        print (page_title,page_id,qitem_specific,admin1,iso3166,coordinates)

        qitem=''
        # try both country code and admin1, at the same time, just in case there is desambiguation ('Punjab' in India (IN) and in Pakistan (PK) for 'pa' language)
        try: qitem=territories[(territories.ISO3166 == iso3166) & (territories.territoryname == admin1)].loc[languagecode]['QitemTerritory']
#            print (qitem); print ('name and country')
        except: pass
        try:
            # try to get qitem from country code.        
            if qitem == '' and iso3166 in ISO3166codes: 
                qitem = ISO3166codes[iso3166]
#                print (qitem); print ('country')
            # try to get qitem from admin1: in territorynames, territorynamesNative and subdivisions.
            else:
                if admin1 in territorynames:
                    qitem=territorynames[admin1]
#                    print (qitem); print ('territorynames in English.')
                else: 
                    if admin1 in territorynamesNative: 
                        qitem=territorynamesNative[admin1]
#                        print (qitem); print ('territorynames in Native.')
                    else: 
                        if admin1 in subdivisions: 
                            iso31662=iso3166+'-'+subdivisions[admin1]
                            if (iso31662 in ISO31662codes): 
                                qitem=ISO31662codes[iso31662]
#                                print (qitem); print ('subdivisions')
        except:
            pass


        if qitem!='':
            ccc_geolocated = 1
            ccc_binary = 1
#            print ((page_id,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
        else: 
            ccc_geolocated = -1
            ccc_binary = 0
#            print ('### NO!\n')
        try:
#            print ((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))
            ccc_geolocated_items.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))
        except:
            pass

        if x%20000 == 0: print (x)
        x = x + 1


    if len(ccc_geolocated_items)==0: print ('No geolocated articles in Wikidata for this language edition.');return
    # Insert to the corresponding CCC database.
    print ('Inserting/Updating articles into the database.')
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,main_territory) = (?,?,?,?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
    cursor2.executemany(query,ccc_geolocated_items)
    conn2.commit()
    print ('All geolocated articles from Wikidata validated through reverse geocoding are in. They are: '+str(len(ccc_geolocated_items))+'.')
    print ('They account for a '+str(100*len(ccc_geolocated_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('Geolocated articles from Wikidata for language '+(languagecode)+' have been inserted.');
    print ('* get_ccc_articles_geolocation_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a country property related to a territory from the list of territories from the language. Label them as CCC.
def get_ccc_articles_country_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db');cursor = conn.cursor()
    print ('\n* Getting articles with Wikidata from items with "country" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

#    print (territories.head())
#    input('')

    # country qitems
    try: countries = territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'].tolist()
    except: 
        try: countries = list(); countries.append(territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'])
        except:
            print ('there are no entire countries where the '+languagecode+' is official')
            return
    print ((countries))

    # get articles
    qitem_properties = {}
    qitem_page_titles = {}
    ccc_country_items = []
    query = 'SELECT country_properties.qitem, country_properties.property, country_properties.qitem2, sitelinks.page_title FROM country_properties INNER JOIN sitelinks ON sitelinks.qitem = country_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')
        if qitem2 not in countries: continue

#        print ((qitem, wdproperty, country_properties[wdproperty], page_title))
        value = wdproperty+':'+qitem2
        if qitem not in qitem_properties: qitem_properties[qitem]=value
        else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value
        qitem_page_titles[qitem]=page_title

    # Get the tuple ready to insert.
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_country_items.append((1,values,qitem_page_titles[qitem],qitem,page_id))
        except: 
            continue

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,country_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_country_items)
    conn2.commit()
    print (str(len(ccc_country_items))+' country related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(100*len(ccc_country_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_country_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a location property that is iteratively associated to the list of territories associated to the language. Label them as CCC.
def get_ccc_articles_location_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

#    page_qitems_titles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM sitelinks WHERE langcode = "'+languagecode+'wiki";'): page_qitems_titles[row[1]]=row[0].replace(' ','_')
    
    qitems_territories=[]
    if languagecode not in languageswithoutterritory:
        try: qitems_territories=territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems_territories=[];qitems_territories.append(territories.loc[languagecode]['QitemTerritory'])

    print ('\n* Getting articles with Wikidata from items with "location" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')
    if len(qitems_territories)==0: print ('Oops. There are no territories for this language.'); return;
    print (qitems_territories)

    selected_qitems = {}
    for QitemTerritory in qitems_territories:
        QitemTerritoryname = territories.loc[territories['QitemTerritory'] == QitemTerritory].loc[languagecode]['territoryname']
        print ('We start with this territory: '+QitemTerritoryname+' '+QitemTerritory)
#        if QitemTerritory == 'Q5705': continue

        target_territories = []
        target_territories.append(QitemTerritory)

        counter = 1
        updated = 0
        round = 1
        number_items_territory = 0
        while counter != 0: # when there is no level below as there is no new items. there are usually 6 levels.
            print ('# Round: '+str(round))
            round_territories = []
            counter = 0

            query = 'SELECT location_properties.qitem, location_properties.property, location_properties.qitem2, sitelinks.page_title FROM location_properties INNER JOIN sitelinks ON sitelinks.qitem = location_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
            for row in cursor.execute(query):
                qitem = row[0]
                wdproperty = row[1]
                qitem2 = row[2]
                page_title = row[3].replace(' ','_')

                if qitem2 in target_territories:
#                    print ((round,qitem,page_title,wdproperty,location_properties[wdproperty],page_qitems_titles[qitem2],page_qitems_titles[QitemTerritory]))
                    if qitem not in selected_qitems:
                        selected_qitems[qitem]=[page_title,wdproperty,qitem2,QitemTerritory]
                        counter = counter + 1
                        round_territories.append(qitem)
                    else:
                        selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2,QitemTerritory]
                        updated = updated + 1

            target_territories = round_territories
            number_items_territory = number_items_territory + len(round_territories)

            print ('In this iteration we added this number of NEW items: '+(str(counter)))
            print ('In this iteration we updated this number of items: '+(str(updated)))
            print ('The current number of selected items for this territory is: '+str(number_items_territory))
            round = round + 1

        print ('- The number of items related to the territory '+QitemTerritoryname+' is: '+str(number_items_territory))
        print ('- The TOTAL number of selected items at this point is: '+str(len(selected_qitems))+'\n')
#        break
#    for keys,values in selected_qitems.items(): print (keys,values)

    # Get the tuple ready to insert.
    ccc_located_items = []
    for qitem, values in selected_qitems.items():
        page_title=values[0]
        try: page_id=page_titles_page_ids[page_title]
        except: continue
        value = ''
        for x in range(0,int((len(values)-1)/3)):
            if value != '': value = value + ';'
            value = value + values[x*3+1]+':'+values[x*3+2]+':'+values[x*3+3]
        ccc_located_items.append((1,value,page_title,qitem,page_id))


    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,location_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_located_items)
    conn2.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_located_items)/num_art
    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_location_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a "strong" language property that is associated the language. Label them as CCC.
def get_ccc_articles_language_strong_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    print ('\n* Getting articles with Wikidata from items with "language" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    # language qitems
    qitemresult = languages.loc[languagecode]['Qitem']
    if ';' in qitemresult: qitemresult = qitemresult.split(';')
    else: qitemresult = [qitemresult];

    # get articles
    qitem_properties = {}
    qitem_page_titles = {}
    ccc_language_items = []
    query = 'SELECT language_strong_properties.qitem, language_strong_properties.property, language_strong_properties.qitem2, sitelinks.page_title FROM language_strong_properties INNER JOIN sitelinks ON sitelinks.qitem = language_strong_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')
        if qitem2 not in qitemresult: continue

#        print ((qitem, wdproperty, language_properties[wdproperty], page_title))
        # Put the items into a dictionary
        value = wdproperty+':'+qitem2
        if qitem not in qitem_properties: qitem_properties[qitem]=value
        else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value
        qitem_page_titles[qitem]=page_title
#    print ("Per P2936, hi ha aquest nombre: ")
#    print (len(qitem_page_titles))
#    input('ja hem acabat')
#    return

    # Get the tuple ready to insert.
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_language_items.append((1,values,qitem_page_titles[qitem],qitem,page_id))
        except: 
            continue

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,language_strong_wd,page_title) = (?,?,?) WHERE qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_language_items)/num_art

    print (str(len(ccc_language_items))+' language related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_articles_wd_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a creation property that is related to the items already retrieved as CCC. Label them as CCC.
def get_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()

    print ('\n* Getting articles with Wikidata from items with "created by" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'): ccc_articles[row[1]]=row[0].replace(' ','_')

#    potential_ccc_articles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
#        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    selected_qitems={}
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()
    query = 'SELECT created_by_properties.qitem, created_by_properties.property, created_by_properties.qitem2, sitelinks.page_title FROM created_by_properties INNER JOIN sitelinks ON sitelinks.qitem = created_by_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 in ccc_articles:
#            if qitem not in potential_ccc_articles: 
#                print ((qitem,page_title, wdproperty, created_by_properties[wdproperty],ccc_articles[qitem2], already_in))
            if qitem not in selected_qitems:
                selected_qitems[qitem]=[page_title,wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]

    ccc_created_by_items = []
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
        ccc_created_by_items.append((1,value,page_title,qitem,page_id))

#    input('')
    # INSERT INTO CCC DATABASE
    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_created_by_items)/num_art

    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,created_by_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_created_by_items)
    conn.commit()
    print (str(len(ccc_created_by_items))+' items/articles created by CCC articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_created_by_properties_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles which are part of items already retrieved as CCC. Label them as CCC.
def get_ccc_articles_part_of_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    print ('\n* Getting articles with Wikidata from items with "part of" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    part_of_properties = {'P361':'part of'} 

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

#    potential_ccc_articles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
#        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    selected_qitems={}
    query = 'SELECT part_of_properties.qitem, part_of_properties.property, part_of_properties.qitem2, sitelinks.page_title FROM part_of_properties INNER JOIN sitelinks ON sitelinks.qitem = part_of_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if (qitem2 in ccc_articles):
#            if (qitem in ccc_articles):
#                continue
#                print ((qitem, page_title, wdproperty, part_of_properties[wdproperty],ccc_articles[qitem2],'ALREADY IN!'))
            
#            elif (qitem in potential_ccc_articles):
#                continue
#                print ((qitem, page_title, wdproperty, part_of_properties[wdproperty],ccc_articles[qitem2],'ALMOST: POTENTIAL. NOW IN.'))

#            else:
#                print ((qitem, page_title, wdproperty, part_of_properties[wdproperty],ccc_articles[qitem2],'NEW NEW NEW NEW NEW!'))

            if qitem not in selected_qitems:
                selected_qitems[qitem]=[page_title,wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]
#    for keys,values in selected_qitems.items(): print (keys,values)

    ccc_part_of_items = []
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
        ccc_part_of_items.append((1,value,page_title,qitem,page_id))

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_part_of_items)/num_art

    # INSERT INTO CCC DATABASE
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,part_of_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_part_of_items)
    conn.commit()
    print (str(len(ccc_part_of_items))+' items/articles created by CCC articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_part_of_properties_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a keyword in title. This is considered potential CCC.
def get_ccc_articles_keywords(languagecode,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n* Getting keywords related articles for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

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
    print (keywordsdictionary)

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    selectedarticles = {}
    for item in keywordsdictionary:
#        print (item)
        for keyword in keywordsdictionary[item]:
            if keyword == '': continue
            keyword = keyword.replace(' ','%')
            query = 'SELECT page_id, page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND CONVERT(page_title USING utf8mb4) COLLATE utf8mb4_general_ci LIKE '+'"%'+keyword+'%"'+' ORDER BY page_id';
           # with this query, we obtain all the combinations for the keyword (no accents). română is romana, Romanați,...

#            query = 'SELECT page_id, page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND  page_title LIKE '+'"%'+keyword+'%"'+' ORDER BY page_id';
            # this is the clean version.
 
            mysql_cur_read.execute(query)
            print ("Number of articles found through this word: " + keyword.replace('%',' '));
            result = mysql_cur_read.fetchall()
            if len(result) == 0: print (str(len(result))+' ALERT!');
            else: print (len(result))

            for row in result:
                page_id = str(row[0])
                page_title = str(row[1].decode('utf-8'))
                if page_id not in selectedarticles:
#                    print (keyword+ '\t'+ page_title + '\t' + page_id + ' dins' + '\n')
                    selectedarticles[page_id] = [page_title, item]
                else:
                    if item not in selectedarticles[page_id]: 
                        selectedarticles[page_id].append(item)
                    #if len(selectedarticles[page_id])>2:print (selectedarticles[page_id])

    print ('The total number of articles by this language dictionary is: ')
    print (len(selectedarticles))


    insertedarticles = []
    for page_id, value in selectedarticles.items():
        page_title = str(value.pop(0))
        keyword_title = str(';'.join(value))
        try: 
            qitem=page_titles_qitems[page_title]
        except: 
            qitem=None
        insertedarticles.append((1,keyword_title,page_title,page_id,qitem))

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,keyword_title,page_title) = (?,?,?) WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,insertedarticles)
    conn.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(selectedarticles)/num_art

    print ('Articles with keywords on titles in Wikipedia language '+(languagecode)+' have been inserted.');
    print ('The number of inserted articles account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_keywords Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



# Obtain the articles with coordinates gelocated in the territories associated to that language. This is considered potential CCC - there are some interferences in this database. This function is unnecessary, since geolocated_reverse_geocoding uses the same data and cleans it, and geolocation_wd covers the data with that from wikidata.
def get_articles_geolocated_geo_tags(languagecode,page_titles_qitems):
    functionstartTime = time.time()
    print ('\n* Getting geolocated articles with geo tags for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    # Getting the ISO code lists for the language.
    ISOcodeslists = {}
    qitems = []
    if languagecode not in languageswithoutterritory:

        try: qitems=territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems=[];qitems.append(territories.loc[languagecode]['QitemTerritory'])

        for qitem in qitems:
            code = ''
            if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='no':
                ISO3166 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166']
                code=ISO3166
            if territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']=='yes':
                ISO31662 = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO31662']
                code=ISO31662
            ISOcodeslists[qitem]=code
    print (ISOcodeslists)

    # Obtaining the articles for the language.
    count = 0
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    for qitem in qitems:
        ccc_geolocated_items=[]
        query = 'SELECT page_title, gt_page_id, gt_lat, gt_lon FROM page INNER JOIN geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 AND '
        code = ISOcodeslists[qitem]
        #print (code)
        if code == '': continue
        if '-' in code:
            parts = code.split('-')
            query = query + '(gt_country="'+parts[0]+'" AND gt_region="'+parts[1]+'")'
        else:
            query = query + '(gt_country="'+str(code)+'")'

#        print (query)
        mysql_cur_read.execute(query)
        result = mysql_cur_read.fetchall()
#        print (len(result))
        for row in result:
            page_title = str(row[0].decode('utf-8'))
            page_id = row[1]
            gt_lat = str(row[2]); gt_lon = str(row[3])
            coordinates = gt_lat+','+gt_lon
            try: qitem_specific=page_titles_qitems[page_title]
            except: qitem_specific = None
            ccc_geolocated_items.append((coordinates,qitem,page_title,page_id,qitem_specific))
            count = count + 1

        # Insert to the corresponding CCC database.
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
        query = 'UPDATE ccc_'+languagecode+'wiki SET (geocoordinates,main_territory) = (?,?) WHERE page_title = ? AND page_id = ? AND qitem=?;'
        cursor.executemany(query,ccc_geolocated_items)
        conn.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*count/num_art

    print ('Geolocated articles from Wikipedia language '+(languagecode)+' geotags have been inserted.');
    print ('They are: '+str(count)+'.')
    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_articles_geolocated_geo_tags Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles contained in the Wikipedia categories with a keyword in title (recursively). This is considered potential CCC.
def get_articles_category_crawling(languagecode,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n * Getting category crawling related articles for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

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

    # STARTING THE CRAWLING
    selectedcategories = dict()
    selectedarticles = dict()
    selectedarticles_level = dict()
    string = languagecode+' Starting selection of raw CCC.'; 
    print ('With language '+ languagecode +" and Keywords: ")
    print (keywordsdictionary)
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()


    for item in keywordsdictionary:
        wordlist = keywordsdictionary[item]
#        wordlist = keywordsdictionary['Q1008']
        print ('\n'+(item))
        print (wordlist)

        # CATEGORIES BY WORDS AT LEVEL 0
        curcategories = dict()
        level = 0
        for keyword in wordlist:
            if keyword == '': continue
            keyword = keyword.replace(' ','%')
            query = 'SELECT cat_id, cat_title FROM category INNER JOIN page ON cat_title = page_title WHERE page_namespace = 14 AND CONVERT(cat_title USING utf8mb4) COLLATE utf8mb4_general_ci LIKE '+'"%'+keyword+'%"'+' ORDER BY cat_id;'
#            print (query)
#            query = 'SELECT cat_id, cat_title FROM category INNER JOIN page ON cat_title = page_title WHERE page_namespace = 14 AND cat_title LIKE '+'"%'+keyword+'%"'+' ORDER BY cat_id;'
            # this is the clean version that obtains the categories without the accents.

            mysql_cur_read.execute(query)
            # print ("The number of categories for this keyword " + keyword + "is:");
            result = 1
            while result:
                result = mysql_cur_read.fetchall()
#                print (len(result))
                for row in result:
                    cat_id = str(row[0])
                    cat_title = str(row[1].decode('utf-8'))
                    curcategories[cat_title] = cat_id

        print("For the item "+ str(item) +" the number of categories at the initial level is: " + str(len(curcategories)))
        selectedcategories.update(curcategories)
        if len(curcategories) == 0: continue

        # LEVELS CRAWLING
        newcategories = dict()
        newarticles = dict()
        keywordscategories = dict()
        level = 1
        num_levels = 25
        if languagecode=='en': num_levels = 10

        n = 3000
        while (level <= num_levels): # Here we choose the number of levels we prefer.

            curcategories_list = list(curcategories.keys()) # This is used to create BATCHES
            n = len(curcategories_list)
            if n == 0: break
            iterations = int(len(curcategories_list)/n)
#            print (len(curcategories_list))

            for x in range(0,iterations+1):
                cur_iteration = curcategories_list[:n]
                del curcategories_list[:n]
#                print('.')
#                print (len(cur_iteration))

                # ARTICLES FROM CATEGORIES
                updated = 0
                if (len(cur_iteration)!=0):
                    page_asstring = ','.join( ['%s'] * len(cur_iteration) )
                    query = 'SELECT page_id, page_title FROM page INNER JOIN categorylinks ON page_id = cl_from WHERE page_namespace=0 AND page_is_redirect=0 AND cl_to IN (%s)' % page_asstring
    #                print (query)
                    mysql_cur_read.execute(query, cur_iteration)
                    result = mysql_cur_read.fetchall()
                    for row in result:
                        page_id = row[0]
                        page_title = str(row[1].decode('utf-8'))
                        if page_id not in selectedarticles:
                            selectedarticles_level[page_id]=level
                            selectedarticles[page_id] = [page_title, item]
                            newarticles[page_id] = [page_title, item]
                        else:
                            if level < selectedarticles_level[page_id]: selectedarticles_level[page_id]=level
                            if item not in selectedarticles[page_id]:
                                selectedarticles[page_id].append(item)
                                updated = updated + 1

                # CATEGORIES FROM CATEGORY
                if (len(cur_iteration)!=0):
                    query = 'SELECT cat_id, cat_title FROM page INNER JOIN categorylinks ON page_id=cl_from INNER JOIN category ON page_title=cat_title WHERE page_namespace=14 AND cl_to IN (%s)' % page_asstring
    #                print (query)
                    mysql_cur_read.execute(query, cur_iteration)
                    result = mysql_cur_read.fetchall()
                    # rows_found += len(result)
                    for row in result: #--> PROBLEMES DE ENCODING
                        cat_id = str(row[0])
                        cat_title = str(row[1].decode('utf-8'))  
                        if cat_title not in newcategories:
                            newcategories[cat_title] = cat_id  # this introduces those that are not in for the next iteration.
#            input('')
            # CLEANING THE CATEGORIES LEVEL - SETTING THEM READY FOR THE NEXT ITERATION.
            for cat_title in keywordscategories: newcategories.pop(cat_title, None)
            curcategories.clear()

            # get the categories ready for the new iteration
            curcategories.update(newcategories)
            selectedcategories.update(newcategories)
            keywordscategories.update(newcategories)
            print("The number of categories gathered at level " + str(level) + " is: " + str(len(newcategories))+ ", while the number of new articles is: " + str(len(newarticles))+". The updated: "+str(updated)+ ".\tThe total number of selected categories is now: "+str(len(selectedcategories))+", while the articles are: "+str(len(selectedarticles))+".");
            newcategories.clear()
            newarticles.clear()
#            if len(newarticles)==0 and updated == 0: break
            level = level + 1

    parameters = []
    for page_id, elements in selectedarticles.items():
        page_title = elements.pop(0)
        categorycrawling = ";".join(elements)
        try: qitem=page_titles_qitems[page_title]
        except: qitem=None
        parameters.append((categorycrawling,selectedarticles_level[page_id],page_title,page_id,qitem))

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (category_crawling_territories,category_crawling_level) = (?,?) WHERE page_title = ? AND page_id = ? AND qitem=?;'
    cursor.executemany(query,parameters)
    conn.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(selectedarticles)/num_art

    # ALL ARTICLES
    wp_number_articles = wikipedialanguage_numberarticles[languagecode]
    string = "The total number of category crawling selected articles is: " + str(len(selectedarticles)); print (string)
    string = "The total number of selected categories is: " + str(len(selectedcategories)); print (string)
    string = "The total number of articles in this Wikipedia is: "+str(wp_number_articles)+"\n"; print (string)
    string = "The percentage of category crawling related articles in this Wikipedia is: "+str(percent)+"\n"; print (string)

    print ('Articles obtained through the category graph crawling in Wikipedia language '+(languagecode)+' have been inserted.');
    print ('* get_articles_category_crawling Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles table with the number of inlinks.
def get_articles_with_inlinks(languagecode,page_titles_page_ids,page_titles_qitems,group):
    functionstartTime = time.time()
    print ("\n* Updating the number of inlinks and the number of inlinks from "+group+" for language: "+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    article_selection = {}
    if group == 'ccc':
        query = 'SELECT page_id, page_title qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            article_selection[row[0]]=row[1]
        print ('- Articles in CCC: '+str(len(article_selection)))
    else:
        query = 'SELECT page_id, page_title qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=0;'
        for row in cursor.execute(query):
            article_selection[row[0]]=row[1]
        print ('- Articles with other language CCC relationships: '+str(len(article_selection)))

    try:
        # get the ccc and potential ccc articles
        print ('Attempt with a MySQL.')

        page_titles_inlinks_group = []
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        page_asstring = ','.join( ['%s'] * len( article_selection ) )
        query = 'SELECT count(pl_from), pl_title FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_from IN (%s) GROUP BY pl_title' % page_asstring
        mysql_cur_read.execute(query,list(article_selection.keys()))
        rows = mysql_cur_read.fetchall()
#        print ('query run.')
        for row in rows:
            pl_title = str(row[1].decode('utf-8'))
            try: 
#                print ((row[0],all_ccc_articles[pl_title],all_ccc_articles_qitems[pl_title]))
                page_titles_inlinks_group.append((row[0],page_titles_page_ids[pl_title],page_titles_qitems[pl_title], pl_title))
            except: pass

        if group == 'ccc':
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks_from_CCC=? WHERE page_id = ? AND qitem = ? AND page_title=?;'
        else:
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks_from_geolocated_abroad=? WHERE page_id = ? AND qitem = ? AND page_title=?;'

        cursor.executemany(query,page_titles_inlinks_group)
        conn.commit()
        print ('- Articles with links coming from the group: '+str(len(page_titles_inlinks_group)))
        print ('* get_articles_with_inlinks completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        # INLINKS
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE num_inlinks IS NOT NULL;'
        cursor.execute(query)
        if cursor.fetchone()[0] == 0:
            page_titles_inlinks = []
            mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
            query = 'SELECT count(pl_from), pl_title FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 GROUP BY pl_title'
            mysql_cur_read.execute(query)
    #        page_asstring = ','.join( ['%s'] * len( all_ccc_articles ) )
    #        query = 'SELECT count(pl_from), pl_title FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_title IN (%s) GROUP BY pl_title' # % page_asstring
    #        mysql_cur_read.execute(query,set(all_ccc_articles.keys()))
            rows = mysql_cur_read.fetchall()
    #        print ('query run.')

            for row in rows:
                try:
                    pl_title=str(row[1].decode('utf-8'))
                    count=row[0]
                    page_titles_inlinks.append((count,float(count),page_titles_page_ids[pl_title],page_titles_qitems[pl_title],pl_title))
    #                print ((row[0],float(row[0]),all_ccc_articles[pl_title],all_ccc_articles_qitems[pl_title]))
                except: continue

            if group == 'ccc':
                query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks=?,percent_inlinks_from_CCC=(num_inlinks_from_CCC/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
            else:
                query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks=?,percent_inlinks_from_geolocated_abroad=(num_inlinks_from_geolocated_abroad/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'

            cursor.executemany(query,page_titles_inlinks)
            conn.commit()
            print ('- Articles with any inlink at all: '+str(len(page_titles_inlinks)))

        else:
            if group == 'ccc':
                query = 'UPDATE ccc_'+languagecode+'wiki SET percent_inlinks_from_CCC=(1.0*num_inlinks_from_CCC/num_inlinks);'
            else:
                query = 'UPDATE ccc_'+languagecode+'wiki SET percent_inlinks_from_geolocated_abroad=(1.0*num_inlinks_from_geolocated_abroad/num_inlinks);'
            cursor.execute(query)
            conn.commit()

        print ('* get_articles_with_inlinks completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    except:
        print ("MySQL has gone away. Let's try to do the joins in the code logics.")

        functionstartTime = time.time()
        article_selection_page_ids = set(article_selection.keys())

        page_titles_inlinks_group = []
        page_ids=[]
        num_art = 0
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

#        print ('Progression.')
        for x,y in page_titles_page_ids.items():
            query = 'SELECT pl_from FROM pagelinks WHERE pl_namespace=0 AND pl_from_namespace=0 AND pl_title = %s;'
            mysql_cur_read.execute(query,(x,))
            rows = mysql_cur_read.fetchall()
            pl_title = x
            for row in rows:
                page_ids.append(row[0])
            try:
                num_art = num_art + 1
                if num_art % 100000 == 0:
                    print (100*num_art/len(page_titles_page_ids))
                    print ('current time: ' + str(time.time() - startTime))

                count=len(list(set(page_ids).intersection(article_selection_page_ids)))
                page_titles_inlinks_group.append((count,len(page_ids),float(count)/float(len(page_ids)),page_titles_page_ids[pl_title],page_titles_qitems[pl_title],pl_title))
                page_ids.clear()
            except: 
                continue

        print ('- Articles with inlinks: '+str(len(page_titles_inlinks_group)))

        if group == 'ccc':
            query = 'UPDATE ccc_'+languagecode+'wiki SET (num_inlinks_from_CCC,num_inlinks,percent_inlinks_from_CCC)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        else:
            query = 'UPDATE ccc_'+languagecode+'wiki SET (num_inlinks_from_geolocated_abroad,num_inlinks,percent_inlinks_from_geolocated_abroad)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'            

        cursor.executemany(query,page_titles_inlinks_group)
        conn.commit()
        print ('* get_articles_with_inlinks Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles table with the number of outlinks.
def get_articles_with_outlinks(languagecode,page_titles_page_ids,page_titles_qitems,group):
    functionstartTime = time.time()
    print ("\n* Get the number of outlinks and the number of outlinks to "+group+" for language: "+languages.loc[languagecode]['languagename']+' '+languagecode+'.')
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    article_selection = {}
    if group == 'ccc':
        query = 'SELECT page_id, page_title qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            article_selection[row[0]]=row[1]
        print ('- Articles in CCC: '+str(len(article_selection)))
    else:
        query = 'SELECT page_id, page_title qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=0;'
        for row in cursor.execute(query):
            article_selection[row[0]]=row[1]
        print ('- Articles with other language CCC relationships: '+str(len(article_selection)))

    try:
        print ('Attempt with a MySQL.')

        page_titles_outlinks_group = []
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        page_asstring = ','.join( ['%s'] * len( article_selection ) )
        query = 'SELECT count(pl_title), pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_title IN (%s) GROUP BY pl_from;' % page_asstring

        mysql_cur_read.execute(query,list(article_selection.values()))
        rows = mysql_cur_read.fetchall()
    #        print ('query run.')
        for row in rows:
            try:
                page_title=page_ids_page_titles[row[1]]
                page_titles_outlinks_group.append((row[0],row[1],page_titles_qitems[page_title],page_title))
            except: pass

        if group == 'ccc':
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks_to_CCC=? WHERE page_id = ? AND qitem = ? AND page_title=?;'
        else:
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks_to_geolocated_abroad=? WHERE page_id = ? AND qitem = ? AND page_title=?;'

        cursor.executemany(query,page_titles_outlinks_group)
        conn.commit()

        print ('- Articles pointing at the group: '+str(len(page_titles_outlinks_group)))
        print ('* get_articles_with_outlinks completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        # OUTLINKS
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE num_outlinks IS NOT NULL;'
        cursor.execute(query)
        if cursor.fetchone()[0] == 0:
            page_titles_outlinks = []
            mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
            query = 'SELECT count(pl_title),pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 GROUP BY pl_from'
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows:
                try:
                    count=row[0]
                    page_title = page_ids_page_titles[row[1]]
                    page_titles_outlinks.append((count,float(count),row[1],page_titles_qitems[page_title],page_title))
                except: continue

            if group == 'ccc':
                query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks=?,percent_outlinks_to_CCC=(num_outlinks_to_CCC/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
            else:
                query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks=?,percent_outlinks_to_geolocated_abroad=(num_outlinks_to_geolocated_abroad/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'

            cursor.executemany(query,page_titles_outlinks)
            conn.commit()
            print ('- Articles with any outlink at all: '+str(len(page_titles_outlinks)))

        else:
            if group == 'ccc':
                query = 'UPDATE ccc_'+languagecode+'wiki SET percent_outlinks_to_CCC=(1.0*num_outlinks_to_CCC/num_outlinks);'
            else:
                query = 'UPDATE ccc_'+languagecode+'wiki SET percent_outlinks_to_geolocated_abroad=(1.0*num_outlinks_to_geolocated_abroad/num_outlinks);'
            cursor.execute(query)
            conn.commit()

        print ('* get_articles_with_outlinks completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    except:
        print ("MySQL has gone away. Let's try to do the joins in the code logics.")
        #input('')
       # get the ccc and potential ccc articles

        articles_page_titles = set(article_selection.values())

        page_titles_outlinks_group = []
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        page_titles=[]
        num_art = 0
#        print ('Progression.')
        for x,y in page_ids_page_titles.items():
    #        print (x)
            query = 'SELECT pl_title FROM pagelinks WHERE pl_namespace=0 AND pl_from_namespace=0 AND pl_from = %s;'
            mysql_cur_read.execute(query,(x,))
            rows = mysql_cur_read.fetchall()
            for row in rows: 
                pl_from = x
                page_titles.append(str(row[0].decode('utf-8')))
            try:
                num_art = num_art + 1
                if num_art % 100000 == 0:
                    print (100*num_art/len(page_ids_page_titles))
                    print ('current time: ' + str(time.time() - startTime))
                count=len(list(set(page_titles).intersection(articles_page_titles)))
                page_title=page_ids_page_titles[pl_from]
                page_titles_outlinks_group.append((count,len(page_titles),float(count)/float(len(page_titles)),pl_from,page_titles_qitems[page_title],page_title))
                page_titles.clear()
            except:
                continue
        print ('Articles with outlinks: '+str(len(page_titles_outlinks_group)))

        if group == 'ccc':
            query = 'UPDATE ccc_'+languagecode+'wiki SET (num_outlinks_to_CCC,num_outlinks,percent_outlinks_to_CCC)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        else:
            query = 'UPDATE ccc_'+languagecode+'wiki SET (num_outlinks_to_geolocated_abroad,num_outlinks,percent_outlinks_to_geolocated_abroad)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'

        cursor.executemany(query,page_titles_outlinks_group)
        conn.commit()
        print ('* get_articles_with_outlinks Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



# Obtain the articles with a "weak" language property that is associated the language. This is considered potential CCC.
def get_articles_language_weak_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    print ('\n* Getting articles with Wikidata from items with "language" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    # language qitems
    qitemresult = languages.loc[languagecode]['Qitem']
    if ';' in qitemresult: qitemresult = qitemresult.split(';')
    else: qitemresult = [qitemresult];

    # get articles
    qitem_properties = {}
    qitem_page_titles = {}
    ccc_language_items = []
    query = 'SELECT language_weak_properties.qitem, language_weak_properties.property, language_weak_properties.qitem2, sitelinks.page_title FROM language_weak_properties INNER JOIN sitelinks ON sitelinks.qitem = language_weak_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')
        if qitem2 not in qitemresult: continue

#        print ((qitem, wdproperty, language_properties_weak[wdproperty], page_title))
        # Put the items into a dictionary
        value = wdproperty+':'+qitem2
        if qitem not in qitem_properties: qitem_properties[qitem]=value
        else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value
        qitem_page_titles[qitem]=page_title

#    print (len(qitem_page_titles))
#    input('ja hem acabat')
#    return

    # Get the tuple ready to insert.
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_language_items.append((values,qitem_page_titles[qitem],qitem,page_id))
        except: continue

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (language_weak_wd) = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_language_items)/num_art

    print (str(len(ccc_language_items))+' language related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('The number of inserted articles account for a '+str(percent)+'% of the entire Wikipedia.')
    print ('* get_articles_wd_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles with the number of affiliation-like properties to other articles already retrieved as CCC.
def get_articles_affiliation_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    print ('\n* Get articles with Wikidata from items with "affiliation" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

    potential_ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

#    print (affiliation_properties)
#    input('')

    selected_qitems={}
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
    print (len(ccc_affiliation_items))

    # INSERT INTO CCC DATABASE
    query = 'UPDATE ccc_'+languagecode+'wiki SET affiliation_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_affiliation_items)
    conn.commit()
    print (str(len(ccc_affiliation_items))+' items/articles which have an affiliation to other CCC items/articles for language '+(languagecode)+' have been updated.');
    print ('* get_articles_affiliation_properties_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles with the properties that state that has articles already retrieved as CCC as part of them.
def get_articles_has_part_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()

    print ('\n* Updating articles with Wikidata from items with "has part" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

    potential_ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

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

#    for keys,values in selected_qitems.items(): print (keys,values)
#    input('')
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

    # INSERT INTO CCC DATABASE
    query = 'UPDATE ccc_'+languagecode+'wiki SET has_part_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_has_part_items)
    conn.commit()

    print (str(len(ccc_has_part_items))+' items/articles which have a part of CCC items/articles for language '+(languagecode)+' have been updated.');
    print ('* get_articles_has_part_properties_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def get_other_ccc_country_properties(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n* Updating the features based on COUNTRY properties for other languages CCC articles in language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    (iso_qitem, label_qitem) = wikilanguages_utils.load_all_countries_qitems()

    try: countries_language = set(territories.loc[languagecode]['ISO3166'].tolist())
    except: 
        try: countries_language = set(); countries_language.add(territories.loc[languagecode]['ISO3166'])
        except: pass

    countries_language = list(set(countries_language)&set(iso_qitem.keys())) # these iso3166 codes
    qitems_countries_language = []
    for country in countries_language: qitems_countries_language.append(iso_qitem[country])

    page_asstring = ','.join( ['?'] * (len(iso_qitem)) ) # total
    query = 'SELECT country_properties.qitem, page_title, count(country_properties.qitem) FROM country_properties INNER JOIN sitelinks ON sitelinks.qitem = country_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki" AND country_properties.qitem2 IN (%s)' % page_asstring

    page_asstring = ','.join( ['?'] * (len(qitems_countries_language)) ) # local
    query += ' AND country_properties.qitem2 NOT IN (%s) GROUP BY country_properties.qitem' % page_asstring

    parameters = list(iso_qitem.values())+qitems_countries_language

#    print (query)
#    print (parameters)
    table_update = []
    for row in cursor.execute(query, parameters):
#        print (row)
        qitem=row[0]
        page_title=row[1].replace(' ','_')
        try: page_id = page_titles_page_ids[page_title]
        except: continue
        count=row[2]
        table_update.append((0,count,page_title,qitem,page_id))

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = ?, other_ccc_country_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,table_update)
    conn2.commit()
    print ('* get_other_ccc_country_properties Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def get_other_ccc_location_properties(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n* Updating the features based on LOCATION properties for other languages CCC articles in language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    qitem_page_titles = {v: k for k, v in page_titles_qitems.items()}

    query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE location_wd IS NOT NULL;'
    ccc_location_qitems=[]
    for row in cursor2.execute(query): ccc_location_qitems.append(row[0])

    table_update = []
    if len(ccc_location_qitems)<200000:
        page_asstring = ','.join( ['?'] * (len(ccc_location_qitems)) ) # local
        query = 'SELECT location_properties.qitem, page_title, count(location_properties.qitem) FROM location_properties INNER JOIN sitelinks ON sitelinks.qitem = location_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki" AND location_properties.qitem NOT IN (%s) GROUP BY location_properties.qitem;' % page_asstring 

        for row in cursor.execute(query, ccc_location_qitems):
    #        print (row)
            qitem=row[0]
            page_title=row[1].replace(' ','_')
            try: page_id = page_titles_page_ids[page_title]
            except: continue
            count=row[2]
            table_update.append((count,page_title,qitem,page_id))
    else:
        query = 'SELECT DISTINCT qitem FROM location_properties;'
        
        location_qitems = []
        for row in cursor.execute(query): location_qitems.append(row[0])
        language_qitems = list(set(location_qitems).intersection(set(page_titles_qitems.values())))

        print (len(language_qitems))

        language_qitems = list(set(language_qitems) & set(set(language_qitems) ^ set(ccc_location_qitems)))

        print (len(language_qitems))

        num_art=0
        for qitem in language_qitems:
            num_art+=1
            try:
                page_title = qitem_page_titles[qitem]
                page_id = page_titles_page_ids[page_title]
            except:
                continue
            table_update.append((0,1,page_title,qitem,page_id))
#            if num_art % 1000 == 0: print (num_art)

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = ?, other_ccc_location_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,table_update)
    conn2.commit()
    print ('* get_other_ccc_location_properties Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def get_other_ccc_wikidata_properties(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n* Updating the features based on SOME WIKIDATA properties for the potential no CCC articles from language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    qitems = page_titles_qitems.values()

    tables = ['language_strong_properties', 'created_by_properties','part_of_properties','language_weak_properties', 'has_part_properties', 'affiliation_properties']

    columns = ['language_strong_wd', 'created_by_wd', 'part_of_wd', 'language_weak_wd', 'has_part_wd', 'affiliation_wd']
    columns_update = ['other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_has_part_wd','other_ccc_affiliation_wd']

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    print (tables)
    for x in range(0,len(tables)):
        print (tables[x])

        table_update = []
        for qitem in qitems:
            negative_ccc_property_count = 0

            total_property_count = 0
            query = 'SELECT count(qitem) FROM '+tables[x]+' WHERE qitem = "'+str(qitem)+'"'
            cursor.execute(query)

            value = cursor.fetchone()
            if value != None: total_property_count = value[0]

            query = 'SELECT '+columns[x]+', page_id, page_title FROM ccc_'+languagecode+'wiki WHERE qitem = "'+str(qitem)+'"'
            cursor2.execute(query)
            ccc_property_count = 0
            value2 = cursor2.fetchone()
            page_id = None
            page_title = None

            if value2 != None: 
                page_id = value2[1]
                properties = value2[0]
                page_title = value2[2]
                if properties != None: 
                    ccc_property_count = properties.count(';')
                    if ccc_property_count == 0: ccc_property_count = 1
#                print (properties,ccc_property_count, page_id)

            negative_ccc_property_count = total_property_count - ccc_property_count
#            print (total_property_count,negative_ccc_property_count,ccc_property_count,qitem,page_id,page_title)

            if negative_ccc_property_count != 0:
                table_update.append((negative_ccc_property_count,qitem,page_id))

        print ('The number of articles that are going to be updated for this table: '+columns_update[x]+' are: '+str(len(table_update)))
        cursor2.executemany('UPDATE ccc_'+languagecode+'wiki SET '+columns_update[x]+'=? WHERE qitem = ? AND page_id = ?', table_update)
        conn2.commit()

    print ('* get_other_ccc_wikidata_properties Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




def get_other_ccc_category_crawling(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    print ('\n* Updating the features based on keywords and category crawling levels for the potential no CCC articles from language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    qitem_page_titles = {v: k for k, v in page_titles_qitems.items()}

    qitems = page_titles_qitems.values()

    qitems_keywords_binary = {}
    qitems_category_crawling_level = {}

    for queried_languagecode in wikilanguagecodes:
        if queried_languagecode == languagecode: continue
        query = 'SELECT MAX(category_crawling_level) FROM ccc_'+queried_languagecode+'wiki;'
        level_max = 0
        cursor.execute(query)
        value = cursor.fetchone()
        if value != None: level_max = value[0]
        if level_max == 0 or level_max == None: continue
        print (queried_languagecode)

        query = 'SELECT ccc_'+queried_languagecode+'wiki.qitem, ccc_'+queried_languagecode+'wiki.category_crawling_level, ccc_'+queried_languagecode+'wiki.keyword_title, ccc_'+languagecode+'wiki.page_title FROM ccc_'+queried_languagecode+'wiki INNER JOIN ccc_'+languagecode+'wiki ON ccc_'+queried_languagecode+'wiki.qitem = ccc_'+languagecode+'wiki.qitem WHERE '
        query += 'ccc_'+languagecode+'wiki.ccc_binary IS NULL AND (ccc_'+languagecode+'wiki.category_crawling_level IS NOT NULL OR ccc_'+languagecode+'wiki.language_weak_wd IS NOT NULL OR ccc_'+languagecode+'wiki.affiliation_wd IS NOT NULL OR ccc_'+languagecode+'wiki.has_part_wd IS NOT NULL);'

        count = 0
        for row in cursor.execute(query):
            qitem=row[0]
#            if queried_languagecode == 'ar': print (row[3],row[1])

            if row[2] != None: qitems_keywords_binary[qitem]=1
            if row[1] != None: 
                category_crawling_level=row[1]
                if category_crawling_level != 0: category_crawling_level=category_crawling_level-1
                if category_crawling_level > 6: category_crawling_level = 6

                relative_level=1-float(category_crawling_level/6)
                # we choose always the lowest category crawling level in case the article is in category crawling of different languages.
                if qitem in qitems_category_crawling_level:
                    if qitems_category_crawling_level[qitem]>relative_level: qitems_category_crawling_level[qitem]=relative_level
                else: 
                    qitems_category_crawling_level[qitem]=relative_level
                    count+=1
        print (count)

    table_update = []
    for qitem in qitems:
        keyword = 0; relative_level = 0;

        page_id=page_titles_page_ids[qitem_page_titles[qitem]]

        if qitem in qitems_keywords_binary: keyword = 1
        else: keyword = 0

        if qitem in qitems_category_crawling_level:
            relative_level = qitems_category_crawling_level[qitem]
        else: relative_level = 0

        if keyword != 0 or relative_level != 0:
            table_update.append((relative_level,keyword,qitem,page_id))

    print ('\nThe number of articles that are going to be updated for this language edition CCC: '+languagecode+' that relate to other language edition keywords/category crawling are: '+str(len(table_update)))
    cursor.executemany('UPDATE ccc_'+languagecode+'wiki SET other_ccc_category_crawling_relative_level = ?, other_ccc_keyword_title = ? WHERE qitem = ? AND page_id = ?', table_update)
    conn.commit()

    print ('* get_other_ccc_category_crawling Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# ARTICLE CCC CLASSIFYING / SCORING FUNCTIONS
#############################################

def get_training_data(languagecode):

    # OBTAIN THE DATA TO FIT.
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
    ccc_df = pd.read_sql_query(query, conn)


    positive_features = ['category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']

    negative_features = ['other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd']  #'other_ccc_country_wd','other_ccc_location_wd' are out because now are considered totally negative groundtruth (25.9.18)

#    negative_features_2 = ['other_ccc_keyword_title','other_ccc_category_crawling_relative_level', 'num_inlinks_from_geolocated_abroad', 'num_outlinks_to_geolocated_abroad', 'percent_inlinks_from_geolocated_abroad', 'percent_outlinks_to_geolocated_abroad']

    features = ['qitem']+positive_features+negative_features
#    features = positive_features

    ccc_df = ccc_df[features]
    ccc_df = ccc_df.set_index(['qitem'])
#    print (ccc_df.head())
    if len(ccc_df.index.tolist())==0: print ('It is not possible to classify Wikipedia articles as there is no groundtruth.'); return (0,0,[],[]) # maxlevel,num_articles_ccc,ccc_df_list,binary_list
    ccc_df = ccc_df.fillna(0)


    # FORMAT THE DATA FEATURES AS NUMERICAL FOR THE MACHINE LEARNING
    category_crawling_paths=ccc_df['category_crawling_territories'].tolist()
    for n, i in enumerate(category_crawling_paths):
        if i is not 0:
            category_crawling_paths[n]=1
            if i.count(';')>=1: category_crawling_paths[n]=i.count(';')+1
        else: category_crawling_paths[n]=0
    ccc_df = ccc_df.assign(category_crawling_territories = category_crawling_paths)

    category_crawling_level=ccc_df['category_crawling_level'].tolist()
    maxlevel = max(category_crawling_level)
    for n, i in enumerate(category_crawling_level):
        if i > 0:
            category_crawling_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_level[n]=0
    ccc_df = ccc_df.assign(category_crawling_level = category_crawling_level)

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


def get_testing_data(languagecode,maxlevel):

    # OBTAIN THE DATA TO TEST
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
#    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NULL;' # ALL
    
    # WE GET THE POTENTIAL CCC ARTICLES THAT HAVE NOT BEEN 1 BY ANY OTHER MEANS.
    # For the testing takes those with one of these features not null (category crawling, language weak, affiliation or has part).
    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NULL AND (category_crawling_territories IS NOT NULL OR category_crawling_level IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL OR has_part_wd IS NOT NULL);'

    # For the testing takes those with one of these features not null (category crawling, language weak, affiliation or has part), and those with keywords on title.
#    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NULL AND (category_crawling_territories IS NOT NULL OR category_crawling_level IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL OR has_part_wd IS NOT NULL) OR keyword_title IS NOT NULL;' # POTENTIAL

    potential_ccc_df = pd.read_sql_query(query, conn)

    positive_features = ['category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC']

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

    category_crawling_level=potential_ccc_df['category_crawling_level'].tolist()
#    print (maxlevel)
#    print (max(category_crawling_level))
    for n, i in enumerate(category_crawling_level):
        if i > 0:
            category_crawling_level[n]=abs(i-(maxlevel+1))
        else:
            category_crawling_level[n]=0
    potential_ccc_df = potential_ccc_df.assign(category_crawling_level = category_crawling_level)

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
    if len(potential_ccc_df)==0: print ('There are not potential CCC articles, so it returns empty'); return
    potential_ccc_df = potential_ccc_df.sample(frac=1) # randomize the rows order

    print ('We selected this number of potential CCC articles: '+str(len(potential_ccc_df)))

    return potential_ccc_df


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# Takes the ccc_score and decides whether it must be in ccc or not.
def calculate_articles_ccc_binary_classifier(languagecode,classifier,page_titles_page_ids,page_titles_qitems):
    functionstartTime = time.time()
    print ('\nObtaining the final CCC for language: '+languagecode)

    # FIT THE SVM MODEL
    maxlevel,num_articles_ccc,ccc_df_list,binary_list = get_training_data(languagecode)
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
    print ('Time fitting the data: '+str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print (clf.feature_importances_.tolist())
#    input('')

    # TEST THE DATA
    print ('Calculating which page is IN or OUT...')
    potential_ccc_df = get_testing_data(languagecode,maxlevel)

    if potential_ccc_df is None: print ('No articles to verify.'); return     
    if len(potential_ccc_df)==0: print ('No articles to verify.'); return

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
                print (['category_crawling_paths','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd'])
                print(i)
                print(clf.predict_proba([i]).tolist())
                print (str(count_yes)+'\tIN\t'+page_title+'.\n')

                try: selected.append((page_titles_page_ids[page_title],page_titles_qitems[page_title]))
                except: pass
            else:
                count_no+=1
                print (['category_crawling_paths','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','other_ccc_language_strong_wd','other_ccc_created_by_wd','other_ccc_part_of_wd','other_ccc_language_weak_wd','other_ccc_affiliation_wd','other_ccc_has_part_wd'])
                print(i)
                print(clf.predict_proba([i]).tolist())
                print (str(count_no)+'\tOUT:\t'+page_title+'.\n')
#                input('')


    print ('Time predicting the testing data: '+str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print ('\nThis Wikipedia ('+languagecode+'wiki) has a total of '+str(wikipedialanguage_numberarticles[languagecode])+' articles.')
    print ('There were already '+str(num_articles_ccc)+' CCC articles selected as groundtruth. This is a: '+str(round(100*num_articles_ccc/wikipedialanguage_numberarticles[languagecode],3))+'% of the WP language edition.')

    print ('\nThis algorithm CLASSIFIED '+str(len(selected))+' articles as ccc_binary = 1 from a total of '+str(len(potential))+' from the testing data. This is a: '+str(round(100*len(selected)/len(potential),3))+'%.')
    print ('With '+str(num_articles_ccc+len(selected))+' articles, the current and updated percentage of CCC is: '+str(round(100*(num_articles_ccc+len(selected))/wikipedialanguage_numberarticles[languagecode],3))+'% of the WP language edition.\n')

#    evaluate_ccc_selection_manual_assessment(languagecode,selected,page_titles_page_ids)
#    input('')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,selected)
    conn.commit()

    print ('Language CCC '+(languagecode)+' created.')
    print ('* calculate_articles_ccc_binary_classifier Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def calculate_articles_ccc_main_territory(languagecode):
    functionstartTime = time.time()
    print ('\nCalculating the ccc main territory for each article in language: '+languagecode)

    if languagecode in languageswithoutterritory: print ('This language has no territories: '+languagecode); return

    number_iterations = 3
    print ('number of iterations: '+str(number_iterations))
    for i in range(1,number_iterations+1):
        print ('* iteration nº: '+str(i))
        # this function verifies the keywords associated territories, category crawling associated territories, and the wikidata associated territories in order to choose one.
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

        if languagecode not in languageswithoutterritory:
            try: qitems=territories.loc[languagecode]['QitemTerritory'].tolist()
            except: qitems=[];qitems.append(territories.loc[languagecode]['QitemTerritory'])

        main_territory_list = []
        main_territory_in = {}
        query = 'SELECT qitem, main_territory FROM ccc_'+languagecode+'wiki WHERE main_territory IS NOT NULL';
        for row in cursor.execute(query):
            main_territory_in[row[0]]=row[1]
    #    print (len(main_territory_in))

        query = 'SELECT qitem, page_id, main_territory, country_wd, location_wd, part_of_wd, has_part_wd, keyword_title, category_crawling_territories, created_by_wd, affiliation_wd FROM ccc_'+languagecode+'wiki'+' WHERE main_territory IS NULL AND ccc_binary=1'
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

        query = 'UPDATE ccc_'+languagecode+'wiki SET main_territory = ? WHERE qitem = ? AND page_id = ?;'
        cursor.executemany(query,main_territory_list)
        conn.commit()

    print ('CCC main territory assigned.')
    print ('* calculate_articles_ccc_main_territory Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Calculates the number of strategies used to retrieve and introduce them into the database.
def calculate_articles_ccc_retrieval_strategies(languagecode):
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn.cursor()

    strategies = []
    query = 'SELECT qitem, page_id, geocoordinates, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC FROM ccc_'+languagecode+'wiki'+';'
    for row in cursor.execute(query):
        num_retrieval_strategies = sum(x is not None for x in row)-2
        qitem = str(row[0])
        page_id = row[1]
        strategies.append((num_retrieval_strategies, qitem, page_id))
    query = 'UPDATE ccc_'+languagecode+'wiki SET num_retrieval_strategies = ? WHERE qitem = ? AND page_id = ?;'
    cursor.executemany(query,strategies)
    conn.commit()

    print ('CCC number of retrieval strategies for each article assigned.')
    print ('* calculate_articles_ccc_retrieval_strategies Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



# CCC VERIFICATION TOOLS FUNCTIONS
#############################################

# Filter: Deletes all the CCC selected qitems from a language which are geolocated but not among the geolocated articles to the territories associated to that language.
def groundtruth_reaffirmation(languagecode):
    functionstartTime = time.time()
    print ('\n* Reaffirming the groundtruth for all the articles from language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

#    print ('cleant. NOW WE START.')
#    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = NULL;'
#    cursor2.execute(query);
#    conn2.commit()

    # POSITIVE GROUNDTRUTH
    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE ccc_geolocated=1;'
    cursor2.execute(query);
    conn2.commit()
    print ('geolocated in, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE country_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('country_wd, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE location_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('location_wd, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE language_strong_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('language_strong_wd, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE created_by_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('created_by_wd, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE part_of_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('part_of_wd, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE keyword_title IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('keyword_title, done.')


    # NEGATIVE GROUNDTRUTH
    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 0 WHERE ccc_geolocated=-1;'
    cursor2.execute(query);
    conn2.commit()
    print ('geolocated abroad, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 0 WHERE other_ccc_location_wd IS NOT NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('location wikidata property abroad, done.')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 0 WHERE other_ccc_country_wd IS NOT NULL AND country_wd IS NULL;'
    cursor2.execute(query);
    conn2.commit()
    print ('country wikidata property abroad, done.')


    print ('* groundtruth_reaffirmation Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def check_current_groundtruth(languagecode):
    functionstartTime = time.time()
    print ('\n* Check the ccc_binary from all the articles from language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    print ('These are the ccc_binary null, zero and one: ')
    query = 'SELECT ccc_binary, count(*) FROM ccc_'+languagecode+'wiki GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    ## BINARY 0
    print ('\nFor those that are ZERO:')
    print ('- geolocated:')
    query = 'SELECT ccc_geolocated, count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 0 GROUP BY ccc_geolocated;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- other_ccc_location_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 0 AND other_ccc_location_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- other_ccc_country_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 0 AND other_ccc_country_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    ## BINARY 1
    print ('\nFor those that are ONE:')
    print ('- geolocated:')
    query = 'SELECT ccc_geolocated, count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 GROUP BY ccc_geolocated;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- country_wd:')
    query = 'SELECT country_wd, count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND country_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- location_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND location_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- language_strong_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND language_strong_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- created_by_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND created_by_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- part_of_wd:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND part_of_wd IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('- keyword_title:')
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary = 1 AND keyword_title IS NOT NULL;'
    for row in cursor.execute(query):
        print (row[0])

    print ('\nFor those that are POTENTIAL ONE, we check the distribution of features in ccc_binary:')
    print ('- category_crawling:')
    query = 'SELECT ccc_binary, count(*) FROM ccc_'+languagecode+'wiki WHERE category_crawling_level IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- language_weak_wd:')
    query = 'SELECT ccc_binary, count(*) FROM ccc_'+languagecode+'wiki WHERE language_weak_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- affiliation_wd:')
    query = 'SELECT ccc_binary, count(*) FROM ccc_'+languagecode+'wiki WHERE affiliation_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])

    print ('- has_part_wd:')
    query = 'SELECT ccc_binary, count(*) FROM ccc_'+languagecode+'wiki WHERE has_part_wd IS NOT NULL GROUP BY ccc_binary;'
    for row in cursor.execute(query):
        print (row[0],row[1])



def evaluate_ccc_selection_manual_assessment(languagecode, selected, page_titles_page_ids):
    print("Start the CCC selection manual assessment ")

    if selected is None:
        print ('Retrieving the CCC articles from the .db')
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
        query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
        ccc_df = pd.read_sql_query(query, conn)
        ccc_df = ccc_df[['page_title','category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']]
        ccc_df = ccc_df.set_index(['page_title'])
        ccc_df = ccc_df.sample(frac=1) # randomize the rows order

        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]
        ccc_df_no = ccc_df.loc[ccc_df['ccc_binary'] == 0]

        sample = 50
        ccc_df_list_yes = ccc_df_yes.index.tolist()[:sample]
        ccc_df_list_no = ccc_df_no.index.tolist()[:sample]

        print (ccc_df_list_yes)
        print (ccc_df_list_no)

        return # we return because this is actually run in another file: ccc_manual_assessment.py as it is not possible to open browsers via ssh.

        binary_list = sample*['c']+sample*['w']

        ccc_df_list = ccc_df_list_yes + ccc_df_list_no
        samplearticles=dict(zip(ccc_df_list,binary_list))

    else:
        page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

        print ('Using the CCC articles that have just been classified.')
        conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
        query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
        ccc_df = pd.read_sql_query(query, conn)
        ccc_df = ccc_df[['page_title','category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']]
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

    print ('The articles are ready for the manual assessment.')
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


def restore_ccc_binary_create_old_ccc_binary(languagecode,file):
    print("Start the CCC selection restore to the original ccc binary for language: "+languagecode)
    functionstartTime = time.time()

    if file == 1:
        filename = databases_path + 'old_ccc/' + languagecode+'_old_ccc.csv'
        output_file = codecs.open(filename, 'a', 'UTF-8')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT qitem, page_id, ccc_geolocated, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, ccc_binary FROM ccc_'+languagecode+'wiki;'

    parameters = []
    for row in cursor.execute(query):
        qitem = row[0]
        page_id = row[1]
        ccc_binary = None
        main_territory = None

        ccc_geolocated = row[2]
        if ccc_geolocated == 1: ccc_binary = 1;
        if ccc_geolocated == -1: ccc_binary = 0;

        for x in range(3,len(row)-2):
            if row[x] != None: ccc_binary = 1

#        print ((ccc_binary,main_territory,qitem,page_id))
        parameters.append((ccc_binary,main_territory,qitem,page_id))

        cur_ccc_binary = row[9]
        if file == 1: output_file.write(qitem + '\t' + str(cur_ccc_binary) + '\n')

    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = ?, main_territory = ? WHERE qitem = ? AND page_id = ?;'
    cursor.executemany(query,parameters)
    conn.commit()

    print ('* restore_ccc_binary Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def check_current_ccc_binary_old_ccc_binary(languagecode):
    print("* Compare current ccc with a previous one.")

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    old_ccc_file_name = databases_path + 'old_ccc/' + languagecode+'_old_ccc.csv'
    old_ccc_file = open(old_ccc_file_name, 'r')    
    old_ccc = {}
    old_number_ccc = 0
    for line in old_ccc_file: # dataset
        page_data = line.strip('\n').split('\t')
#        page_data = line.strip('\n').split(',')
        ccc_binary = str(page_data[1])
        qitem = page_data[0]
        qitem=str(qitem)
        old_ccc[qitem] = ccc_binary
        if ccc_binary == 1: old_number_ccc+=1

    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
    cursor.execute(query)
    current_number_ccc=cursor.fetchone()[0]

    print ('In old CCC there were: '+str(old_number_ccc)+' articles, a percentage of '+str(float(100*old_number_ccc/wikipedialanguage_numberarticles[languagecode])))
    print ('In current CCC there are: '+str(current_number_ccc)+' articles, a pecentage of '+str(float(100*current_number_ccc/wikipedialanguage_numberarticles[languagecode])))

    print ('\nProceeding now with the article comparison: ')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
#    query = 'SELECT qitem, page_id, page_title, ccc_binary FROM ccc_'+languagecode+'wiki;'
    query = 'SELECT qitem, page_id, page_title, ccc_binary, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC, percent_inlinks_from_CCC, percent_outlinks_to_CCC, other_ccc_country_wd, other_ccc_location_wd, other_ccc_language_strong_wd, other_ccc_created_by_wd, other_ccc_part_of_wd, other_ccc_language_weak_wd, other_ccc_affiliation_wd, other_ccc_has_part_wd, other_ccc_keyword_title, other_ccc_category_crawling_relative_level, num_inlinks_from_geolocated_abroad, num_outlinks_to_geolocated_abroad, percent_inlinks_from_geolocated_abroad, percent_outlinks_to_geolocated_abroad FROM ccc_'+languagecode+'wiki;'

    i = 0
    j = 0

    for row in cursor.execute(query):
        qitem = row[0]
        page_id = str(row[1])
        page_title = row[2]

        ccc_binary = row[3]
        if ccc_binary == None or ccc_binary == 'None': ccc_binary = 0
        if ccc_binary == '1': ccc_binary = 1

        category_crawling_territories = str(row[4])
        language_weak_wd = str(row[5])
        affiliation_wd = str(row[6])
        has_part_wd = str(row[7])
        num_inlinks_from_CCC = str(row[8])
        num_outlinks_to_CCC = str(row[9])
        percent_inlinks_from_CCC = str(row[10])
        percent_outlinks_to_CCC = str(row[11])
        other_ccc_country_wd = str(row[12])
        other_ccc_location_wd = str(row[13])
        other_ccc_language_strong_wd = str(row[14])
        other_ccc_created_by_wd = str(row[15])
        other_ccc_part_of_wd = str(row[16])
        other_ccc_language_weak_wd = str(row[17])
        other_ccc_affiliation_wd = str(row[18])
        other_ccc_has_part_wd = str(row[19])
        other_ccc_keyword_title = str(row[20])
        other_ccc_category_crawling_relative_level = str(row[21])
        num_inlinks_from_geolocated_abroad = str(row[22])
        num_outlinks_to_geolocated_abroad = str(row[23])
        percent_inlinks_from_geolocated_abroad = str(row[24])
        percent_outlinks_to_geolocated_abroad = str(row[25])

        old_ccc_binary = old_ccc[qitem]
        if old_ccc_binary == None or old_ccc_binary == 'None': old_ccc_binary = 0
        if old_ccc_binary == '1' or old_ccc_binary == 1: old_ccc_binary = 1

        line = page_title+' , '+page_id+'\n\tcategory_crawling_territories\t'+category_crawling_territories+'\tlanguage_weak_wd\t'+language_weak_wd+'\taffiliation_wd\t'+affiliation_wd+'\thas_part_wd\t'+has_part_wd+'\tnum_inlinks_from_CCC\t'+num_inlinks_from_CCC+'\tnum_outlinks_to_CCC\t'+num_outlinks_to_CCC+'\tpercent_inlinks_from_CCC\t'+percent_inlinks_from_CCC+'\tpercent_outlinks_to_CCC\t'+percent_outlinks_to_CCC+'\tother_ccc_country_wd\t'+other_ccc_country_wd+'\tother_ccc_location_wd\t'+other_ccc_location_wd+'\tother_ccc_language_strong_wd\t'+other_ccc_language_strong_wd+'\tother_ccc_created_by_wd\t'+other_ccc_created_by_wd+'\tother_ccc_part_of_wd\t'+other_ccc_part_of_wd+'\tother_ccc_language_weak_wd\t'+other_ccc_language_weak_wd+'\tother_ccc_affiliation_wd\t'+other_ccc_affiliation_wd+'\tother_ccc_has_part_wd\t'+other_ccc_has_part_wd+'\tother_ccc_keyword_title\t'+other_ccc_keyword_title+'\tother_ccc_category_crawling_relative_level\t'+other_ccc_category_crawling_relative_level+'\tnum_inlinks_from_geolocated_abroad\t'+num_inlinks_from_geolocated_abroad+'\tnum_outlinks_to_geolocated_abroad\t'+num_outlinks_to_geolocated_abroad+'\tpercent_inlinks_from_geolocated_abroad\t'+percent_inlinks_from_geolocated_abroad+'\tpercent_outlinks_to_geolocated_abroad\t'+percent_outlinks_to_geolocated_abroad

#        if ccc_binary == 1: 

        if ccc_binary == 1 and old_ccc_binary == 0:
            print ('* '+line + '\n old_ccc_binary: '+str(old_ccc_binary)+', ccc_binary: '+str(ccc_binary))
            print ('now ccc (only positive), before non-ccc'+'\n')
            j += 1

        if ccc_binary == 0 and old_ccc_binary == 1:
            print ('* '+line + '\n old_ccc_binary: '+str(old_ccc_binary)+', ccc_binary: '+str(ccc_binary))
            print ('before ccc (with positive and negative), now non-ccc'+'\n')
            i += 1

    print ("*\n")
    print ("There are "+str(i)+" articles that are non-CCC now but they were.")
    print ("There are "+str(j)+" articles that are CCC now but they were non-CCC before.")
    print ("* End of the comparison")

"""
def copy_ccc_current_to_another():

    print ('coyping ccc_current to another .db')
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + 'ccc_current_backup.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode)

        # RETRIEVING
        query = ('SELECT '+
            # general
        'qitem, '+
        'page_id, '+
        'page_title, '+
        'date_created, '+
        'geocoordinates, '+
        'iso3166, '+
        'iso31662, '+

        # calculations:
#        'ccc_binary, '+
#        'main_territory, '+
#        'num_retrieval_strategies, '+ # this is a number

        # set as CCC
        'ccc_geolocated,'+ # 1, -1 o null.
#        'country_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'location_wd, '+ # 'P1:QX1:Q; P2:QX2:Q' Q is the main territory
        'language_strong_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'created_by_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'part_of_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'keyword_title, '+ # 'QX1;QX2'

        # retrieved as potential CCC:
        'category_crawling_territories, '+ # 'QX1;QX2'
        'category_crawling_level, '+ # 'level'
#        'language_weak_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'affiliation_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'has_part_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'num_inlinks_from_CCC, '+
        'num_outlinks_to_CCC, '+
        'percent_inlinks_from_CCC, '+
        'percent_outlinks_to_CCC, '+

        # retrieved as potential other CCC (part of other CCC)
        # from wikidata properties
#        'other_ccc_country_wd, '+
#        'other_ccc_location_wd, '+
#        'other_ccc_language_strong_wd, '+
#        'other_ccc_created_by_wd, '+
#        'other_ccc_part_of_wd, '+
#        'other_ccc_language_weak_wd, '+
#        'other_ccc_affiliation_wd, '+
#        'other_ccc_has_part_wd, '+
        # from other wikipedia ccc database.
#        'other_ccc_keyword_title, '+
#        'other_ccc_category_crawling_relative_level, '+
        # from links to/from non-ccc geolocated articles.
#        'num_inlinks_from_geolocated_abroad, '+
#        'num_outlinks_to_geolocated_abroad, '+
#        'percent_inlinks_from_geolocated_abroad, '+
#        'percent_outlinks_to_geolocated_abroad, '+

        # characteristics of rellevance
        'num_inlinks, '+
        'num_outlinks, '+
        'num_bytes, '+
        'num_references, '+
        'num_edits, '+
        'num_editors, '+
        'num_discussions, '+
        'num_pageviews, '+
        'num_wdproperty, '+
        'num_interwiki, '+
        'featured_article '+

        'FROM ccc_'+languagecode+'wiki;')

#        print (query)
        parameters = []
        for row in cursor.execute(query):
            parameters.append(tuple(row))

        # INSERTING
        page_asstring = ','.join( ['?'] * (query.count(',')+1) )
        query = ('INSERT INTO ccc_'+languagecode+'wiki ('+
            # general
        'qitem, '+
        'page_id, '+
        'page_title, '+
        'date_created, '+
        'geocoordinates, '+
        'iso3166, '+
        'iso31662, '+

        # calculations:
#        'ccc_binary, '+
#        'main_territory, '+
#        'num_retrieval_strategies, '+ # this is a number

        # set as CCC
        'ccc_geolocated,'+ # 1, -1 o null.
#        'country_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'location_wd, '+ # 'P1:QX1:Q; P2:QX2:Q' Q is the main territory
        'language_strong_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'created_by_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'part_of_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'keyword_title, '+ # 'QX1;QX2'

        # retrieved as potential CCC:
        'category_crawling_territories, '+ # 'QX1;QX2'
        'category_crawling_level, '+ # 'level'
#        'language_weak_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'affiliation_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
#        'has_part_wd, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'num_inlinks_from_CCC, '+
        'num_outlinks_to_CCC, '+
        'percent_inlinks_from_CCC, '+
        'percent_outlinks_to_CCC, '+

        # retrieved as potential other CCC (part of other CCC)
        # from wikidata properties
#        'other_ccc_country_wd, '+
#        'other_ccc_location_wd, '+
#        'other_ccc_language_strong_wd, '+
#        'other_ccc_created_by_wd, '+
#        'other_ccc_part_of_wd, '+
#        'other_ccc_language_weak_wd, '+
#        'other_ccc_affiliation_wd, '+
#        'other_ccc_has_part_wd, '+
        # from other wikipedia ccc database.
#        'other_ccc_keyword_title, '+
#        'other_ccc_category_crawling_relative_level, '+
        # from links to/from non-ccc geolocated articles.
#        'num_inlinks_from_geolocated_abroad, '+
#        'num_outlinks_to_geolocated_abroad, '+
#        'percent_inlinks_from_geolocated_abroad, '+
#        'percent_outlinks_to_geolocated_abroad, '+

        # characteristics of rellevance
        'num_inlinks, '+
        'num_outlinks, '+
        'num_bytes, '+
        'num_references, '+
        'num_edits, '+
        'num_editors, '+
        'num_discussions, '+
        'num_pageviews, '+
        'num_wdproperty, '+
        'num_interwiki, '+
        'featured_article '+
        ') '+

        'VALUES ('+
        page_asstring+
        ');')

        cursor2.executemany(query,parameters)
        conn2.commit()

    print ('* copy_ccc_current_to_another Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))
"""

#######################################################################################


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    startTime = time.time()

    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'

    datasets_path = '/srv/wcdo/datasets/'
    current_datasets_path = datasets_path + year_month + '/'

    # Import the language-territories mappings
    territories = wikilanguages_utils.load_languageterritories_mapping()

    # Import the Wikipedia languages characteristics / UPGRADE CASE: in case of extending the project to WMF SISTER PROJECTS, a) this should be extended with other lists for Wikimedia sister projects b) along with more get functions in the MAIN for each sister project.
    languages = wikilanguages_utils.load_wiki_projects_information(territories);
    wikilanguages_utils.extract_check_new_wiki_projects();

    wikilanguagecodes = languages.index.tolist() 

    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None: wikilanguagecodes.remove(a)

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes)

    print (wikilanguagecodes)

    # Final Wikipedia languages to process
#    wikilanguagecodes = wikilanguages_utils.obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('su')+1:]
#    wikilanguagecodes = ['ca']
#    languagecode = sys.argv[1]

    print ('\n* Starting the CCC SELECTION CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* CCC SELECTION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))

#    wikilanguages_utils.finish_email(startTime,'ccc_selection.out','CCC Selection')