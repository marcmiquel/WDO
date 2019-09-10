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


# EXECUTION THE 7TH DAY OF EVERY MONTH!

# MAIN
def main():


    wd_dump_iterator()

    wd_geolocated_update_db()
    wd_instance_of_subclass_of_property_crawling()
    # here we are in the current version of wikidata.db

    create_wikipedia_diversity_db()
    insert_page_ids_page_titles_qitems_wikipedia_diversity_db()
    insert_geolocation_wd()

    wd_check_and_introduce_wikipedia_missing_qitems('')


################################################################

# Creates a CCC database for a list of languages.
def create_wikipedia_diversity_db():

    function_name = 'create_wikipedia_diversity_db'
    if create_function_account_db(function_name, 'check','')==1: return
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    """
    # Removes CCC temp database (just for code debugging)
    try:
        os.remove(databases_path + wikipedia_diversity_db); print (wikipedia_diversity_db+' deleted.');
    except:
        pass
    """

    # Creates a table for each Wikipedia language edition.
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
        query = ('CREATE TABLE '+languagecode+'wiki ('+

        # general
        'qitem text, '+
        'page_id integer, '+
        'page_title text, '+
        'date_created integer, '+
        'geocoordinates text, '+ # coordinate1,coordinate2
        'iso3166 text, '+ # code
        'iso31662 text, '+ # code
        'region text, '+ # continent

        # calculations:
        'ccc_binary integer, '+
        'ccc_binary_last_cycle integer, '+
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
            # from links to/from non-ccc geolocated Articles.
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
        'num_images integer, '+
        'num_edits_last_month integer,'+        
        'featured_article integer, '+
        'wikirank real, '+
        'first_timestamp_lang text,'+

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

        # diversity types       
        'gender text, '+
        'sexual_orientation text, '+ # wikidata properties
        'religion text, '+
        'race_and_ethnia text, '+
        'time integer, '+

        # diversity features
        'lgbt_binary integer, '
        'category_crawling_lgbt text, '
        'num_outlinks_to_female integer, '
        'num_outlinks_to_male integer, '
        'num_outlinks_to_lgbt integer, '
        'num_inlinks_from_female integer, '
        'num_inlinks_from_male integer, '
        'num_inlinks_from_lgbt integer, '


        'PRIMARY KEY (qitem,page_id));')

        try:
            cursor.execute(query)
            conn.commit()
            print ('Created the Wikipedia Diversity table for language: '+languagecode)
        except:
            print (languagecode+' already has a Wikipedia Diversity table.')

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        create_function_account_db(function_name, 'mark', duration)



def create_wikidata_db():
    try: os.remove(databases_path + wikidata_db);
    except: pass;
    conn = sqlite3.connect(databases_path + wikidata_db)
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

    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (qitem text, properties integer, sitelinks integer, PRIMARY KEY (qitem));")
    cursor.execute("CREATE TABLE IF NOT EXISTS labels (qitem text, langcode text, label text, PRIMARY KEY (qitem, langcode));")

    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS people_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS sexual_orientation_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS religion_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    cursor.execute("CREATE TABLE IF NOT EXISTS race_and_ethnia_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS time_properties (qitem text, property text, value text, PRIMARY KEY (qitem, value));")
    ###

    cursor.execute("CREATE TABLE IF NOT EXISTS instance_of_subclasses_of_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS industry_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS folk (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS monuments_and_buildings (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS earth (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS music_creations_and_organizations (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS sport_and_teams (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS food (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS paintings (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS glam (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS books (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS clothing_and_fashion (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    ###
    print ('Created the Wikidata sqlite3 file and tables.')
    return conn


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


def wd_dump_iterator():
    function_name = 'wd_dump_iterator'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()

    # Locate the dump
    read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
    dump_in = gzip.open(read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    n_qitems = 60865657
    conn = create_wikidata_db(); cursor = conn.cursor()
    conn.commit()
    number_qitems = wd_all_qitems(); #conn.commit() # getting all the qitems
    n_qitems = len(number_qitems); nqi = n_qitems/500

    print ('Iterating the dump.')
    while line != '':
        iter += 1
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]

        try:
            entity = json.loads(line)
            qitem = entity['id']
            try: p=number_qitems[qitem]
            except: continue
#            if not wd_check_qitem(cursor,qitem)=='1': continue
            if not qitem.startswith('Q'): continue
        except:
            print ('JSON error.')

        sitelinks = wd_sitelinks_insert_db(cursor, qitem, entity['sitelinks'])
        wd_labels_insert_db(cursor, qitem, sitelinks, entity['labels'])
        wd_entity_claims_insert_db(cursor, entity)

        if iter % 100000 == 0:
#        if iter % nqi == 0:
            print (iter)
            print (100*iter/n_qitems)
            print ('current time: ' + str(time.time() - startTime))
            conn.commit()
#            break

    conn.commit()
    conn.close()
    print ('DONE with the JSON.')
    print ('It has this number of lines: '+str(iter))
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


def wd_all_qitems():
    # cursor.execute("CREATE TABLE qitems (qitem text PRIMARY KEY);")
    dumps_path = '/public/dumps/public/wikidatawiki/latest/wikidatawiki-latest-page.sql.gz'
    dump_in = gzip.open(dumps_path, 'r')

    qitems = {}
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
                page_namespace = int(row[1])
                if page_namespace != 0: continue
                qitem = row[2]
                # query = "INSERT INTO qitems (qitem) VALUES ('"+qitem+"');"
                # cursor.execute(query)
                qitems[qitem]=None

    print(len(qitems))
    return qitems


def wd_check_qitem(cursor,qitem):
    query='SELECT 1 FROM qitems WHERE qitem = "'+qitem+'"'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row = str(row[0]);
    return row


def wd_entity_claims_insert_db(cursor, entity):
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

            if wdproperty in time_properties:
                try:
                    value = str(mainsnak['datavalue']['value']['time'])
                except:
                    continue

                value = int(value[0]+value[1:].split('-')[0])
                values = [qitem,wdproperty,value]
                cursor.execute("INSERT OR IGNORE INTO time_properties (qitem, property, value) VALUES (?,?,?)",values)
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
#                print ('affiliation_properties')
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

            if wdproperty in industry_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('industry properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO industry_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue


            if wdproperty in sexual_orientation_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('sexual_orientation_properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO sexual_orientation_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in religion_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('religion_properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO religion_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in race_and_ethnia_properties:
                values = [qitem,wdproperty,qitem2]
#                print ('race_and_ethnia_properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO race_and_ethnia_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue

            if wdproperty in people_properties:
                if wdproperty == 'P21' or (wdproperty == 'P31' and qitem2 == 'Q5'):
                    values = [qitem,wdproperty,qitem2]
    #                print ('people properties')
    #                print (qitem,wdproperty,qitem2)
                    cursor.execute("INSERT OR IGNORE INTO people_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                    continue

            if wdproperty in instance_of_subclasses_of_properties:
                if wdproperty == 'P31' and qitem2 == 'Q5': continue # if human, continue
                values = [qitem,wdproperty,qitem2]
#                print ('instance_of_subclasses_of_properties properties')
#                print (qitem,wdproperty,qitem2)
                cursor.execute("INSERT OR IGNORE INTO instance_of_subclasses_of_properties (qitem, property, qitem2) VALUES (?,?,?)",values)
                continue


def wd_sitelinks_insert_db(cursor, qitem, wd_sitelinks):
#    print (wd_sitelinks)
    for code, title in wd_sitelinks.items():

        sitelinks = []
        # in case of extension to wikibooks or other sister projects (e.g. cawikitionary) it would be necessary to introduce another 'if code in wikilanguaagescodeswikitionary'.
        if code in wikilanguagecodeswiki:
            values=[qitem,code,title['title']]
#            print (values)
            try: 
                cursor.execute("INSERT INTO sitelinks (qitem, langcode, page_title) VALUES (?,?,?)",values)
                sitelinks.append(code)
            except: 
                print ('This Q is already in: '+qitem)

        return sitelinks


def wd_labels_insert_db(cursor, qitem, wd_sitelinks, wd_labels):

    # print (qitem)
    # print ('these are the languages sitelinks:')
    # print (wd_sitelinks)
    # print ('these are the languages labels:')
    # print (wd_labels)   

    if wd_sitelinks == None: return
    
    for code, title in wd_labels.items(): # bucle de llengües
        code = code + 'wiki'
        if code not in wd_sitelinks and code in wikilanguagecodeswiki:
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


def wd_check_and_introduce_wikipedia_missing_qitems(languagecode):

    function_name = 'wd_check_and_introduce_wikipedia_missing_qitems'
    if create_function_account_db(function_name, 'check','')==1: return
    functionstartTime = time.time()

    langcodes = []

    if languagecode != '' and languagecode!= None: langcodes.append(languagecode)
    else: langcodes = wikilanguagecodes


    for languagecode in langcodes:
        print ('\n* '+languagecode)

        page_titles=list()
        page_ids=list()
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'SELECT page_title, page_id FROM '+languagecode+'wiki;'
        for row in cursor.execute(query):
            page_title=str(row[0])
            page_id = int(row[1])
            page_titles.append(str(row[0]))
            page_ids.append(row[1])

        print ('this is the number of page_ids: '+str(len(page_ids)))

        parameters = []
#            mysql_con_read = mdb.connect(host=languagecode+'wiki.analytics.db.svc.eqiad.wmflabs',db=languagecode+'wiki_p', read_default_file='./my.cnf', cursorclass=mdb_cursors.SSCursor,charset='utf8mb4', connect_timeout=60); mysql_cur_read = mysql_con_read.cursor()
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        try:
            query = 'SELECT page_title, page_id FROM page WHERE page_namespace=0 AND page_is_redirect=0;'
        #            query = 'SELECT /*+ MAX_EXECUTION_TIME(1000) */ page_title, page_id FROM page WHERE page_namespace=0 AND page_is_redirect=0;'
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            print ('query done')
            Articles_namespace_zero = len(rows)
            all_articles = {}
            for row in rows: 
                page_title = row[0].decode('utf-8')
                page_id = int(row[1])
                all_articles[page_id]=page_title

            for page_id in page_ids:
                del all_articles[page_id]

            for page_id, page_title in all_articles.items():               
        #                print (page_title, page_id)
                parameters.append((page_title,page_id,''))

            print ('* '+languagecode+' language edition has '+str(articles_namespace_zero)+' Articles non redirect with namespace 0.')
            print ('* '+languagecode+' language edition has '+str(len(parameters))+' Articles that have no qitem in Wikidata and therefore are not in the CCC database.\n')

            conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (page_title,page_id,qitem) VALUES (?,?,?);';
            cursor.executemany(query,parameters)
            conn.commit()
            print ('page ids for this language are in: '+languagecode+'\n')
        except:
            print ('this language replica reached timeout: '+languagecode+'\n')
        #            input('')
    create_function_account_db(function_name, 'mark', duration)
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))


# Runs the reverse geocoder and update the database. It needs 5000m.
def wd_geolocated_update_db():

    function_name = 'wd_geolocated_update_db'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()  


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

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



# fills the tables of qitems about folk, earth, music_creations_and_organizations, etc.
def wd_instance_of_subclass_of_property_crawling():

    function_name = 'wd_instance_of_subclass_of_property_crawling'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()


    # query = 'DELETE FROM instance_of_subclasses_of_properties WHERE qitem NOT IN (SELECT DISTINCT qitem FROM sitelinks);' Delete qitems with no Wikipedia articles. Such a bad idea, there are middle classes such as 'football club' or 'professional sports team' with no Wikipedia article. Therefore, some items are missing.

    """
    query = 'DELETE FROM instance_of_subclasses_of_properties WHERE qitem2 = "Q5";'
    cursor.execute(query)
    conn.commit()
    print ('DELETED.')
    """

    # DICTIONARIES OF TOPICS
    # places
    earth = {'Q271669': 'landform', 'Q205895': 'landmass'} # instance of
    glam = {'Q33506':'museum', 'Q166118':'archive', 'library':'Q7075'}
    monuments_and_buildings = {'Q811979': 'architectural structure'}
    folk = {'Q132241' : 'festival', 'Q288514' : 'fair', 'Q4384751': 'folk culture', 'Q36192': 'folklore'}

    # things
    food = {'Q2095' : 'food'}
    books = {'Q7725634':'literary work','Q571':'book', 'Q234460':'text', 'Q47461344':'written work'}
    paintings = {'Q3305213' : 'painting'}
    clothing_and_fashion = {'Q11460' : 'clothing', 'Q3661311': 'clothing_and_fashion house', 'Q1618899':'clothing_and_fashion label'}

    # organizations
    sport_and_teams = {'Q327245':'team','Q41323':'type of sport','Q349':'sport'}
    music_creations_and_organizations = {'Q188451': 'music genre', 'Q2088357': 'music organization', 'Q482994':'music term'}

    # LOOP TOPICS
    topics_dict = {'folk':folk,'monuments_and_buildings':monuments_and_buildings,'earth':earth,'music_creations_and_organizations':music_creations_and_organizations,'sport_and_teams':sport_and_teams,'food':food,'paintings':paintings,'glam':glam,'books':books,'clothing_and_fashion':clothing_and_fashion}


#    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,'en')
#    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

    print (topics_dict.keys())
 
    for topic, list_qitems in topics_dict.items():
        print ('\nSTARTING WITH THE TOPIC: '+topic)
        print (list(list_qitems.keys()))

        topic_dict_all = {}
        level_qitems = []
        new_level_qitems = []

        # LOOP QITEMS IN TOPIC LIST
        for x in list(list_qitems.keys()):
            print ('\n* NOW the qitem: '+x+'\t'+str(list_qitems[x]))

            # LOOP CRAWLING LEVELS
            num_levels = 10
            if topic == 'books': num_levels = 2
            level = 0

            new_level_qitems = []
            new_level_qitems.append(x)

            while (level <= num_levels and len(new_level_qitems)!=0): # Here we choose the number of levels we prefer.

                print ('level: '+str(level))
                level_qitems = new_level_qitems
                new_level_qitems = []

                increment = 100000
#                print ('if len(level_qitems[:increment]) > 0 enters, then: '+str(len(level_qitems[:increment])))
                while len(level_qitems[:increment]) > 0:
                    sample = level_qitems[:increment] # sample we take
                    level_qitems = level_qitems[increment:] # remaining

                    print (len(sample),len(level_qitems))

                    if level > 0:
                        page_asstring = ','.join( ['?'] * len(sample) )
                    else:
                        page_asstring = '?'
                    

                    query = 'SELECT qitem, property, qitem2 FROM instance_of_subclasses_of_properties WHERE qitem2 IN (%s);' % page_asstring

                    i = 0
                    for row in cursor.execute(query,sample):
                        i = i + 1
                        qitem=row[0]
                        wdproperty=row[1]
                        qitem2=row[2]

                        if i < -1:
                            try:
                                print (qitems_page_titles[qitem]+'\t'+qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x])+'\t'+qitems_page_titles[qitem2])
                            except:
                                print ('not in english.'+'\t'+qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x]))

                        """
                        if qitem == 'Q16773055':
                            print ('Data_aggregation')
                            print (qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x])+'\t'+qitems_page_titles[qitem2])
                            input('')
                        """

                        if qitem not in topic_dict_all:
                            new_level_qitems.append(qitem)
                            topic_dict_all[qitem]=[wdproperty,qitem2]

#                    print ('end of iteration')
#                    print (len(level_qitems[:increment])) # next sample we would take


                print ('*** At level '+str(level)+', the number of search results is '+str(i)+' and NEW elements gathered is '+str(len(new_level_qitems))+' and the total number of accumulated elements is '+str(len(topic_dict_all)))

                level = level + 1
                print ('ready for starting '+str(level))


        print ('\nThe total number of gathered elements is: '+str(len(topic_dict_all)))
        print ('We are done with the levels for this topic: '+topic+'\n')

        parameters = []
        for qitem, values in topic_dict_all.items():
            parameters.append((qitem,values[0],values[1]))

        query = "INSERT OR IGNORE INTO "+topic+" (qitem, property, qitem2) VALUES (?,?,?)"
        cursor.executemany(query,parameters)
        conn.commit()
        print ('Topic In.')

    print ('all topics introduced into the wikidata database.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


# Checks all the databses and updates the database.
def insert_page_ids_page_titles_qitems_wikipedia_diversity_db():

    function_name = 'insert_page_ids_page_titles_qitems_wikipedia_diversity_db'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:

        print (languagecode)
        
        # get all the articles from the last month version of the db
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        page_ids_qitems = {}
        for page_title, page_id in page_titles_page_ids.items():
            page_ids_qitems[page_id]=page_titles_qitems[page_title]
        page_ids = list(page_ids_qitems.keys())

        # get all articles from wikidata / page_title_qitems_wd
        page_titles_qitems_wd={}
        query = 'SELECT page_title, qitem FROM sitelinks WHERE langcode = "'+languagecode+'wiki";'
        for row in cursor.execute(query):
            page_title=row[0].replace(' ','_')
            page_titles_qitems_wd[page_title]=row[1]
        print (len(page_titles_qitems_wd))
        print ('qitems obtained.')
        # IMPORTANT: not all Articles (page_namespace=0 and page_is_redirect=0) from every Wikipedia have a Qitem related, as sometimes the link is not created. This is relevant for very small Wikipedias.

        # get all articles from dump
        # create parameters (page_id, page_title, qitem)
        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
        dump_in = gzip.open(dumps_path, 'r')
        parameters = []; parameters2 = []
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
                    page_id = int(row[0])
                    page_namespace = int(row[1])
                    if page_namespace != 0 or page_is_redirect!=0: continue
                    page_title = str(row[2].decode('utf-8'))
                    page_is_redirect = int(row[4])
                    page_len = int(row[10])

                    try:
                        parameters.append((num_bytes,page_title,page_id,page_titles_qitems_wd[page_title]))
                    except:
                        parameters2.append((num_bytes,page_title,page_id,None))

                    page_ids.remove(page_id)

        print ('done with the dump.')

        # Update / Insert the articles and the number of bytes.
        query = 'UPDATE '+languagecode+'wiki SET num_bytes=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,parameters)
        conn.commit()
        
        query = 'UPDATE '+languagecode+'wiki SET num_bytes=? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,parameters2)
        conn.commit()

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (num_bytes, page_id, qitem) VALUES (?,?,?);'
        cursor2.executemany(query,parameters)
        conn2.commit()
        
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (num_bytes, page_id, qitem) VALUES (?,?,?);'
        cursor2.executemany(query,parameters2)
        conn2.commit()


        # Delete articles that are not in the dump
        parameters_purge = []
        for page_id in page_ids:
            parameters_purge.append((page_id,page_ids_qitems[page_id]))

        # delete those without a qitem
        query = 'DELETE FROM '+languagecode+'wiki WHERE page_id = ? AND qitem = ?;'
        cursor2.executemany(query,parameters_purge)
        conn2.commit()
        print(str(len(parameters_purge))+' articles purged from the last cycle.')

        print ('articles for this language are in and updated: '+languagecode+'\n')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



def insert_geolocation_wd():

    function_name = 'insert_geolocation_wd'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()  
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()  

    country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()

    qitem_geolocation = {}
    qitem_iso3166 = {}
    query = "SELECT qitem, coordinates, iso3166 FROM geolocated_property;"   # (qitem text, property text, coordinates text, admin1 text, iso3166 text)
    for row in cursor.execute(query):
        qitem=row[0]
        qitem_geolocation[qitem]=row[1]
        qitem_iso3166[qitem]=row[2]

    for languagecode in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

        parameters=[]
        for page_title, qitem in page_titles_qitems.items():
            try:
                iso3166 = qitem_iso3166[qitem]
                parameters.append((qitem_geolocation[qitem],iso3166,regions[iso3166],qitem,page_titles_page_ids[page_title]))
                # print ((qitem_geolocation[qitem],iso3166,regions[iso3166],qitem,page_titles_page_ids[page_title]))
            except:
                pass

        cursor2.executemany("UPDATE "+languagecode+"wiki SET geocoordinates = ?, iso3166 = ?, region = ? WHERE qitem = ? AND page_id = ?", parameters)
        conn2.commit()

        print (languagecode,len(parameters))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)
    

def delete_latest_wikidata_dump():
    function_name = 'delete_latest_wikidata_dump'
    functionstartTime = time.time()

    if create_function_account_db(function_name, 'check','')==1: return
    os.remove(dumps_path + "latest-all.json.gz")

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)

# OUTDATED
def copy_ccc_temp_to_another():

    print ('coyping wikipedia_diversity to another .db')

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + 'wikipedia_diversity_backup.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

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
        # from links to/from non-ccc geolocated Articles.
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

        'FROM '+languagecode+'wiki;')

#        print (query)
        parameters = []

        for row in cursor.execute(query):
            parameters.append(tuple(row))

        # INSERTING
        page_asstring = ','.join( ['?'] * (query.count(',')+1)) 
        query = ('INSERT INTO '+languagecode+'wiki ('+
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
        # from links to/from non-ccc geolocated Articles.
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



def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WCDO - WIKIPEDIA DIVERSITY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/diversity_wikipedia.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WCDO - WIKIPEDIA DIVERSITY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue



#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("diversity_wikipedia"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("diversity_wikipedia"+".err", "w")
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


    # WIKIDATA DIVERSITY PROPERTIES
#    while True:
#    time.sleep(84600)
#    print ("Good morning. It is: "+time.today()+". Let's see if today is the day to download the Wikipedia Diversity data and start another entire cycle...")

    startTime = time.time()

    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
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
    
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)]
    biggest = wikilanguagecodes_by_size[:20]; smallest = wikilanguagecodes_by_size[20:]

    allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties, industry_properties, instance_of_subclasses_of_properties,sexual_orientation_properties,religion_properties,race_and_ethnia_properties,time_properties = wd_properties()

    print ('* Starting the WIKIPEDIA DIVERSITY CYCLE '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now()))

    main()
#    main_with_exception_email()
#    main_loop_retry()

    finishTime = time.time()
    print ('* Done with the WIKIPEDIA DIVERSITY CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'diversity_wikipedia.out', 'WIKIPEDIA DIVERSITY')
