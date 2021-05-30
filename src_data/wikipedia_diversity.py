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
# classifier
from sklearn import svm, linear_model
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# Twice the same table in a short period of time not ok.
# Load all page_titles from all languages is not ok.
import gc


# MAIN
def main():

    wd_dump_iterator()
    print ('dump iterated')

    wd_geolocated_update_db()
    wd_instance_of_subclass_of_property_crawling()

    create_wikipedia_diversity_db()
    insert_page_ids_page_titles_qitems_wikipedia_diversity_db()

    store_ethnic_groups_wd_diversity_categories_db()
    store_religious_groups_wd_diversity_categories_db()


################################################################

# Creates a CCC database for a list of languages.
def create_wikipedia_diversity_db():

    function_name = 'create_wikipedia_diversity_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    try:
        os.remove(databases_path + wikipedia_diversity_db); print (wikipedia_diversity_db+' deleted.');
    except:
        pass

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

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
        'date_last_edit integer, '+
        'date_last_discussion integer, '+
        'first_timestamp_lang text,'+ # language of the oldest timestamp for the article

        # GEOGRAPHY DIVERSITY
        # set as geography diversity features:
        'geographical_location text,'+
        'territorial_entity text,'+
        'country_qitem text, '+
        'geocoordinates text, '+ # coordinate1,coordinate2
        'iso3166 text, '+ # code
        'iso31662 text, '+ # code
        'region text, '+ # continent

        # PEOPLE DIVERSITY
        # set as people diversity features:
        'gender text, '+
        'ethnic_group text, '+
        'supra_ethnic_group text, '+
        'sexual_orientation_property text, '+ # wikidata properties
        'sexual_orientation_partner text, '+ # wikidata properties
        'religious_group text, '+ # wikidata religious adscription for people
        'occupation_and_field text, '+ 

            # from links to/from women articles.
        'num_inlinks_from_women integer, '+
        'num_outlinks_to_women integer, '+
        'percent_inlinks_from_women real, '+
        'percent_outlinks_to_women real, '+

            # from links to/from men articles.
        'num_inlinks_from_men integer, '+
        'num_outlinks_to_men integer, '+
        'percent_inlinks_from_men real, '+
        'percent_outlinks_to_men real, '+

            # from links to/from LGBT+ biographies.
        'num_inlinks_from_lgbt integer, '+
        'num_outlinks_to_lgbt integer, '+
        'percent_inlinks_from_lgbt real, '+
        'percent_outlinks_to_lgbt real, '+

            # from links to/from LGBT+ articles.
        'num_inlinks_from_ccc_wikiprojects integer, '+
        'percent_inlinks_from_ccc_wikiprojects real, '+


        # CULTURAL CONTEXT DIVERSITY TOPICS
        # calculations:
        'ccc_binary integer, '+
        'ccc text, '+
        'missing_ccc text, '+
        'main_territory text, '+ # Q from the main territory with which is associated.
        'num_retrieval_strategies integer, '+ # this is a number

        # set as CCC features:
        'ccc_geolocated integer,'+ # 1, -1 o null.
        'country_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'location_wd text, '+ # 'P1:QX1:Q; P2:QX2:Q' Q is the main territory
        'language_strong_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'created_by_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'part_of_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'keyword_title text, '+ # 'QX1;QX2'

        # retrieved as potential CCC features:
        'category_crawling_territories text, '+ # 'QX1;QX2'
        'category_crawling_territories_level integer, '+ # 'level'
        'language_weak_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'affiliation_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'has_part_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'num_inlinks_from_CCC integer, '+
        'num_outlinks_to_CCC integer, '+
        'percent_inlinks_from_CCC real, '+
        'percent_outlinks_to_CCC real, '+

        # retrieved as potential other CCC (part of other CCC) features:
            # from wikidata properties
        'other_ccc_country_wd integer, '+
        'other_ccc_location_wd integer, '+
        'other_ccc_language_strong_wd integer, '+
        'other_ccc_created_by_wd integer, '+
        'other_ccc_part_of_wd integer, '+
        'other_ccc_language_weak_wd integer, '+
        'other_ccc_affiliation_wd integer, '+
        'other_ccc_has_part_wd integer, '+

            # from links to/from non-ccc geolocated Articles.
        'num_inlinks_from_geolocated_abroad integer, '+
        'num_outlinks_to_geolocated_abroad integer, '+
        'percent_inlinks_from_geolocated_abroad real, '+
        'percent_outlinks_to_geolocated_abroad real, '+



        # GENERAL TOPICS DIVERSITY
        # topics: people, organizations, things and places
        'folk text, '+
        'earth text, '+
        'monuments_and_buildings text, '+
        'music_creations_and_organizations text, '+
        'sport_and_teams text, '+
        'food text, '+
        'paintings text, '+
        'glam text, '+
        'books text, '+
        'clothing_and_fashion text, '+
        'industry text, '+
        'religion text, '+ # as a topic
        'medicine text, '+


        'time_interval text, '+
        'start_time integer, '+
        'end_time integer, '+

        # other diversity topics
        'ethnic_group_topic text, '+
        'lgbt_topic integer, '+
        'lgbt_keyword_title text, '+

        # characteristics of article relevance
        'num_bytes integer, '+
        'num_references integer, '+
        'num_images integer, '+

        'num_inlinks integer, '+
        'num_outlinks integer, '+

        'num_edits integer, '+
        'num_edits_last_month integer, '+
        'num_discussions integer, '+

        'num_anonymous_edits integer, '+
        'num_bot_edits integer, '+
        'num_reverts integer, '+

        'num_editors integer, '+
        'num_admin_editors integer, '+


        'median_year_first_edit integer, '+
        'median_editors_edits integer, '+

        'num_pageviews integer, '+
        'num_interwiki integer, '+

        'sister_projects text, '+
        'num_multilingual_sisterprojects integer, '+
        'num_wdproperty integer, '+
        'num_wdidentifiers integer, '+

        # quality
        'featured_article integer, '+
        'wikirank real, '+


        'PRIMARY KEY (qitem,page_id));')

        try:
            cursor.execute(query)
            conn.commit()
            print ('Created the Wikipedia Diversity OBSERVATORY table for language: '+languagecode)
        except:
            print (languagecode+' already has a Wikipedia Diversity OBSERVATORY table.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def create_wikidata_db():

    conn = sqlite3.connect(databases_path + wikidata_db)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS sitelinks (qitem text, langcode text, page_title text, sisterprojects text, PRIMARY KEY (qitem, langcode));")

    cursor.execute("CREATE TABLE IF NOT EXISTS labels (qitem text, langcode text, label text, PRIMARY KEY (qitem, langcode));")

    cursor.execute("CREATE TABLE IF NOT EXISTS metadata (qitem text, properties integer, sitelinks integer, wd_identifiers integer, sisterprojects_sitelinks integer, PRIMARY KEY (qitem));")



    # GEOGRAPHY
    cursor.execute("CREATE TABLE IF NOT EXISTS geolocated_property (qitem text, property text, coordinates text, admin1 text, iso3166 text, PRIMARY KEY (qitem, coordinates));")


    cursor.execute("CREATE TABLE IF NOT EXISTS geographical_location (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    cursor.execute("CREATE TABLE IF NOT EXISTS territorial_entity (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")


    # CCC
    cursor.execute("CREATE TABLE IF NOT EXISTS language_strong_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS country_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS location_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS created_by_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS part_of_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS language_weak_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS has_part_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS affiliation_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    # PEOPLE
    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS people_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS sexual_orientation_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS religious_group_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS ethnic_group_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")

    cursor.execute("CREATE TABLE IF NOT EXISTS occupation_and_field_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")


    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS time_properties (qitem text, property text, value text, PRIMARY KEY (qitem, property, value));")
    cursor.execute("CREATE TABLE IF NOT EXISTS time_interval (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    ###

    cursor.execute("CREATE TABLE IF NOT EXISTS instance_of_subclasses_of_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")


    # GENERAL TOPICS
    ###
    cursor.execute("CREATE TABLE IF NOT EXISTS industry_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS medicine_properties (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")


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
    cursor.execute("CREATE TABLE IF NOT EXISTS religion (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")
    cursor.execute("CREATE TABLE IF NOT EXISTS medicine (qitem text, property text, qitem2 text, PRIMARY KEY (qitem, qitem2));")



    ###
    print ('Created the Wikidata sqlite3 file and tables.')
    return conn



# Sets the ccc_binary to zero for another cycle.
def switch_ccc_binary_to_ccc_last_cycle_binary():
    function_name = 'switch_ccc_binary_to_ccc_last_cycle_binary'
    if wikilanguages_utils.verify_function_run(script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        query = 'UPDATE '+languagecode+'wiki SET ccc_binary_last_cycle = ccc_binary;'
        cursor.execute(query)

        query = 'UPDATE '+languagecode+'wiki SET ccc_binary = NULL;'
        cursor.execute(query)
    conn.commit()
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(script_name, function_name, 'mark', duration)



def wd_properties():

    # Properties to be used in wp_dump_iterator
    allproperties={}

    # GEOGRAPHY AND CCC
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

    # TOPICS
    industry_properties = {'P452':'industry'}; allproperties.update(industry_properties);
    medicine_properties = {'P927':'anatomical location', 'P1060':'pathogen transmission process', 'P2176':'drug used for treatment', 'P923':'possible treatment','P923':'medical examinations', 'P780':'symptoms', 'P1995':'health specialty'}; allproperties.update(medicine_properties);





    # PEOPLE
    # other types of diversity
    people_properties = {'P31':'instance of','P21':'sex or gender'}; allproperties.update(people_properties); # obtain people and groups

    sexual_orientation_properties = {'P91':'sexual orientation', 'P26':'spouse', 'P451': 'partner'}; allproperties.update(sexual_orientation_properties);
    religious_group_properties = {'P140': 'religion', 'P708': 'diocese'}; allproperties.update(religious_group_properties);
    ethnic_group_properties = {'P172': 'ethnic group'}; allproperties.update(ethnic_group_properties);

    occupation_and_field_properties = {'P106':'occupation', 'P101':'field of work'}; allproperties.update(occupation_and_field_properties);


    # OTHER
    time_properties = {'P569': 'date of birth', 'P570': 'date of death', 'P580':'start time', 'P582': 'end time', 'P577': 'publication date', 'P571': 'inception', 'P1619': 'date of official opening', 'P576':'dissolved, abolished or demolished', 'P1191': 'date of first performance', 'P813': 'retrieved', 'P1249': 'time of earliest written record', 'P575':'time of discovery or invention'}; allproperties.update(time_properties);

    # all instances of and subclasses of
    instance_of_subclasses_of_properties = {'P31':'instance of','P279':'subclass of'}; allproperties.update(instance_of_subclasses_of_properties); # obtain people and groups

    return allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties, industry_properties, medicine_properties, occupation_and_field_properties, instance_of_subclasses_of_properties,sexual_orientation_properties,religious_group_properties,ethnic_group_properties,time_properties



def wd_dump_iterator():
    function_name = 'wd_dump_iterator'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    try: os.remove(databases_path + wikidata_db);
    except: pass;

    conn = create_wikidata_db(); cursor = conn.cursor()
    conn.commit()

    option = 'read'
    if option == 'download':
        print ('* Downloading the latest Wikidata dump.')
        url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz" # download the dump: https://dumps.wikimedia.org/wikidatawiki/entities/20180212/
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter
        r = requests.get(url, stream=True)
        with open(dumps_path + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=10240): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
        read_dump = databases_path + local_filename

    if option == 'copy':
        filename = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
        local_filename = url.split('/')[-1]
        shutil.copyfile(filename, dumps_path + local_filename)
        read_dump = databases_path + local_filename
        print ('Wikidata Dump copied.')


    if option == 'read': # 8 hours to process the 2% when read from the other server. sembla que hi ha un problema i és que llegir el dump és més lent que descarregar-lo.
        read_dump = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'

    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    # return

    wikilanguages_utils.check_dump(read_dump, script_name)

    dump_in = gzip.open(read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    n_qitems = 84601660
 
    sitelinks_values = []
    labels_values = []
    metadata_list = []; geolocated_property_list = []; time_properties_list = []; language_strong_properties_list = []; language_weak_properties_list = []; country_properties_list = []; location_properties_list = []; has_part_properties_list = []; affiliation_properties_list = []; created_by_properties_list = []; part_of_properties_list = []; industry_properties_list = []; medicine_properties_list = []; sexual_orientation_properties_list = []; religious_group_properties_list = []; ethnic_group_properties_list = []; occupation_and_field_properties_list = []; people_properties_list = []; instance_of_subclasses_of_properties_list = []

    print ('Iterating the dump.')
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

        sitelinks = []
        wd_sitelinks = entity['sitelinks']
        if len(wd_sitelinks) == 0: continue

        if ':' in list(wd_sitelinks.values())[0]: continue # it means it is a category or any other type of page.

      
        lang_sisters = ''

        # SITELINKS
        for code, title in sorted(wd_sitelinks.items(), reverse=True):
            # print (code, title)
            # input('')
#            if code in wikilanguagecodes: 
            
            cd = code.split('wik')
            l = cd[0]
            p = 'wik'+cd[1]

            if p == 'wiki':
#            if code in wikilanguagecodeswiki: # wikipedia article
                sitelinks_values.append((qitem,code,title['title'],lang_sisters))
                sitelinks.append(code)
                lang_sisters = ''
            else:
                lang_sisters+= p+';'


        sisterprojects_sitelinks = len(wd_sitelinks) - len(sitelinks)

        # LABELS
        if len(sitelinks) != 0:           
            for code, title in entity['labels'].items(): # bucle de llengües
                code = code + 'wiki'
                if code not in wd_sitelinks and code in wikilanguagecodeswiki:
                    labels_values.append((qitem,code,title['value']))


        # PROPERTIES
    #    print ([qitem,len(claims),len(entity['sitelinks'])])
        claims = entity['claims']
        identifiers = []

        # properties
        for wdproperty, claimlist in claims.items():
            try:
                if claimlist[0]['mainsnak']['datatype'] == 'external-id': identifiers.append(wdproperty)
            except:
                pass
            if wdproperty not in allproperties: continue

            for snak in claimlist:
                mainsnak = snak['mainsnak']

                if wdproperty in geolocated_property:
                    try:
                        coordinates = str(mainsnak['datavalue']['value']['latitude'])+','+str(mainsnak['datavalue']['value']['longitude'])
                    except:
                        continue

                    geolocated_property_list.append((qitem,wdproperty,coordinates))
                    continue

                if wdproperty in time_properties:
                    try:
                        value = str(mainsnak['datavalue']['value']['time'])
                    except:
                        continue

                    value = int(value[0]+value[1:].split('-')[0])
                    time_properties_list.append((qitem,wdproperty,value))
                    continue

                # the rest of properties
                try:
                    qitem2 = 'Q{}'.format(mainsnak['datavalue']['value']['numeric-id'])
                except:
                    continue

                if wdproperty in language_strong_properties:
    #                print ('language properties')
    #                print (qitem,wdproperty,qitem2)
                    language_strong_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in language_weak_properties:
    #                print ('language properties')
    #                print (qitem,wdproperty,qitem2)
                    language_weak_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in country_properties:
    #                print ('country properties')
    #                print (qitem,wdproperty,qitem2)
                    country_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in location_properties:
    #                print ('location properties')
    #                print (qitem,wdproperty,qitem2)
                    location_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in has_part_properties:
    #                print ('has part properties')
    #                print (qitem,wdproperty,qitem2)
                    has_part_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in affiliation_properties:
    #                print ('affiliation_properties')
    #                print (qitem,wdproperty,qitem2)
                    affiliation_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in created_by_properties:
    #                print ('created by properties')
    #                print (qitem,wdproperty,qitem2)
                    created_by_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in part_of_properties:
    #                print ('part of properties')
    #                print (qitem,wdproperty,qitem2)
                    part_of_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in industry_properties:
    #                print ('industry properties')
    #                print (qitem,wdproperty,qitem2)
                    industry_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in medicine_properties:
    #                print ('medicine_properties')
    #                print (qitem,wdproperty,qitem2)
                    medicine_properties_list.append((qitem,wdproperty,qitem2))
                    continue


                if wdproperty in sexual_orientation_properties:
    #                print ('sexual_orientation_properties')
    #                print (qitem,wdproperty,qitem2)
                    sexual_orientation_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in religious_group_properties:
    #                print ('religious_group_properties')
    #                print (qitem,wdproperty,qitem2)
                    religious_group_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in ethnic_group_properties:
    #                print ('ethnic_group_properties')
    #                print (qitem,wdproperty,qitem2)
                    ethnic_group_properties_list.append((qitem,wdproperty,qitem2))
                    continue

                if wdproperty in occupation_and_field_properties:
    #                print ('occupation_and_field_properties')
    #                print (qitem,wdproperty,qitem2)
                    occupation_and_field_properties_list.append((qitem,wdproperty,qitem2))
                    continue


                if wdproperty in people_properties:
                    if wdproperty == 'P21' or (wdproperty == 'P31' and qitem2 == 'Q5'):
        #                print ('people properties')
        #                print (qitem,wdproperty,qitem2)
                        people_properties_list.append((qitem,wdproperty,qitem2))
                        continue

                if wdproperty in instance_of_subclasses_of_properties:
                    if wdproperty == 'P31' and qitem2 == 'Q5': continue # if human, continue
                    # values = [qitem,wdproperty,qitem2]
    #                print ('instance_of_subclasses_of_properties properties')
    #                print (qitem,wdproperty,qitem2)
                    instance_of_subclasses_of_properties_list.append((qitem,wdproperty,qitem2))
                    continue


        # meta info
        metadata_list.append((qitem,len(claims),len(sitelinks)-1, len(identifiers), sisterprojects_sitelinks))
        # print ((qitem,len(claims),len(sitelinks)-1, len(identifiers), sisterprojects_sitelinks))
        # input('')


#        if iter % 850000 == 0:
        if iter % 850000 == 0:
            # insert
            cursor.executemany("INSERT INTO sitelinks (qitem, langcode, page_title, sisterprojects) VALUES (?,?,?,?)",sitelinks_values)

            cursor.executemany("INSERT INTO labels (qitem, langcode, label) VALUES (?,?,?)",labels_values)

            cursor.executemany("INSERT OR IGNORE INTO metadata (qitem, properties, sitelinks, wd_identifiers, sisterprojects_sitelinks) VALUES (?,?,?,?,?)", metadata_list)

            cursor.executemany("INSERT OR IGNORE INTO geolocated_property (qitem, property, coordinates) VALUES (?,?,?)",geolocated_property_list)
            cursor.executemany("INSERT OR IGNORE INTO time_properties (qitem, property, value) VALUES (?,?,?)",time_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO language_strong_properties (qitem, property, qitem2) VALUES (?,?,?)",language_strong_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO language_weak_properties (qitem, property, qitem2) VALUES (?,?,?)",language_weak_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO country_properties (qitem, property, qitem2) VALUES (?,?,?)",country_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO location_properties (qitem, property, qitem2) VALUES (?,?,?)",location_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO has_part_properties (qitem, property, qitem2) VALUES (?,?,?)",has_part_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO affiliation_properties (qitem, property, qitem2) VALUES (?,?,?)",affiliation_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO created_by_properties (qitem, property, qitem2) VALUES (?,?,?)",created_by_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO part_of_properties (qitem, property, qitem2) VALUES (?,?,?)",part_of_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO industry_properties (qitem, property, qitem2) VALUES (?,?,?)",industry_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO medicine_properties (qitem, property, qitem2) VALUES (?,?,?)",medicine_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO sexual_orientation_properties (qitem, property, qitem2) VALUES (?,?,?)",sexual_orientation_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO religious_group_properties (qitem, property, qitem2) VALUES (?,?,?)",religious_group_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO ethnic_group_properties (qitem, property, qitem2) VALUES (?,?,?)",ethnic_group_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO occupation_and_field_properties (qitem, property, qitem2) VALUES (?,?,?)",occupation_and_field_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO people_properties (qitem, property, qitem2) VALUES (?,?,?)",people_properties_list)
            cursor.executemany("INSERT OR IGNORE INTO instance_of_subclasses_of_properties (qitem, property, qitem2) VALUES (?,?,?)",instance_of_subclasses_of_properties_list)
            conn.commit()

            sitelinks_values = []
            labels_values = []
            metadata_list = []; geolocated_property_list = []; time_properties_list = []; language_strong_properties_list = []; language_weak_properties_list = []; country_properties_list = []; location_properties_list = []; has_part_properties_list = []; affiliation_properties_list = []; created_by_properties_list = []; part_of_properties_list = []; industry_properties_list = []; medicine_properties_list = []; sexual_orientation_properties_list = []; religious_group_properties_list = []; ethnic_group_properties_list = []; occupation_and_field_properties_list = []; people_properties_list = []; instance_of_subclasses_of_properties_list = []

            print (iter)
            print (100*iter/n_qitems)
            print ('current time: ' + str(time.time() - functionstartTime))
            print ('number of line per second: '+str(iter/(time.time() - functionstartTime)))
#            break


    # last round 
    # insert
    cursor.executemany("INSERT OR IGNORE INTO sitelinks (qitem, langcode, page_title, sisterprojects) VALUES (?,?,?,?)",sitelinks_values)  

    cursor.executemany("INSERT OR IGNORE INTO labels (qitem, langcode, label) VALUES (?,?,?)",labels_values)

    cursor.executemany("INSERT OR IGNORE INTO metadata (qitem, properties, sitelinks, wd_identifiers, sisterprojects_sitelinks) VALUES (?,?,?,?,?)", metadata_list)

    cursor.executemany("INSERT OR IGNORE INTO geolocated_property (qitem, property, coordinates) VALUES (?,?,?)",geolocated_property_list)
    cursor.executemany("INSERT OR IGNORE INTO time_properties (qitem, property, value) VALUES (?,?,?)",time_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO language_strong_properties (qitem, property, qitem2) VALUES (?,?,?)",language_strong_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO language_weak_properties (qitem, property, qitem2) VALUES (?,?,?)",language_weak_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO country_properties (qitem, property, qitem2) VALUES (?,?,?)",country_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO location_properties (qitem, property, qitem2) VALUES (?,?,?)",location_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO has_part_properties (qitem, property, qitem2) VALUES (?,?,?)",has_part_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO affiliation_properties (qitem, property, qitem2) VALUES (?,?,?)",affiliation_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO created_by_properties (qitem, property, qitem2) VALUES (?,?,?)",created_by_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO part_of_properties (qitem, property, qitem2) VALUES (?,?,?)",part_of_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO industry_properties (qitem, property, qitem2) VALUES (?,?,?)",industry_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO medicine_properties (qitem, property, qitem2) VALUES (?,?,?)",medicine_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO sexual_orientation_properties (qitem, property, qitem2) VALUES (?,?,?)",sexual_orientation_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO religious_group_properties (qitem, property, qitem2) VALUES (?,?,?)",religious_group_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO ethnic_group_properties (qitem, property, qitem2) VALUES (?,?,?)",ethnic_group_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO occupation_and_field_properties (qitem, property, qitem2) VALUES (?,?,?)",occupation_and_field_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO people_properties (qitem, property, qitem2) VALUES (?,?,?)",people_properties_list)
    cursor.executemany("INSERT OR IGNORE INTO instance_of_subclasses_of_properties (qitem, property, qitem2) VALUES (?,?,?)",instance_of_subclasses_of_properties_list)
    conn.commit()
    conn.close()

    print ('DONE with the JSON.')
    print ('It has this number of lines: '+str(iter))


    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    wikilanguages_utils.store_lines_per_second((time.time() - functionstartTime), iter, function_name, read_dump, cycle_year_month)



# Runs the reverse geocoder and update the database. It needs 5000m.
def wd_geolocated_update_db():

    function_name = 'wd_geolocated_update_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

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
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# fills the tables of qitems about folk, earth, music_creations_and_organizations, etc.
def wd_instance_of_subclass_of_property_crawling():

    function_name = 'wd_instance_of_subclass_of_property_crawling'
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()


    # query = 'DELETE FROM instance_of_subclasses_of_properties WHERE qitem NOT IN (SELECT DISTINCT qitem FROM sitelinks);' Delete qitems with no Wikipedia articles. Such a bad idea, there are middle classes such as 'football club' or 'professional sports team' with no Wikipedia article. Therefore, some items are missing.

    """
    query = 'DELETE FROM instance_of_subclasses_of_properties WHERE qitem2 = "Q5";'
    cursor.execute(query)
    conn.commit()
    print ('DELETED.')
    """

    geographical_location = {'Q2221906':'geographical location'}
    territorial_entity = {'Q15617994':'designation for an administrative territorial entity','Q104098715':'territorial_entity type','Q1048835':'political territorial entity','Q1496967':'territorial entity'}


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

    # other
    religion = {'Q9174':'religion'}
    time_interval = {'Q186081': 'time interval', 'Q1790144': 'unit of time'}

    medicine = {'Q843601':'health science', 'Q44597158':'health specialty', 'Q12136':'disease', 'Q808':'virus','Q86746756':'medicinal product'}


    # LOOP TOPICS
    topics_dict = {'folk':folk,'monuments_and_buildings':monuments_and_buildings,'earth':earth,'music_creations_and_organizations':music_creations_and_organizations,'sport_and_teams':sport_and_teams,'food':food,'paintings':paintings,'glam':glam,'books':books,'clothing_and_fashion':clothing_and_fashion, 'religion':religion, 'time_interval': time_interval, 'medicine': medicine, 'geographical_location':geographical_location, 'territorial_entity':territorial_entity}


#    topics_dict = {'medicine': medicine}


    # (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,'en')
    # qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}


    print (topics_dict)
 
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
            if topic == 'time_interval': num_levels = 2
            level = 0

            new_level_qitems = []
            new_level_qitems.append(x)

#            print (new_level_qitems)
#            input('')

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


                        # if i > -1:
                        #     try:
                        #         print (qitems_page_titles[qitem]+'\t'+qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x])+'\t'+qitems_page_titles[qitem2])
                        #     except:
                        #         print ('not in english.'+'\t'+qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x]))




                        """
                        if qitem == 'Q16773055':
                            print ('Data_aggregation')
                            print (qitem+'\t'+qitem2+'\tlevel '+str(level)+'\t'+str(list_qitems[x])+'\t'+qitems_page_titles[qitem2])
                            input('')
                        """

                        if qitem not in topic_dict_all:
                            new_level_qitems.append(qitem)
                            topic_dict_all[qitem]=[wdproperty,qitem2]



                    print ('end of iteration')
                    print (len(level_qitems[:increment])) # next sample we would take


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

    print ('topics introduced into the wikidata database.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# Checks all the databses and updates the database.
def insert_page_ids_page_titles_qitems_wikipedia_diversity_db():

    function_name = 'insert_page_ids_page_titles_qitems_wikipedia_diversity_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    try: os.remove(databases_path + wikipedia_namespace_db)
    except: pass
    conn3 = sqlite3.connect(databases_path + wikipedia_namespace_db); cursor3 = conn3.cursor()

 


    for languagecode in wikilanguagecodes:

        print (languagecode)

        cursor3.execute("CREATE TABLE IF NOT EXISTS "+languagecode+"wiki_wikipedia_namespace (page_id text, page_title text, ccc_keyword_title text, PRIMARY KEY (page_id));")       
        conn3.commit()

        # # get all the articles from the last month version of the db
        # (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        # page_ids_qitems = {}
        # for page_title, page_id in page_titles_page_ids.items():
        #     page_ids_qitems[page_id]=page_titles_qitems[page_title]
        # page_ids = list(page_ids_qitems.keys())

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
        wikilanguages_utils.check_dump(dumps_path, script_name)

        dump_in = gzip.open(dumps_path, 'r')
        parameters = []; parameters2 = []; parameters3 = []
        print ('Iterating the dump.')
        iter = 0
        while True:
            line = dump_in.readline()
            try: line = line.decode("utf-8", "ignore") # https://phabricator.wikimedia.org/
            except UnicodeDecodeError: line = str(line)
            iter += 1

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
                    page_id = int(row[0])
                    page_namespace = int(row[1])
                    page_title = str(row[2]) #.decode('utf-8')
                    page_is_redirect = int(row[4])

                    if page_namespace == 4 or page_namespace == 100: # wikipedia namespace: https://en.wikipedia.org/wiki/Wikipedia:Namespace

                    # Wikipedia for Wikiprojects (4) and Portals (100)

                        parameters3.append((page_title,page_id))

                    if page_namespace != 0 or page_is_redirect!=0: continue
                    page_len = int(row[10])

                    try:
                        parameters.append((page_len,page_title,page_id,page_titles_qitems_wd[page_title]))
                    except:
                        parameters2.append((page_len,page_title,page_id,None))

            if iter % 10000 == 0:
                print (iter)
                print ('current time: ' + str(time.time() - functionstartTime))
                print ('number of lines per second: '+str(iter/(time.time() - functionstartTime)))

                    # page_ids.remove(page_id)
        print ('done with the dump.')

        # # Update / Insert the articles and the number of bytes.
        # query = 'UPDATE '+languagecode+'wiki SET num_bytes=? WHERE page_title = ? AND page_id = ? AND qitem = ?;'
        # cursor2.executemany(query,parameters)
        # conn.commit()
        
        # query = 'UPDATE '+languagecode+'wiki SET num_bytes=? WHERE page_title = ? AND page_id = ? AND qitem = ?;'
        # cursor2.executemany(query,parameters2)
        # conn.commit()

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (num_bytes, page_title, page_id, qitem) VALUES (?,?,?,?);'
        cursor2.executemany(query,parameters)
        conn2.commit()
        
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (num_bytes, page_title, page_id, qitem) VALUES (?,?,?,?);'
        cursor2.executemany(query,parameters2)
        conn2.commit()

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_wikipedia_namespace (page_title, page_id) VALUES (?,?);'
        cursor3.executemany(query,parameters3)
        conn3.commit()

        # # Delete articles that are not in the dump
        # parameters_purge = []
        # for page_id in page_ids:
        #     parameters_purge.append((page_id,page_ids_qitems[page_id]))

        # # delete those without a qitem
        # query = 'DELETE FROM '+languagecode+'wiki WHERE page_id = ? AND qitem = ?;'
        # cursor2.executemany(query,parameters_purge)
        # conn2.commit()
        # print(str(len(parameters_purge))+' articles purged from the last cycle.')

        print (len(parameters))
        print (len(parameters2))
        print (len(parameters3))
        print ('articles for this language are in and updated: '+languagecode+'\n')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def store_religious_groups_wd_diversity_categories_db():

    function_name = 'store_religious_groups_wd_diversity_categories_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()

    try:
        cursor2.execute('DROP TABLE religious_groups;')
    except:
        pass
    cursor2.execute("CREATE TABLE IF NOT EXISTS religious_groups (religious_group_qitem text, supra_religious_group_qitem text, religious_group_page_title text, supra_religious_group_page_title text, PRIMARY KEY (religious_group_qitem, supra_religious_group_qitem));")

    qitems_en_page_titles = {}
    query = "SELECT DISTINCT instance_of_subclasses_of_properties.qitem, instance_of_subclasses_of_properties.qitem2 FROM instance_of_subclasses_of_properties INNER JOIN religious_group_properties ON religious_group_properties.qitem2 = instance_of_subclasses_of_properties.qitem AND instance_of_subclasses_of_properties.qitem2 IN ('Q13414953','Q9174','Q6957341','Q19842652','Q1189816','');"


    # SELECT DISTINCT instance_of_subclasses_of_properties.qitem2, count(instance_of_subclasses_of_properties.qitem) FROM instance_of_subclasses_of_properties INNER JOIN religious_group_properties ON religious_group_properties.qitem2 = instance_of_subclasses_of_properties.qitem INNER JOIN sitelinks ON instance_of_subclasses_of_properties.qitem = sitelinks.qitem AND langcode = "cawiki" GROUP BY 1 order by 2 desc LIMIT 50;

    # this is to count suprareligions.

    religious_groups = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]
        religious_groups[qitem] = qitem2 

    qitem_page_titles = {}
    query = "SELECT qitem, page_title FROM sitelinks WHERE langcode = 'enwiki';"
    for row in cursor.execute(query):
        qitem = row[0]
        page_title = row[1]
        qitem_page_titles[qitem]=page_title.replace(' ','_')

    params = []
    for qitem, qitem2 in religious_groups.items():
        try:
            params.append((qitem, qitem2, qitem_page_titles[qitem], qitem_page_titles[qitem2]))
        except:
            pass

    query = 'INSERT OR IGNORE INTO religious_groups (religious_group_qitem, supra_religious_group_qitem, religious_group_page_title, supra_religious_group_page_title) VALUES (?,?,?,?);'
    cursor2.executemany(query,params)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def store_ethnic_groups_wd_diversity_categories_db():

    function_name = 'store_ethnic_groups_wd_diversity_categories_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    # get groups: property of ethnic group requires instance of ethnic group Q41710 or instance of people Q2472587.
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()

    qitems_en_page_titles = {}
    query = "SELECT qitem, qitem2, 1 FROM instance_of_subclasses_of_properties WHERE qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311') ORDER BY qitem;"

    qualifiers = {'Q41710':'ethnic_group','Q2472587':'people','Q1467740':'ethnic_community_(diaspora)','Q231002':'nationality','Q11197007':'ethnoreligious_group','Q133311':'tribe'}

    df = pd.read_sql_query(query, conn)
    df.set_index('qitem')
    df = df.pivot(index='qitem',columns='qitem2', values = '1')
    df = df.rename(columns=qualifiers)
    df = df.fillna(0)
    df = df.reset_index()

    query = "SELECT instance_of_subclasses_of_properties.qitem, sitelinks.page_title FROM instance_of_subclasses_of_properties INNER JOIN sitelinks ON sitelinks.qitem = instance_of_subclasses_of_properties.qitem WHERE sitelinks.langcode = 'enwiki' AND instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311');"
    qitem_page_title_en = {}
    for row in cursor.execute(query):
        qitem_page_title_en[row[0]]=row[1]

    df['page_title_en'] = df['qitem'].map(qitem_page_title_en)

    iso_qitem, label_qitem = load_all_countries_qitems()
    qitem_iso = {v: k for k, v in iso_qitem.items()}

    query = "SELECT country_properties.qitem, country_properties.qitem2 FROM country_properties INNER JOIN instance_of_subclasses_of_properties ON country_properties.qitem = instance_of_subclasses_of_properties.qitem WHERE instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311')"

    country_qitems = {}
    iso3166_qitems = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]

        try:
            iso3166_qitems[qitem]=iso3166_qitems[qitem]+';'+qitem_iso[qitem2]
            country_qitems[qitem]=country_qitems[qitem]+';'+qitem2
        except:
            try:
                iso3166_qitems[qitem]=qitem_iso[qitem2]
                country_qitems[qitem]=qitem2
            except:
                pass

    df['country_qitems'] = df['qitem'].map(country_qitems)
    df['ISO3166'] = df['qitem'].map(iso3166_qitems)


    query = "SELECT religious_group_properties.qitem, religious_group_properties.qitem2 FROM religious_group_properties INNER JOIN instance_of_subclasses_of_properties ON religious_group_properties.qitem = instance_of_subclasses_of_properties.qitem WHERE instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311');"

    religious_group = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]

        try:
            religious_group[qitem]=religious_group[qitem]+';'+religious_group[qitem2]
        except:
            religious_group[qitem]=qitem2

    df['religious_group'] = df['qitem'].map(religious_group)


    query = "SELECT time_interval.qitem, time_interval.qitem2 FROM time_interval INNER JOIN instance_of_subclasses_of_properties ON time_interval.qitem = instance_of_subclasses_of_properties.qitem WHERE instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311') AND time_interval.property IN ('P570','P582','P576');"

    end_time = {}
    for row in cursor.execute(query):
        qitem = row[0]
        value = row[1]

        try:
            end_time[qitem]=end_time[qitem]+';'+end_time[value]
        except:
            end_time[qitem]=value

    df['end_time'] = df['qitem'].map(end_time)

    query = 'SELECT DISTINCT all_languages_wikidata.qitem FROM language_countries_mapping INNER JOIN all_languages_wikidata ON all_languages_wikidata.languageISO3 = language_countries_mapping.language_code_ISO639_3 WHERE language_countries_mapping.language_code_ISO639_3 NOT IN (SELECT language_code_ISO639_3 FROM language_countries_mapping WHERE language_status_code IN ("1","2"));'
    minoritized_languages = {}
    for row in cursor2.execute(query):
        minoritized_languages[row[0]]=1


    query = "SELECT language_weak_properties.qitem, language_weak_properties.qitem2 FROM language_weak_properties INNER JOIN instance_of_subclasses_of_properties ON language_weak_properties.qitem = instance_of_subclasses_of_properties.qitem WHERE instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311')"

    language_used_qitems = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]

        try:
            language_used_qitems[qitem]=language_used_qitems[qitem]+';'+qitem2
        except:
            language_used_qitems[qitem]=qitem2

    df['language_used'] = df['qitem'].map(language_used_qitems)


    query = "SELECT language_strong_properties.qitem, language_strong_properties.qitem2 FROM language_strong_properties INNER JOIN instance_of_subclasses_of_properties ON language_strong_properties.qitem = instance_of_subclasses_of_properties.qitem WHERE instance_of_subclasses_of_properties.qitem2 IN ('Q41710','Q2472587','Q1467740','Q231002','Q11197007','Q133311')"

    language_strong_qitems = {}
    minoritized_language = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]

        try:
            minoritized_language[qitem]=minoritized_languages[qitem2]
        except:
            pass

        try:
            language_strong_qitems[qitem]=language_strong_qitems[qitem]+';'+qitem2
        except:
            language_strong_qitems[qitem]=qitem2

    df['native_language'] = df['qitem'].map(language_strong_qitems)
    df['minoritized_language'] = df['qitem'].map(minoritized_language)
    df = df.fillna('')

    query = "WITH ethnic_groups_list AS (SELECT DISTINCT qitem FROM instance_of_subclasses_of_properties WHERE qitem2 = 'Q41710') SELECT is1.qitem, is1.qitem2 FROM instance_of_subclasses_of_properties is1 WHERE is1.qitem IN ethnic_groups_list AND is1.qitem2 IN ethnic_groups_list;"
    supra_ethnic_group = {}
    for row in cursor.execute(query):
        qitem = row[0]
        qitem2 = row[1]

        supra_ethnic_group[row[0]]=row[1]

    df['supra_ethnic_group'] = df['qitem'].map(supra_ethnic_group)
    df = df.fillna('')

    # indigenous = minoritized language, no nationality.

    try:
        cursor2.execute("DROP TABLE ethnic_groups;")
    except:
        pass

    df.to_sql('ethnic_groups', conn2, index = False);
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


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
            path = '/srv/wcdo/src_data/wikipedia_diversity.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WDO - WIKIPEDIA DIVERSITY OBSERVATORY ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue


#######################################################################################
class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("wikipedia_diversity"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("wikipedia_diversity"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    startTime = time.time()

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'wikipedia_diversity.py'


    cycle_year_month = wikilanguages_utils.get_new_cycle_year_month() 
#    check_time_for_script_run(script_name, cycle_year_month)

    # Verify whether there is a new language or not
    wikilanguages_utils.extract_check_new_wiki_projects();

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

    allproperties, geolocated_property, language_strong_properties, country_properties, location_properties, created_by_properties, part_of_properties, language_weak_properties, has_part_properties, affiliation_properties, people_properties, industry_properties, medicine_properties, occupation_and_field_properties, instance_of_subclasses_of_properties,sexual_orientation_properties,religious_group_properties,ethnic_group_properties,time_properties = wd_properties()

#    if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()

    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))

#    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)
    

    print ('* Done with the WIKIPEDIA DIVERSITY OBSERVATORY CYCLE completed successfuly after: ' + str(duration))
    wikilanguages_utils.finish_email(startTime,'wikipedia_diversity.out', 'WIKIPEDIA DIVERSITY OBSERVATORY')