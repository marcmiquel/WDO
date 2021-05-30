update_pull_missing_ccc_wikipedia_diversity# -*- coding: utf-8 -*-

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


# Twice the same table in a short period of time not ok.
# Load all page_titles from all languages is not ok.
import gc

# MAIN
def main():


    # create_wikipedia_missing_ccc_db()
#    wikilanguagecodes = ['es','en','fr']

    
    for languagecode in wikilanguagecodes[wikilanguagecodes.index('sk'):]:
        print('\n* '+languages.loc[languagecode]['languagename']+' '+languagecode)

        try:
            languagecodes_higher = pairs.loc[languagecode]['wikimedia_higher']

            if isinstance(languagecodes_higher,str): languagecodes_higher = [languagecodes_higher]
            else: languagecodes_higher = languagecodes_higher.unique()

            if len(languagecodes_higher) == 0: print (languages.loc[languagecode]['languagename']+' language is not overlapped with other Wikipedia languages of higher status.')
            else:
                print ('There is overlap with these languages: ')
                print (languagecodes_higher)               
        except:
            print (languages.loc[languagecode]['languagename']+' language is not overlapped with other Wikipedia languages of higher status.')
            languagecodes_higher = []

        # if len(languagecodes_higher) > 0:
        #     label_missing_ccc_articles_keywords(languagecode,languagecodes_higher)

        # # WIKIDATA PROPERTIES
        # label_missing_ccc_articles_geolocation_wd(languagecode,languagecodes_higher)
        # label_missing_ccc_articles_country_wd(languagecode,languagecodes_higher)
        # label_missing_ccc_articles_location_wd(languagecode,languagecodes_higher)
        # label_missing_ccc_articles_language_strong_wd(languagecode,languagecodes_higher)

        # # DEPENDENT ON THE PREVIOUS RESULTS
        # label_missing_ccc_articles_part_of_properties_wd(languagecode,languagecodes_higher)
        # label_missing_ccc_articles_created_by_properties_wd(languagecode,languagecodes_higher)

        # # INTRODUCE THE FEATURES OF THE ORIGINAL LANGUAGE AND OTHERS
        # copy_features_into_missing_ccc_from_original_lang(languagecode)

        copy_features_into_missing_ccc_from_language_pairs(languagecode, languagecodes_higher)
        copy_features_into_missing_ccc_from_non_language_pairs(languagecode, languagecodes_higher)

        # INTRODUCE THE LABELS
        # copy_labels_into_missing_ccc(languagecode, languagecodes_higher)


    # # EXTEND LINKS TO AND FROM ORIGINAL CCC
    # for languagecode in wikilanguagecodes:
    #     print (languagecode)
    #     (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
    #     extend_links_from_to_original_ccc(languagecode,page_titles_page_ids,page_titles_qitems)


    update_push_missing_ccc_wikipedia_diversity()

    
    wikilanguages_utils.copy_db_for_production(missing_ccc_db, 'missing_ccc_selection.py', databases_path)


################################################################



def create_wikipedia_missing_ccc_db():

    function_name = 'create_wikipedia_missing_ccc_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + missing_ccc_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        # Checks whether the Wikipedia currently exists.
        query = ('CREATE TABLE IF NOT EXISTS '+languagecode+'wiki ('+

        # general
        'qitem text NOT NULL, '+
        'languagecode text NOT NULL, '+
        'non_language_pairs integer,'+
        'page_id integer NOT NULL, '+
        'page_title text, '+

        'label text,'+
        'label_lang text,'+

        'geocoordinates text, '+ # coordinate1,coordinate2
        'iso3166 text, '+ # code
        'iso31662 text, '+ # code

        # calculations:
        'ccc_binary integer, '+

        # set as CCC
        'ccc_geolocated integer,'+ # 1, -1 o null.
        'country_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'location_wd text, '+ # 'P1:QX1:Q; P2:QX2:Q' Q is the main territory
        'created_by_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'part_of_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'
        'keyword_title text, '+ # 'QX1;QX2'
        'language_strong_wd text, '+ # 'P1:Q1;P2:Q2;P3:Q3'

        # characteristics of topic
        'gender text, '+
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
        'featured_article integer, '+

        'num_inlinks_from_original_CCC integer, '+
        'num_outlinks_to_original_CCC integer, '+

        'page_title_original_lang integer, '+
        'page_id_original_lang integer, '+
        'num_bytes_original_lang integer, '+
        'num_references_original_lang integer, '+
        'num_editors_original_lang integer, '+


        'PRIMARY KEY (qitem,page_id,languagecode));')

        try:
            cursor.execute(query)
            print ('Created the Missing CCC table for language: '+languagecode)
        except:
            print (languagecode+' already has a Missing CCC table.')


    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a keyword in title. This is considered potential CCC.
def label_missing_ccc_articles_keywords(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_keywords '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
#    print ('\n* Getting keywords related Articles for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    for languagecode_higher in languagecodes_higher:

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        if len(page_titles_qitems)==0: continue


        # CREATING KEYWORDS DICTIONARY 
        try:
            keywordsdictionary = pairs.loc[pairs['wikimedia_higher'] == languagecode_higher].loc[languagecode][['qitem','territoryname_higher']]
            keywordsdictionary = keywordsdictionary.set_index('qitem')
            keywordsdictionary = keywordsdictionary.to_dict()['territoryname_higher']
            for qitem,keyword in keywordsdictionary.items():
                keywordsdictionary[qitem]=[keyword]
        except:
            keywordsdictionary = {}

    #    print (page_title[0].encode('utf-8'))

        # language name
        qitemresult = languages.loc[languagecode]['Qitem']
        words =[]
        conn = sqlite3.connect(databases_path+'wikidata.db'); cursor = conn.cursor();
        query = 'SELECT page_title FROM sitelinks WHERE qitem=? AND langcode=?'

        if ';' in qitemresult:
            for qitem in qitemresult.split(';'):
                cursor.execute(query,(qitem,languagecode_higher+'wiki'))
                page_title = cursor.fetchone()
                if page_title != None: page_title = page_title[0]
                else: page_title = ''
                words.append(page_title)
        else:
            cursor.execute(query,(qitemresult,languagecode_higher+'wiki'))
            page_title = cursor.fetchone()
            if page_title != None: page_title = page_title[0]
            else: page_title = ''
#            page_title = page_title.replace(' language','')
            words.append(page_title)

        keywordsdictionary[qitemresult]=words
        print (keywordsdictionary)

        selectedarticles = {}
        for item, keywords in keywordsdictionary.items():
    #        print (item)
            for keyword in keywords:
                if keyword == '': continue
                keyword_rect = unidecode.unidecode(keyword).lower().replace(' ','_')

                for page_title,page_id in page_titles_page_ids.items():
                    page_title_rect = unidecode.unidecode(page_title).lower().replace(' ','_')
                   
                    if keyword_rect in page_title_rect:

                        try:
                            selectedarticles[page_id].add(item)
                        except:
                            va = set()
                            va.add(page_title)
                            va.add(item)
                            selectedarticles[page_id] = va


        insertedarticles = []
        for page_id, value in selectedarticles.items():
            value=list(value)
            page_title = str(value.pop(0))
            keyword_title = str(';'.join(value))
            try: 
                qitem=page_titles_qitems[page_title]
            except: 
                qitem=None
            insertedarticles.append((languagecode_higher,1,keyword_title,page_title,page_id,qitem))

        print ('The total number of Articles by this language dictionary is: ')
        print (len(selectedarticles))

        conn = sqlite3.connect(databases_path + missing_ccc_db); cursor = conn.cursor()
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (languagecode,ccc_binary,keyword_title,page_title,page_id,qitem) VALUES (?,?,?,?,?,?);'
        cursor.executemany(query,insertedarticles)
        conn.commit()

    print ('articles with keywords on titles in Wikipedia language '+(languagecode)+' have been inserted.');

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles whose WikiData items have properties linked to territories and language names (groundtruth). Label them as CCC.
# There is margin for optimization: Articles could be updated more regularly to the database, so in every loop it is not necessary to go through all the items.
def label_missing_ccc_articles_geolocation_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_geolocation_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
#    print ('\n* Missing CCC - Getting Articles with Wikidata from items with "geolocation" property and reverse geocoding for language: '+languages.loc[languagecode_higher]['languagename']+' that should be in '+languagecode+'.')

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

#        print (territorynameNative,territoryname)
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

    print ('these are the countries of the geolocated articles:')
    print (allISO3166)

    print ('these are the full countries:')
    print (ISO3166codes)

    print ('these are the subdivisions:')
    print (ISO31662codes)


    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    subdivisions = wikilanguages_utils.load_world_subdivisions_multilingual()

    # Get the Articles, evaluate them and insert the good ones.   
    ccc_geolocated_items0 = []
    ccc_geolocated_items1 = []
    ccc_geolocated_items2 = []
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
#    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON sitelinks.qitem=geolocated_property.qitem WHERE sitelinks.langcode="'+languagecode+'wiki";'
    
    langs_sets = {}
    languagecodes_dicts = {}
    languagecode_higher = ''
    for languagecode_higher in languagecodes_higher:
        languagecodes_higher_dicts = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts
        langs_sets[languagecode_higher]=set()

    page_asstring = ','.join( ['?'] * len(allISO3166) )
    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166 FROM geolocated_property WHERE geolocated_property.iso3166 IN (%s) ORDER BY geolocated_property.iso3166, geolocated_property.admin1;' % page_asstring

    x = 0
    i = 0
    for row in cursor.execute(query,allISO3166):
#        print (row)
#        input('')
        qitem_specific=str(row[0])

        coordinates=str(row[1])
        admin1=str(row[2]) # it's the Territory Name according to: https://github.com/thampiman/reverse-geocoder
        iso3166=str(row[3])
        iso31662=''; 
        try: iso31662=iso3166+'-'+subdivisions[admin1].split('-')[1]
        except: pass

#        print (page_title,page_id,qitem_specific,admin1,iso3166,coordinates)

        qitem=''
        try: qitem=territories[(territories.ISO3166 == iso3166) & (territories.territoryname == admin1)].loc[languagecode]['QitemTerritory']
#            print (qitem); print ('name and country')
        except: pass
        
        try:
            # try to get qitem from country code.        
            if qitem == '':
                try:
                    qitem = ISO3166codes[iso3166]
        #                print (qitem); print ('country')
                    # try to get qitem from admin1: in territorynames, territorynamesNative and subdivisions.
                except:
                    try:
                        qitem=territorynames[admin1]
    #                    print (qitem); print ('territorynames in English.')
                    except:
                        try:
                            qitem=territorynamesNative[admin1]
    #                        print (qitem); print ('territorynames in Native.')
                        except:
                            try: 
                                qitem=ISO31662codes[iso31662]
    #                                print (qitem); print ('subdivisions')
                            except:
                                pass
        except: pass

        if qitem!='':
            i = i + 1
            ccc_geolocated = 1
            ccc_binary = 1
#            print ((page_id,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
            in_parameters = []
            for languagecode_higher in languagecodes_higher:
                try:
                    page_title=languagecodes_dicts[languagecode_higher][0][qitem_specific]
                    page_id=languagecodes_dicts[languagecode_higher][1][page_title]

                    if qitem_specific in langs_sets[languagecode_higher]: 
                        continue
                    else:
                        langs_sets[languagecode_higher].add(qitem_specific)

                        ccc_geolocated_items0.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem_specific))

                        ccc_geolocated_items1.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,page_title,page_id,qitem_specific,languagecode_higher))

                        in_parameters.append(languagecode_higher)
#                        print ((languagecode_higher,ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,page_title,page_id,qitem_specific))
                except:
                    pass

            # These are those which are not even in the language where they are overlapped in the territory.
            if len(in_parameters)==0:
                ccc_geolocated_items2.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,None,0,qitem_specific,''))
#                print ((None,ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,0,None,qitem_specific))

        if x%20000 == 0 and x!=0: print (x)
        x = x + 1

#    print (len(ccc_geolocated_items))
#    input('')

    if len(ccc_geolocated_items0)==0 and len(ccc_geolocated_items2)==0: 
#        print ('No geolocated Articles in Wikidata for this language edition.');
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return

    # Insert to the corresponding CCC database.
    print ('Inserting/Updating Articles into the database.')

    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()


    # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates) = (?,?,?,?,?) WHERE qitem = ?;'
    cursor2.executemany(query,ccc_geolocated_items0)
    conn2.commit()

    # INSERT THOSE NEW LANGUAGE-RELATED
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,page_title,page_id,qitem,languagecode) VALUES (?,?,?,?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_geolocated_items1)
    conn2.commit()

    # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,page_title,page_id,qitem,languagecode) VALUES (?,?,?,?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_geolocated_items2)
    conn2.commit()



    print ('All geolocated Articles from Wikidata validated through reverse geocoding are in. They are: '+str(len(ccc_geolocated_items0))+' assigned to a language pair and '+str(len(ccc_geolocated_items2))+' in Wikidata.')
    print ('Geolocated Articles from Wikidata for language '+(languagecode_higher)+' have been inserted.');

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a country property related to a territory from the list of territories from the language. Label them as CCC.
def label_missing_ccc_articles_country_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_country_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db');cursor = conn.cursor()

#    print ('\n* Missing CCC - Getting Articles with Wikidata from items with "country" properties for language: '+languages.loc[languagecode_higher]['languagename']+' that should be in '+languagecode+'.')

#    print (territories.head())
#    input('')

    # country qitems
    try: countries = territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'].tolist()
    except: 
        try: countries = list(); countries.append(territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'])
        except:
            print ('there are no entire countries where the '+languagecode+' is official')
            duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
            wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
            return
    print ((countries))

    # get Articles
    qitem_properties = {}

    ccc_country_items0 = []
    ccc_country_items1 = []
    ccc_country_items2 = []
    page_asstring = ','.join( ['?'] * len(countries) )
    query = 'SELECT country_properties.qitem, country_properties.property, country_properties.qitem2 FROM country_properties WHERE country_properties.qitem2 IN (%s)' % page_asstring
    for row in cursor.execute(query,countries):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]

#        print ((qitem, wdproperty, country_properties[wdproperty], page_title))
        value = wdproperty+':'+qitem2
        try:
            qitem_properties[qitem]=qitem_properties[qitem]+';'+value
        except:
            qitem_properties[qitem]=value

    # Get the tuple ready to insert.
    languagecodes_dicts = {}
    for languagecode_higher in languagecodes_higher:
        languagecodes_higher_dicts = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts

    for qitem, values in qitem_properties.items():
        in_parameters = []
        for languagecode_higher in languagecodes_higher:
            try:
                page_title=languagecodes_dicts[languagecode_higher][0][qitem_specific]
                page_id=languagecodes_dicts[languagecode_higher][1][page_title]
                ccc_country_items0.append((1,values,qitem))
                ccc_country_items1.append((1,values,page_title,qitem,page_id,languagecode_higher))

                in_parameters.append(languagecode_higher)
            except: 
                pass
        if len(in_parameters)==0:
            ccc_country_items2.append((1,values,None,qitem,0,''))

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()


    # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,country_wd) = (?,?) WHERE qitem = ?;'
    cursor2.executemany(query,ccc_country_items0)
    conn2.commit()

    # INSERT THOSE NEW LANGUAGE-RELATED
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,country_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_country_items1)
    conn2.commit()

    # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,country_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_country_items2)
    conn2.commit()

    print (str(len(ccc_country_items0)+len(ccc_country_items2))+' country related Articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a location property that is iteratively associated to the list of territories associated to the language. Label them as CCC.
def label_missing_ccc_articles_location_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_location_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    languagecodes_dicts = {}
    languagecode_higher = ''
    for languagecode_higher in languagecodes_higher:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}       

        languagecodes_higher_dicts = []
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts

#    pairs = wikilanguages_utils.load_language_pairs_territory_status()
    new_dict = {}
    try:
        pairs_pd = pairs.loc[languagecode][['qitem','wikimedia_higher']]

        try:
            pairs_dict = pairs_pd.to_dict('records')
            for pair in pairs_dict:
                qitem=pair['qitem']
                wikimedia_higher=pair['wikimedia_higher']
                if wikimedia_higher == languagecode: continue
                if qitem not in new_dict:
                    new_dict[qitem]=[wikimedia_higher]
                else:
                    new_dict[qitem].append(wikimedia_higher)
        except:
            l = pairs_pd.tolist()
            wikimedia_higher = l[1]
            if wikimedia_higher == languagecode:
                print ('There are no pairs for language: '+languagecode)
            else:
                new_dict[l[0]]=wikimedia_higher
    except:
        print ('There are no pairs for language: '+languagecode)


    if len(new_dict)==0: 
        print ('Oops. There are no territories for this language.');
        qitems=[]
        if languagecode not in languageswithoutterritory:
            try: qitems=territories.loc[languagecode]['QitemTerritory'].tolist()
            except: qitems=[];qitems.append(territories.loc[languagecode]['QitemTerritory'])
    else:
        qitems = list(new_dict.keys())

    selected_qitems = {}



    print ('These are the qitems to start with: ')
    print (qitems)
    if len(qitems) == 0:
        print ('No qitems. Then return.')
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return;

    query = 'SELECT location_properties.qitem, location_properties.property, location_properties.qitem2 FROM location_properties;'

    rows = []
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        rows.append([qitem,wdproperty,qitem2])
    print (len(rows))


    for QitemTerritory in qitems:
#        QitemTerritoryname = territories.loc[territories['QitemTerritory'] == QitemTerritory].loc[languagecode]['territoryname']
#        print ('We start with this territory: '+QitemTerritoryname+' '+QitemTerritory)
        print ('We start with this territory: '+QitemTerritory)

        target_territories = {QitemTerritory : 0}

        counter = 1
        updated = 0
        round = 1
        number_items_territory = 0
        while counter != 0: # when there is no level below as there is no new items. there are usually 6 levels.
            print ('# Round: '+str(round))
            round_territories = {}
            counter = 0

            for row in rows:
                qitem = row[0]
                wdproperty = row[1]
                qitem2 = row[2]

                if qitem2 in target_territories:
                    try:
                        selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2,QitemTerritory]
                        updated = updated + 1
                    except:
                        selected_qitems[qitem]=[wdproperty,qitem2,QitemTerritory]
                        counter = counter + 1
                        round_territories[qitem]=0

            target_territories = round_territories
            number_items_territory = number_items_territory + len(round_territories)

            print ('In this iteration we added this number of NEW items: '+(str(counter)))
            print ('In this iteration we updated this number of items: '+(str(updated)))
            print ('The current number of selected items for this territory is: '+str(number_items_territory))
            round = round + 1


        print ('- The number of items related to the territory '+QitemTerritory+' is: '+str(number_items_territory))
        print ('- The TOTAL number of selected items at this point is: '+str(len(selected_qitems))+'\n')
#        break
#    for keys,values in selected_qitems.items(): print (keys,values)
        
        
        try:
            if type(new_dict[QitemTerritory]) is str:
                languagecodes_higher=[new_dict[QitemTerritory]]
            else:
                languagecodes_higher=new_dict[QitemTerritory]
        except:
            pass


        ccc_located_items0 = []
        ccc_located_items1 = []
        ccc_located_items2 = []
        # ccc_located_items3 = []
        for qitem, values in selected_qitems.items():
            in_parameters = []
            for languagecode_higher in languagecodes_higher:
                try:
                    value = ''
                    for x in range(0,int((len(values))/3)):
                        if value != '': value = value + ';'
                        value = value + values[x*3]+':'+values[x*3+1]+':'+values[x*3+2]

                    try: 
                        page_title=languagecodes_dicts[languagecode_higher][0][qitem]
                        page_id=languagecodes_dicts[languagecode_higher][1][page_title]
                        ccc_located_items0.append((1,value,qitem))
                        # if qitem == 'Q924549': 
                        #     print (1,value,qitem)
                        #     input('')

                        ccc_located_items1.append((1,value,page_title,qitem,page_id,languagecode_higher))
                        in_parameters.append(languagecode_higher)
                    except: 
                        continue
                except: 
                    pass
            if len(in_parameters)==0:
                value = ''
                for x in range(0,int((len(values))/3)):
                    if value != '': value = value + ';'
                    value = value + values[x*3]+':'+values[x*3+1]+':'+values[x*3+2]
                ccc_located_items2.append((1,value,None,qitem,0,''))
                # ccc_located_items3.append((1,value,qitem))

        print ('Introduced this number of qitems: '+str(len(ccc_located_items0)+len(ccc_located_items2)))

        conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()


        # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
        query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,location_wd) = (?,?) WHERE qitem = ?;'
        cursor2.executemany(query,ccc_located_items0)
        conn2.commit()

        # INSERT THOSE NEW LANGUAGE-RELATED
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,location_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
        cursor2.executemany(query,ccc_located_items1)
        conn2.commit()



        # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,location_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
        cursor2.executemany(query,ccc_located_items2)
        conn2.commit()

        # # UPDATE (WIKIDATA)
        # query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,location_wd) = (?,?) WHERE qitem = ?;'
        # cursor2.executemany(query,ccc_located_items3)
        # conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a "strong" language property that is associated the language. Label them as CCC.
def label_missing_ccc_articles_language_strong_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_language_strong_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

#    print ('\n* Getting Articles with Wikidata from items with "language" properties for language: '+languages.loc[languagecode_higher]['languagename']+' that should be in '+languagecode+'.')

    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

    # language qitems
    qitemresult = languages.loc[languagecode]['Qitem']
    if ';' in qitemresult: qitemresult = qitemresult.split(';')
    else: qitemresult = [qitemresult];

    # get Articles
    qitem_properties = {}
    qitem_page_titles = {}
    ccc_language_items0 = []
    ccc_language_items1 = []
    ccc_language_items2 = []

    query = 'SELECT language_strong_properties.qitem, language_strong_properties.property, language_strong_properties.qitem2 FROM language_strong_properties'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        if qitem2 not in qitemresult: continue

#        print ((qitem, wdproperty, language_properties[wdproperty], page_title))
        # Put the items into a dictionary
        value = wdproperty+':'+qitem2
        if qitem not in qitem_properties: qitem_properties[qitem]=value
        else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value

#    print ("Per P2936, hi ha aquest nombre: ")
#    print (len(qitem_page_titles))
#    input('ja hem acabat')
#    return


    languagecodes_dicts = {}
    for languagecode_higher in languagecodes_higher:
        languagecodes_higher_dicts = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts

    for qitem, values in qitem_properties.items():
        in_parameters = []
        for languagecode_higher in languagecodes_higher:
            try:
                page_title=languagecodes_dicts[languagecode_higher][0][qitem_specific]
                page_id=languagecodes_dicts[languagecode_higher][1][page_title]
                ccc_language_items0.append((1,values,qitem))
                ccc_language_items1.append((1,values,qitem_page_titles[qitem],qitem,page_id,languagecode_higher))

                in_parameters.append(languagecode_higher)
            except: 
                pass
        if len(in_parameters)==0:
            ccc_language_items2.append((1,values,None,qitem,0,''))


    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()


    # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,language_strong_wd) = (?,?) WHERE qitem = ?;'
    cursor2.executemany(query,ccc_language_items0)
    conn2.commit()

    # INSERT THOSE NEW LANGUAGE-RELATED
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,language_strong_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_language_items1)
    conn2.commit()

    # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,language_strong_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor2.executemany(query,ccc_language_items2)
    conn2.commit()

    print (str(len(ccc_language_items0)+len(ccc_language_items2))+' language related Articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a creation property that is related to the items already retrieved as CCC. Label them as CCC.
def label_missing_ccc_articles_created_by_properties_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_created_by_properties_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

#    print ('\n* Getting Articles with Wikidata from items with "created by" properties for language: '+languages.loc[languagecode_higher]['languagename']+' that should be in '+languagecode+'.')

    conn = sqlite3.connect(databases_path + missing_ccc_db); cursor = conn.cursor()
    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'): 
        if row[0]==None:
            ccc_articles[row[1]]=None
        else:
            ccc_articles[row[1]]=row[0].replace(' ','_')

    selected_qitems={}
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()
    query = 'SELECT created_by_properties.qitem, created_by_properties.property, created_by_properties.qitem2 FROM created_by_properties'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]

        if qitem2 in ccc_articles:
#            if qitem not in potential_ccc_articles: 
#                print ((qitem,page_title, wdproperty, created_by_properties[wdproperty],ccc_articles[qitem2], already_in))
            if qitem not in selected_qitems:
                selected_qitems[qitem]=[wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]

    ccc_created_by_items0 = []
    ccc_created_by_items1 = []
    ccc_created_by_items2 = []

    languagecodes_dicts = {}
    for languagecode_higher in languagecodes_higher:
        languagecodes_higher_dicts = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts

    for qitem, values in selected_qitems.items():
        in_parameters = []
        for languagecode_higher in languagecodes_higher:
            try:
                page_title=languagecodes_dicts[languagecode_higher][0][qitem_specific]
                page_id=languagecodes_dicts[languagecode_higher][1][page_title]

                value = ''
        #        print (values)
                for x in range(0,int((len(values)-1)/2)):
                    if x >= 1: value = value + ';'
                    value = value + values[x*2+1]+':'+values[x*2+2]
        #        print ((value,page_title,qitem,page_id))
                ccc_created_by_items0.append((1,value,qitem))
                ccc_created_by_items1.append((1,value,page_title,qitem,page_id,languagecode_higher))
#                print((1,value,page_title,qitem,page_id,languagecode_higher))
                in_parameters.append(languagecode_higher)
            except: 
                pass
        if len(in_parameters)==0:
            value = ''
            for x in range(0,int((len(values)-1)/2)):
                if x >= 1: value = value + ';'
                value = value + values[x*2+1]+':'+values[x*2+2]
            ccc_created_by_items2.append((1,value,None,qitem,0,''))


    # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,created_by_wd) = (?,?) WHERE  qitem = ?;'
    cursor.executemany(query,ccc_created_by_items0)
    conn.commit()

    # INSERT THOSE NEW LANGUAGE-RELATED
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,created_by_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,ccc_created_by_items1)
    conn.commit()

    # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,created_by_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,ccc_created_by_items2)
    conn.commit()

    print (str(len(ccc_created_by_items0)+len(ccc_created_by_items2))+' items/articles created by CCC Articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');

    time.sleep(10)
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles which are part of items already retrieved as CCC. Label them as CCC.
def label_missing_ccc_articles_part_of_properties_wd(languagecode,languagecodes_higher):

    function_name = 'label_missing_ccc_articles_part_of_properties_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()

#    print ('\n* Getting Articles with Wikidata from items with "part of" properties for language: '+languages.loc[languagecode_higher]['languagename']+' that should be in '+languagecode+'.')

    part_of_properties = {'P361':'part of'} 

    conn = sqlite3.connect(databases_path + missing_ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wikidata.db'); cursor2 = conn2.cursor()

    ccc_articles={}
    query = 'SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'
    for row in cursor.execute(query):
        page_title = row[0]
        if page_title != None:
            ccc_articles[row[1]]=row[0].replace(' ','_')
        else:
            ccc_articles[row[1]]=None


#    potential_ccc_articles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
#        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    selected_qitems={}
    query = 'SELECT part_of_properties.qitem, part_of_properties.property, part_of_properties.qitem2 FROM part_of_properties'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]

        if (qitem2 in ccc_articles):
            if qitem not in selected_qitems:
                selected_qitems[qitem]=[wdproperty,qitem2]
            else:
                selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2]
#    for keys,values in selected_qitems.items(): print (keys,values)

    ccc_part_of_items0 = []
    ccc_part_of_items1 = []
    ccc_part_of_items2 = []

    languagecodes_dicts = {}
    for languagecode_higher in languagecodes_higher:
        languagecodes_higher_dicts = []
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_higher)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
        languagecodes_higher_dicts.append(qitems_page_titles)
        languagecodes_higher_dicts.append(page_titles_page_ids)
        languagecodes_dicts[languagecode_higher]=languagecodes_higher_dicts

    for qitem, values in selected_qitems.items():
        in_parameters = []
        for languagecode_higher in languagecodes_higher:
            try:
                page_title=languagecodes_dicts[languagecode_higher][0][qitem_specific]
                page_id=languagecodes_dicts[languagecode_higher][1][page_title]

                value = ''
        #        print (values)
                for x in range(0,int((len(values)-1)/2)):
                    if x >= 1: value = value + ';'
                    value = value + values[x*2+1]+':'+values[x*2+2]
        #        print ((value,page_title,qitem,page_id))
                ccc_part_of_items0.append((1,value,qitem))
                ccc_part_of_items1.append((1,value,page_title,qitem,page_id,languagecode_higher))
                in_parameters.append(languagecode_higher)
            except: 
                pass
        if len(in_parameters)==0:
            value = ''
            for x in range(0,int((len(values)-1)/2)):
                if x >= 1: value = value + ';'
                value = value + values[x*2+1]+':'+values[x*2+2]
            ccc_part_of_items2.append((1,value,None,qitem,0,''))


    # UPDATE THOSE LANGUAGES-RELATED ARTICLES IN THE DB
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,part_of_wd) = (?,?) WHERE qitem = ?;'
    cursor.executemany(query,ccc_part_of_items0)
    conn.commit()

    # INSERT THOSE NEW LANGUAGE-RELATED
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,part_of_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,ccc_part_of_items1)
    conn.commit()

    # INSERT THOSE NEW NON LANGUAGE-RELATED ARTICLES (WIKIDATA)
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki (ccc_binary,part_of_wd,page_title,qitem,page_id,languagecode) VALUES (?,?,?,?,?,?);'
    cursor.executemany(query,ccc_part_of_items2)
    conn.commit()


    print (str(len(ccc_part_of_items0)+len(ccc_part_of_items2))+' items/articles created by CCC Articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# This function verifies that the article does exist or not in the original language and introduce some of their features.
def copy_features_into_missing_ccc_from_original_lang(languagecode):

    function_name = 'copy_features_into_missing_ccc_from_original_lang '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

    qitems = []
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()
    query = 'SELECT DISTINCT qitem FROM '+languagecode+'wiki;'
    for row in cursor2.execute(query):
        qitems.append(row[0])

    suma = 0
    while len(qitems)>0:
        increment = 10000
        suma = suma + increment
#        print (suma)
        qs = qitems[:increment]
        qitems = qitems[increment:]

        page_asstring = ','.join( ['?'] * len(qs) )
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'SELECT page_title, qitem, page_id, num_editors, num_references, num_bytes FROM '+languagecode+'wiki WHERE qitem IN (%s);' % page_asstring

        parameters = []
        for row in cursor.execute(query,qs):
            page_title=row[0]
            qitem = row[1]
            page_id = row[2]
            num_editors = row[3]
            num_references = row[4]

            num_bytes = row[5]
            parameters.append((page_id,page_title,num_bytes,num_references,num_editors,qitem))

        query = 'UPDATE '+languagecode+'wiki SET (page_id_original_lang,page_title_original_lang,num_bytes_original_lang,num_references_original_lang,num_editors_original_lang) = (?,?,?,?,?) WHERE qitem = ?;'
        cursor2.executemany(query,parameters)
        conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# This function copies the features from the language pairs and enriches the articles in the missing ccc table.
def copy_features_into_missing_ccc_from_language_pairs(languagecode,languagecodes_higher):

    function_name = 'copy_features_into_missing_ccc_from_language_pairs '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()

    if len(languagecodes_higher) == 0: 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return

#    print('\n* '+languages.loc[languagecode]['languagename']+' '+languagecode)
#    print ('This language is overlapped with these languages: ')
#    print (languagecodes_higher)

    for languagecode_higher in languagecodes_higher:
        print (languagecode_higher)
        if languagecode_higher not in wikilanguagecodes: continue

        qitems = {}
        query = 'SELECT DISTINCT qitem FROM '+languagecode+'wiki WHERE page_title_original_lang IS NULL AND languagecode = "'+languagecode_higher+'";'
        for row in cursor2.execute(query):
            qitems[row[0]]=[0,None]
        print(len(qitems))

        parameters = []
        qitems_list = list(qitems.keys())
        while len(qitems_list)>0:
            increment = 5000
            qs = qitems_list[:increment]
            qitems_list = qitems_list[increment:]

            page_asstring = ','.join( ['?'] * len(qs) )

            # RETRIEVING
            query = ('SELECT '+
            # topics
            'gender, '+
            'folk, '+
            'earth, '+
            'monuments_and_buildings, '+
            'music_creations_and_organizations, '+
            'sport_and_teams, '+
            'food, '+
            'paintings, '+
            'glam,'+
            'books,'+
            'clothing_and_fashion,'+
            'industry, '+

            # relevance
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
            'num_images, '+
            'featured_article, '+
            'page_id, '+          
            'qitem '+

            'FROM '+languagecode_higher+'wiki WHERE qitem IN (%s);') % page_asstring

    #        print (query)
            for row in cursor.execute(query, qs):
                row = list(row)
                row.append(languagecode_higher)
                parameters.append(tuple(row))

        print(len(parameters))

        # INSERTING
        query = ('UPDATE '+languagecode+'wiki SET ('+
        # topics
        'gender, '+
        'folk, '+
        'earth, '+
        'monuments_and_buildings, '+
        'music_creations_and_organizations, '+
        'sport_and_teams, '+
        'food, '+
        'paintings, '+
        'glam,'+
        'books,'+
        'clothing_and_fashion,'+
        'industry, '+

        # relevance
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
        'num_images, '+
        'featured_article) = '+
        '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND languagecode = ?;')

#        print (len(parameters))
        cursor2.executemany(query,parameters)
        conn2.commit()

        # this function gets all the features from the original ccc_xwiki tables and copies them into the articles in the missing_ccc tables.
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# This function sets the feature "non-language pairs" which shows which articles are missing in the language CCC but are not available either in a language-pair but in another language.
def copy_features_into_missing_ccc_from_non_language_pairs(languagecode, languagecodes_higher):

    function_name = 'copy_features_into_missing_ccc_from_non_language_pairs '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()

#    print('\n* '+languages.loc[languagecode]['languagename']+' '+languagecode)

    query = 'UPDATE '+languagecode+'wiki SET non_language_pairs = 1 WHERE page_title_original_lang IS NULL AND languagecode = "";'
    cursor2.execute(query)
    conn2.commit()

    qitems = {}
    query = 'SELECT qitem FROM '+languagecode+'wiki WHERE non_language_pairs = 1;'

    for row in cursor2.execute(query):
        qitems[row[0]]=[0,None]

    print ('This is the number of articles obtained through Wikidata and without a language: '+str(len(qitems)))


    if len(qitems) == 0:
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)       

    lang_qitems_num = {}
    lang_qitems = {}
    if len(languagecodes_higher) != 0:
        # print ('This language is overlapped with these languages: ')
        # print (languagecodes_higher)

        for lang in languagecodes_higher:
            if lang not in wikilanguagecodes: continue

            qitems_list = list(qitems.keys())
            while len(qitems_list)>0:
                increment = 10000
                qs = qitems_list[:increment]
                qitems_list = qitems_list[increment:]

                page_asstring = ','.join( ['?'] * len(qs) )

                query = 'SELECT qitem, num_bytes FROM '+lang+'wiki WHERE num_bytes IS NOT NULL AND qitem IN (%s);' % page_asstring
                qitems_lang = {}
                for row in cursor.execute(query,qs):
                    qitems_lang[row[0]]=row[1]

                for qitem in qs:
                    values = qitems[qitem]
                    try:
                        num_bytes = qitems_lang[qitem]
                    except:
                        continue
                    if num_bytes > values[0]:
                        qitems[qitem]=[num_bytes,lang]


        for qitem,values in qitems.items():
            lang=values[1]

            if lang in lang_qitems:
                lang_qitems[lang].append(qitem)
                lang_qitems_num[lang]=lang_qitems_num[lang]+1
            else:
                lang_qitems[lang]=[qitem]
                lang_qitems_num[lang]=0

        try:
            for qitem in lang_qitems[None]:
                qitems[qitem]=[0,None]
        except:
            pass


    for lang in wikilanguagecodes:

        qitems_list = list(qitems.keys())
        while len(qitems_list)>0:
            increment = 10000
            qs = qitems_list[:increment]
            qitems_list = qitems_list[increment:]

            page_asstring = ','.join( ['?'] * len(qs) )

            query = 'SELECT qitem, num_bytes FROM '+lang+'wiki WHERE num_bytes IS NOT NULL AND qitem IN (%s);' % page_asstring
            qitems_lang2 = {}

            for row in cursor.execute(query,qs):
                qitems_lang2[row[0]]=row[1]

            for qitem in qs:
                values = qitems[qitem]
                try:
                    num_bytes = qitems_lang2[qitem]
                except:
                    continue
                if num_bytes > values[0]:
                    qitems[qitem]=[num_bytes,lang]

    lang_qitems_num2 = {}
    lang_qitems2 = {}
    for qitem,values in qitems.items():
        lang=values[1]

        if lang in lang_qitems2:
            lang_qitems2[lang].append(qitem)
            lang_qitems_num2[lang]=lang_qitems_num2[lang]+1
        else:
            lang_qitems2[lang]=[qitem]
            lang_qitems_num2[lang]=0


    lang_qitems.update(lang_qitems2)
    lang_qitems_num.update(lang_qitems_num2)

#        lang_qitems_num = [(k, lang_qitems_num[k]) for k in sorted(lang_qitems_num, key=lang_qitems_num.get, reverse=True)]
    
    print (lang_qitems_num)


    for languagecode_higher, qs in lang_qitems.items():
        if languagecode_higher not in wikilanguagecodes: continue
        if languagecode_higher is None: continue
        if len(qs) == 0: continue
        qstr = str(len(qs))
        print (languagecode_higher)

        increment = 50000
        while len(qs[:increment]) > 0:
            sample = qs[:increment] # sample we take
            qs = qs[increment:] # remaining
            page_asstring = ','.join( ['?'] * len(sample) )
               
            # RETRIEVING
            query = ('SELECT '+
            # topics
            'gender, '+
            'folk, '+
            'earth, '+
            'monuments_and_buildings, '+
            'music_creations_and_organizations, '+
            'sport_and_teams, '+
            'food, '+
            'paintings, '+
            'glam,'+
            'books,'+
            'clothing_and_fashion,'+
            'industry, '+

            # relevance
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
            'num_images, '+
            'featured_article, '+

            'page_id, '+     
            'page_title, '+
            'qitem '+

            'FROM '+languagecode_higher+'wiki WHERE qitem IN (%s);') % page_asstring


            parameters = []
            for row in cursor.execute(query, sample):
                row = list(row)
                qitem = row[len(row)-1]
                row[len(row)-1] = languagecode_higher
                row.append(qitem)
                parameters.append(tuple(row))
#                print (row)

        if len(parameters)==0: continue

        # INSERTING
        query = ('UPDATE '+languagecode+'wiki SET ('+
        # topics
        'gender, '+
        'folk, '+
        'earth, '+
        'monuments_and_buildings, '+
        'music_creations_and_organizations, '+
        'sport_and_teams, '+
        'food, '+
        'paintings, '+
        'glam,'+
        'books,'+
        'clothing_and_fashion,'+
        'industry, '+

        # relevance
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
        'num_images, '+
        'featured_article, '+

        'page_id, '+
        'page_title, '+
        'languagecode'+
        ') = '+
        '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) WHERE qitem = ? AND non_language_pairs = 1;') #page_title = "" and page_id = 0 and languagecode = ""

        print (languagecode_higher, lang_qitems_num[languagecode_higher], qstr, str(len(parameters)))

        for param in parameters:
#                print (param)
            try:
                cursor2.execute(query,param)
            except:
                pass

        conn2.commit()
        # this function gets all the features from ccc_xwiki tables (language where the article is the largest) and copies them into the articles in the missing_ccc tables.


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# Introduce the label in the original language, the biggest with overlapping, otherwise in english.
def copy_labels_into_missing_ccc(languagecode, languagecodes_higher):

    function_name = 'copy_labels_into_missing_ccc '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()

    highest_pair = ['',0]
    for lang in languagecodes_higher:
        if lang not in wikilanguagecodes: continue

        if wikipedialanguage_numberarticles[lang] > highest_pair[1]:
            highest_pair=[lang,wikipedialanguage_numberarticles[lang]]

    qitems = []
    query = 'SELECT DISTINCT qitem FROM '+languagecode+'wiki;'
    for row in cursor2.execute(query):
        qitems.append(row[0])

    print (len(qitems))

    langs = [languagecode+'wiki',highest_pair[0]+'wiki','enwiki']

    parameters = []
    page_asstring = ','.join( ['?'] * len(langs) )
    print (langs)

    query = 'SELECT label, langcode FROM labels WHERE qitem = ? AND langcode IN (%s);' % page_asstring
    for q in qitems:
        p = [q]+langs
        label_langs = {}
        for row in cursor.execute(query,p):
            lab = row[0]
            l = row[1]
            label_langs[l]=lab

        if langs[0] in label_langs:
            parameters.append((languagecode,label_langs[langs[0]],q))
#            print ((languagecode,label_langs[langs[0]],q))
        elif langs[1] in label_langs:
            parameters.append((highest_pair[0],label_langs[langs[1]],q))
#            print ((highest_pair[0],label_langs[langs[1]],q))
        elif langs[2] in label_langs:
            parameters.append(('en',label_langs['enwiki'],q))
#            print (('en',label_langs['enwiki'],q))

    print (len(parameters))
    query = 'UPDATE '+languagecode+'wiki SET(label_lang,label) = (?,?) WHERE qitem = ?;'
    cursor2.executemany(query,parameters)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def extend_links_from_to_original_ccc(languagecode,page_titles_page_ids,page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'extend_links_from_to_original_ccc '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + missing_ccc_db); cursor2 = conn2.cursor()
    print (len(page_titles_page_ids))

    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}    

	# PREPARAR ELS CCC DELS ALTRES A LA LLENGUA ORIGINAL
    original_CCC_in_lang = {}
    for languagecode2 in wikilanguagecodes:
        query = "SELECT a.page_id FROM "+languagecode+"wiki a INNER JOIN "+languagecode2+"wiki b ON a.qitem = b.qitem WHERE b.ccc_binary = 1;"
        for row in cursor.execute(query):
            page_id = row[0]
            try:
            	original_CCC_in_lang[page_id].append(languagecode2)
            except:
            	original_CCC_in_lang[page_id] = [languagecode2]
    print (len(original_CCC_in_lang))

    langs_set = set()
    # PREPARAR EL MISSING CCC
    missing_CCC_langs = {}
    for languagecode2 in wikilanguagecodes:

        query = 'SELECT page_id FROM '+languagecode2+'wiki WHERE languagecode = "'+languagecode+'";'
        for row in cursor2.execute(query):
            page_id = row[0]
            try:
            	missing_CCC_langs[page_id].append(languagecode2)
            except:
            	missing_CCC_langs[page_id] = [languagecode2]

            langs_set.add(languagecode2)

    print (len(missing_CCC_langs))

    # PREPARAR ELS DICCIONARIS DESTÍ PELS OUTLINKS-INLINKS DEL MISSING CCC DELS ALTRES
    outlinks_ccc = {}
    inlinks_ccc = {}
    for languagecode2 in wikilanguagecodes:

    	page_ids_inlinks_ccc={}
    	page_ids_outlinks_ccc={}
    	
    	query = 'SELECT page_id FROM '+languagecode2+'wiki WHERE languagecode = "'+languagecode+'";'
    	for row in cursor2.execute(query):
    		page_id = row[0]
    		page_ids_inlinks_ccc[page_id]=0
    		page_ids_outlinks_ccc[page_id]=0

    	inlinks_ccc[languagecode2] = page_ids_inlinks_ccc
    	outlinks_ccc[languagecode2] = page_ids_outlinks_ccc


    dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-pagelinks.sql.gz'
    #    dumps_path = 'gnwiki-20190720-pagelinks.sql.gz' # read_dump = '/public/dumps/public/wikidatawiki/latest-all.json.gz'
    dump_in = gzip.open(dumps_path, 'r')
    wikilanguages_utils.check_dump(dumps_path, script_name)

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
    #                print(row)
                pl_from_page_id = int(row[0])
                pl_from_namespace = row[1]
                pl_title = str(row[2])
                pl_namespace = row[3]

                try:
                    pl_title_page_id = page_titles_page_ids[pl_title]
                except:
                    pl_title_page_id = None

                if pl_from_namespace != '0' or pl_namespace != '0': continue


#				check if is missing_ccc
                try:
                    pl_from_langs = missing_CCC_langs[pl_from_page_id]
                except:
                    pl_from_langs = []
                try:
                    pl_to_langs = missing_CCC_langs[pl_title_page_id]
                except:
                    pl_to_langs = []


#				check if is inlink or outlink to ccc
                try:
                    pl_from_page_id_CCC = original_CCC_in_lang[pl_from_page_id]
                except:
                    pl_from_page_id_CCC = []
                try:
                    pl_to_page_id_CCC = original_CCC_in_lang[pl_title_page_id]
                except:
                    pl_to_page_id_CCC = []

#				introduce to the outlinks-inlinks dict according to the missing ccc
                for lang in pl_to_langs:
                	if lang in pl_from_page_id_CCC:
                		inlinks_ccc[lang][pl_title_page_id]=inlinks_ccc[lang][pl_title_page_id]+1
                		
                for lang in pl_from_langs:
                	if lang in pl_to_page_id_CCC:
                		outlinks_ccc[lang][pl_from_page_id]=outlinks_ccc[lang][pl_from_page_id]+1

                # if pl_from_page_id == 1502710 or pl_title_page_id == 1502710:
                #     print ('\n')
                #     print (pl_from_page_id,pl_title_page_id)
                #     try:
                #         print (page_ids_page_titles[pl_from_page_id])
                #         print (pl_title+'\n')
                #     except:
                #         pass

                #     print (pl_from_langs)
                #     print (pl_to_langs)
                #     print (pl_from_page_id_CCC)
                #     print (pl_to_page_id_CCC)

                #     print (inlinks_ccc['ca'][1502710])
                #     print (outlinks_ccc['ca'][1502710])
                #     print ('\n')
                #     print ('\n')


    print ('Done with the dump.')

    for lang in langs_set:

        params = []
        for page_id, count in outlinks_ccc[lang].items():
            outlinks = count
            inlinks = inlinks_ccc[lang][page_id]
            qitem = page_titles_qitems[page_ids_page_titles[page_id]]
            params.append((inlinks,outlinks,qitem,page_id,languagecode))
#            print ((lang,inlinks,outlinks,qitem,page_id,languagecode))

        query = 'UPDATE '+lang+'wiki SET (num_inlinks_from_original_CCC,num_outlinks_to_original_CCC)=(?,?) WHERE qitem = ? AND page_id = ? AND languagecode=?;'
        cursor2.executemany(query,params)
        conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def update_push_missing_ccc_wikipedia_diversity():

    function_name = 'update_push_missing_ccc_wikipedia_diversity'
    # if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn2 = sqlite3.connect(databases_path + missing_ccc_production_db); cursor2 = conn2.cursor()

    for languagecode1 in wikilanguagecodes:

        qitems_langs = {} 
        for languagecode2 in wikilanguagecodes:

            if languagecode1 == languagecode2: continue

            if cursor2.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='"+languagecode2+"wiki';").fetchone() == None: continue

            query = 'SELECT qitem, page_id, page_title FROM '+languagecode2+'wiki WHERE languagecode = "'+languagecode1+'"' # languagecode2 is the target we want to bridge; languagecode1 is the source.

            for row in cursor2.execute(query): 
                qitem = row[0]
                try: qitems_langs[qitem] = qitems_langs[qitem] + ';' + languagecode2
                except: qitems_langs[qitem] = languagecode2

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode1)
        qitems_page_ids = {v: page_titles_page_ids[k] for k, v in page_titles_qitems.items()}

        params = []
        for qitem, langs in qitems_langs.items():
            try:
                params.append((langs, qitems_page_ids[qitem], qitem))
            except:
                pass

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        query = 'UPDATE '+languagecode1+'wiki SET missing_ccc = ? WHERE page_id = ? AND qitem = ?;'
        cursor.executemany(query,params)
        conn.commit()

        print (languagecode1, str(len(params)))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    # wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





#######################################################################################

### SAFETY FUNCTIONS ###

def main_with_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('MISSING CCC SELECTION: '+ cycle_year_month, 'ERROR.')

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
            wikilanguages_utils.send_email_toolaccount('MISSING CCC SELECTION: '+ cycle_year_month, 'ERROR.' + lines); print("Now let's try it again...")
            continue





#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("missing_ccc_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("missing_ccc_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':

    script_name = 'missing_ccc_selection.py'

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

    languageswithoutterritory=list(set(languages.index.tolist()) - set(list(territories.index.tolist())))
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


    wikilanguages_utils.finish_email(startTime,'missing_ccc_selection.out','Missing CCC Selection')
