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
import unidecode
# requests and others
import reverse_geocoder as rg
import numpy as np
from random import shuffle
# data
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# Twice the same table in a short period of time not ok.
# Load all page_titles from all languages is not ok.
import gc



# MAIN
def main():

    languagecode = 'it'
    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)

    label_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids)

    input('')
    return
    execution_block_diversity_groups_features()
    execution_block_ccc_features()


################################################################

def execution_block_diversity_groups_features():
    wikilanguages_utils.send_email_toolaccount('WCDO - CONTENT RETRIEVAL', '# INTRODUCE THE WIKIDATA TOPICS FEATURES')

    with ThreadPoolExecutor(max_workers=3) as executor:
        for languagecode in wikilanguagecodes: 
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            for topic in ['gender', 'ethnic_group', 'supra_ethnic_group', 'religious_group', 'religion', 'folk', 'monuments_and_buildings', 'earth', 'music_creations_and_organizations', 'sport_and_teams', 'food', 'paintings', 'glam', 'books', 'clothing_and_fashion','industry']:
                executor.submit(label_diversity_groups_related_topics_wd, topic, languagecode,page_titles_page_ids, page_titles_qitems)

            label_sexual_orientation_wd(languagecode, page_titles_page_ids, page_titles_qitems)
            label_start_and_end_time(languagecode, page_titles_page_ids, page_titles_qitems)


def execution_block_ccc_features():

    wikilanguages_utils.send_email_toolaccount('WCDO - CONTENT RETRIEVAL', '# INTRODUCE THE ARTICLE CCC FEATURES')

    # Obtaining CCC features for all WP  
    # DATA STRATEGIES:
    # 1. RETRIEVE AND SET ARTICLES AS CCC:
    with ThreadPoolExecutor(max_workers=2) as executor:
        for languagecode in wikilanguagecodes:
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            executor.submit(label_ccc_articles_keywords,languagecode,page_titles_qitems,page_titles_page_ids)
            del (page_titles_qitems, page_titles_page_ids); gc.collect()

            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            executor.submit(label_ccc_articles_geolocation_wd, languagecode,page_titles_page_ids)
            del (page_titles_qitems, page_titles_page_ids); gc.collect()

    with ProcessPoolExecutor(max_workers=3) as executor:
        for languagecode in wikilanguagecodes: 
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            executor.submit(label_ccc_articles_country_wd, languagecode,page_titles_page_ids)
            executor.submit(label_ccc_articles_language_strong_wd,languagecode,page_titles_page_ids)
            del (page_titles_qitems, page_titles_page_ids); gc.collect()

    for languagecode in biggest:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        label_ccc_articles_geolocated_reverse_geocoding(languagecode,page_titles_qitems,page_titles_page_ids)
        del (page_titles_qitems, page_titles_page_ids); gc.collect()

    with ProcessPoolExecutor(max_workers=5) as executor:
        for languagecode in smallest:
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            executor.submit(label_ccc_articles_geolocated_reverse_geocoding, languagecode,page_titles_qitems,page_titles_page_ids)
            del (page_titles_qitems, page_titles_page_ids); gc.collect()

    with ProcessPoolExecutor(max_workers=3) as executor:
        for languagecode in wikilanguagecodes: 
            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
            executor.submit(label_ccc_articles_part_of_properties_wd,languagecode,page_titles_page_ids)
            executor.submit(label_ccc_articles_created_by_properties_wd,languagecode,page_titles_page_ids)
            del (page_titles_qitems, page_titles_page_ids); gc.collect()

    for languagecode in wikilanguagecodes: 
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        label_ccc_articles_location_wd(languagecode,page_titles_page_ids)
        del (page_titles_qitems, page_titles_page_ids); gc.collect()




################################################################



# FEATURES STRATEGIES
#################
################################################################


# Obtain the Articles with coordinates gelocated in the territories associated to that language by reverse geocoding. Label them as CCC.
def label_ccc_articles_geolocated_reverse_geocoding(languagecode,page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    function_name = 'label_ccc_articles_geolocated_reverse_geocoding '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

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
#    print (qitems)
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
            if ISO31662 != '':
                ISO31662codes[ISO31662]=qitem

    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    subdivisions = wikilanguages_utils.load_world_subdivisions_multilingual()
    geolocated_pageids = {}
    geolocated_pageids_coords = {}
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()


    # It gets all the Articles with coordinates from a language. It stores them into an adhoc database.
#    query = 'SELECT page_title, gt_page_id, gt_lat, gt_lon FROM '+languagecode+'wiki_p.page INNER JOIN '+languagecode+'wiki_p.geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_title' # if there is an Article with more than one geolocation it takes the first!

    # now it takes the last.
    query = 'SELECT gt_page_id, gt_lat, gt_lon FROM geo_tags;'

#    query = 'SELECT page_title, gt_page_id, gt_lat, gt_lon FROM page INNER JOIN geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by gt_page_id;'

    mysql_cur_read.execute(query)
    result = mysql_cur_read.fetchall()
    for row in result:
        page_id = row[0]
        lat = row[1]
        lon = row[2]
        try:
            page_title=page_ids_page_titles[page_id]
            geolocated_pageids[page_id] = [page_title, page_titles_qitems[page_title], lat, lon]
            geolocated_pageids_coords[page_id]=(lat,lon)
        except:
            pass

    # It calculates the reverse geocoding data and updates the database.
    if len(geolocated_pageids)==0: 
#        print ('No Article geolocation in this language.'); 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
        return;


    results = rg.search(list(geolocated_pageids_coords.values()))
    print (str(len(results))+' coordinates reversed.')

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    ccc_geolocated_items = []
    page_ids = list(geolocated_pageids_coords.keys())
    i = 0
    for result in results:
        page_id=page_ids[i]
        i+=1

        data = geolocated_pageids[page_id]
        page_title = data[0]
        qitem_specific = data[1]
        lat = str(data[2])
        lon = str(data[3])

        del geolocated_pageids[page_id]

        admin1=result['admin1']
        iso3166=result['cc']
        iso31662=''; 
        try: iso31662=iso3166+'-'+subdivisions[admin1].split('-')[1]
        except: pass
        # print (page_title,page_id,admin1,iso3166,iso31662)

        qitem=''
        # try both country code and admin1, at the same time, just in case there is desambiguation ('Punjab' in India (IN) and in Pakistan (PK) for 'pa' language)
        try: 
            qitem=territories[(territories.ISO3166 == iso3166) & (territories.territoryname == admin1)].loc[languagecode]['QitemTerritory']
            # print (qitem); print ('name and country')
        except: 
            pass
        try:
            # try to get qitem from country code.        
            if qitem == '':
                try:
                    qitem = ISO3166codes[iso3166]
                    # print (qitem); print ('country')
                    # try to get qitem from admin1: in territorynames, territorynamesNative and subdivisions.
                except:
                    try:
                        qitem=territorynames[admin1]
                        # print (qitem); print ('territorynames in English.')
                    except: 
                        try:
                            qitem=territorynamesNative[admin1]
                            # print (qitem); print ('territorynames in Native.')
                        except: 
                            try:
                                qitem=ISO31662codes[iso31662]
                                # print (qitem); print ('ISO31662codes.')
                            except:
                                pass
        except:
            pass

        coordinates = lat+','+lon
        if qitem!='': 
            ccc_geolocated = 1
            ccc_binary = 1
            # print ((page_id,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
        else: 
            ccc_geolocated = -1
            ccc_binary = 0
#            print ('### NO!\n')


        ccc_geolocated_items.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))

        if i%20000 == 0: 

            query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,main_territory) = (?,?,?,?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
            cursor.executemany(query,ccc_geolocated_items)
            conn.commit()

            print (i)
            ccc_geolocated_items = []

    # It inserts the right Articles into the corresponding CCC database.
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,main_territory) = (?,?,?,?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
    cursor.executemany(query,ccc_geolocated_items)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


# Obtain the Articles whose WikiData items have properties linked to territories and language names (groundtruth). Label them as CCC.
# There is margin for optimization: Articles could be updated more regularly to the database, so in every loop it is not necessary to go through all the items.
def label_ccc_articles_geolocation_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_geolocation_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return


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
            if ISO31662 != '':
                ISO31662codes[ISO31662]=qitem

        allISO3166.append(territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166'])
    allISO3166 = list(set(allISO3166))
#    print (allISO3166)

    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    subdivisions = wikilanguages_utils.load_world_subdivisions_multilingual()

    # Get the Articles, evaluate them and insert the good ones.   
    ccc_geolocated_items = []
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
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
        try: iso31662=iso3166+'-'+subdivisions[admin1].split('-')[1]
        except: pass
        page_title=str(row[4]).replace(' ','_')
        try: page_id = page_titles_page_ids[page_title]
        except: continue

#        print (page_title,page_id,qitem_specific,admin1,iso3166,coordinates)

        qitem=''
        # try both country code and admin1, at the same time, just in case there is desambiguation ('Punjab' in India (IN) and in Pakistan (PK) for 'pa' language)
        try: 
            qitem=territories[(territories.ISO3166 == iso3166) & (territories.territoryname == admin1)].loc[languagecode]['QitemTerritory']
#            print (qitem); print ('name and country')
        except: 
            pass
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
                            except:
                                pass
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

        # if page_title == 'Nefertiti':
        #     print (ccc_binary, ccc_geolocated, 'Nefertiti')
        #     input('')


        try:
#            print ((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))
            ccc_geolocated_items.append((ccc_binary,ccc_geolocated,iso3166,iso31662,coordinates,qitem,page_id,page_title,qitem_specific))

        except:
            pass

        if x%20000 == 0: print (x)
        x = x + 1

    num_art = wikipedialanguage_numberarticles[languagecode]
    if num_art == 0: percent = 0
    else: percent = 100*len(ccc_geolocated_items)/num_art

    if len(ccc_geolocated_items)==0: 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
#        print ('No geolocated Articles in Wikidata for this language edition.');
        return
    # Insert to the corresponding CCC database.
#    print ('Inserting/Updating Articles into the database.')
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,ccc_geolocated,iso3166,iso31662,geocoordinates,main_territory) = (?,?,?,?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
    cursor2.executemany(query,ccc_geolocated_items)
    conn2.commit()

#    print ('All geolocated Articles from Wikidata validated through reverse geocoding are in. They are: '+str(len(ccc_geolocated_items))+'.')
#    print ('They account for a '+str(percent)+'% of the entire Wikipedia.')

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a country property related to a territory from the list of territories from the language. Label them as CCC.
def label_ccc_articles_country_wd(languagecode,page_titles_page_ids):

    function_name = 'label_ccc_articles_country_wd '+languagecode
#    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    # country qitems
    try: countries = territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'].tolist()
    except: 
        try: countries = list(); countries.append(territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'])
        except:
            duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
            wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
#            print ('there are no entire countries where the '+languagecode+' is official')
            return
    ccc_countries = countries + list(set(wikilanguages_utils.get_old_current_countries_pairs(languagecode,'regional').keys()))


    (iso_qitem, label_qitem) = wikilanguages_utils.load_all_countries_qitems()
    try: countries_language = set(territories.loc[languagecode]['ISO3166'].tolist())
    except: 
        try: countries_language = set(); countries_language.add(territories.loc[languagecode]['ISO3166'])
        except: pass
    countries_language = list(set(countries_language)&set(iso_qitem.keys())) # these iso3166 codes
    ccc_regional_countries = []
    for country in countries_language: ccc_regional_countries.append(iso_qitem[country])
    ccc_regional_countries = ccc_regional_countries + list(set(wikilanguages_utils.get_old_current_countries_pairs(languagecode,'').keys()))


    # get Articles
    qitem_properties = {}
    qitem_page_titles = {}
    ccc_country_items = []

    qitem_properties_other = {}
    qitem_page_titles_other = {}
    other_ccc_country_items = []

    query = 'SELECT country_properties.qitem, country_properties.property, country_properties.qitem2, sitelinks.page_title FROM country_properties INNER JOIN sitelinks ON sitelinks.qitem = country_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 in ccc_countries:
    #        print ((qitem, wdproperty, country_properties[wdproperty], page_title))
            value = wdproperty+':'+qitem2
            if qitem not in qitem_properties: qitem_properties[qitem]=value
            else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value
            qitem_page_titles[qitem]=page_title

        elif qitem2 not in ccc_regional_countries:
            if qitem not in qitem_properties_other: qitem_properties_other[qitem]=1
            else: qitem_properties_other[qitem]=qitem_properties_other[qitem]+1
            qitem_page_titles_other[qitem]=page_title


    # Get the tuple ready to insert.
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_country_items.append((0,values,qitem_page_titles[qitem],qitem,page_id))
        except: 
            continue

    for qitem, values in qitem_properties_other.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_country_items.append((1,str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            continue

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,other_ccc_country_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,other_ccc_country_items)
    conn2.commit()


    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,country_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_country_items)
    conn2.commit()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




# Obtain the Articles with a location property that is iteratively associated to the list of territories associated to the language. Label them as CCC.
def label_ccc_articles_location_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_location_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    qitems_territories=[]
    if languagecode not in languageswithoutterritory:
        try: qitems_territories=territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems_territories=[];qitems_territories.append(territories.loc[languagecode]['QitemTerritory'])

    if len(qitems_territories)==0: 
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
#        print ('Oops. There are no territories for this language.'); 
        return;


    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    query = 'SELECT location_properties.qitem, location_properties.property, location_properties.qitem2, sitelinks.page_title FROM location_properties INNER JOIN sitelinks ON sitelinks.qitem = location_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    rows = []
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')
        rows.append([qitem,wdproperty,qitem2,page_title])
    


    # Articles CCC Location
    selected_qitems = {}
    for QitemTerritory in qitems_territories:
        QitemTerritoryname = territories.loc[territories['QitemTerritory'] == QitemTerritory].loc[languagecode]['territoryname']
#        print ('We start with this territory: '+QitemTerritoryname+' '+QitemTerritory)
#        if QitemTerritory == 'Q5705': continue

        target_territories={QitemTerritory:0}

        counter = 1
        updated = 0
        round = 1
        number_items_territory = 0
        while counter != 0: # when there is no level below as there is no new items. there are usually 6 levels.
#            print ('# Round: '+str(round))
            round_territories = {}
            counter = 0

            for row in rows:
                qitem = row[0]
                wdproperty = row[1]
                qitem2 = row[2]
                page_title = row[3].replace(' ','_')

                if qitem2 in target_territories:
#                    print ((round,qitem,page_title,wdproperty,location_properties[wdproperty],page_qitems_titles[qitem2],page_qitems_titles[QitemTerritory]))
                    try:
                        selected_qitems[qitem]=selected_qitems[qitem]+[wdproperty,qitem2,QitemTerritory]
                        updated = updated + 1
                    except:
                        selected_qitems[qitem]=[page_title,wdproperty,qitem2,QitemTerritory]
                        counter = counter + 1
                        round_territories[qitem]=0

            target_territories = round_territories
            number_items_territory = number_items_territory + len(round_territories)

#            print ('In this iteration we added this number of NEW items: '+(str(counter)))
#            print ('In this iteration we updated this number of items: '+(str(updated)))
#            print ('The current number of selected items for this territory is: '+str(number_items_territory))
            round = round + 1

#        print ('- The number of items related to the territory '+QitemTerritoryname+' is: '+str(number_items_territory))
#        print ('- The TOTAL number of selected items at this point is: '+str(len(selected_qitems))+'\n')
#        break
#    for keys,values in selected_qitems.items(): print (keys,values)
   

    # Articles Other CCC Location
    other_ccc_qitems  = {}
    for row in rows:
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 not in selected_qitems:
            if qitem not in other_ccc_qitems: other_ccc_qitems[qitem]=1
            else: other_ccc_qitems[qitem]=other_ccc_qitems[qitem]+1


    # Get the tuple ready to insert the CCC Located Items.
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


    # Get the tuple ready to insert.
    other_ccc_located_items = []
    for qitem in other_ccc_qitems:
        try:
            page_title = qitem_page_titles[qitem]
            page_id = page_titles_page_ids[page_title]
            other_ccc_located_items.append((0,other_ccc_qitems[qitem],page_title,qitem,page_id))
        except:
            pass

    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    query = 'UPDATE '+languagecode+'wiki SET ccc_binary = ?, other_ccc_location_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,other_ccc_located_items)
    conn2.commit()

    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,location_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_located_items)
    conn2.commit()

    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a "strong" language property that is associated the language. Label them as CCC.
def label_ccc_articles_language_strong_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_language_strong_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()

    # language qitems
    qitemresult = languages.loc[languagecode]['Qitem']
    if ';' in qitemresult: qitemresult = qitemresult.split(';')
    else: qitemresult = [qitemresult];

    other_ccc_language = {}

    # get Articles
    qitem_properties = {}
    qitem_page_titles = {}
    query = 'SELECT language_strong_properties.qitem, language_strong_properties.property, language_strong_properties.qitem2, sitelinks.page_title FROM language_strong_properties INNER JOIN sitelinks ON sitelinks.qitem = language_strong_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 not in qitemresult:
            if qitem2 not in other_ccc_language: other_ccc_language[qitem]=1
            else: other_ccc_language[qitem]=other_ccc_language[qitem]+1

        else:
    #        print ((qitem, wdproperty, language_properties[wdproperty], page_title))
            # Put the items into a dictionary
            value = wdproperty+':'+qitem2
            if qitem not in qitem_properties: qitem_properties[qitem]=value
            else: qitem_properties[qitem]=qitem_properties[qitem]+';'+value

        qitem_page_titles[qitem]=page_title


    # Get the tuple ready to insert for language strong CCC.
    ccc_language_items = []
    for qitem, values in qitem_properties.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            ccc_language_items.append((1,values,qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass

    # Get the tuple ready to insert for other language strong CCC.
    other_ccc_language_items = []
    for qitem, values in other_ccc_language.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_language_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass

    # Insert to the corresponding CCC database.
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,language_strong_wd,page_title) = (?,?,?) WHERE qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()

    # Insert to the corresponding CCC database.
    query = 'UPDATE '+languagecode+'wiki SET other_ccc_language_strong_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,other_ccc_language_items)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a creation property that is related to the items already retrieved as CCC. Label them as CCC.
def label_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_created_by_properties_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'): ccc_articles[row[1]]=row[0].replace(' ','_')


    other_ccc_created_by_qitems = {}
    selected_qitems={}
    conn2 = sqlite3.connect(databases_path + wikidata_db); cursor2 = conn2.cursor()
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

        else:
            if qitem not in other_ccc_created_by_qitems: other_ccc_created_by_qitems[qitem]=1
            else: other_ccc_created_by_qitems[qitem]=other_ccc_created_by_qitems[qitem]+1

        qitem_page_titles[qitem]=page_title

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

    other_ccc_created_by_items = []
    for qitem, values in other_ccc_created_by_qitems.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            other_ccc_created_by_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass

    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,created_by_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_created_by_items)
    conn.commit()

    query = 'UPDATE '+languagecode+'wiki SET other_ccc_created_by_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,other_ccc_created_by_items)
    conn.commit()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles which are part of items already retrieved as CCC. Label them as CCC.
def label_ccc_articles_part_of_properties_wd(languagecode,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_part_of_properties_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()

    part_of_properties = {'P361':'part of'} 

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikidata_db); cursor2 = conn2.cursor()

    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
        ccc_articles[row[1]]=row[0].replace(' ','_')

#    potential_ccc_articles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM '+languagecode+'wiki;'):
#        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    other_ccc_part_qitems = {}
    selected_qitems={}
    qitem_page_titles = {}
    query = 'SELECT part_of_properties.qitem, part_of_properties.property, part_of_properties.qitem2, sitelinks.page_title FROM part_of_properties INNER JOIN sitelinks ON sitelinks.qitem = part_of_properties.qitem WHERE sitelinks.langcode ="'+languagecode+'wiki"'
    for row in cursor2.execute(query):
        qitem = row[0]
        wdproperty = row[1]
        qitem2 = row[2]
        page_title = row[3].replace(' ','_')

        if qitem2 in ccc_articles:
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


        else:
            if qitem not in other_ccc_part_qitems: other_ccc_part_qitems[qitem]=1
            else: other_ccc_part_qitems[qitem]=other_ccc_part_qitems[qitem]+1

        qitem_page_titles[qitem]=page_title


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


    othe_ccc_part_of_items = []
    for qitem, values in other_ccc_part_qitems.items():
        try: 
            page_id=page_titles_page_ids[qitem_page_titles[qitem]]
            othe_ccc_part_of_items.append((str(values),qitem_page_titles[qitem],qitem,page_id))
        except: 
            pass

    # INSERT INTO CCC DATABASE
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,part_of_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_part_of_items)
    conn.commit()

    query = 'UPDATE '+languagecode+'wiki SET other_ccc_part_of_wd = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,othe_ccc_part_of_items)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Obtain the Articles with a keyword in title. This is considered potential CCC.
def label_ccc_articles_keywords(languagecode,page_titles_qitems,page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'label_ccc_articles_keywords '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

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
        insertedarticles.append((1,keyword_title,page_title,page_id,qitem))


    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'UPDATE '+languagecode+'wiki SET (ccc_binary,keyword_title,page_title) = (?,?,?) WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,insertedarticles)
    conn.commit()

    print ('articles with keywords on titles in Wikipedia language '+(languagecode)+' have been inserted.');
    print ('The number of inserted Articles are '+str(len(insertedarticles)))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Extends the Articles table with the topic (e.g. gender) from wikidata.
#topic = ['gender', 'folk', 'monuments_and_buildings', 'earth', 'music_creations_and_organizations', 'sport', 'food', 'paintings', 'glam', 'books', 'clothing_and_fashion', 'ethnic_gruop', 'supra_ethnic_group', 'religion', 'religious_group']

def label_diversity_groups_related_topics_wd(topic, languagecode, page_titles_page_ids, page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'label_diversity_groups_related_topics_wd '+languagecode+' '+topic
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

    updated = []

    if topic == 'gender':
        query = "SELECT people_properties.qitem, people_properties.qitem2 FROM people_properties WHERE people_properties.property = 'P21' AND people_properties.qitem IN (SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON sitelinks.qitem = people_properties.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki' AND people_properties.qitem2 = 'Q5');"

    elif topic == 'industry':
        query = "SELECT industry_properties.qitem, industry_properties.qitem2 FROM industry_properties INNER JOIN sitelinks ON sitelinks.qitem = industry_properties.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki';"

    elif topic == "supra_ethnic_group":
        query = "SELECT instance_of_subclasses_of_properties.qitem, instance_of_subclasses_of_properties.qitem2 FROM instance_of_subclasses_of_properties WHERE qitem2 IN (SELECT DISTINCT ethnic_group.qitem2 FROM ethnic_group) AND sitelinks.langcode = '"+languagecode+"wiki';"

    else:
        query = "SELECT "+topic+".qitem, "+topic+".qitem2 FROM "+topic+" INNER JOIN sitelinks ON sitelinks.qitem = "+topic+".qitem WHERE sitelinks.langcode = '"+languagecode+"wiki';"


    for row in cursor.execute(query):
        try:
            qitem=row[0]
            qualifier=row[1] # for people the qualifier is gender
            page_id=page_titles_page_ids[qitems_page_titles[qitem]]
            updated.append((qualifier,page_id,qitem))
#            print (page_id,qitem,iw_count)
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki SET '+topic+'=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    print (len(updated))
    print (topic+' properties updated.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def label_sexual_orientation_wd(languagecode, page_titles_page_ids, page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'label_sexual_orientation_wd '+languagecode
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}


    # SEXUAL ORIENTATION BASED ON SEXUAL ORIENTATION PROPERTY
    updated = []
    query = "SELECT sexual_orientation.qitem, sexual_orientation.qitem2 FROM sexual_orientation_properties INNER JOIN sitelinks ON sitelinks.qitem = sexual_orientation_properties.qitem WHERE sexual_orientation.property = 'P91' AND sitelinks.langcode = '"+languagecode+"wiki'"

    for row in cursor.execute(query):
        try:
            qitem=row[0]
            sexual_orientation=row[1]
            if sexual_orientation == 'Q592' or sexual_orientation == 'Q6649': sexual_orientation = 'Q6636'
            page_id=page_titles_page_ids[qitems_page_titles[qitem]]
            updated.append((sexual_orientation,page_id,qitem))
        except:
            pass

    query = 'UPDATE '+languagecode+'wiki SET '+topic+'=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()


    query = "SELECT sex.qitem, sex.qitem2, pe1.qitem2, pe2.qitem2 FROM sexual_orientation_properties sex LEFT JOIN people pe1 ON sex.qitem = pe1.qitem LEFT JOIN people pe2 ON sex.qitem2 = pe2.qitem INNER JOIN sitelinks ON sitelinks.qitem = sex.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki' AND sex.property != 'P91' ORDER BY 1, 2;"



    # SEXUAL ORIENTATION BASED ON PARTNERS AND SPOUSES
    updated = []
    sexual_orientation_occasions = []

    sexual_orientation = ''
    old_subject = ''
    for row in cursor.execute(query):
        sexual_orientation = ''
        subject=row[0]
        partner=row[1]
       
        if subject != old_subject and old_subject != '':
            sexual_orientation_occasions = set(sexual_orientation_occasions)
            if len(sexual_orientation_occasions)==1: sexual_orientation = sexual_orientation_occasions[0]
            if len(sexual_orientation_occasions)==2: sexual_orientation = 'Q43200'

            try:
                page_id=page_titles_page_ids[qitems_page_titles[qitem]]
                updated.append((sexual_orientation,page_id,old_subject))
            except:
                pass

            sexual_orientation_occasions = []


        subject_gender = row[2]
        partner_gender = row[3]

        if subject_gender == partner_gender: sexual_orientation_occasions.append('Q6636')
        if subject_gender != partner_gender: sexual_orientation_occasions.append('Q1035954')

        old_subject = subject
        old_partner = partner

    sexual_orientation_occasions = set(sexual_orientation_occasions)
    if len(sexual_orientation_occasions)==1: sexual_orientation = sexual_orientation_occasions[0]
    if len(sexual_orientation_occasions)==2: sexual_orientation = 'Q43200'

    try:
        page_id=page_titles_page_ids[qitems_page_titles[qitem]]
        updated.append((sexual_orientation,page_id,old_subject))
    except:
        pass

    query = 'UPDATE '+languagecode+'wiki SET '+topic+'=? WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def label_start_and_end_time(languagecode, page_titles_page_ids, page_titles_qitems):

    functionstartTime = time.time()
    function_name = 'label_diversity_groups_related_topics_wd '+languagecode+' '+topic
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

    start_time_properties = {'P569': 'date of birth', 'P580':'start time', 'P577': 'publication date', 'P571': 'inception', 'P1619': 'date of official opening', 'P1191': 'date of first performance', 'P813': 'retrieved', 'P1249': 'time of earliest written record', 'P575':'time of discovery or invention'};

    end_time_properties = {'P570': 'date of death', 'P582': 'end time', 'P576':'dissolved, abolished or demolished'}


    query = "SELECT time_properties.qitem, time_properties.property, time_properties.qitem2 FROM time_properties INNER JOIN sitelinks ON sitelinks.qitem = time_properties.qitem WHERE sitelinks.langcode = '"+languagecode+"wiki';"


    old_qitem = ''
    props_start_time = []
    qualifiers_start_time = []
    props_end_time = []
    qualifiers_end_time = []      

    for row in cursor.execute(query):
        start_time = ''
        end_time = ''

        qitem = row[0]
        prop = row[1]
        qualifier = row[2]

        if qitem != old_qitem and old_qitem != '':

            if len(qualifiers_start_time)>0: start_time = qualifiers_start_time[0]
            if len(qualifiers_end_time)>0: end_time = qualifiers_end_time[0]

            if start_time != '' and end_time == '': end_time = '*'

            page_id=page_titles_page_ids[qitems_page_titles[qitem]]
            updated.append((start_time,end_time,page_id,qitem))

            # print ((props_start_time[0],start_time,qualifiers_end_time[0],end_time,page_id,qitem))

            qualifiers_start_time = []
            qualifiers_end_time = []

        old_qitem = qitem

        if qualifier in start_time_properties:
    #        props_start_time.append(prop)
            qualifiers_start_time.append(qualifier)

        if qualifier in end_time_properties:
    #        props_end_time.append(prop)
            qualifiers_end_time.append(qualifier)        

    query = 'UPDATE '+languagecode+'wiki SET (start_time, end_time) = (?,?) WHERE page_id = ? AND qitem = ?;'
    cursor2.executemany(query,updated)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



#######################################################################################

def main_with_exception_email():
    try:
        main()
    except:
    	wikilanguages_utils.send_email_toolaccount('WCDO - CONTENT RETRIEVAL ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/content_retrieval.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('WCDO - CONTENT RETRIEVAL ERROR: '+ wikilanguages_utils.get_current_cycle_year_month(), 'ERROR.' + lines); print("Now let's try it again...")
            time.sleep(900)
            continue


#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("content_retrieval"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("content_retrieval"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'content_retrieval.py'   
    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
#    check_time_for_script_run(script_name, cycle_year_month)
    startTime = time.time()

    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();


    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)
    print (len(wikilanguagecodes))

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']

    # Get the number of Articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')
#    print (wikilanguagecodes)
    
    wikilanguagecodes_by_size = [k for k in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)]
    biggest = wikilanguagecodes_by_size[:20]; smallest = wikilanguagecodes_by_size[20:]


    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '')
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)

    wikilanguages_utils.finish_email(startTime,'content_retrieval.out','Content Retrieval')


