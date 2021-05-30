# -*- coding: utf-8 -*-

# time
import time
import datetime
import dateutil
import calendar
# system
import os
import sys
import re
import csv
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# requests and others
import requests
# data
import pandas as pd
import colour


databases_path = '/srv/wcdo/databases/'
datasets_path = '/srv/wcdo/datasets/'
dumps_path = '/srv/wcdo/dumps/'

wikidata_db = 'wikidata.db'

diversity_categories_db = 'diversity_categories.db'
diversity_categories_production_db  = 'diversity_categories_production.db'

diversity_observatory_log = 'diversity_observatory_log.db'

wikipedia_diversity_db = 'wikipedia_diversity.db'
wikipedia_diversity_production_db = 'wikipedia_diversity_production.db'

stats_db = 'stats.db'
stats_production_db = 'stats_production.db'

top_diversity_db = 'top_diversity_articles.db'
top_diversity_production_db = 'top_diversity_articles_production.db'

missing_ccc_db = 'missing_ccc.db'
missing_ccc_production_db = 'missing_ccc_production.db'

ethnic_groups_content_db = 'ethnic_groups_content.db'
ethnic_groups_content_production_db = 'ethnic_groups_content_production.db'

lgbt_content_db = 'lgbt_content.db'
lgbt_content_production_db = 'lgbt_content_production.db'

images_db = 'images.db'
images_production_db = 'images_production.db'

revision_db = 'revision.db'
imageslinks_db = 'imagelinks.db'

wikipedia_namespace_db = 'wikipedia_namespace.db'

community_health_metrics_db = 'community_health_metrics.db'



# Loads language_territories_mapping.csv file
def load_wikipedia_languages_territories_mapping():

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();  

    query = 'SELECT WikimediaLanguagecode, languagenameEnglishethnologue, territoryname, territorynameNative, QitemTerritory, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, officialnationalorregional, region, subregion, intermediateregion FROM wikipedia_languages_territories_mapping;'

    territories = pd.read_sql_query(query, conn)
    territories = territories.set_index(['WikimediaLanguagecode'])

#    print (territories.head(20))

# READ FROM META: 
# check if there is a table in meta:
#    generate_print_language_territories_mapping_table()
# uses pywikibot and verifies the differences with the file.
# sends an e-mail with the difference and the 'new proposed file'
#    send_email_toolaccount('subject', 'message')
# stops.
# we verify this e-mail, we re-start the task.
    return territories


# Loads wikipedia_language_editions.csv file
def load_wiki_projects_information():
    # in case of extending the project to other WMF sister projects, it would be necessary to revise these columns and create a new file where a column would specify whether it is a language edition, a wikictionary, etc.

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();

    query = 'SELECT languagename, Qitem, WikimediaLanguagecode, Wikipedia, WikipedialanguagearticleEnglish, languageISO, languageISO3, languageISO5, nativeLabel, region, subregion, intermediateregion FROM wiki_projects;'

    languages = pd.read_sql_query(query, conn)
    languages = languages.set_index(['WikimediaLanguagecode'])


    return languages


def load_language_pairs_territory_status():

    conn = sqlite3.connect(databases_path+diversity_categories_production_db); cursor = conn.cursor();

    query = 'SELECT qitem,territoryname_english, territoryname_higher, ISO3166, ISO3166_2, language_lower_name, language_higher_name, wikimedia_lower, wikimedia_higher, type_overlap, status_lower, status_higher, equal_status, indigenous_lower, indigenous_higher FROM wikipedia_language_pairs_territory_status WHERE equal_status=0;'

    pairs = pd.read_sql_query(query, conn)
    pairs = pairs.set_index(['wikimedia_lower'])

    return pairs


def load_all_languages_information():
    print ('')
    # in this case, we obtain denomynm, language name, etc. for a language without a wikipedia.
    # This comes from Wikidata.

def load_all_territories_information():
    print ('')
    # in this case, we obtain territories for a language without a wikipedia.
    # This comes from Ethnologue and complemented.


def load_wikipedia_language_editions_numberofarticles(wikilanguagecode, db):
    wikipedialanguage_numberarticles = {}

    if db=='production': 
        database = wikipedia_diversity_db.split('.')[0]+'_production.db';
    else:
        database = wikipedia_diversity_db

    print ('database in use in load_wikipedia_language_editions_numberofarticles: '+database)

    conn = sqlite3.connect(databases_path + database); cursor = conn.cursor()

    count = 0
    # Obtaining CCC for all WP
    for languagecode in wikilanguagecode:
        try:
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki;'
            cursor.execute(query)
            number = cursor.fetchone()[0]
            count+= number
            wikipedialanguage_numberarticles[languagecode]=number
        except:
            print ('this language is not in the database yet: '+languagecode)

    print ('wikipedialanguage_numberarticles loaded.')
    print ('this is the total number of articles in the Diversity Database: '+str(count))
    return wikipedialanguage_numberarticles



def load_dicts_page_ids_qitems(printme, languagecode):
    page_titles_qitems = {}
    page_titles_page_ids = {}

    # conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    a='1'
    try:
        query = 'SELECT 1 FROM '+languagecode+'wiki;'
        cursor.execute(query)
        a='0'
    except:
        print ('sqlite3.OperationalError: no such table: '+languagecode)
    if a=='1':
        return (page_titles_qitems, page_titles_page_ids)

    i=1
    while (i!=0):
        try:
            query = 'SELECT page_title, qitem, page_id FROM '+languagecode+'wiki;'
#            query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
            for row in cursor.execute(query):
                page_title=row[0].replace(' ','_')
                page_titles_page_ids[page_title]=row[2]
                page_titles_qitems[page_title]=row[1]
            i = 0
        except:
            print('Database is lock. We try again.')
            time.sleep(120)


    if printme == 1:
        print ('language: '+languagecode)
        print ('page_ids loaded.')
        print ('qitems loaded.')
        print ('they are:')
        print (len(page_titles_qitems))

    return (page_titles_qitems, page_titles_page_ids)



def load_countries_from_language(languagecode,territories):
    country_territories = {}
    countries=territories.loc[languagecode].ISO3166
    if isinstance(countries,str): countries = [countries]
    else: countries = list(set(countries))

    return countries


def load_territories_from_language_country(ISO3166, languagecode, territories):
    qitems = territories.loc[territories['ISO3166'] == ISO3166].loc[languagecode]['QitemTerritory']
    if isinstance(qitems,str): qitems = [qitems]
    else: qitems = list(qitems)
    
    return qitems

def load_territories_names_from_language_country(ISO3166, languagecode, territories):
    territorynames = territories.loc[territories['ISO3166'] == ISO3166].loc[languagecode]['territoryname']
    if isinstance(territorynames,str): territorynames = [territorynames]
    else: territorynames = list(territorynames)
    
    return territorynames



def load_iso_3166_to_geographical_regions():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    query = 'SELECT alpha_2, name, region, sub_region FROM country_regions;'

    country_regions = pd.read_sql_query(query, conn)
    country_regions = country_regions.rename(columns={'sub_region': 'subregion'})
    country_regions = country_regions.set_index(['alpha_2'])

    country_names = country_regions.name.to_dict()
    regions = country_regions.region.to_dict()
    subregions = country_regions.subregion.to_dict()

#    print (country_names)
    
    return country_names, regions, subregions


def load_countries_subdivisions_from_language(languagecode,territories):
    # print (languagecode)
    subdivisions=territories.loc[languagecode].ISO31662

    if isinstance(subdivisions,str): subdivisions = [subdivisions]
    elif subdivisions is not None: subdivisions = list(subdivisions.unique())
    else: subdivisions = []

    return subdivisions


def load_iso_31662_to_subdivisions():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT territoryname, ISO31662 FROM wikipedia_languages_territories_mapping;'
    subdivisions = pd.read_sql_query(query, conn)

    subdivisions2 = subdivisions.set_index(['ISO31662'])
    ISO31662_subdivisions_dict = subdivisions2.territoryname.to_dict()
    for isocode in list(ISO31662_subdivisions_dict.keys()):
        subdivision = ISO31662_subdivisions_dict[isocode]
        if subdivision == None or subdivision == '':
            del ISO31662_subdivisions_dict[isocode]

    subdivisions3 = subdivisions.set_index(['territoryname'])
    subdivisions_ISO31662_dict = subdivisions3.ISO31662.to_dict()
    for subdivision in list(subdivisions_ISO31662_dict.keys()):
        isocode = subdivisions_ISO31662_dict[subdivision]
        if isocode == None or isocode == '':
            del subdivisions_ISO31662_dict[subdivision]

    return ISO31662_subdivisions_dict, subdivisions_ISO31662_dict


def load_iso_31662_to_subdivisions_names():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT subdivision_name as territoryname, subdivision_code as ISO31662 FROM ISO3166_2_subdivisions;'
    subdivisions = pd.read_sql_query(query, conn)

    subdivisions2 = subdivisions.set_index(['ISO31662'])
    ISO31662_subdivisions_dict = subdivisions2.territoryname.to_dict()
    for isocode in list(ISO31662_subdivisions_dict.keys()):
        subdivision = ISO31662_subdivisions_dict[isocode]
        if subdivision == None or subdivision == '':
            del ISO31662_subdivisions_dict[isocode]

    subdivisions3 = subdivisions.set_index(['territoryname'])
    subdivisions_ISO31662_dict = subdivisions3.ISO31662.to_dict()
    for subdivision in list(subdivisions_ISO31662_dict.keys()):
        isocode = subdivisions_ISO31662_dict[subdivision]
        if isocode == None or isocode == '':
            del subdivisions_ISO31662_dict[subdivision]

    return subdivisions_ISO31662_dict, ISO31662_subdivisions_dict


def load_world_subdivisions():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT name, subdivision_code FROM world_subdivisions;'

    world_subdivisions = {}
    for row in cursor.execute(query):
        world_subdivisions[row[0]]=row[1]

    return world_subdivisions


def load_world_subdivisions_ip2location():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT subdivision_name, subdivision_code FROM ISO3166_2_ip2location;'

    world_subdivisions = {}
    for row in cursor.execute(query):
        world_subdivisions[row[0]]=row[1]

    return world_subdivisions



def load_world_subdivisions_multilingual():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT subdivision_name, subdivision_code FROM multilingual_ISO3166_2;'

    world_subdivisions = {}
    for row in cursor.execute(query):
        world_subdivisions[row[0]]=row[1]

    return world_subdivisions



def load_language_pairs_apertium(wikilanguagecodes):

    functionstartTime = time.time()
    print ('* load_language_pairs_apertium')

#    r = requests.get("https://cxserver.wikimedia.org/v1/list/languagepairs", timeout=0.5)
#    print (r.text)

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    languagecode_translated_from={}

    try:
        query = 'SELECT * FROM apertium_language_pairs;'
        for row in cursor.execute(query):
            if row[0] not in languagecode_translated_from:
                languagecode_translated_from[row[0]]=[row[1]]
            else:
                languagecode_translated_from[row[0]].append(row[1])

        print (0/len(languagecode_translated_from))        
        print ('apertium_language_pairs loaded from the database.')

    except:
        print ('apertium_language_pairs does not exist. creating it...')
        query = ('CREATE TABLE IF NOT EXISTS apertium_language_pairs ('+
        'origin text,'+
        'target text,'+
        'PRIMARY KEY (origin,target));')
        cursor.execute(query); conn.commit()


        query = 'INSERT OR IGNORE INTO apertium_language_pairs (origin, target) VALUES (?,?)'

        for languagecode_1 in wikilanguagecodes:
            languagecode_translated_from[languagecode_1]=[]
            for languagecode_2 in wikilanguagecodes:
                if languagecode_1 == languagecode_2: continue
                title=''
                try:
                    r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+languagecode_2+"/"+languagecode_1+"/Apertium", data={'html': '<div>'+title+'</div>'}, timeout=0.5) # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                    if r!= None and r.text!='Provider not supported': 
                        print (languagecode_1,'from',languagecode_2, 'paired')
                        languagecode_translated_from[languagecode_1].append(languagecode_2)

                        cursor.execute(query,[languagecode_1,languagecode_2]);
                except:
                    print (languagecode_1,languagecode_2,'timeout')
        conn.commit()

    print ('* load_language_pairs_apertium Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    # for a target, it returns you the origin.
    return languagecode_translated_from


# It returns a dictionary with qitems and countries
def load_all_countries_qitems():
    country_names, regions, subregions = load_iso_3166_to_geographical_regions()

    territories = load_wikipedia_languages_territories_mapping()
    list_territories_ISO3166 = territories['ISO3166'].unique().tolist()

    iso_qitem = {}
    label_qitem = {}

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?item ?itemLabel ?isocode WHERE {
          ?item wdt:P297 ?isocode .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }'''

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
#    url = 'https://query.wikidata.org/sparql'
    data = requests.get(url,headers={'User-Agent': 'https://wikitech.wikimedia.org/wiki/User:Marcmiquel'}, params={'query': query, 'format': 'json'})
#    print (data)
    data = data.json()
#    print (data)

    for item in data['results']['bindings']:

        try:
            qitem_value = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            iso_value = item['isocode']['value']
            item_name_value = item['itemLabel']['value']

#            if iso_value not in country_names: continue

            iso_qitem[iso_value]=qitem_value
            label_qitem[item_name_value]=qitem_value
        except:
            pass

    """


    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?item ?itemLabel ?isocode WHERE {
          ?item wdt:P31 wd:Q6256.
          OPTIONAL{?item wdt:P297 ?isocode .}
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    for item in data['results']['bindings']:

        try:
            qitem_value = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            iso_value = item['isocode']['value']
            item_name_value = item['itemLabel']['value']

            if iso_value not in country_names: continue

            iso_qitem[iso_value]=qitem_value
            label_qitem[item_name_value]=qitem_value
        except:
            pass


    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?item ?itemLabel ?isocode WHERE {
          ?item wdt:P31 wd:Q3624078.
          ?item wdt:P297 ?isocode .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    for item in data['results']['bindings']:

        try:
            qitem_value = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            iso_value = item['isocode']['value']
            item_name_value = item['itemLabel']['value']

            if iso_value not in country_names: continue

            iso_qitem[iso_value]=qitem_value
            label_qitem[item_name_value]=qitem_value
        except:
            pass



    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?item ?itemLabel ?isocode WHERE {
          ?item wdt:P31 wd:Q7275.
          ?item wdt:P297 ?isocode .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    for item in data['results']['bindings']:

        try:
            qitem_value = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            iso_value = item['isocode']['value']
            item_name_value = item['itemLabel']['value']

            if iso_value not in country_names: continue

            iso_qitem[iso_value]=qitem_value
            label_qitem[item_name_value]=qitem_value
        except:
            pass

    """

#    print (len(label_qitem))
#    print (len(iso_qitem))


    for iso,qitem_value in iso_qitem.items():
        if iso in list_territories_ISO3166:
            list_territories_ISO3166.remove(iso)
        else:
#            print ('This is in Wikidata but not in the territories: '+iso)
            pass

#    print ('These are territories remaining without a Qitem: ')
#    print (list_territories_ISO3166)

    return (iso_qitem, label_qitem)



# It returns the pairs of other values introduced as country (usually former countries) in wikidata and their current country
def get_old_current_countries_pairs(languagecode,regional):

    territories = load_wikipedia_languages_territories_mapping()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    (iso_qitem, label_qitem) = load_all_countries_qitems()

    if languagecode == '':
        country_qitems = list(label_qitem.values())

    elif languagecode != '' and regional == '':
        try: countries_language = set(territories.loc[languagecode]['ISO3166'].tolist())
        except: 
            try: countries_language = set(); countries_language.add(territories.loc[languagecode]['ISO3166'])
            except: pass

        countries_language = list(set(countries_language)&set(iso_qitem.keys())) # these iso3166 codes
        country_qitems = []
        for country in countries_language: country_qitems.append(iso_qitem[country])

    elif languagecode != '' and regional == 'regional':
        try: country_qitems = territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'].tolist()
        except: 
            try: country_qitems = list(); country_qitems.append(territories.loc[territories['regional'] == 'no'].loc[languagecode]['QitemTerritory'])
            except:
                print ('there are no entire countries where the '+languagecode+' is official')
                return

    page_asstring = ','.join( ['?'] * (len(country_qitems)) ) # total 
    query = 'SELECT qitem, page_title FROM sitelinks WHERE langcode ="enwiki" AND qitem IN (SELECT DISTINCT country_properties.qitem2 FROM country_properties WHERE qitem2 NOT IN (%s))' % page_asstring

    qitems = []
    i = 0
    for row in cursor.execute(query, country_qitems):
        i+=1
#        print (row)
        qitem=row[0]
        page_title=row[1].replace(' ','_')
#        print (qitem,page_title)
        qitems.append(qitem)
#    print ('This is the number of not current countries: '+str(len(qitems)))

    old_country_qitems = {}

    """
    # COUNTRY PROPERTY
    query = 'SELECT qitem, qitem2 FROM country_properties WHERE qitem IN (%s)' % ','.join( ['?'] * (len(qitems)) )
    query+= ' AND qitem2 IN (%s)' % ','.join( ['?'] * (len(country_qitems)) )
    for row in cursor.execute(query, qitems + country_qitems):
        old_country_qitems[row[0]] = row[1]
    """

#    old_country_qitems = {}
    # PART OF PROPERTY
    query = 'SELECT qitem, qitem2 FROM part_of_properties WHERE qitem IN (%s)' % ','.join( ['?'] * (len(qitems)) )
    query+= ' AND qitem2 IN (%s)' % ','.join( ['?'] * (len(country_qitems)) )
    for row in cursor.execute(query, qitems + country_qitems):
        old_country_qitems[row[0]] = row[1]

#    old_country_qitems = {}
    # LOCATION PROPERTY
    query = 'SELECT qitem, qitem2 FROM location_properties WHERE qitem IN (%s)' % ','.join( ['?'] * (len(qitems)) )
    query+= ' AND qitem2 IN (%s)' % ','.join( ['?'] * (len(country_qitems)) )
    for row in cursor.execute(query, qitems + country_qitems):
        old_country_qitems[row[0]] = row[1]

#    old_country_qitems = {}
    # COORDINATES
    query = 'SELECT qitem, iso3166 FROM geolocated_property WHERE qitem IN (%s)' % ','.join( ['?'] * (len(qitems)) )
    for row in cursor.execute(query, qitems):
        qitem2 = iso_qitem[row[1]]
        if qitem2 in country_qitems:
            old_country_qitems[row[0]] = qitem2

#    print('This is the number of pairs of old countries with current countries: '+str(len(old_country_qitems)))

    return old_country_qitems



# It returns three lists with 20 languages according to different criteria.
def obtain_proximity_wikipedia_languages_lists(languagecode, wikipedialanguage_numberarticles, top_val, upperlower_vals, closest_val):

    # BIGGEST    # recommended 19
    top = []
    if top_val!=None:
        wikipedialanguage_numberarticles_sorted = sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)

        top = wikipedialanguage_numberarticles_sorted[:top_val]
        if languagecode in top: top.append(wikipedialanguage_numberarticles_sorted[top_val+1])
        else: top.append(languagecode)

    # MIDDLE ONES     # recommended 9 and 10
    upperlower = []
    if upperlower_vals!=None:
        i=wikipedialanguage_numberarticles_sorted.index(languagecode)
        upperlower = wikipedialanguage_numberarticles_sorted[i-upperlower_vals[0]:i]+wikipedialanguage_numberarticles_sorted[i:i+upperlower_vals[1]]

    # COVERING BEST CCC    # recommended 19
    closest = []
    if closest_val!=None:
        conn = sqlite3.connect(databases_path+stats_db); cursor = conn.cursor()
        query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE set1 = "'+languagecode+'" AND set1descriptor = "ccc" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY 2 DESC LIMIT '+str(closest_val)
        for row in cursor.execute(query):
            closest.append(row[0])

    return top, upperlower, closest


"""
obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, num):

    closest_langs = {}
    if not os.path.exists(databases_path + 'obtain_closest_for_all_languages.csv'):
        with open(databases_path + 'obtain_closest_for_all_languages.csv', 'w') as f:
            for languagecode in wikilanguagecodes:
                top, upperlower, closest= obtain_proximity_wikipedia_languages_lists(languagecode,wikipedialanguage_numberarticles, None, None, num)
                if len(closest)==0: continue
                row=''
                for x in range(0,num): row+=closest[x]+'\t'
                f.write(languagecode+'\t'+row+'\n')
                closest_langs[languagecode] = closest
    else:
        with open(databases_path + 'obtain_closest_for_all_languages.csv', 'r') as f:
            for line in f:
                line = line[:len(line)-2]
                closest=line.split('\t')
                languagecode = closest.pop(0)
                closest_langs[languagecode] = closest


    return closest_langs

"""


def obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, num):

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    query = ('CREATE TABLE IF NOT EXISTS obtain_closest_for_all_languages ('+
    'langcode text,'+
    'languages text,'+
    'PRIMARY KEY (langcode));')
    cursor.execute(query); conn.commit()

    query = 'SELECT * FROM obtain_closest_for_all_languages;'
    rows = cursor.execute(query);

    closest_langs = {}

    if rows == None:
        for languagecode in wikilanguagecodes:
            top, upperlower, closest= obtain_proximity_wikipedia_languages_lists(languagecode,wikipedialanguage_numberarticles, None, None, num)
            if len(closest)==0: continue
            row=''
            for x in range(0,num): row+=closest[x]+'\t'
            parameters.append((languagecode,row))
            query = 'INSERT OR IGNORE INTO obtain_closest_for_all_languages (langcode, languages) VALUES (?,?)'
            cursor.executemany(query,parameters)
            conn.commit()
            closest_langs[languagecode] = closest

    else:
        for row in rows:
            closest = row[1]
            closest = closest.split('\t')
            languagecode = row[0]
            closest_langs[languagecode] = closest

    return closest_langs


def get_langs_group(all_groups, topX, region, subregion, wikipedialanguage_numberarticles, territories, languages):
    if all_groups != None:
        if 'Top' in all_groups:
            topX = int(all_groups.split('Top ')[1])

        if all_groups in territories['subregion'].unique().tolist():
            subregion = all_groups

        if all_groups in territories['region'].unique().tolist():
            region = all_groups

    if all_groups == "All languages":
        topX = len(wikipedialanguage_numberarticles)

    langlist = []
    if topX != None:
        i = 0
        for w in sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True):
            if i==topX: 
                break
            langlist.append(w)
            i+=1

    if region != None:
        langlist = list(set(territories.loc[territories['region']==region].index.tolist()))

    if subregion != None:
        langlist = list(set(territories.loc[territories['subregion']==subregion].index.tolist()))

    langlistnames = {}
    for languagecode in langlist:
        lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
        langlistnames[lang_name] = languagecode

    return langlist, langlistnames



# It returns a list of languages based on the region preference introduced.
def obtain_region_wikipedia_language_list(languages, region, subregion, intermediateregion):
# use as: wikilanguagecodes = wikilanguages_utils.obtain_region_wikipedia_language_list('Asia', '', '').index.tolist()
#    print('* This is the list of continents combinations: '+' | '.join(languages.region.unique())+'\n')
#    print('* This is the list of subregions (in-continents) combinations: '+' | '.join(languages.subregion.unique())+'\n')
#    print('* This is the list of intermediate regions (in continents and regions) combinations: '+' | '.join(languages.intermediateregion.unique())+'\n')
#    print (languages)
    if region!='':
        languages_region = languages.loc[languages['region'] == region]

    if subregion!='':
        languages_region = languages[languages['subregion'].str.contains(subregion)]

    if intermediateregion!='':
        languages_region = languages[languages['intermediateregion'].str.contains(intermediateregion)]

    return languages_region



def get_store_diversity_categories_labels(wikilanguagecodes):

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikidata_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + diversity_categories_db); cursor3 = conn3.cursor()

    cursor3.execute("DROP TABLE IF EXISTS diversity_categories_labels_production;")
    conn3.commit()
    cursor3.execute("CREATE TABLE IF NOT EXISTS diversity_categories_labels_production (qitem text, label text, page_title text, code text, english_title text, lang text, category_label text, PRIMARY KEY (qitem, label, lang, category_label));")
    conn3.commit()


    category_labels = ['gender', 'ethnic_groups', 'LGBT', 'sexual_orientation', 'religious_groups', 'regions', 'subregions', 'countries', 'country_subdivision', 'languages']


    all_languages = pd.read_sql_query('SELECT Qitem, englishLabel, languageISO3 FROM all_languages_wikidata;', conn3)

    countries = pd.read_sql_query('SELECT Qitem, territoryLabel, ISO3166 FROM all_countries_wikidata;', conn3)

    country_subdivisions = pd.read_sql_query('SELECT Qitem, territoryLabel, ISO31662 FROM all_countries_subdivisions_wikidata;', conn3)

    subregions_dict = {"Q771405": "Southern Asia", "Q27479": "Northern Europe", "Q27449":"Southern Europe", "Q27381" : "Northern Africa", "Q35942":"Polynesia", "Q132959":"Sub-Saharan Africa", "Q72829598":"Latin America and the Caribbean", "Q1555938":"Antarctic", "Q7204":"Western Asia", "Q45256":"Australia and New Zealand", "Q27496":"Western Europe", "Q27468":"Eastern Europe", "Q2017699":"Northern America", "Q99929155":"South-eastern Asia", "Q27231":"Eastern Asia", "Q37394":"Melanesia", "Q702":"Micronesia", "Q27275":"Central Asia"}

    regions_dict = {"Q48" : "Asia", "Q46" : "Europe", "Q15" : "Africa", "Q538" : "Oceania", "Q828" : "Americas", "Q51" : "Antarctica"}

    religious_groups = pd.read_sql_query('SELECT religious_group_qitem, religious_group_page_title FROM religious_groups;', conn3).set_index('religious_group_qitem').to_dict()['religious_group_page_title']

    indigenous_groups = pd.read_sql_query('SELECT qitem, page_title_en FROM ethnic_groups WHERE minoritized_language = "1.0";', conn3).set_index('qitem').to_dict()['page_title_en']

    ethnic_groups = pd.read_sql_query('SELECT qitem, page_title_en FROM ethnic_groups;', conn3).set_index('qitem').to_dict()['page_title_en']

    lgbtq_dict = {'Q17884' : 'LGBT'}

    sexual_orientation = ["Q1035954", "Q6636", "Q43200", "Q339014", "Q724351", "Q271534", "Q93771184", "Q23912283", "Q51415", "Q52746927"]

    gender = ["Q6581097", "Q6581072", "Q1052281", "Q2449503", "Q1097630", "Q48270", "Q27679684", "Q18116794", "Q12964198", "Q301702", "Q189125", "Q505371", "Q859614", "Q1289754", "Q207959", "Q48279", "Q52261234", "Q15145779", "Q93954933", "Q179294"]

    qitems = all_languages.Qitem.tolist() + countries.Qitem.tolist() + country_subdivisions.Qitem.tolist() + list(subregions_dict.keys()) + list(regions_dict.keys()) + list(religious_groups.keys()) + list(indigenous_groups.keys()) + list(ethnic_groups.keys()) + list(lgbtq_dict.keys()) + sexual_orientation + gender


    for languagecode in wikilanguagecodes:

        qitems_page_titles = {}
        query = 'SELECT page_title, qitem FROM '+languagecode+'wiki;'
        for row in cursor2.execute(query):
            page_title=row[0].replace(' ','_')
            qitems_page_titles[row[1]]=page_title

        qitems_labels = {}
        page_asstring = ','.join( ['?'] * (len(qitems)) ) # total 
        query = 'SELECT qitem, label FROM labels WHERE langcode = "'+languagecode+'wiki" AND qitem IN (%s);' % page_asstring
        for row in cursor.execute(query, qitems):
            qitems_labels[row[0]]=row[1]

        print (languagecode, str(len(qitems_labels)))

        params = []

        for index, row in all_languages.iterrows():
            qitem = row['Qitem']
            code = row['languageISO3']
            english_title = row['englishLabel']

            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'languages'))


        for index, row in countries.iterrows():
            qitem = row['Qitem']
            code = row['ISO3166']
            english_title = row['territoryLabel']

            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'countries'))


        for index, row in country_subdivisions.iterrows():
            qitem = row['Qitem']
            code = row['ISO31662']
            english_title = row['territoryLabel']

            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'countries_subdivision'))

        for qitem, english_title in subregions_dict.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'subregions'))


        for qitem, english_title in regions_dict.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'regions'))


        for qitem, english_title in religious_groups.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'religious_groups'))

        for qitem, english_title in indigenous_groups.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'indigenous_groups'))

        for qitem, english_title in ethnic_groups.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'ethnic_groups'))

        for qitem, english_title in ethnic_groups.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'ethnic_groups'))

        for qitem, english_title in lgbtq_dict.items():
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, english_title, languagecode, 'lgbtq'))

        for qitem in sexual_orientation:
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, '', languagecode, 'sexual_orientation'))

        for qitem in gender:
            code = ''
            try: label = qitems_labels[qitem]
            except: label = ''
            try: page_title = qitems_page_titles[qitem]
            except: page_title = ''
            if label == '' and page_title == '': continue
            params.append((qitem, label, page_title, code, '', languagecode, 'gender'))


        query = 'INSERT OR IGNORE INTO diversity_categories_labels_production (qitem, label, page_title, code, english_title, lang, category_label) VALUES (?, ?, ?, ?, ?, ?, ?);'
        cursor3.executemany(query, params)
        conn3.commit()

        print (languagecode,str(len(params)))


    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def get_ethnic_groups_labels():

    conn3 = sqlite3.connect(databases_path + diversity_categories_db); cursor3 = conn3.cursor()
    query = 'SELECT qitem, label, lang FROM diversity_categories_labels_production WHERE category_label = "ethnic_groups";'
    df = pd.read_sql_query(query, conn3)

    # qitems = df.loc[(df["lang"].isin([langcode,'en'])) & (df["group_label"] == group_label)][['qitem','label','lang']]
    # qitems['label'] = qitems['label'].str.replace('_',' ')
    # qitems = qitems.set_index('qitem')


    # qitem_df = qitems.loc['Q576065']
    # try:
    #     a = qitem_df.loc[qitem_df["lang"]==langcode]
    # except:
    #     a = qitem_df.loc[qitem_df["lang"]=="en"]
    # label = qitems.loc['Q576065']["label"].values[0]

    return df



def get_diversity_categories_labels():

    conn3 = sqlite3.connect(databases_path + diversity_categories_db); cursor3 = conn3.cursor()
    query = 'SELECT qitem, REPLACE(label,"_"," ") as label, REPLACE(page_title,"_"," ") as page_title, lang, category_label, code FROM diversity_categories_labels_production;'
    df = pd.read_sql_query(query, conn3)

    # qitems = df.loc[(df["lang"].isin([langcode,'en'])) & (df["group_label"] == group_label)][['qitem','label','lang']]
    # qitems['label'] = qitems['label'].str.replace('_',' ')
    # qitems = qitems.set_index('qitem')


    # qitem_df = qitems.loc['Q576065']
    # try:
    #     a = qitem_df.loc[qitem_df["lang"]==langcode]
    # except:
    #     a = qitem_df.loc[qitem_df["lang"]=="en"]
    # label = qitems.loc['Q576065']["label"].values[0]

    return df



def store_lgbt_label(store_get):
    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + wikipedia_diversity_production_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + diversity_categories_db); cursor2 = conn2.cursor()

    function_name = 'store_lgbt_label'
    functionstartTime = time.time()

    if store_get == 'store':
        query = 'CREATE TABLE IF NOT EXISTS LGBT_label_languages (label, lang, PRIMARY KEY (label, lang));'
        cursor2.execute(query);
        conn2.commit()

        param = []
        for languagecode in wikilanguagecodes:
            try:
                query = 'SELECT page_title FROM '+languagecode+'wiki WHERE qitem = "Q17884";'
                cursor.execute(query);
                label = cursor.fetchone()[0]
                print ((label, languagecode))
                param.append((label, languagecode))
            except:
                pass

        query = 'INSERT OR IGNORE INTO LGBT_label_languages (label, lang) VALUES (?, ?)'
        cursor2.executemany(query, param)
        conn2.commit()

    else:

        langs_LGBT_label = {}
        query = 'SELECT lang, label FROM LGBT_label_languages;'
        for row in cursor2.execute(query):
            langs_LGBT_label[row[0]]=row[1]

        return langs_LGBT_label

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)



# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4') # utf8mb4
        return mysql_con_read
    except:
        print ('This language has no database or we cannot connect to it: '+languagecode)
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')

# Create a database connection.
def establish_mysql_connection_write():
    mysql_con_write = mdb.connect(host="tools.db.svc.eqiad.wmflabs",db='s53619__wcdo',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
    return mysql_con_write

# Additional sources: Pycountry and Babel libraries for countries and their languages.
def extract_check_new_wiki_projects():
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?itemLabel ?language ?languageLabel ?alias ?nativeLabel ?languageISO ?languageISO3 ?languageISO5 ?languagelink ?WikimediaLanguagecode WHERE {
      ?item wdt:P31 wd:Q10876391.
      ?item wdt:P407 ?language.
      
      OPTIONAL{?language wdt:P1705 ?nativeLabel.}
#     ?item wdt:P856 ?officialwebsite.
#     ?item wdt:P1800 ?bbddwp.
      ?item wdt:P424 ?WikimediaLanguagecode.

      OPTIONAL {?language skos:altLabel ?alias FILTER (LANG (?alias) = ?WikimediaLanguagecode).}
      OPTIONAL{?language wdt:P218 ?languageISO .}
      OPTIONAL{?language wdt:P220 ?languageISO3 .}
      OPTIONAL{?language wdt:P1798 ?languageISO5 .}
      
      OPTIONAL{
      ?languagelink schema:about ?language.
      ?languagelink schema:inLanguage "en". 
      ?languagelink schema:isPartOf <https://en.wikipedia.org/>
      }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?WikimediaLanguagecode'''

    
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
#    url = 'https://query.wikidata.org/sparql'

    data = requests.get(url,headers={'User-Agent': 'https://wikitech.wikimedia.org/wiki/User:Marcmiquel'}, params={'query': query, 'format': 'json'})
    print (data)
    data = data.json()

    #print (data)

    extracted_languages = []
    wikimedialanguagecode = ''
    Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];
    for item in data['results']['bindings']:    
#        print (item)
        #input('tell me')
        if wikimedialanguagecode != item['WikimediaLanguagecode']['value'] and wikimedialanguagecode!='':

            try:
                currentLanguagename=Locale.parse(wikimedialanguagecode).get_display_name(wikimedialanguagecode).lower()
                if currentLanguagename not in nativeLabel:
#                    print ('YAHOO')
                    nativeLabel.append(currentLanguagename)
#                    print(currentLanguagename)
            except:
                pass

            extracted_languages.append({
            'languagename': ";".join(languagename),          
            'Qitem': ";".join(Qitem),
            'WikimediaLanguagecode': wikimedialanguagecode.replace('-','_'),
            'Wikipedia':wikipedia,
            'WikipedialanguagearticleEnglish': englisharticle,
            'languageISO': ";".join(languageISO),
            'languageISO3': ";".join(languageISO3),
            'languageISO5': ";".join(languageISO5),
            'nativeLabel': ";".join(nativeLabel),
            })

            #print (extracted_languages)
            #input('common')
            Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];

        Qitemcurrent = item['language']['value'].replace("http://www.wikidata.org/entity/","")
        if Qitemcurrent not in Qitem:
            Qitem.append(Qitemcurrent)
        languagenamecurrent = item['languageLabel']['value']
        if languagenamecurrent not in languagename: languagename.append(languagenamecurrent)

        try:
            nativeLabelcurrent = item['nativeLabel']['value'].lower()
            if nativeLabelcurrent not in nativeLabel: nativeLabel.append(nativeLabelcurrent)
        except:
            pass

        try: 
            aliascurrent = item['alias']['value'].lower()
#            print (aliascurrent)
            if aliascurrent not in nativeLabel and len(aliascurrent)>3: nativeLabel.append(aliascurrent)
        except:
            pass

        try: 
            languageISOcurrent = item['languageISO']['value']
            if languageISOcurrent not in languageISO: languageISO.append(languageISOcurrent)
        except:
            pass

        try:
            languageISO3current = item['languageISO3']['value']
            if languageISO3current not in languageISO3: languageISO3.append(languageISO3current)
        except:
            pass

        try:
            languageISO5current = item['languageISO5']['value']
            if languageISO5current not in languageISO5: languageISO5.append(languageISO5current)
        except:
            pass

        try: englisharticle = item['languagelink']['value'] 
        except: englisharticle = 'no link'
        wikimedialanguagecode = item['WikimediaLanguagecode']['value'] # si 
        wikipedia = item['itemLabel']['value']

        #print (result)
    df = pd.DataFrame(extracted_languages)
    df = df.set_index(['languagename'])
 
#    df = df.set_index(['WikimediaLanguagecode'])
    filename= 'new_wikipedia_language_editions'
    newlanguages = []

    # CHECK IF THERE IS ANY NEW LANGUAGE
    languages = load_wiki_projects_information()
    langs_qitems = languages.Qitem.tolist()
    df_qitems = df.Qitem.tolist()
    for q in df_qitems:
        if q in langs_qitems:
            df.drop(df.loc[df['Qitem']==q].index, inplace=True)
    
    # exceptions
    languages=languages.rename(index={'be_x_old': 'be_tarask'})
    languages=languages.rename(index={'zh_min_nan': 'nan'})
    languageid_file = languages.index.tolist();
    languageid_file.append('nb')

    languageid_calculated = df['WikimediaLanguagecode'].tolist();
#    print ('These are the ones just extracted from Wikidata: ')
#    print (languageid_calculated)

    newlanguages = list(set(languageid_calculated) - set(languageid_file))

    exceptions = ['mo']
    indexs = []
    for x in newlanguages:
        if x in exceptions: continue
        indexs = indexs + df.index[(df['WikimediaLanguagecode'] == x)].tolist()
    newlanguages = indexs

    if len(newlanguages)>0: 
        message = 'These are the new languages: '+', '.join(newlanguages)
        print (message)
        df = df.loc[~df.index.duplicated(keep='first')]
        df=df.reindex(newlanguages)
        send_email_toolaccount('WDO: New languages to introduce into the file.', message)
        print ('The new languages are in a file named: ')
        print (databases_path + filename+'.csv')
        df.to_csv(databases_path + filename+'.csv',sep='\t')
    else:
        print ('There are no new Wikipedia language editions.')

    return newlanguages



def extract_database_tables_to_files():

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        c = csv.writer(open(path,'a'), lineterminator = '\n', delimiter='\t')
        query = 'SELECT qitem, page_id, page_title, date_created, date_last_edit, first_timestamp_lang, geocoordinates, iso3166, iso3662, region, gender, ethnic_group, supra_ethnic_group, sexual_orientation, num_inlinks_from_women, num_outlinks_towomen, percent_inlinks_from_women, percent_outlinks_to_women, num_inlinks_from_men, num_outlinks_to_men, percent_inlinks_from_men, percent_outlinks_to_men, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, ccc_binary, folk, earth, monument_and_buidings, music_creations_and_organizations, sports_and_teams, food, paintings, glam, books, clothing_and_fashion, industry, religion, time_interval, start_time, end_time, lgbt_topic, lgbt_keyword_title, num_bytes, num_references, num_images, num_inlinks, num_outlinks, num_edits, num_edits_last_month, num_editors, num_discussions, num_pageviews, num_interwiki, num_wd_property, featured_article, wikirank FROM '+languagecode+'wiki;'

#        query = 'SELECT qitem, page_id, page_title, lgbt_biography, keyword, category_crawling_level, num_inlinks_from_lgbt, num_outlinks_to_lgbt, percent_inlinks_from_lgbt, percent_outlinks_to_lgbt, lgbt_binary FROM '+languagecode+'wiki;'

        cursor.execute(query) # ->>>>>>> canviar * per les columnes. les de rellevància potser no cal.

        i = 0
        c.writerow([d[0] for d in cursor.description])
        for result in cursor:
            i+=1
            c.writerow(result)



# Creates a dataset from the Wikipedia Diversity database for a list of languages.
# COMMAND LINE: sqlite3 -header -csv ccc_temp.db "SELECT * FROM ccc_cawiki;" > ccc_cawiki.csv
def extract_wikipedia_diversity_tables_to_files():

    for languagecode in wikilanguagecodes:
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

        # These are the folders.
        superfolder = datasets_path+cycle_year_month
        languagefolder = superfolder+'/'+languagecode+'wiki/'
        latestfolder = datasets_path+'latest/'
        if not os.path.exists(languagefolder): os.makedirs(languagefolder)
        if not os.path.exists(latestfolder): os.makedirs(latestfolder)

        # These are the files.
        ccc_filename_archived = languagecode + 'wiki_' + str(cycle_year_month.replace('-',''))+'_diversity.csv' # (e.g. 'cawiki_20180215_ccc.csv')
        ccc_filename_latest = languagecode + 'wiki_latest_diversity.csv.bz2' # (e.g. cawiki_latest_diversity.csv)

        # These are the final paths and files.
        path_latest = latestfolder + ccc_filename_latest
        path_language = languagefolder + ccc_filename_archived
        print ('Extracting the Diversity Tables from language '+languagecode+' into the file: '+path_language)
        print ('This is the path for the latest files altogether: '+path_latest)

        # Here we prepare the streams.
        path_language_file = codecs.open(path_language, 'w', 'UTF-8')

        c = csv.writer(open(path_language,'w'), lineterminator = '\n', delimiter='\t')

        cursor.execute("SELECT * FROM "+languagecode+"wiki;") # ->>>>>>> canviar * per les columnes. les de rellevància potser no cal.

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

        print (languagecode+' language has this number of rows: '+str(i))
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
    

    data = {
      "@context":"http://schema.org/",
      "@type":"Dataset",
      "name":"Wikipedia Cultural Context Content Dataset",
      "description":"Cultural Context Content is the group of Articles in a Wikipedia language edition that relates to the editors' geographical and cultural context (places, traditions, language, politics, agriculture, biographies, events, etcetera.). Cultural Context Content is collected as a dataset, which is available in a monthly basis, and allows the Wikipedia Diversity Observatory project to show and depict several statistics on the state of knowledge equality and cross-cultural coverage. These datasets are computed for all Wikipedia language editions. They allow answering questions such as: \n* How self-centered any Wikipedia is (the extent of ccc as percentage and number of Articles)? Are the CCC Articles responding to readers demand for information?\n* How well any Wikipedia covers the existing world cultural diversity (gaps)?\n* Are the Articles created each month dedicated to fill these gaps?\n* Which are the most relevant Articles from each Wikipedia’s related cultural context and particular topics?",
      "url":"https://wcdo.wmflabs.org/datasets/",
      "sameAs":"https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content#Datasets",
      "license": "Creative Commons CC0 dedication",
      "keywords":[
         "Content Imbalances > Language Gap > Culture gap",
         "Online Communities > Wikipedia > Wiki Studies",
         "Cultural Diversity > Cross-cultural data",
         "Big Data > Data mining > Public repositories"
      ],
      "creator":{
         "@type":"Organization",
         "url": "https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory",
         "name":"Wikipedia Cultural Diversity Observatory",
         "contactPoint":{
            "@type":"ContactPoint",
            "contactType": "customer service",
            "email":"tools.wcdo@tools.wmflabs.org"
         }
      },
      "includedInDataCatalog":{
         "@type":"Wikimedia datasets",
         "name":"https://meta.wikimedia.org/wiki/Datasets"
      },
      "distribution":[
         {
            "@type":"DataDownload",
            "encodingFormat":"CSV",
         },
      ],
      "temporalCoverage":"2001-01-01/2018-"+cycle_year_month
    }

    with open(datasets_path+'/latest/'+'Wikipedia_Diversity_datasets.json', 'w') as f:
      json.dump(data, f, ensure_ascii=False)
      
    """
    https://developers.google.com/search/docs/data-types/dataset
    https://www.blog.google/products/search/making-it-easier-discover-datasets/amp/ 
    https://search.google.com/structured-data/testing-tool
    https://toolbox.google.com/datasetsearch
    """

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# Get me the articles that are also in the wikipedia_diversity_production.db and the diversity categories it belongs to.
def get_interwikilinks_articles(sourcelang, targetlangs, df, mysql_con_read):

    page_ids = df.page_id.tolist()
    params = page_ids + targetlangs

    page_asstring = ','.join( ['%s'] * len(page_ids) ) 
    query = 'SELECT ll_from as page_id, CONVERT(ll_title USING utf8mb4) as page_title, CONVERT(ll_lang USING utf8mb4) as lang FROM langlinks WHERE ll_from IN (%s) ' % page_asstring

    page_asstring = ','.join( ['%s'] * len(targetlangs) )
    query += 'AND ll_lang IN (%s);' % page_asstring

    df_y = pd.read_sql_query(query, mysql_con_read, params = params);
    df_y = df_y.set_index('page_id')

    i = 0
    for lang in targetlangs:
        i += 1
        df_z = df_y.loc[(df_y['lang']==lang)]
        df_z = df_z.rename(columns={"page_title": "page_title_"+str(i)})
        df = df.merge(df_z["page_title_"+str(i)], how='left', on='page_id')

    return df




def extract_ccc_count(languagecode, filename, message):
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary=1;'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row1 = str(row[0]);

    query = 'SELECT count(*) FROM '+languagecode+'wiki;'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row2 = str(row[0]);

    languagename = languages.loc[languagecode]['languagename']

    with open(filename, 'a') as f:
        f.write(languagename+'\t'+message+'\t'+row1+'\t'+row2+'\n')



def get_months_queries():

    def datespan(startDate, endDate, delta=datetime.timedelta(days=1)):
        currentDate = startDate
        while currentDate < endDate:
            yield currentDate
            currentDate += delta

    periods_accum = {}
    periods_monthly = {}

    current_cycle = get_current_cycle_year_month()
    endDay = datetime.datetime.strptime(str(current_cycle),'%Y-%m').date()+datetime.timedelta(days=30)

#    endDay = datetime.date.today()
    for day in datespan(datetime.date(2001, 1, 16), endDay, delta=datetime.timedelta(days=30)):
        month_period = day.strftime('%Y-%m')

        first_day = day.replace(day = 1).strftime('%Y%m%d%H%M%S')
        last_day = day.replace(day = calendar.monthrange(day.year, day.month)[1]).strftime('%Y%m%d%H%M%S')

#        print ('monthly:')
        month_condition = 'date_created >= "'+ first_day +'" AND date_created < "'+last_day+'"'
        periods_monthly[month_period]=month_condition
#        print (month_condition)    

#        print ('accumulated: ')
        if month_period == datetime.date.today().strftime('%Y-%m'):
            month_condition = 'date_created < '+last_day + ' OR date_created IS NULL'
        else:
            month_condition = 'date_created < '+last_day

        periods_accum[month_period]=month_condition
#        print (month_condition)

#    print (periods_monthly,periods_accum)
    return periods_monthly,periods_accum



def get_new_cycle_year_month():

    pathf = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
    current_cycle_date = time.strftime('%Y%m%d%H%M%S', time.gmtime(os.path.getmtime(pathf)))
    current_cycle_date = datetime.datetime.strptime(current_cycle_date,'%Y%m%d%H%M%S')-dateutil.relativedelta.relativedelta(months=1)
    current_cycle = current_cycle_date.strftime('%Y-%m')

    print ('new cycle for the data: '+current_cycle)
    return current_cycle


def get_current_cycle_year_month():

    query = 'SELECT MAX(year_month) FROM function_account WHERE script_name = "wikipedia_diversity.py" AND function_name = "wd_dump_iterator";'
    conn = sqlite3.connect(databases_path + diversity_observatory_log); cursor = conn.cursor()

    cursor.execute(query)
    current_cycle = cursor.fetchone()[0]

    print ('current cycle for the data: '+current_cycle)
    return current_cycle


def get_last_accumulated_period_year_month():

    query = 'SELECT MAX(period) FROM wdo_intersections_accumulated;'
    conn = sqlite3.connect(databases_path + stats_production_db); cursor = conn.cursor()

    cursor.execute(query)
    current_cycle = cursor.fetchone()[0]

    print ('last accumulated period: '+current_cycle)
    return current_cycle


def is_insert(line):
    return 'INSERT INTO' in line or False

def get_values(line):
    return line.partition(' VALUES ')[2]

def get_table_name(line):
    match = re.search('INSERT INTO `([0-9_a-zA-Z]+)`', line)
    if match:
        return match.group(1)
    else:
        print(line)

def get_columns(line):
    match = re.search('INSERT INTO `.*` \(([^\)]+)\)', line)
    if match:
        return list(map(lambda x: x.replace('`', '').strip(), match.group(1).split(',')))

def values_sanity_check(values):
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True

def parse_values(values):
    rows = []
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    latest_row = ['' if field == '\x00' else field for field in latest_row]

                    rows.append(latest_row)
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            latest_row = ['' if field == '\x00' else field for field in latest_row]
            rows.append(latest_row)

        return rows




def copy_db_for_production(dbname, scriptname, databases_path):
    function_name = 'copy_db_for_production'
    dbname_production = dbname.split('.')[0]+'_production.db'

    # if verify_function_run_db(function_name, 'check','')==1: return
    functionstartTime = time.time()
    try:
        shutil.copyfile(databases_path + dbname, databases_path + dbname_production)
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        print ('File '+dbname+' copied as '+dbname_production+' at the end of the '+scriptname+' script. It took: '+duration)
    except:
        print ('Not possible to create the production version.')
    # verify_function_run_db(function_name, 'mark', duration)



def delete_wikidata_db():
    os.remove(databases_path + wikidata_db)


############################################################################

# Sends an e-mail
def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

# Finish e-mail
def finish_email(startTime, filename, title):
    try:
        sys.stdout=None; send_email_toolaccount(title + ' completed successfuly', open(filename, 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount(title + ' aborted because of an error', open(filename, 'r').read()+'err')


def check_dump(dumps_path, script_name):
    try:
        os.path.isfile(dumps_path)
        return True
    except:
        print ('dump error at script '+script_name)
        send_email_toolaccount('dump error at script '+script_name, dumps_path)
        quit()



############################################################################


def backup_db():
    try:
        shutil.copyfile(databases_path + wikipedia_diversity_db, databases_path + "wikipedia_diversity_backup.db")
        print ('File wikipedia_diversity.db copied as wikipedia_diversity_backup.db at the end of the content_retrieval.py script.')
    except:
        print ('Not possible to create the backup.')






def verify_function_run(cycle_year_month, script_name, function_name, action, duration):
    function_name_string = function_name

    # print ('\n\n',cycle_year_month, script_name, function_name, action, duration); return # comment this.


    conn = sqlite3.connect(databases_path + diversity_observatory_log); cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS function_account (script_name text, function_name text, year_month text, finish_time text, duration text, PRIMARY KEY (script_name, function_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM function_account WHERE script_name = ? AND function_name = ? AND year_month = ?;'
        cursor.execute(query,(script_name, function_name, cycle_year_month))
        duration = cursor.fetchone()
        if duration != None:
            print ('= Process Accountant: The function "'+function_name_string+'" has already been run. It lasted: '+duration[0])
            return 1
        else:
            print ('- Process Accountant: The function "'+function_name_string+'" has not run yet. Do it! Now: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S")+'. Year Month Cycle: '+cycle_year_month))
            return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S");
        query = "INSERT INTO function_account (script_name, function_name, year_month, finish_time, duration) VALUES (?,?,?,?,?);"
        cursor.execute(query,(script_name, function_name, cycle_year_month, finish_time, duration))
        conn.commit()
        print ('+ Process Accountant. Function: '+function_name+' is NOW RUN! Script: '+script_name+'. After '+duration+'.\n')



def verify_script_run(cycle_year_month, script_name, action, duration):
    script_name_string = script_name

    conn = sqlite3.connect(databases_path + diversity_observatory_log)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS script_account (script_name text, year_month text, finish_time text, duration text, PRIMARY KEY (script_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM script_account WHERE script_name = ? AND year_month = ?;'
        cursor.execute(query,(script_name, cycle_year_month))
        duration = cursor.fetchone()
        if duration != None:
            print ('= Process Accountant: The script "'+script_name_string+'" has already been run. It lasted: '+duration[0])
            return 1
        else:
            print ('- Process Accountant: The script "'+script_name_string+'" has not run yet. Do it! Now: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S")+'. Year Month Cycle: '+cycle_year_month))
            return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S");
        query = "INSERT INTO script_account (script_name, year_month, finish_time, duration) VALUES (?,?,?,?);"
        cursor.execute(query,(script_name, cycle_year_month, finish_time, duration))
        conn.commit()
        print ('+ Process Accountant. Script: '+script_name+' is NOW RUN! After '+duration+'.\n')



# CONTENT RETRIEVAL SCRIPT
def check_time_for_script_run(script_name, cycle_year_month):

    not_ready = True
    while (not_ready):

        print ("Let's check if it is time for "+script_name+' to run: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S"))+'. Cycle: '+cycle_year_month)

        conn = sqlite3.connect(databases_path + diversity_observatory_log); cursor = conn.cursor()
        query = 'SELECT function_name FROM script_account WHERE function_name = "" AND year_month = ?;'
        scripts_run = []

        for row in cursor.execute(query,cycle_year_month):
            script_run.append(row[0])

        if script_name == 'wikipedia_diversity.py': not_ready = False

        if script_name == 'content_retrieval.py':
            if 'wikipedia_diversity.py' in scripts_run: not_ready = False

        if script_name == 'article_features.py':
            if 'wikipedia_diversity.py' in scripts_run: not_ready = False

        if script_name == 'content_selection.py':
            if 'content_retrieval.py' in scripts_run: not_ready = False

        if script_name == 'top_diversity_selection.py':
            if 'content_selection.py' in scripts_run and 'article_features.py' in scripts_run: not_ready = False

        if script_name == 'stats_generation.py':
            if 'content_selection.py' in scripts_run and 'top_diversity_selection.py' in scripts_run: not_ready = False

        if script_name == 'missing_ccc_selection.py':
            if 'content_selection.py' in scripts_run and 'article_features.py' in scripts_run: not_ready = False

        if script_name == 'group_identities_selection.py':
            if 'content_selection.py' in scripts_run and 'article_features.py' in scripts_run: not_ready = False

        if not_ready == True:
            print ('Not ready yet: waiting one more day before asking again.')
            time.sleep(86400)

    print ('* It is time for '+script_name+' to run: '+str(datetime.datetime.utcnow().strftime("%Y/%m/%d-%H:%M:%S"))+'. Cycle: '+cycle_year_month)



def store_lines_per_second(duration, lines, function_name, file, period):

    conn = sqlite3.connect(databases_path + diversity_observatory_log)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS lines_per_second (linespersecond real, lines integer, duration integer, function_name text, file text, year_month text, PRIMARY KEY (function_name, year_month));")

    linespersecond = lines/duration

    query = "INSERT OR IGNORE INTO lines_per_second (linespersecond, lines, duration, function_name, file, year_month) VALUES (?,?,?,?,?,?);"
    cursor.execute(query,(linespersecond, lines, duration, function_name, file, period))
    conn.commit()

    print ('in function '+function_name+' reading the dump '+file+', the speed is '+str(linespersecond)+' lines/second, at this period: '+period)



def check_run_failed_functions():

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    function_parameters = [{'label_ccc_articles_geolocation_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_geolocated_reverse_geocoding':(languagecode,page_titles_qitems, page_titles_page_ids),'label_ccc_articles_country_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_location_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_language_strong_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_created_by_properties_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_part_of_properties_wd':(languagecode,page_titles_page_ids),'label_ccc_articles_keywords':(languagecode,page_titles_qitems)},{'label_potential_ccc_articles_category_crawling':(languagecode,page_titles_page_ids,page_titles_qitems),'label_potential_ccc_articles_with_inlinks ccc':(languagecode,page_titles_page_ids,page_titles_qitems,'ccc'),'label_potencial_ccc_articles_with_outlinks ccc':(languagecode,page_titles_page_ids,page_titles_qitems,'ccc'),'label_potential_ccc_articles_language_weak_wd':(languagecode,page_titles_page_ids),'label_potential_ccc_articles_affiliation_properties_wd':(languagecode,page_titles_page_ids),'label_potential_ccc_articles_has_part_properties_wd':(languagecode,page_titles_page_ids)},{'label_other_ccc_wikidata_properties':(languagecode,page_titles_page_ids,page_titles_qitems),'label_potential_ccc_articles_with_inlinks no_ccc':(languagecode,page_titles_qitems,'no ccc'),'label_potencial_ccc_articles_with_outlinks no_ccc':(languagecode,page_titles_page_ids,'no ccc')},{'calculate_articles_ccc_binary_classifier':(languagecode,'RandomForest',page_titles_page_ids,page_titles_qitems),'calculate_articles_ccc_main_territory':(languagecode),'calculate_articles_ccc_retrieval_strategies':(languagecode)}]

    for languagecode in wikilanguagecodes:
        for functions in function_parameters:
            for function,parameters in functions.items():
                query = 'SELECT count(*) FROM function_account WHERE function_name = "'+function+ ' ' + languagecode
                cursor.execute(query)
                row = cursor.fetchone()
                if row != None: num = 0
                else: num = 1

                if num == 0:                
                    (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
                    exec(function,parameters)





def check_language_article_features():

    features = ['num_inlinks','num_outlinks','num_bytes','num_references','num_edits','num_editors','num_discussions','num_pageviews','num_wdproperty','num_interwiki','num_images','num_edits_last_month','featured_article','wikirank']

    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print(languagecode)

        for feature in features:
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+feature+' IS NOT NULL;'
            cursor2.execute(query)
            value = cursor2.fetchone()
            if value != None: 
                count = value[0]
            if count != 0: 
                print(languagecode+' '+feature+' '+str(count))
            if count == 0 or count== None:
                print(languagecode+' '+feature+' MISSING!')

        print ('\n')


def copy_language_article_features():

    features = ['num_inlinks','num_outlinks','num_bytes','num_references','num_edits','num_editors','num_discussions','num_pageviews','num_wdproperty','num_interwiki','num_images','num_edits_last_month','featured_article','wikirank']

    conn = sqlite3.connect(databases_path + 'ccc.db'); cursor = conn.cursor()

    conn2 = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print(languagecode)

        for feature in features:
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+feature+' IS NOT NULL;'
            cursor2.execute(query)
            value = cursor2.fetchone()
            if value != None: 
                count = value[0]
            if count != 0: 
                print(languagecode+' '+feature+' '+str(count))
            if count == 0 or count== None:
                print(languagecode+' '+feature+' MISSING!')

                params = []
                query = 'SELECT '+feature+', qitem, page_id FROM ccc_'+languagecode+'wiki WHERE '+feature+' IS NOT NULL;'
    #            query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
                try:
                    for row in cursor.execute(query):
                        params.append((row[0],row[1],row[2]))
                except:
                    continue

                if len(params)!=0:
                    print ('In the old table we found: '+str(len(params)))
                    query = 'UPDATE '+languagecode+'wiki SET '+feature+' = ? WHERE qitem=? AND page_id=?'
                    cursor2.executemany(query,params);
                    conn2.commit()

                    query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+feature+' IS NOT NULL;'
                    cursor2.execute(query)
                    value = cursor2.fetchone()
                    if value != None: 
                        count = value[0]
                    if count != 0: 
                        print(languagecode+' '+feature+' '+str(count))
                        print ('FILLED WITH OTHER DATA.')

        print ('\n')



def clean_failed_function_account():

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    function_parameters = {'label_diversity_categories_related_topics_wd':'gender','label_ccc_articles_geolocation_wd':'geocoordinates','label_ccc_articles_geolocated_reverse_geocoding':'ccc_geolocated','label_ccc_articles_country_wd':'country_wd','label_ccc_articles_location_wd':'location_wd','label_ccc_articles_language_strong_wd':'language_strong_wd','label_ccc_articles_created_by_properties_wd':'created_by_wd','label_ccc_articles_part_of_properties_wd':'part_of_wd','label_ccc_articles_keywords':'keyword_title','label_potential_ccc_articles_category_crawling':'category_crawling_level','label_potential_ccc_articles_with_inlinks ccc':'num_inlinks','label_potencial_ccc_articles_with_outlinks ccc':'num_outlinks','label_potential_ccc_articles_language_weak_wd':'language_weak_wd','label_potential_ccc_articles_affiliation_properties_wd':'affiliation_wd','label_potential_ccc_articles_has_part_properties_wd':'has_part_wd','label_other_ccc_wikidata_properties':'other_ccc_language_strong_wd','label_other_ccc_category_crawling':'other_ccc_category_crawling_relative_level','label_potential_ccc_articles_with_inlinks no_ccc':'num_inlinks_from_geolocated_abroad','label_potencial_ccc_articles_with_outlinks no_ccc':'num_outlinks_to_geolocated_abroad','calculate_articles_ccc_main_territory':'main_territory','calculate_articles_ccc_retrieval_strategies':'num_retrieval_strategies'}


    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode)

        for function, parameter in function_parameters.items():
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+parameter+' IS NOT NULL;'
            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num = row[0]

            query = 'SELECT count(*) FROM function_account WHERE function_name = "'+function+ ' ' + languagecode
            if function == 'label_diversity_categories_related_topics_wd': query+= ' gender"'
            else: query+='"'

            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num2 = row[0]

#            if num2 != 0 and num != 0:
#                print(function+' exists and the database is full. nothing to do, everything is fine.')
            if num2 != 0 and num == 0:
                print(function+' '+languagecode+' exists and the database is empty. we delete function account so the function runs again next time.')
                query = 'DELETE FROM function_account WHERE function_name = "'+function+' '+languagecode
                if function == 'label_diversity_categories_related_topics_wd': query+= ' gender"'
                else: query+='"'

                cursor.execute(query)
            if num != 0 and num2 == 0:
                print(parameter+' '+languagecode+' is in the database but the function '+function+' '+languagecode+' has not been accounted.')
        conn.commit()


    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    function_parameters = {'extend_articles_timestamp':'date_created', 'extend_articles_editors':'num_editors', 'extend_articles_discussions':'num_discussions','extend_article_edits':'num_edits','extend_articles_edits_last_month':'num_edits_last_month','extend_articles_references':'num_references','extend_articles_bytes':'num_bytes','extend_articles_interwiki':'num_interwiki','extend_articles_qitems_properties':'num_wdproperty','extend_articles_featured':'featured_article','extend_articles_images':'num_images','extend_articles_pageviews':'num_pageviews'}


    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode)

        for function, parameter in function_parameters.items():
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+parameter+' IS NOT NULL;'
            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num = row[0]

            query = 'SELECT count(*) FROM function_account WHERE function_name = "'+function+ ' ' + languagecode
            if function == 'extend_articles_wikidata_topics': query+= ' gender"'
            else: query+='"'

            cursor.execute(query)
            row = cursor.fetchone()
            if row != None: num2 = row[0]

#            if num2 != 0 and num != 0:
#                print(function+' exists and the database is full. nothing to do, everything is fine.')
            if num2 != 0 and num == 0:
                print(function+' '+languagecode+' exists and the database is empty. we delete function account so the function runs again next time.')
                query = 'DELETE FROM function_account WHERE function_name = "'+function+' '+languagecode
                if function == 'extend_articles_wikidata_topics': query+= ' gender"'
                else: query+='"'

                cursor.execute(query)
            if num != 0 and num2 == 0:
                print(parameter+' '+languagecode+' is in the database but the function '+function+' '+languagecode+' has not been accounted.')
        conn.commit()
