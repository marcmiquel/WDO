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


# Loads language_territories_mapping.csv file
def load_wikipedia_languages_territories_mapping():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();  

    query = 'SELECT WikimediaLanguagecode, languagenameEnglishethnologue, territoryname, territorynameNative, QitemTerritory, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, officialnationalorregional, region, subregion, intermediateregion FROM wikipedia_languages_territories_mapping;'

    territories = pd.read_sql_query(query, conn)
    territories = territories.set_index(['WikimediaLanguagecode'])

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

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();

    query = 'SELECT languagename, Qitem, WikimediaLanguagecode, Wikipedia, WikipedialanguagearticleEnglish, languageISO, languageISO3, languageISO5, languageofficialnational, languageofficialregional, languageofficialsinglecountry, nativeLabel, numbercountriesOfficialorRegional, region, subregion, intermediateregion FROM wiki_projects;'

    languages = pd.read_sql_query(query, conn)
    languages = languages.set_index(['WikimediaLanguagecode'])


    return languages


def load_language_pairs_territory_status():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();

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

    if db=='temp': 
        database = 'wikipedia_diversity_temp.db';
    else:
        database = 'wikipedia_diversity.db'

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

    conn = sqlite3.connect(databases_path + 'wikipedia_diversity.db'); cursor = conn.cursor()
#    conn = sqlite3.connect(databases_path + 'ccc.db'); cursor = conn.cursor()
    
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

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();

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
    subdivisions=territories.loc[languagecode].ISO31662
    if isinstance(subdivisions,str): subdivisions = [subdivisions]
    else: subdivisions = list(set(subdivisions))

    return subdivisions


def load_iso_31662_to_subdivisions():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();
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


def load_world_subdivisions():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();
    query = 'SELECT name, subdivision_code FROM world_subdivisions;'

    world_subdivisions = {}
    for row in cursor.execute(query):
        world_subdivisions[row[0]]=row[1]

    return world_subdivisions


def load_world_subdivisions_ip2location():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();
    query = 'SELECT subdivision_name, subdivision_code FROM ISO3166_2_ip2location;'

    world_subdivisions = {}
    for row in cursor.execute(query):
        world_subdivisions[row[0]]=row[1]

    return world_subdivisions



def load_world_subdivisions_multilingual():

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();
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

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();

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
    conn = sqlite3.connect(databases_path + 'wikidata.db'); cursor = conn.cursor()
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
        conn = sqlite3.connect(databases_path+'wcdo_stats.db'); cursor = conn.cursor()
        query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE set1 = "'+languagecode+'" AND set1descriptor = "ccc" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY 2 DESC LIMIT '+str(closest_val)
        for row in cursor.execute(query):
            closest.append(row[0])

    return top, upperlower, closest


"""
def obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, num):

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

    conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();

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


# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
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
            'languageofficialnational': '',
            'languageofficialregional': '',
            'languageofficialsinglecountry': '',
            'nativeLabel': ";".join(nativeLabel),
            'numbercountriesOfficialorRegional':''
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

    # exceptions
    languages=languages.rename(index={'be_x_old': 'be_tarask'})
    languages=languages.rename(index={'zh_min_nan': 'nan'})
    languageid_file = languages.index.tolist();
    languageid_file.append('nb')

    languageid_calculated = df['WikimediaLanguagecode'].tolist();

#    print ('These are the ones just extracted from Wikidata: ')
#    print (languageid_calculated)

    newlanguages = list(set(languageid_calculated) - set(languageid_file))

    indexs = []
    for x in newlanguages: indexs = indexs + df.index[(df['WikimediaLanguagecode'] == x)].tolist()
    newlanguages = indexs

    if len(newlanguages)>0: 
        message = 'These are the new languages: '+', '.join(newlanguages)
        print (message)
        df=df.reindex(newlanguages)
        send_email_toolaccount('WCDO: New languages to introduce into the file.', message)
        print ('The new languages are in a file named: ')
        print (databases_path + filename+'.csv')
        df.to_csv(databases_path + filename+'.csv',sep='\t')
    else:
        print ('There are no new Wikipedia language editions.')


    return newlanguages


def get_months_queries():

    def datespan(startDate, endDate, delta=datetime.timedelta(days=1)):
        currentDate = startDate
        while currentDate < endDate:
            yield currentDate
            currentDate += delta

    periods_accum = {}
    periods_monthly = {}


    """
    # calculate last month
    last_month_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=61)
    first_day = last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S')
    last_day = last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S')
    month_condition = 'date_created >= "'+ first_day +'" AND date_created < "'+last_day+'"'
    print (month_condition)
    """

    for day in datespan(datetime.date(2001, 1, 16), datetime.date.today(),delta=datetime.timedelta(days=30)):
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

    return periods_monthly,periods_accum


def get_current_cycle_year_month():
    try:
        pathf = databases_path+'wikidata.db'
        current_cycle_date = time.strftime('%Y%m%d%H%M%S', time.gmtime(os.path.getmtime(pathf)))
        current_cycle_date = datetime.datetime.strptime(current_cycle_date,'%Y%m%d%H%M%S')-dateutil.relativedelta.relativedelta(months=1)
        current_cycle = current_cycle_date.strftime('%Y-%m')
        print ('wikidata.db exists.')
    except:
        pathf = '/public/dumps/public/wikidatawiki/entities/latest-all.json.gz'
        current_cycle_date = time.strftime('%Y%m%d%H%M%S', time.gmtime(os.path.getmtime(pathf)))
        current_cycle_date = datetime.datetime.strptime(current_cycle_date,'%Y%m%d%H%M%S')-dateutil.relativedelta.relativedelta(months=1)
        current_cycle = current_cycle_date.strftime('%Y-%m')
        print ('wikidata.db does not exist.')
    print (current_cycle)

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


