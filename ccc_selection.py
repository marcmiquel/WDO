# -*- coding: utf-8 -*-

# time
import time
import datetime
# system
import os
import sys
import re
import random
import operator
from IPython.display import HTML
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import gzip
import bz2
import json
import csv
import codecs
# requests
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
# bokeh
import bokeh
from bokeh.plotting import figure, show, output_file
from bokeh.core.properties import value
from bokeh.io import show, output_file
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter, LogColorMapper
# pywikibot
import pywikibot, pywikibot.pagegenerators as pg
from pywikibot.bot import suggest_help
from pywikibot.specialbots import UploadRobot
from pywikibot import Category
PYWIKIBOT2_DIR = '/srv/wcdo/user-config.py'


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("ccc_selection_temp.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass

# MAIN
def main():


    # Obtaining CCC for all WP
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)


        extend_articles_editors(languagecode,page_titles_qitems,page_titles_page_ids)

        return

        wiki_path = ''

        # EDITORS - 1000
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # EDITORS - 100
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.999,'num_inlinks_from_CCC': 0.001}, 'positive', 100, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)


        # FEATURED
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'featured_article': 0.9,'num_inlinks_from_CCC': 0.05, 'num_retrieval_strategies':0.05}, 'positive', 100, 'minimum', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # GL MOST INLINKED FROM CCC
        generate_ccc_vital_articles_list(languagecode, 'ca', ['gl'], '', '', {'num_inlinks_from_CCC': 0.9, 'num_retrieval_strategies': 0.1}, 'positive', 100, 'minimum', ['num_inlinks_from_CCC','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # KEYWORDS ON TITLE WITH MOST BYTES
        generate_ccc_vital_articles_list(languagecode, 'ca', ['kw'], '', '', {'num_bytes': 0.9,'num_inlinks_from_CCC': 0.05, 'num_retrieval_strategies': 0.05}, 'positive', 100, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # MOST EDITED DURING FIRST THREE YEARS
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', 'first_three_years', {'num_edits': 0.999,'num_inlinks_from_CCC': 0.001}, 'positive', 100, 'minimum', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # MOST EDITED DURING LAST THREE MONTHS
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', 'last_three_months', {'num_edits': 0.999,'num_inlinks_from_CCC': 0.001}, 'positive', 100, 'minimum', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        
        # MOST EDITED WOMEN BIOGRAPHY
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc','female'], '', '', {'num_edits': 0.8,'num_inlinks_from_CCC': 0.1,'num_retrieval_strategies':0.1}, 'positive', 100, 'minimum', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # MOST EDITED MEN BIOGRAPHY
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc','male'], '', '', {'num_edits': 0.8,'num_inlinks_from_CCC': 0.1,'num_retrieval_strategies':0.1}, 'positive', 100, 'minimum', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_pageviews': 0.999,'num_inlinks_from_CCC': 0.001}, 'positive', 100, 'minimum', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)

        # MOST DISCUSSED (EDITS DISCUSSIONS)
        generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_discussions': 0.999,'num_inlinks_from_CCC': 0.001}, 'positive', 100, 'minimum', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)


        return
        publish_wcdo_updates()



"""
# MAIN
######################## WCDO SCRIPT ######################## 

# (A) -> RAW DATA PHASE
    # CREATE THE WIKIDATA DB
    download_latest_wikidata_dump()
    wd_dump_iterator()
    wd_geolocated_update_db()

    # CREATE THE PAGEVIEWS DB
    download_latest_pageviews_dump()
    pageviews_dump_iterator()

    # * CREATE THE WIKIPEDIA CCC DB
    create_wikipedia_ccc_db()
    insert_page_ids_page_titles_qitems_ccc_db()
    for languagecode in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)
        extend_articles_discussions(languagecode, page_titles_qitems, page_titles_page_ids)
        extend_articles_timestamp(languagecode,page_titles_qitems)
        extend_articles_edits(languagecode, page_titles_qitems)
        extend_articles_editors(languagecode,page_titles_qitems,page_titles_page_ids)
        extend_articles_references(languagecode,page_titles_qitems,page_titles_page_ids)
        extend_articles_bytes(languagecode, page_titles_qitems)      
        extend_articles_featured(languagecode, page_titles_qitems)
        extend_articles_interwiki(languagecode,page_titles_page_ids)
        extend_articles_qitems_properties(languagecode,page_titles_page_ids)
        extend_articles_pageviews(languagecode,page_titles_qitems,page_titles_page_ids)
    print ('ready for the Wikipedia CCC Data Phase.')

# (B) -> WIKIPEDIA CCC DATA PHASE
    # Obtaining CCC for all WP
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])
        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)

        # DATA STRATEGIES:
        # B1. RETRIEVE AND SET ARTICLES AS CCC:
        # * retrieve direct
        get_ccc_articles_geolocation_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_geolocation_wd');
        get_ccc_articles_geolocated_reverse_geocoding(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_geolocated_reverse_geocoding');
        get_ccc_articles_country_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_country_wd');
        get_ccc_articles_location_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_location_wd');
        get_ccc_articles_language_strong_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_language_strong_wd');
        get_ccc_articles_keywords(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_keywords');
        # * retrieve indirect
        get_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_created_by_properties_wd');
        get_ccc_articles_part_of_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_ccc_articles_part_of_properties_wd');
        get_ccc_articles_from_community_vital_list(languagecode)

        # B2. RETRIEVE (POTENTIAL) CCC ARTICLES THAT RELATE TO CCC:
        # * retrieve direct
        get_articles_geolocated_geo_tags(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_geolocated_geo_tags');
        get_articles_category_crawling(languagecode,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_category_crawling');
        get_articles_language_weak_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_language_weak_wd');
        get_articles_with_inlinks(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_with_inlinks');
        get_articles_with_outlinks(languagecode,page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_with_outlinks');
        get_articles_affiliation_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_affiliation_properties_wd');
        get_articles_has_part_properties_wd(languagecode,page_titles_page_ids); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' get_articles_has_part_properties_wd');

        # B3. FILTERING AND CREATING THE DEFINITIVE CCC
        filter_articles_geolocated_elsewhere(languagecode)
        calculate_articles_ccc_binary_classifier(languagecode,'RandomForest',page_titles_page_ids,page_titles_qitems); extract_ccc_count(languagecode,'ccc_database.txt', languagecode+' calculate_articles_ccc_binary_classifier');
        calculate_articles_ccc_main_territory(languagecode)
        calculate_articles_ccc_retrieval_strategies(languagecode)
        calculate_articles_ccc_algorithm_vital_list(languagecode)

    # EXTRACT CCC DATASETS INTO CSV AND CLEAN OLD DATABASES
    extract_ccc_tables_to_files()
    delete_wikidata_db()
    delete_pageviews_db()
    delete_latest_wikidata_dump()
    delete_latest_pageviews_dump()
    rename_and_drop_ccc_db()

# (C) -> STATS DATA PHASE / CREATE CCC OUTPUT DATA FOR DISSEMINATION -> idea: these phases could be split into another file.
    create_table_ccc_allwiki()
    create_table_ccc_extent_by_language()
    create_table_ccc_extent_by_qitem()
    create_table_ccc_gaps()
    create_table_ccc_bridging_last15days()
    create_table_ccc_topical_coverage()

# (D) -> PUBLISHING FORMATTED DATA PHASE (TABLES AND GRAPHS)
    publish_wcdo_updates()

# (E) -> NOTIFICATION:
    send_email_toolaccount('CCC created successfuly', '')
"""


# DATABASE AND DATASETS MAINTENANCE FUNCTIONS (CCC AND WIKIDATA)
################################################################

# Loads Wikipedia_language_territories_mapping_quality.csv file
def load_languageterritories_mapping():
# READ FROM STORED FILE:
    territories = pd.read_csv('Wikipedia_language_territories_mapping_quality.csv',sep='\t',na_filter = False)
    territories = territories[['territoryname','territorynameNative','QitemTerritory','languagenameEnglishethnologue','WikimediaLanguagecode','demonym','demonymNative','ISO3166','ISO31662','regional','country','indigenous','languagestatuscountry','officialnationalorregional']]
    territories = territories.set_index(['WikimediaLanguagecode'])

    territorylanguagecodes = territories.index.tolist()
    for n, i in enumerate(territorylanguagecodes): territorylanguagecodes[n]=i.replace('-','_')
    territories.index = territorylanguagecodes
    territories=territories.rename(index={'be_tarask': 'be_x_old'})
    territories=territories.rename(index={'nan': 'zh_min_nan'})

    ISO3166=territories['ISO3166'].tolist()
    regions = pd.read_csv('Wikipedia_country_regions.csv',sep=',',na_filter = False)
    regions = regions[['alpha-2','region','sub-region','intermediate-region']]
    regions = regions.set_index(['alpha-2'])
    region=[]; subregion=[]; intermediateregion=[]
    
    for code in ISO3166:
        if code=='': reg=''; subreg=''; interreg='';
        else:
            reg=regions.loc[code]['region']
            subreg=regions.loc[code]['sub-region']
            interreg=regions.loc[code]['intermediate-region']
        region.append(reg)
        subregion.append(subreg)
        intermediateregion.append(interreg)

    territories['region']=region
    territories['subregion']=subregion
    territories['intermediateregion']=intermediateregion

# READ FROM META: 
# check if there is a table in meta:
#    generate_print_language_territories_mapping_table()
# uses pywikibot and verifies the differences with the file.
# sends an e-mail with the difference and the 'new proposed file'
#    send_email_toolaccount('subject', 'message')
# stops.
# we verify this e-mail, we re-start the task.
    return territories



# Loads Wikipedia_language_editions.csv file
def load_wiki_projects_information():
    # in case of extending the project to other WMF sister projects, it would be necessary to revise these columns and create a new file where a column would specify whether it is a language edition, a wikictionary, etc.

# READ FROM STORED FILE:
    languages = pd.read_csv('Wikipedia_language_editions.csv',sep='\t',na_filter = False)
    languages=languages[['languagename','Qitem','WikimediaLanguagecode','Wikipedia','WikipedialanguagearticleEnglish','languageISO','languageISO3','languageISO5','languageofficialnational','languageofficialregional','languageofficialsinglecountry','nativeLabel','numbercountriesOfficialorRegional']]
    languages = languages.set_index(['WikimediaLanguagecode'])
#    print (list(languages.columns.values))

    wikilanguagecodes = languages.index.tolist()
    for n, i in enumerate(wikilanguagecodes): wikilanguagecodes[n]=i.replace('-','_')
    languages.index = wikilanguagecodes
    languages=languages.rename(index={'be_tarask': 'be_x_old'})
    languages=languages.rename(index={'nan': 'zh_min_nan'})

    language_region={}; language_subregion={}; language_intermediateregion={}
    regions=territories[['region','subregion','intermediateregion']]
    for index, row in territories.iterrows():
        region=row['region']
        subregion=row['subregion']
        intermediateregion=row['intermediateregion']

        if index not in language_region:
            language_region[index]=region
            language_subregion[index]=subregion
            language_intermediateregion[index]=intermediateregion
        else:
            if region not in language_region[index]: language_region[index]=language_region[index]+';'+region
            if subregion not in language_subregion[index]: language_subregion[index]=language_subregion[index]+';'+subregion
            if intermediateregion not in language_intermediateregion[index] and language_intermediateregion[index]!='': language_intermediateregion[index]=language_intermediateregion[index]+';'+intermediateregion

    wikilanguagecodes = languages.index.tolist()
    new_language_region=[]; new_language_subregion=[]; new_language_intermediateregion=[]
    for lang in wikilanguagecodes:
        new_language_region.append(language_region[lang])
        new_language_subregion.append(language_subregion[lang])
        new_language_intermediateregion.append(language_intermediateregion[lang])

    languages['region']=new_language_region
    languages['subregion']=new_language_subregion
    languages['intermediateregion']=new_language_intermediateregion

# READ FROM META: 
# uses pywikibot and verifies the differences with the file. 
#    read_meta_language_territories_mapping_table()
# sends an e-mail with the difference and the 'new proposed file'
#    send_email_toolaccount('subject', 'message')
# stops.
# we verify this e-mail, we re-start the task.

#    filename = 'language_editions_with_regions'
#    languages.to_csv(filename+'.csv',sep='\t')

    return languages


def load_wikipedia_language_editions_numberofarticles():
    wikipedialanguage_numberarticles = {}
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    # Obtaining CCC for all WP
    for languagecode in wikilanguagecodes:
        query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki;'
        cursor.execute(query)
        wikipedialanguage_numberarticles[languagecode]=cursor.fetchone()[0]

    return wikipedialanguage_numberarticles


def generate_print_language_territories_mapping_table():
    site = pywikibot.Site("meta", '')
    # get the text from the html created for the Nikola website.
    text = ''
    page = pywikibot.Page(site, "Language Territories Mapping")
    page.save(summary="Updating Table", watch=None, minor=False,
                    botflag=False, force=False, async=False, callback=None,
                    apply_cosmetic_changes=None, text=text)
# publicar la taula de territoris a meta.

def read_meta_language_territories_mapping_table():
    site = pywikibot.Site("meta", '')
    # get the text from the meta page and see any editor had changed it. send an e-mail with changes.
    text = ''
    page = pywikibot.Page(site, "List of Wikipedias/table")
    page.text.split('\n')
    # https://meta.wikimedia.org/w/index.php?title=List_of_Wikipedias/Table&action=edit
    print ('')

def read_community_generated_article_lists():
# this gets the links from each language edition version page with the list of articles.
# 1.3 publicar una taula amb els encarregats de tirar endavant el projecte en cada llengua.
# publicar una taula amb els links a les llistes d'articles col·locats per les comunitats en cada llengua. lectura d'aquesta taula amb links de 1.5. 
# 1.5 lectura pàgina de llista d'articles 'List of articles from X Wikipedia Cultural Contexts that every other should have' en cada llengua.
    print ('')


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
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
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
            'WikimediaLanguagecode': wikimedialanguagecode,
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
    filename= 'Wikipedia_language_editions'
    newlanguages = []

    # CHECK IF THERE IS ANY NEW LANGUAGE
    if os.path.isfile(filename+'.csv'): # FILE EXISTS: CREATE IT WITH THE NEW LANGUAGES IN THE FILENAME
        languages = pd.read_csv(filename+'.csv',sep='\t')
        languages = languages.set_index(['languagename'])

        languagecodes_file = languages['WikimediaLanguagecode'].tolist(); languagecodes_file.append('nan')
        languagecodes_calculated = df['WikimediaLanguagecode'].tolist();
        newlanguages = list(set(languagecodes_calculated) - set(languagecodes_file))

        indexs = []
        for x in newlanguages: indexs = indexs + df.index[(df['WikimediaLanguagecode'] == x)].tolist()
        newlanguages = indexs

        if len(newlanguages)>0: 
            message = 'These are the new languages: '+', '.join(newlanguages)
            print (message)
            df=df.reindex(newlanguages)
            send_email_toolaccount('WCDO: New languages to introduce into the file.', message)
            filename="_".join(newlanguages)+'_'+filename
            print (df.head())
            df.to_csv(filename+'.csv',sep='\t')
        else:
            print ('There are no new Wikipedia language editions.')

    else: # FILE DOES NOT EXIST: CREATE IT WITH THE WHOLE NAME
        df.to_csv(filename+'.csv',sep='\t')

    return newlanguages


# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
        return mysql_con_read
    except:
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')

# Create a database connection.
def establish_mysql_connection_write():
    mysql_con_write = mdb.connect(host="tools.db.svc.eqiad.wmflabs",db='s53619__wcdo',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
    return mysql_con_write

# Creates a CCC database for a list of languages.
def create_wikipedia_ccc_db():
    functionstartTime = time.time()
    print ('* create_wikipedia_ccc_db')
    # Removes current CCC database (just for code debugging)
#    try:
#        os.remove("ccc_current.db"); print ('ccc_current.db deleted.');
#    except:
#        pass

    # Creates the current CCC database.
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor(); print ('ccc_current.db created.');

    # Creates a table for each Wikipedia language edition CCC.
    nonexistingwp = []
    for languagecode in wikilanguagecodes:

        # Checks whether the Wikipedia currently exists.
        try:
            establish_mysql_connection_read(languagecode)
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
        'date_created text, '+

        # calculations:
        'ccc_binary integer, '+
        'main_territory text, '+ # here there would be a Q with the main territory to which is associated.
        'num_retrieval_strategies integer, '+ # this is an index

        # set as CCC
        'geocoordinates text, '+ # coordinate1,coordinate2
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

        # top 100 rank position
#        'vital100_algorithm integer, '+
        'vital100_community integer, '+
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
def rename_and_drop_ccc_db(): # ara això s'hauria de pensar amb uns altres noms.
    os.remove("ccc.db")
    os.rename("ccc_current.db","ccc.db")


# Creates a dataset from the CCC database for a list of languages.
# COMMAND LINE: sqlite3 -header -csv ccc_current.db "SELECT * FROM ccc_cawiki;" > ccc_cawiki.csv
def extract_ccc_tables_to_files():
    conn = sqlite3.connect('ccc.db'); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        superfolder = './datasets'
        languagefolder = superfolder+'/'+languagecode+'wiki/'
        latestfolder = superfolder+'/latest/'

        if not os.path.exists(languagefolder): os.makedirs(languagefolder)
        if not os.path.exists(latestfolder): os.makedirs(latestfolder)

        # These are the files.
        ccc_filename_archived = languagecode + 'wiki_' + str(datetime.date.today()).replace('-','')+'_ccc.csv' # (e.g. 'cawiki_20180215_ccc.csv')
        ccc_filename_latest = languagecode + 'wiki_latest_ccc.csv' # (e.g. cawiki_latest_ccc.csv)

        # These are the final paths and files.
        path_latest = latestfolder + ccc_filename_latest
        path_language = languagefolder + ccc_filename_archived
        print ('Extracting the CCC from language '+languagecode+' into the file: '+path_language)

        # These are the files.
#        path_language_file = codecs.open(path_language, 'w', 'UTF-8')
        c = csv.writer(open(path_language,'w'), lineterminator = '\n', delimiter='\t')

        # Extract database into a dataset file. Only the rows marked with CCC.
        cursor.execute("SELECT * FROM ccc_"+languagecode+"wiki WHERE ccc_binary IS NOT NULL;") # ->>>>>>> canviar * per les columnes. les de rellevància potser no cal.
        print (languagecode+' language CCC has this number of rows: '+str(len(cursor)))
        with c as out_csv_file:
          csv_out = csv.writer(out_csv_file)
          csv_out.writerow([d[0] for d in cursor.description])
          for result in cursor:
            csv_out.writerow(result)

        # Delete the old 'latest' file and copy the new language file as a latest file.
        try: 
            os.remove(path_latest); 
        except: pass
        cursor.close()

#        shutil.copyfile(path_language,path_latest)
#        print ('Creating the latest_file for: '+languagecode+' with name: '+path_latest)

        # Count the number of files in the language folders and in case they are more than X, we delete them.
#        filenamelist = sorted(os.listdir(languagefolder), key = os.path.getctime)

        # Reference Datasets:
        # http://whgi.wmflabs.org/snapshot_data/
        # https://dumps.wikimedia.org/wikidatawiki/entities/
        # http://ftp.acc.umu.se/mirror/wikimedia.org/dumps/cawiki/20180201/


def extract_ccc_count(languagecode, filename, message):
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki;'
    cursor.execute(query)
    row = cursor.fetchone()
    if row: row = str(row[0]);
    languagename = languages.loc[languagecode]['languagename']

    with open(filename, 'a') as f:
        f.write(languagename+'\t'+message+'\t'+row+'\n')


def load_dicts_page_ids_qitems(languagecode):
    page_titles_qitems = {}
    page_titles_page_ids = {}

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
    for row in cursor.execute(query):
        page_title=row[0].replace(' ','_')
        page_titles_page_ids[page_title]=row[2]
        page_titles_qitems[page_title]=row[1]
    print ('page_ids loaded.')
    print ('qitems loaded.')
    print ('they are:')
    print (len(page_titles_qitems))

    return (page_titles_qitems, page_titles_page_ids)


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
    try: os.remove("pageviews.db");
    except: pass;
    conn = sqlite3.connect('pageviews.db')
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
    os.remove("latest_pageviews.bz2")

def delete_pageviews_db():
    os.remove("pageviews.db")


def download_latest_wikidata_dump():
    functionstartTime = time.time()
    print ('* Downloading the latest Wikidata dump.')
    url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz" # download the dump: https://dumps.wikimedia.org/wikidatawiki/entities/20180212/
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=10240): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    print ('* download_latest_wikidata_dump Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_wikidata_db():
    try: os.remove("wikidata.db");
    except: pass;
    conn = sqlite3.connect('wikidata.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS sitelinks (qitem text, langcode text, page_title text, page_id integer, PRIMARY KEY (qitem, langcode));")

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
    dump_in = gzip.open(read_dump, 'r')
    line = dump_in.readline()
    iter = 0

    conn = create_wikidata_db(); cursor = conn.cursor()
    qitems = wd_all_qitems(cursor); conn.commit() # getting all the qitems

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
            print (100*iter/45138856)
            print ('current time: ' + str(time.time() - startTime))
#            break

    cursor.execute("DROP TABLE qitems;")
    conn.commit()
    conn.close()
    print ('DONE with the JSON.')
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
            cursor.execute("INSERT INTO sitelinks (qitem, langcode, page_title) VALUES (?,?,?)",values)





# Runs the reverse geocoder and update the database. It needs 5000m.
def wd_geolocated_update_db():
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

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


# Checks all the databses and updates the database.
def insert_page_ids_page_titles_qitems_ccc_db():
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode)

        page_titles_qitems={}
        query = 'SELECT page_title, qitem FROM sitelinks WHERE langcode = "'+languagecode+'wiki";'
        for row in cursor.execute(query):
            page_title=row[0].replace(' ','_')
            page_titles_qitems[page_title]=row[1]
        print (len(page_titles_qitems))
        print ('qitems obtained.')

        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    page_ids_timestamps = []

    try:
        i=1/0
        print ('Trying to run the entire query.')
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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

        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 1000000
        while (maxval > 0):
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
                try: page_ids_timestamps.append((row[0],row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
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
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    try:
        i=1/0
        print ('Trying to run the entire query.')
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute('SELECT COUNT(DISTINCT rev_user_text),page_id,page_title FROM revision INNER JOIN page ON rev_page = page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_id')
        rows = mysql_cur_read.fetchall()
        for row in rows: 
            try: page_titles_editors.append((row[0],row[1],page_titles_qitems[str(row[2].decode('utf-8'))]))
            except: continue
    except:       
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = cursor.fetchone()[0]
        print (maxval)

        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 100000
        range_values = 0
        while (range_values < maxval):
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
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute("SELECT page_title, COUNT(*) FROM revision r, page p WHERE r.rev_page=p.page_id AND p.page_namespace=1 GROUP by page_title;")
        rows = mysql_cur_read.fetchall()
        for row in rows:
            page_title=str(row[0].decode('utf-8'))
            try: updated.append((row[1],page_titles_page_ids[str(row[0].decode('utf-8'))],page_titles_qitems[page_title]))
            except: continue
    except:
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = cursor.fetchone()[0]
        print (maxval)

        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 10000000
        while (maxval > 0):
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
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    try:
        print ('Trying to run the entire query.')
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = "SELECT page_title, page_id, COUNT(*) FROM revision INNER JOIN page on rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by rev_page;"
        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()
        for row in rows:
            page_title=str(row[0].decode('utf-8'))
            page_id=row[1]
            count=row[2]
            try: page_ids_edits.append((count,page_id,page_titles_qitems[page_title]))
            except: pass
    except:
        print ('Trying to run the query with batches.')
        cursor.execute("SELECT max(page_id) FROM ccc_"+languagecode+'wiki;')
        maxval = int(cursor.fetchone()[0])
        print (maxval)

        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        increment = 1000000
        range_values = 0
        while (range_values < maxval):
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


# Extends the articles table with the number of bytes per article.
def extend_articles_bytes(languagecode, page_titles_qitems):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_bytes')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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


# Extends the articles table with the number of references per article.
def extend_articles_references(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('* extend_articles_references')
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute("SELECT el_from, COUNT(*) FROM externallinks INNER JOIN page ON el_from=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by el_from;")
    rows = mysql_cur_read.fetchall()
    for row in rows:
        page_id=row[0]
        try: 
            page_title=page_ids_page_titles[page_id]
            qitem = page_titles_qitems[page_title]
            count=row[1]
            query = 'UPDATE ccc_'+languagecode+'wiki SET num_references=? WHERE page_id = ? AND qitem = ?;'
            cursor.execute(query,(count,page_id,qitem))
        except: 
            pass
    conn.commit()

    print ('References updated.')
    print ('* extend_articles_references Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Extends the articles table with the number of interwiki links.
def extend_articles_interwiki(languagecode, page_titles_page_ids):
    functionstartTime = time.time()
    print ('* extend_articles_interwiki')

    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()

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

    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()

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


# Extends the articles table with the number of pageviews.
def extend_articles_pageviews(languagecode, page_titles_qitems, page_titles_page_ids):
    functionstartTime = time.time()
    print ('* extend_articles_pageviews')

    conn = sqlite3.connect('pageviews.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()

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
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    featuredarticleslanguages = {}
    featuredarticleslanguages['enwiki']="Featured_articles"
    mysql_con_read = establish_mysql_connection_read('en'); mysql_cur_read = mysql_con_read.cursor()
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
#    input('')

    featuredarticles=[]
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    mysql_cur_read.execute('SELECT page_title, page_id FROM categorylinks INNER JOIN page on cl_from=page_id WHERE CONVERT(cl_to USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s', (featuredarticleslanguages[languagecode+'wiki'],)) # Extreure
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
            print ('This article does not exist: '+page_title)

    print ('We obtained '+str(len(featuredarticles))+' featured articles from this language: '+languagecode)
    print ('* extend_articles_featured Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def delete_wikidata_db():
    os.remove("wikidata.db")

def delete_latest_wikidata_dump():
    os.remove("latest-all.json.gz")


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
    input_subdivisions_filename = 'world_subdivisions.csv'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = {}
    for line in input_subdivisions_file: 
        info = line.strip('\n').split(',');
        subdivisions[info[0]] = info[1]

    geolocated_pageids_titles = {}
    geolocated_pageids_coords = {}
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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
        lat = str(geolocated_pageids_coords[page_id][0])
        lon = str(geolocated_pageids_coords[page_id][1])
#        print (page_title,page_id,admin1,iso3166)

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

        # the Qitem is decided.
        if qitem!='':
            coordinates = lat+','+lon
            try: 
                qitem_specific=page_titles_qitems[page_title]
                ccc_geolocated_items.append((1,coordinates,qitem,page_id,page_title,qitem_specific))
            except: continue
#            print ((page_id,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
#        else:
#            print ('### NO!\n')
        if x%20000 == 0: print (x)


    if len(ccc_geolocated_items)==0: print ('No geolocated articles in Wikidata for this language edition.');return
    # It inserts the right articles into the corresponding CCC database.
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,geocoordinates,main_territory) = (?,?,?) WHERE page_id = ? AND page_title = ? AND qitem = ?;'
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
        allISO3166.append(territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166'])
    allISO3166 = list(set(allISO3166))

    # with a subdivision name you get a ISO 31662 (without the ISO3166 part), that allows you to get a Qitem
    input_subdivisions_filename = 'world_subdivisions.csv'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = {}
    for line in input_subdivisions_file: 
        info = line.strip('\n').split(',');
        subdivisions[info[0]] = info[1]

    # Get the articles, evaluate them and insert the good ones.   
    ccc_geolocated_items = []
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()
#    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON sitelinks.qitem=geolocated_property.qitem WHERE sitelinks.langcode="'+languagecode+'wiki";'

    page_asstring = ','.join( ['?'] * len(allISO3166) )
    query = 'SELECT geolocated_property.qitem, geolocated_property.coordinates, geolocated_property.admin1, geolocated_property.iso3166, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON sitelinks.qitem=geolocated_property.qitem WHERE sitelinks.langcode="'+languagecode+'wiki" AND geolocated_property.iso3166 IN (%s) ORDER BY geolocated_property.iso3166, geolocated_property.admin1;' % page_asstring

    x = 0
    for row in cursor.execute(query,allISO3166):
        qitem_specific=str(row[0])
        coordinates=str(row[1])
        admin1=str(row[2]) # it's the Territory Name according to: https://github.com/thampiman/reverse-geocoder
        iso3166=str(row[3])
        page_title=str(row[4]).replace(' ','_')

#        print (page_title,qitem_specific,admin1,iso3166,coordinates)
#        input('un moment')
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

        # the Qitem is decided.
        if qitem!='':
            try: 
                page_id=page_titles_page_ids[page_title]
                ccc_geolocated_items.append((1,coordinates,qitem,qitem_specific,page_title,page_id)) 
            except: continue
#            print ((qitem_specific,page_title,coordinates,qitem)); print ('*** IN! ENTRA!\n')
#            input('')
#        else:
#            print ('### NO!\n')
        if x%20000 == 0: print (x);
        x = x + 1

    if len(ccc_geolocated_items)==0: print ('No geolocated articles in Wikidata for this language edition.');return
    # Insert to the corresponding CCC database.
    print ('Inserting/Updating articles into the database.')
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,geocoordinates,main_territory) = (?,?,?) WHERE qitem = ? AND page_title = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_geolocated_items)
    conn2.commit()
    print ('All geolocated articles from Wikidata validated through reverse geocoding are in. They are: '+str(len(ccc_geolocated_items))+'.')
    print ('They account for a '+str(100*len(ccc_geolocated_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('Geolocated articles from Wikidata for language '+(languagecode)+' have been inserted.');
    print ('* get_ccc_articles_geolocation_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a country property related to a territory from the list of territories from the language. Label them as CCC.
def get_ccc_articles_country_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db');cursor = conn.cursor()
    print ('\n* Getting articles with Wikidata from items with "country" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    # country qitems
    try: countries = territories.loc[territories['region'] == 'no'].loc[languagecode]['QitemTerritory'].tolist()
    except: 
        try: countries = list(); countries.append(territories.loc[territories['region'] == 'no'].loc[languagecode]['QitemTerritory'])
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
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,country_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_country_items)
    conn2.commit()
    print (str(len(ccc_country_items))+' country related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(100*len(ccc_country_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_country_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a location property that is iteratively associated to the list of territories associated to the language. Label them as CCC.
def get_ccc_articles_location_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

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


    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,location_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_located_items)
    conn2.commit()

    print ('They account for a '+str(100*len(ccc_located_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_location_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a "strong" language property that is associated the language. Label them as CCC.
def get_ccc_articles_language_strong_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    print ('\n* Getting articles with Wikidata from items with "language" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

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
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,language_strong_wd,page_title) = (?,?,?) WHERE qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()
    print (str(len(ccc_language_items))+' language related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(100*len(ccc_language_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_articles_wd_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a creation property that is related to the items already retrieved as CCC. Label them as CCC.
def get_ccc_articles_created_by_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()

    print ('\n* Getting articles with Wikidata from items with "created by" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    ccc_articles={}
    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'): ccc_articles[row[1]]=row[0].replace(' ','_')

#    potential_ccc_articles={}
#    for row in cursor.execute('SELECT page_title, qitem FROM ccc_'+languagecode+'wiki;'):
#        potential_ccc_articles[row[1]]=row[0].replace(' ','_')

    selected_qitems={}
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()
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
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,created_by_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_created_by_items)
    conn.commit()
    print (str(len(ccc_created_by_items))+' items/articles created by CCC articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(100*len(ccc_created_by_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_created_by_properties_wd Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles which are part of items already retrieved as CCC. Label them as CCC.
def get_ccc_articles_part_of_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

    print ('\n* Getting articles with Wikidata from items with "part of" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    part_of_properties = {'P361':'part of'} 

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()

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

    # INSERT INTO CCC DATABASE
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,part_of_wd) = (?,?) WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor.executemany(query,ccc_part_of_items)
    conn.commit()
    print (str(len(ccc_part_of_items))+' items/articles created by CCC articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('They account for a '+str(100*len(ccc_part_of_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
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

    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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
        try: qitem=page_titles_qitems[page_title]
        except: continue
        insertedarticles.append((1,keyword_title,page_title,page_id,qitem))

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (ccc_binary,keyword_title,page_title) = (?,?,?) WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,insertedarticles)
    conn.commit()
    print ('Articles with keywords on titles in Wikipedia language '+(languagecode)+' have been inserted.');
    print ('The number of inserted articles account for a '+str(100*len(selectedarticles)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_ccc_articles_keywords Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def get_ccc_articles_from_community_vital_list(languagecode):
    # update the CCC_binary with more groundtruth.
    # update to calculate the ccc_gaps later.

    read_community_generated_article_lists()
    print ('vital100_community integer')

    

# Obtain the articles with coordinates gelocated in the territories associated to that language. This is considered potential CCC.
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
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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
            except: continue
            ccc_geolocated_items.append((coordinates,qitem,page_title,page_id,qitem_specific))
            count = count + 1

        # Insert to the corresponding CCC database.
        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
        query = 'UPDATE ccc_'+languagecode+'wiki SET (geocoordinates,main_territory) = (?,?) WHERE page_title = ? AND page_id = ? AND qitem=?;'
        cursor.executemany(query,ccc_geolocated_items)
        conn.commit()

    print ('Geolocated articles from Wikipedia language '+(languagecode)+' geotags have been inserted.');
    print ('They are: '+str(count)+'.')
    print ('They account for a '+str(100*count/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
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
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()


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
        except: continue
        parameters.append((categorycrawling,selectedarticles_level[page_id],page_title,page_id,qitem))

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (category_crawling_territories,category_crawling_level) = (?,?) WHERE page_title = ? AND page_id = ? AND qitem=?;'
    cursor.executemany(query,parameters)
    conn.commit()

    # ALL ARTICLES
    wp_number_articles = wikipedialanguage_numberarticles[languagecode]
    string = "The total number of category crawling selected articles is: " + str(len(selectedarticles)); print (string)
    string = "The total number of selected categories is: " + str(len(selectedcategories)); print (string)
    string = "The total number of articles in this Wikipedia is: "+str(wp_number_articles)+"\n"; print (string)
    string = "The percentage of category crawling related articles in this Wikipedia is: "+str(100*len(selectedarticles)/wp_number_articles)+"\n"; print (string)

    print ('Articles obtained through the category graph crawling in Wikipedia language '+(languagecode)+' have been inserted.');
    print ('* get_articles_category_crawling Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Obtain the articles with a "weak" language property that is associated the language. This is considered potential CCC.
def get_articles_language_weak_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    print ('\n* Getting articles with Wikidata from items with "language" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

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
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET (language_weak_wd) = ? WHERE page_title = ? AND qitem = ? AND page_id = ?;'
    cursor2.executemany(query,ccc_language_items)
    conn2.commit()
    print (str(len(ccc_language_items))+' language related articles from Wikidata for language '+(languagecode)+' have been inserted/updated.');
    print ('The number of inserted articles account for a '+str(100*len(ccc_language_items)/wikipedialanguage_numberarticles[languagecode])+'% of the entire Wikipedia.')
    print ('* get_articles_wd_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles table with the number of inlinks.
def get_articles_with_inlinks(languagecode,page_titles_page_ids,page_titles_qitems):
    functionstartTime = time.time()
    print ("\n* Updating the number of inlinks and the number of inlinks from CCC for language: "+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    try:
        # INLINKS FROM CCC
        # get the ccc and potential ccc articles
        print ('Attempt with a MySQL.')
#        a=0
#        print (a/a)

        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
        query = 'SELECT page_id, page_title, ccc_binary, qitem FROM ccc_'+languagecode+'wiki;'
        ccc_articles = {}
#        all_ccc_articles = {}
#        all_ccc_articles_qitems = {}
        for row in cursor.execute(query):
            if row[2]==1: ccc_articles[row[0]]=row[1]
#            all_ccc_articles[row[1]]=row[0]
#            all_ccc_articles_qitems[row[1]]=row[3]
        print ('- Articles in CCC: '+str(len(ccc_articles)))
#        print (str(len(all_ccc_articles)))

        page_titles_inlinks_ccc = []
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        page_asstring = ','.join( ['%s'] * len( ccc_articles ) )
        query = 'SELECT count(pl_from), pl_title FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_from IN (%s) GROUP BY pl_title' % page_asstring
        mysql_cur_read.execute(query,list(ccc_articles.keys()))
        rows = mysql_cur_read.fetchall()
#        print ('query run.')
        for row in rows:
            pl_title = str(row[1].decode('utf-8'))
            try: 
#                print ((row[0],all_ccc_articles[pl_title],all_ccc_articles_qitems[pl_title]))
                page_titles_inlinks_ccc.append((row[0],page_titles_page_ids[pl_title],page_titles_qitems[pl_title], pl_title))
            except: pass

        query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks_from_CCC=? WHERE page_id = ? AND qitem = ? AND page_title=?;'
        cursor.executemany(query,page_titles_inlinks_ccc)
        conn.commit()
        print ('- Articles with links coming from CCC: '+str(len(page_titles_inlinks_ccc)))
        print ('* get_articles_with_inlinks_ccc completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        # INLINKS
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE num_inlinks IS NOT NULL;'
        cursor.execute(query)
        if cursor.fetchone()[0] == 0:
            page_titles_inlinks = []
            mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
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

            query = 'UPDATE ccc_'+languagecode+'wiki SET num_inlinks=?,percent_inlinks_from_CCC=(num_inlinks_from_CCC/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
            cursor.executemany(query,page_titles_inlinks)
            conn.commit()
            print ('- Articles with any inlink at all: '+str(len(page_titles_inlinks)))
            print ('* get_articles_with_inlinks  completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    except:
        print ("MySQL has gone away. Let's try to do the joins in the code logics.")

        functionstartTime = time.time()
        # get the ccc and potential ccc articles
        conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
        query = 'SELECT page_id, page_title, ccc_binary, qitem FROM ccc_'+languagecode+'wiki;'
        ccc_articles = {}
#        all_ccc_articles = {}
#        all_ccc_articles_qitems = {}
        for row in cursor2.execute(query):
            if row[2]==1: ccc_articles[row[0]]=row[1]
#            all_ccc_articles[row[1]]=row[0]
#            all_ccc_articles_qitems[row[1]]=row[3]
        print ('- Articles in CCC: '+str(len(ccc_articles)))
#        num_all_ccc_articles=len(all_ccc_articles)
#        print (str(num_all_ccc_articles))
        ccc_articles_page_ids = set(ccc_articles.keys())

        page_titles_inlinks_ccc = []
        page_ids=[]
        num_art = 0
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

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

                count=len(list(set(page_ids).intersection(ccc_articles_page_ids)))
#                print (list(set(page_ids).intersection(ccc_articles_page_ids)))
                page_titles_inlinks_ccc.append((count,len(page_ids),float(count)/float(len(page_ids)),page_titles_page_ids[pl_title],page_titles_qitems[pl_title],pl_title))
#                print (count,str(len(page_ids)),float(count)/float(len(page_ids)),pl_title,all_ccc_articles[pl_title],all_ccc_articles_qitems[pl_title])
                page_ids.clear()
            except: 
                continue

        print ('- Articles with inlinks: '+str(len(page_titles_inlinks_ccc)))

        query = 'UPDATE ccc_'+languagecode+'wiki SET (num_inlinks_from_CCC,num_inlinks,percent_inlinks_from_CCC)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        cursor2.executemany(query,page_titles_inlinks_ccc)
        conn2.commit()
        print ('* get_articles_with_inlinks_ccc Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles table with the number of outlinks.
def get_articles_with_outlinks(languagecode,page_titles_page_ids,page_titles_qitems):
    functionstartTime = time.time()
    print ("\n* Get the number of outlinks and the number of outlinks to CCC for language: "+languages.loc[languagecode]['languagename']+' '+languagecode+'.')
    page_ids_page_titles = {v: k for k, v in page_titles_page_ids.items()}

    try:
        print ('Attempt with a MySQL.')
#        a=0
#        print (a/a)

        # get the ccc and potential ccc articles
        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
        query = 'SELECT page_id, page_title, ccc_binary, qitem FROM ccc_'+languagecode+'wiki;'
        ccc_articles = {}
    #        all_ccc_articles = {}
    #        all_ccc_articles_qitems = {}
        for row in cursor.execute(query):
            if row[2]==1: ccc_articles[row[0]]=row[1]
    #            all_ccc_articles[row[0]]=row[1]
    #            all_ccc_articles_qitems[row[0]]=row[3]
        print ('- Articles in CCC: '+str(len(ccc_articles)))
    #        print (str(len(all_ccc_articles)))

        page_titles_outlinks_ccc = []
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    #    mysql_con_read = mdb.connect(host=languagecode+'wiki.analytics.db.svc.eqiad.wmflabs',db=languagecode+'wiki_p', read_default_file='./my.cnf', cursorclass=mdb_cursors.SSCursor,charset='utf8mb4'); mysql_cur_read = mysql_con_read.cursor()

        page_asstring = ','.join( ['%s'] * len( ccc_articles ) )
        query = 'SELECT count(pl_title), pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_title IN (%s) GROUP BY pl_from;' % page_asstring

        mysql_cur_read.execute(query,list(ccc_articles.values()))
        rows = mysql_cur_read.fetchall()
    #        print ('query run.')
        for row in rows:
            try:
                page_title=page_ids_page_titles[row[1]]
                page_titles_outlinks_ccc.append((row[0],row[1],page_titles_qitems[page_title],page_title))
            except: pass

        query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks_to_CCC=? WHERE page_id = ? AND qitem = ? AND page_title=?;'
        cursor.executemany(query,page_titles_outlinks_ccc)
        conn.commit()

        print ('- Articles pointing at CCC: '+str(len(page_titles_outlinks_ccc)))
        print ('* get_articles_with_outlinks_ccc completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE num_outlinks IS NOT NULL;'
        cursor.execute(query)
        if cursor.fetchone()[0] == 0:
            page_titles_outlinks = []
            mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
            query = 'SELECT count(pl_title),pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 GROUP BY pl_from'
            mysql_cur_read.execute(query)
        #        page_asstring = ','.join( ['%s'] * len( all_ccc_articles ) )
        #        query = 'SELECT count(pl_title),pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 AND pl_from IN (%s) GROUP BY pl_from' % page_asstring
        #        mysql_cur_read.execute(query,set(all_ccc_articles.keys()))

        #        mysql_cur_read.execute('SELECT count(pl_title),pl_from FROM pagelinks WHERE pl_from_namespace=0 AND pl_namespace=0 GROUP BY pl_from')
            rows = mysql_cur_read.fetchall()
        #        print ('query run.')
            for row in rows:
                try:
                    count=row[0]
                    page_title = page_ids_page_titles[row[1]]
                    page_titles_outlinks.append((count,float(count),row[1],page_titles_qitems[page_title],page_title))
                except: continue
            print ('- Articles with any outlink at all: '+str(len(page_titles_outlinks)))

            query = 'UPDATE ccc_'+languagecode+'wiki SET num_outlinks=?,percent_outlinks_to_CCC=(num_outlinks_to_CCC/?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
            cursor.executemany(query,page_titles_outlinks)
            conn.commit()
            print ('* get_articles_with_outlinks completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    except:
        print ("MySQL has gone away. Let's try to do the joins in the code logics.")
        #input('')
       # get the ccc and potential ccc articles
        conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
        query = 'SELECT page_id, page_title, ccc_binary, qitem FROM ccc_'+languagecode+'wiki;'
        ccc_articles = {}
#        all_ccc_articles = {}
#        all_ccc_articles_qitems = {}
        for row in cursor2.execute(query):
            if row[2]==1: ccc_articles[row[0]]=row[1]
#            all_ccc_articles[row[0]]=row[1]
#            all_ccc_articles_qitems[row[0]]=row[3]
        print ('- Articles in CCC: '+str(len(ccc_articles)))
#        num_all_ccc_articles=len(all_ccc_articles)
#        print (str(num_all_ccc_articles))
        ccc_articles_page_titles = set(ccc_articles.values())

        page_titles_outlinks_ccc = []
        mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

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
                count=len(list(set(page_titles).intersection(ccc_articles_page_titles)))
                page_title=page_ids_page_titles[pl_from]
                page_titles_outlinks_ccc.append((count,len(page_titles),float(count)/float(len(page_titles)),pl_from,page_titles_qitems[page_title],page_title))
#                print (count,str(len(page_titles)),float(count)/float(len(page_titles)),all_ccc_articles[pl_from],pl_from,all_ccc_articles_qitems[pl_from])
                page_titles.clear()
            except:
                continue
        print ('Articles with outlinks: '+str(len(page_titles_outlinks_ccc)))

        query = 'UPDATE ccc_'+languagecode+'wiki SET (num_outlinks_to_CCC,num_outlinks,percent_outlinks_to_CCC)=(?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
        cursor2.executemany(query,page_titles_outlinks_ccc)
        conn2.commit()
        print ('* get_articles_with_outlinks_ccc Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# Get the articles with the number of affiliation-like properties to other articles already retrieved as CCC.
def get_articles_affiliation_properties_wd(languagecode,page_titles_page_ids):
    functionstartTime = time.time()
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()

    print ('\n* Get articles with Wikidata from items with "affiliation" properties for language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()

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

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()

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


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# ARTICLE CCC CLASSIFYING / SCORING FUNCTIONS
#############################################

# Filter: Deletes all the CCC selected qitems from a language which are geolocated but not among the geolocated articles to the territories associated to that language.
def filter_articles_geolocated_elsewhere(languagecode):
    functionstartTime = time.time()
    print ('\n* Filtering all the CCC selected qitems which are geolocated but not among the geolocated articles to the territories associated to the language: '+languages.loc[languagecode]['languagename']+' '+languagecode+'.')

    qitems={}
    conn = sqlite3.connect('wikidata.db'); cursor = conn.cursor()
    query = 'SELECT sitelinks.qitem, sitelinks.page_title FROM geolocated_property INNER JOIN sitelinks ON geolocated_property.qitem = sitelinks.qitem WHERE langcode = "'+languagecode+'wiki'+'";'
    for row in cursor.execute(query):
        qitems[row[0]]=row[1]
#    print ('The number of geolocated articles in the Italian Wikipedia is: '+str(len(qitems)))

    not_ccc=[]
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn2.cursor()
    query = 'SELECT qitem, page_id FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NOT 1;'
    for row in cursor2.execute(query):
        qitem = str(row[0])
        page_id = row[1]
        if qitem in qitems:
            not_ccc.append((qitem,page_id))

    print ('The number of articles that are going to be categorized as ccc binary = 0 is: '+str(len(not_ccc)))
    cursor2.executemany('UPDATE ccc_'+languagecode+'wiki SET ccc_binary=0 WHERE qitem = ? AND page_id = ?', not_ccc)

#    cursor2.executemany('DELETE FROM ccc_'+languagecode+'wiki WHERE qitem = ? AND page_id = ?', not_ccc)
    conn2.commit()
    print ('* filter_articles_geolocated_elsewhere Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

def get_training_data(languagecode):
    # OBTAIN THE DATA TO FIT.
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NOT NULL;'
    ccc_df = pd.read_sql_query(query, conn)

    ccc_df = ccc_df[['qitem','category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC','ccc_binary']]
    ccc_df = ccc_df.set_index(['qitem'])
#    print (ccc_df.head())
    if len(ccc_df.index.tolist())==0: print ('It is not possible to classify Wikipedia articles as there is no groundtruth.'); return (0,[],[])
    ccc_df = ccc_df.fillna(0)

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

    
    sampling_method = 'negative_sampling'

    if sampling_method == 'negative_sampling':
        ccc_df_yes = ccc_df.loc[ccc_df['ccc_binary'] == 1]
        ccc_df_yes = ccc_df_yes.drop(columns=['ccc_binary'])
        ccc_df_list_yes = ccc_df_yes.values.tolist()
        num_articles_ccc = len(ccc_df_list_yes)

        ccc_df_list_probably_no = []
        size_sample = 10
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

        sample = 5000 # the number samples per class
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
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
#    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NULL;' # ALL
    query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary IS NULL AND (category_crawling_territories IS NOT NULL OR category_crawling_level IS NOT NULL OR language_weak_wd IS NOT NULL OR affiliation_wd IS NOT NULL OR has_part_wd IS NOT NULL);' # POTENTIAL

    potential_ccc_df = pd.read_sql_query(query, conn)
    potential_ccc_df = potential_ccc_df[['page_title','category_crawling_territories','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC']]
    potential_ccc_df = potential_ccc_df.set_index(['page_title'])
    potential_ccc_df = potential_ccc_df.fillna(0)

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

    potential_ccc_df = potential_ccc_df.sample(frac=1) # randomize the rows order

    print ('We selected this number of potential CCC articles: '+str(len(potential_ccc_df)))

    return potential_ccc_df


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# Takes the ccc_score and decides whether it must be in ccc or not.
def calculate_articles_ccc_binary_classifier(languagecode,classifier,page_titles_page_ids,page_titles_qitems):
    functionstartTime = time.time()

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
    page_titles = potential_ccc_df.index.tolist()
    potential = potential_ccc_df.values.tolist()

    testdict = {}

    selected=[]
    result = clf.predict(potential)
    i = 0
    for x in result:
        testdict[page_titles[i]]=(x,potential[i])
        if x == 1:
            page_title=page_titles[i]
            selected.append((page_titles_page_ids[page_title],page_titles_qitems[page_title]))
        i += 1

#    print (testdict)
    print (len(selected))


#    print (potential)
    # provisional
#    input('')
#    print (potential[:15])
#    print (page_titles[:15])

#    count_yes=0
#    count_no=0
#    for n,i in enumerate(potential):
#        print (i)
#        result = clf.predict([i])
#        page_title=page_titles[n]
#        if result[0] == 1:
#            count_yes+=1
#            print (['category_crawling_paths','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC'])
#            print(i)
#            print(clf.predict_proba([i]).tolist())
#            print (str(count_yes)+'\tIN\t'+page_title+'.\n')

#            try: selected.append((page_titles_page_ids[page_title],page_titles_qitems[page_title]))
#            except: pass
#        else:
#            count_no+=1
#            print (['category_crawling_paths','category_crawling_level','language_weak_wd','affiliation_wd','has_part_wd','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_inlinks_from_CCC','percent_outlinks_to_CCC'])
#            print(i)
#            print(clf.predict_proba([i]).tolist())
#            print (str(count_no)+'\tOUT:\t'+page_title+'.\n')
#        input('')


    print ('Time predicting the testing data: '+str(datetime.timedelta(seconds=time.time() - functionstartTime)))
    print ('\nThis Wikipedia ('+languagecode+'wiki) has a total of '+str(wikipedialanguage_numberarticles[languagecode])+' articles.')
    print ('There were already '+str(num_articles_ccc)+' CCC articles selected as groundtruth. This is a: '+str(round(100*num_articles_ccc/wikipedialanguage_numberarticles[languagecode],3))+'% of the WP language edition.')
    print ('This algorithm CLASSIFIED '+str(len(selected))+' articles as ccc_binary = 1 from a total of '+str(len(potential))+' from the testing data. This is a: '+str(round(100*len(selected)/len(potential),3))+'%.')
    print ('With '+str(num_articles_ccc+len(selected))+' articles, the current and updated percentage of CCC is: '+str(round(100*(num_articles_ccc+len(selected))/wikipedialanguage_numberarticles[languagecode],3))+'% of the WP language edition.\n')

    evaluate_ccc_selection_manual_assessment(languagecode,selected,page_titles_page_ids)
#    input('')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    query = 'UPDATE ccc_'+languagecode+'wiki SET ccc_binary = 1 WHERE page_id = ? AND qitem = ?;'
    cursor.executemany(query,selected)
    conn.commit()

    print ('Language CCC '+(languagecode)+' created.')
    print ('* calculate_articles_ccc_binary_classifier Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def calculate_articles_ccc_main_territory(languagecode):
    functionstartTime = time.time()

    for i in range(1,4):
        print ('* iteration nº: '+str(i))
        # this function verifies the keywords associated territories, category crawling associated territories, and the wikidata associated territories in order to choose one.
        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

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

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_current.db'); cursor2 = conn.cursor()

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


# Calculates the top 100 vital articles list for each CCC and updates the database with their ranking position.
def calculate_articles_ccc_algorithm_vital_list(languagecode):
    functionstartTime = time.time()

    vital_list=ccc_df.index.tolist()
    parameters=[]
    pos=0
    for qitem in vital_list:
        pos=pos+1
        parameters.append((pos,qitem,ccc_df.loc[qitem]['page_id']))

    query = 'UPDATE ccc_'+languagecode+'wiki SET vital100_algorithm = ? WHERE qitem = ? AND page_id = ?;'
    cursor.executemany(query,parameters)
    conn.commit()

    print ('CCC vital articles list calculated.')
    print ('* calculate_articles_ccc_algorithm_vital_list Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def evaluate_ccc_selection_manual_assessment(languagecode, selected, page_titles_page_ids):
    print("Start the CCC selection manual assessment ")

    if selected is None:
        print ('Retrieving the CCC articles from the .db')
        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
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
        conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
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


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

### LISTS
# It returns three lists with 20 languages according to different criteria.
def obtain_proximity_wikipedia_languages_lists(languagecode):

    wikipedialanguage_numberarticles_sorted = sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)

    top19 = wikipedialanguage_numberarticles_sorted[:19]
    if languagecode in top19: top19.append(wikipedialanguage_numberarticles_sorted[20])
    else: top19.append(languagecode)

    i=wikipedialanguage_numberarticles_sorted.index(languagecode)
    upper9lower10 = wikipedialanguage_numberarticles_sorted[i-9:i]+wikipedialanguage_numberarticles_sorted[i:i+11]

    conn = sqlite3.connect('ccc_data.db'); cursor = conn.cursor()
    query = "SELECT languagecode_covering, percentage FROM ccc_gaps WHERE languagecode_covered='"+languagecode+"' AND group_type='ccc' AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_gaps WHERE group_type='ccc') AND reference='gap' ORDER BY percentage DESC";

    closest19 = []
    i = 1
    for row in cursor.execute(query):
#        print (row)
        closest19.append(row[0])
        if i==19: break
        i = i + 1

    wikipedia_proximity_lists = (top19, upper9lower10, closest19)
    return wikipedia_proximity_lists


# It returns a list of languages based on the region preference introduced.
def obtain_region_wikipedia_language_list(region, subregion, intermediateregion):
# use as: wikilanguagecodes = obtain_region_wikipedia_language_list('Asia', '', '').index.tolist()
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



### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 


### OUTPUT DATA TABLES

def create_table_ccc_allwiki():
    functionstartTime = time.time()

    # create the db
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()   
    cursor.execute("CREATE TABLE IF NOT EXISTS ccc_allwiki (languagecode text,  qitem text, PRIMARY KEY (languagecode, qitem))")
    for languagecode in wikilanguagecodes:
        query = 'INSERT INTO ccc_allwiki SELECT languagecode, qitem FROM ccc_'+languagecode+'wiki;'
        cursor.execute(query)
    conn.commit()

    print ('CCC vital articles list calculated.')
    print ('* calculate_articles_ccc_algorithm_vital_list Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# COMMAND LINE: sqlite3 -header -csv ccc_data.db "SELECT * FROM ccc_extent_language;" > ccc_extent_language.csv
def create_table_ccc_extent_by_language():
    functionstartTime = time.time()
    print ('* create_table_ccc_extent_by_language')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_data.db'); cursor2 = conn2.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists ccc_extent_language ('+
    'languagecode text , '+
    'languagename text, '+
    'wp_number_articles integer, '+

    'ccc_number_articles integer, '+
    'ccc_percent_wp real, '+

    'geolocated_number_articles integer, '+ 
    'geolocated_percent_wp real, '+ 
    'country_wd integer, '+
    'location_wd integer, '+
    'language_strong_wd integer, '+
    'created_by_wd integer, '+
    'part_of_wd integer, '+

    'keyword_title integer, '+
    'category_crawling_territories integer, '+
    'language_weak_wd integer, '+
    'affiliation_wd integer, '+
    'has_part_wd integer, '+

    'num_inlinks_from_CCC integer, '+
    'num_outlinks_to_CCC integer, '+
    'percent_inlinks_from_CCC real, '+
    'percent_outlinks_to_CCC real, '+

    'average_num_retrieval_strategies integer, '+
    'date_created text, '+

    'female integer, '+
    'male integer, '+
    'female_ccc integer, '+
    'male_ccc integer, '+
    'people_ccc_percent real, '+

    'measurement_date text, '+
    'PRIMARY KEY (languagecode, measurement_date));')
#    print (query)
    cursor2.execute(query)
    print ('\nTable ccc_extent_language has just been created in case it did not exist.')
    print ('Filling it with languages:')

    # We fill the database.
    for languagecode in wikilanguagecodes:
        print (languagecode)
        languagename = languages.loc[languagecode]['languagename']

        # wp number articles
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");

        # Obtaining CCC Qitems
        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query): qitems.append(row[0])
#        print (len(qitems))

        # Obtaining People
        conn3 = sqlite3.connect('wikidata.db'); cursor3 = conn3.cursor()
        male=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query):
            male.append(row[0])
        malecount=len(male)
        male = set(male).intersection(set(qitems))
        ccc_malecount=len(male)
#        print (malecount,ccc_malecount)

        female=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female = set(female).intersection(set(qitems))
        ccc_femalecount=len(female)
#        print (femalecount,ccc_femalecount)
        ccc_peoplecount=ccc_malecount+ccc_femalecount
        ccc_peoplecount_percent = round(100*ccc_peoplecount/wpnumberofarticles,2)

        # Obtaining CCC
        query = 'SELECT AVG(date_created), '
        query = query+'COUNT(ccc_binary), ' # FINAL CCC
        query = query+'COUNT(geocoordinates), COUNT(country_wd), COUNT(location_wd), COUNT(language_strong_wd), COUNT(created_by_wd), COUNT(part_of_wd), ' # CCC
        query = query+'COUNT(keyword_title), COUNT(category_crawling_territories), COUNT(language_weak_wd), ' # POTENTIAL CCC
        query = query+'COUNT(affiliation_wd), COUNT(has_part_wd), AVG(num_inlinks_from_CCC), AVG(num_outlinks_to_CCC), AVG(percent_inlinks_from_CCC), AVG(percent_outlinks_to_CCC), ' # RELATED QUALIFIERS
        query = query+'AVG(num_retrieval_strategies) '
        query = query+'FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;';
#        print (query)

        cursor.execute(query)
        row = cursor.fetchone()

        try: date_created = datetime.datetime.strptime(str(int(row[0]/1000)),'%Y%m%d%H%M%S').strftime('%Y')
        except: date_created = ''

        cccnumberofarticles = row[1];
        cccnumberofarticles_percent = round(100*cccnumberofarticles/wpnumberofarticles,2)

        geocoordinates = row[2]
        geocoordinates_percent = 100*geocoordinates/wpnumberofarticles

        country_wd = row[3]
        location_wd = row[4]
        language_strong_wd = row[5]
        created_by_wd = row[6]
        part_of_wd = row[7]
        keyword_title = row[8]
        category_crawling_territories = row[9]
        language_weak_wd = row[10]
        affiliation_wd = row[11]
        has_part_wd = row[12]

        num_inlinks_from_CCC = row[13]
        num_outlinks_to_CCC = row[14]
        percent_inlinks_from_CCC = row[15]
        percent_outlinks_to_CCC = row[16]
        average_num_retrieval_strategies = row[17]

        parameters = (languagecode, languagename, wpnumberofarticles, cccnumberofarticles, cccnumberofarticles_percent, geocoordinates, geocoordinates_percent, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC, percent_inlinks_from_CCC, percent_outlinks_to_CCC, average_num_retrieval_strategies, date_created, femalecount, malecount, ccc_femalecount, ccc_malecount, ccc_peoplecount_percent, measurement_date) #30
        print (parameters)

        param = 'languagecode, languagename, wp_number_articles, ccc_number_articles, ccc_percent_wp, geolocated_number_articles, geolocated_percent_wp, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC, percent_inlinks_from_CCC, percent_outlinks_to_CCC, average_num_retrieval_strategies, date_created, female, male, female_ccc, male_ccc, people_ccc_percent, measurement_date'
        query = 'INSERT INTO ccc_extent_language ('+param+') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
        cursor2.execute(query,parameters)
        conn2.commit()
    print ('* All languages CCC extent have been calculated and inserted.')
    print ('* create_table_ccc_extent_by_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# It creates a table to account for the number of articles obtained for each qitem and for each strategy.
# COMMAND LINE: sqlite3 -header -csv ccc_data.db "SELECT * FROM ccc_extent_qitem;" > ccc_extent_qitem.csv
def create_table_ccc_extent_by_qitem():
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_data.db'); cursor2 = conn2.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists ccc_extent_qitem ('+
    'languagecode text, '+
    'languagename text, '+
    'qitem text, '+
    'territory_name text, '+   
    'wp_number_articles integer, '+

    'ccc_articles_percent_wp real, '+
    'ccc_articles_qitem integer, '+
    'ccc_articles_qitem_percent_ccc real, '+

    'geolocated_number_articles integer, '+ 
    'geolocated_percent_wp real, '+ 
    'country_wd integer, '+
    'location_wd integer, '+
    'language_strong_wd integer, '+
    'created_by_wd integer, '+
    'part_of_wd integer, '+

    'keyword_title integer, '+
    'category_crawling_territories integer, '+
    'language_weak_wd real, '+
    'affiliation_wd real, '+
    'has_part_wd real, '+

    'num_inlinks_from_CCC integer, '+
    'num_outlinks_to_CCC integer, '+
    'percent_inlinks_from_CCC real, '+
    'percent_outlinks_to_CCC real, '+

    'average_num_retrieval_strategies real, '+
    'date_created text,'+
    'measurement_date text, '+
    'PRIMARY KEY (languagecode, qitem, measurement_date));')
    print (query)
    cursor2.execute(query)
    print ('\nTable ccc_extent_qitem has just been created in case it did not exist.')
    print ('Now filling it...')

    # We fill the database.
    for languagecode in wikilanguagecodes:
 
        print (languagecode)
        languagename=languages.loc[languagecode]['languagename']
         # wp number articles
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        measurementdate = datetime.datetime.utcnow().strftime("%Y%m%d");

        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        cursor.execute(query)
        num_ccc_articles = cursor.fetchone()[0]
        ccc_percent_wp = round(100*num_ccc_articles/wpnumberofarticles,2)


        query = 'SELECT AVG(date_created), '
        query = query+'COUNT(ccc_binary), ' # FINAL CCC
        query = query+'COUNT(geocoordinates), COUNT(country_wd), COUNT(location_wd), COUNT(language_strong_wd), COUNT(created_by_wd), COUNT(part_of_wd), ' # CCC
        query = query+'COUNT(keyword_title), COUNT(category_crawling_territories), COUNT(language_weak_wd), ' # POTENTIAL CCC
        query = query+'COUNT(affiliation_wd), COUNT(has_part_wd), AVG(num_inlinks_from_CCC), AVG(num_outlinks_to_CCC), AVG(percent_inlinks_from_CCC), AVG(percent_outlinks_to_CCC), ' # RELATED QUALIFIERS
        query = query+'main_territory, AVG(num_retrieval_strategies) '
        query = query+'FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 GROUP BY main_territory;';
#        print (query)

        for row in cursor.execute(query):
#            print (row)
#            input('')
            try: date_created = datetime.datetime.strptime(str(int(row[0]/10000000000)),'%Y').strftime('%Y')
            except: date_created = ''
            
            cccnumberofarticles = row[1]
            try: cccnumberofarticles_percent = round((100*cccnumberofarticles/num_ccc_articles),2)
            except: cccnumberofarticles_percent = 0

            qitem = row[17]
            if qitem != None:
                try: territory_name = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territoryname']
                except: print ('ERROR: There is a Qitem as main territory that is not main territory: '+str(qitem))
            else:
                territory_name = 'Unclassified'

            geocoordinates = row[2]
            geocoordinates_percent = 100*geocoordinates/wpnumberofarticles

            country_wd = row[3]
            location_wd = row[4]
            language_strong_wd = row[5]
            created_by_wd = row[6]
            part_of_wd = row[7]
            keyword_title = row[8]
            category_crawling_territories = row[9]
            language_weak_wd = row[10]
            affiliation_wd = row[11]
            has_part_wd = row[12]

            num_inlinks_from_CCC = row[13];
            if num_inlinks_from_CCC != None: num_inlinks_from_CCC = round(num_inlinks_from_CCC,2);

            num_outlinks_to_CCC = row[14]; 
            if num_outlinks_to_CCC != None: num_outlinks_to_CCC = round(num_outlinks_to_CCC,2);

            percent_inlinks_from_CCC = row[15]; 
            if percent_inlinks_from_CCC != None: percent_inlinks_from_CCC = round(percent_inlinks_from_CCC,2);

            percent_outlinks_to_CCC = row[16];
            if percent_outlinks_to_CCC != None: percent_outlinks_to_CCC = round(percent_outlinks_to_CCC,2); 

            average_num_retrieval_strategies = row[18]; 
            if average_num_retrieval_strategies != None: average_num_retrieval_strategies = round(average_num_retrieval_strategies,2)

            parameters = (languagecode, languagename, qitem, territory_name, wpnumberofarticles, ccc_percent_wp, cccnumberofarticles, cccnumberofarticles_percent, geocoordinates, geocoordinates_percent, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC, percent_inlinks_from_CCC, percent_outlinks_to_CCC, average_num_retrieval_strategies,date_created, measurementdate) #27

            param = 'languagecode, languagename, qitem, territory_name, wp_number_articles, ccc_articles_percent_wp, ccc_articles_qitem, ccc_articles_qitem_percent_ccc, geolocated_number_articles, geolocated_percent_wp, country_wd, location_wd, language_strong_wd, created_by_wd, part_of_wd, keyword_title, category_crawling_territories, language_weak_wd, affiliation_wd, has_part_wd, num_inlinks_from_CCC, num_outlinks_to_CCC, percent_inlinks_from_CCC, percent_outlinks_to_CCC, average_num_retrieval_strategies,date_created, measurement_date'

            query = 'INSERT INTO ccc_extent_qitem ('+param+') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
            cursor2.execute(query,parameters)
        conn2.commit()


# It creates a table for the culture gap total wp, keywords, geolocated, ccc, etc.
# COMMAND LINE: sqlite3 -header -csv ccc_data.db "SELECT * FROM ccc_gaps;" > ccc_gaps.csv
def create_table_ccc_gaps(): # això es podria fer amb Qitems / o amb llengües. -> pensar si guardar el nombre absolut d'articles també.

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('ccc_data.db'); cursor2 = conn2.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists ccc_gaps ('+
    'languagecode_covered text, '+
    'languagecode_covering text, '+
    'percentage real, '+
    'number_articles integer,'+
    'group_type text,'+
    'reference text, '+
    'measurement_date text, '+
    'PRIMARY KEY (languagecode_covered, languagecode_covering, group_type, reference, measurement_date));')
    cursor2.execute(query)
    print ('Table ccc_gaps has been created in case it did not exist.')
    measurementdate = datetime.datetime.utcnow().strftime("%Y%m%d");

    for languagecode in wikilanguagecodes:

#        languagecode = 'gn'
        # wp number articles
        print ('\n* Calculating the gaps for '+languagecode+'wiki.')
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        print ('This language has '+str(wpnumberofarticles)+' articles.')

        groups = ['wp','ccc','gl','kw','vital100_community','vital100_algorithm']
        for group_type in groups: # WE ARE TESTING. In the final version the tables should be without the _current.
            if group_type == 'ccc': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where ccc_binary = 1'
            if group_type == 'kw': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where keyword_title IS NOT NULL'
            if group_type == 'gl': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where geocoordinates IS NOT NULL'
            if group_type == 'vital100_community': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where vital100_community IS NOT NULL'
            if group_type == 'vital100_algorithm': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where vital100_algorithm IS NOT NULL'
            if group_type == 'wp': query = ''
            mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

            if query == '':
                mysql_cur_read.execute('SELECT ll_lang, COUNT(*) FROM page INNER JOIN langlinks ON ll_from=page_id WHERE page_is_redirect=0 AND page_namespace=0 GROUP BY 1 ORDER BY 2 DESC;')
                result = mysql_cur_read.fetchall()
                total_articles = wpnumberofarticles
#                print ('wp all articles.')

            if query != '':
                articles_pageids = []
                for row in conn.execute(query): 
                    articles_pageids.append(row[0])
                if len(articles_pageids)==0: continue
                total_articles=len(articles_pageids)
                page_asstring = ','.join( ['%s'] * len( articles_pageids ) )
                query = 'SELECT ll_lang, COUNT(*) FROM page INNER JOIN langlinks ON ll_from=page_id WHERE page_id IN (%s) AND page_is_redirect=0 AND page_namespace=0 GROUP BY 1 ORDER BY 2 DESC;' % page_asstring;
                mysql_cur_read.execute(query, (articles_pageids))
                result = mysql_cur_read.fetchall()
#                input('')

            parameters_gap = []
            parameters_share = []
            uniquelanglist = []
            for row in result:
#                print (row)
                languagecode_covered = languagecode

                languagecode_covering = str(row[0].decode('utf-8'))
                languagecode_covering = languagecode_covering.replace('-','_')
                if languagecode_covering == 'be_tarask': languagecode_covering = languagecode_covering.replace('be_tarask','be_x_old')
                if languagecode_covering == 'nan': languagecode_covering = languagecode_covering.replace('nan','zh_min_nan')
                if languagecode_covering not in wikipedialanguage_numberarticles: print (languagecode_covering+' language has not been included yet in WCDO or is deprecated.'); continue

                if languagecode_covering not in uniquelanglist: uniquelanglist.append(languagecode_covering);
                else: continue

                number_articles = row[1]
                percentage = round(100*number_articles/total_articles,3)
                parameters_gap.append((languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'gap',measurementdate))

#                print((languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'gap',measurementdate))
#                cursor2.execute('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,group_type,reference,measurement_date) VALUES (?,?,?,?,?,?,?)',(languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'gap',measurementdate))

                percentage = round(100*number_articles/wikipedialanguage_numberarticles[languagecode_covering],3)
                parameters_share.append((languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'share',measurementdate))

            print ('inserting...'+group_type+'.')
            cursor2.executemany('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,group_type,reference,measurement_date) VALUES (?,?,?,?,?,?,?)', parameters_gap)
            conn2.commit()
            cursor2.executemany('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,group_type,reference,measurement_date) VALUES (?,?,?,?,?,?,?)', parameters_share)
            conn2.commit()

    print ('All languages CCC gap have been calculated and inserted.')


# It creates a table to account for the number of articles created for each language to bridge other CCC during the last fifteen days (i.e. How much effort do editors dedicate to bridging CCC from other languages?).
def create_table_ccc_bridging_last15days(languagecode):

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect('ccc_data.db'); cursor3 = conn3.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists ccc_bridges ('+
    'languagecode_covered text, '+
    'languagecode_covering text, '+
    'percentage_workload real, '+
    'number_articles integer,'+
    'measurement_date text'+
    'PRIMARY KEY (languagecode_covered, languagecode_covering, measurement_date)')
    cursor2.execute(query)
    print ('Table ccc_gaps has been created in case it did not exist.')
    measurementdate = datetime.datetime.utcnow().strftime("%Y%m%d");

    for language_covering in wikilanguagecodes:
        parameters = []

        mysql_con_read = establish_mysql_connection_read(language_covering); mysql_cur_read = mysql_con_read.cursor()
        page_titles=[]
        query = "SELECT page_title FROM revision INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 AND rev_timestamp < "+ (datetime.date.today() - datetime.timedelta(days=14)).strftime('%Y%m%d%H%M%S') # this is a week ago.
        result = mysql_cur_read.fetchall()
        for row in result: page_titles.append(str(row[0].decode('utf-8')).replace('_',' '))

        qitems=[]
        query = 'SELECT qitem FROM sitelinks WHERE sitelinks.langcode ="'+language_covering+'wiki" AND page_title in (%s)'
        for row in conn2.execute(query,page_titles): qitems.append(row[1].replace(' ','_'))

        for language_covered in wikilanguagecodes:
            page_asstring = ','.join( ['?'] * len( qitems ) )
            query = 'SELECT count(*) FROM ccc_'+language_covered+'wiki where ccc_binary = 1 AND qitem IN (%s) GROUP BY qitem' % page_asstring
            cursor.execute(query,qitems)
            number_articles = cursor.fetchone()[0]
            percentage_workload=round(100*number_articles/len(page_titles),3)
            parameters.append((language_covering,language_covered,percentage_workload,number_articles,measurementdate))

        cursor3.executemany('INSERT INTO ccc_bridges (languagecode_covering,languagecode_covered,percentage_workload,number_articles,measurement_date) VALUES (?,?,?,?,?)', parameters)
        conn3.commit()
    print ('All languages CCC gap have been calculated and inserted.')




# It creates a table for the topical coverage of each Wikipedia language edition.
# hauria de fer l'alineament de categories disponible i consultar-lo per si algú em pot dir que està malament?
def create_table_ccc_topical_coverage(lang): # AIXÒ NO HAURIA D'ANAR AQUÍ
# STEPS
# 1. llegir de meta la taula de categories per cada llengua / llegir de l'arxiu la taula de categories per cada llengua.
# 2. loop de llengües per totes les llengües.
# obrir ccc_current.db.
# crear taules topics_langcodewiki per posar-hi els articles.

# 3. iterar llengua a llengua per anar assignar les categories a temes i introduïr articles.
# 4. crear o utilitzar topical.db i crear la taula de resultats per ser utilitzats per bokeh després.

    output_iso_name = data_dir + 'source/cira_topicalcoverage.csv'
    output_file_iso = codecs.open(output_iso_name, 'w', 'UTF-8')
    # versió de Kittur topic groups list: “Culture and the arts; People and self; Geography and places; Society and social sciences; History and events; Natural and pysical sciences; Technology and applied sciences; Religion and belief systems; Health and fitness; Mathematics and logic; Philosohpy and thinkinking."
    # versió del David Laniado ['Agricultura','Biografies','Ciència','Ciències_naturals', 'Ciències_socials','Cultura','Educació','Empresa','Esdeveniments','Esports','Geografia','Història','Humanitats','Matemàtiques','Medi_Ambient','Lleis','Política','Societat','Tecnologia'];
    if lang=='cawiki': wordlist = ['Agricultura','Biografies','Ciència','Ciències_socials','Cultura','Dret','Educació','Empresa','Esdeveniments','Esports','Geografia','Matemàtiques','Medi_ambient','Política','Religió','Salut_i_benestar_social','Societat','Tecnologia'];

    print ('El número de categories de la llengua base '+ lang +' és de: '+str(len(wordlist))+'\n')
    output_file_iso.write(lang+'\t')
    for category in wordlist:
        output_file_iso.write(category+'\t')
    output_file_iso.write('\n')

    lang_llista = []
    input_iso_file = data_dir + 'source/cira_iso3166.csv'
    input_iso_file = open(input_iso_file, 'r')
    for line in input_iso_file:
        page_data = line.strip('\n').split(',')
        lang_llista.append(page_data[0])

    categorypattern = re.compile(r'(.+:+)(.*)')
    for llengua in lang_llista:
        if llengua == lang: continue
        query = 'SELECT DISTINCT(ll_title) FROM '+lang+'_p.page pa INNER JOIN '+lang+'_p.langlinks la ON pa.page_id=la.ll_from WHERE page_namespace=14 AND pa.page_title IN (%s)'
        query = query + ' AND ll_lang="'+llengua[0:len(llengua)-4]+'"'

        page_asstring = ','.join( ['%s'] * len( wordlist ) )
    #                print query

        output_file_iso.write(llengua)
        mysql_cur.execute(query % page_asstring, tuple(wordlist))
        result = mysql_cur.fetchall()

        print ('La següent llengua té aquest nombre de categories '+llengua+': '+str(len(result)))
        for row in result:
            title = str(row[0])
            titleC = categorypattern.match(title)
            titledef = titleC.group(2)
            titledef = titledef.replace(' ', '_')
    #                    print titledef
            output_file_iso.write('\t'+titledef)
        output_file_iso.write('\n')

        mysql_con = mdb.connect(lang + '.labsdb', 'p50380g50517', 'aiyiangahthiefay', lang + '_p')

    #    mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
        mysql_con.set_character_set('utf8')
        with mysql_con:
            mysql_cur = mysql_con.cursor()

    #       dataset_name = 'CIRA_articles'
            dataset_name = 'WP_articles'
    #       dataset_name = 'CIRA_coords_def'

    # DATASET
        input_dataset = data_dir + lang +'_'+ dataset_name +'.csv'
        input_file_for_CIRA_def = open(input_dataset, 'r')
        page_titles_cira_def = {}

        # fer que si es queda a mitges el topical coverage que revisi l'arxiu creat i faci un pop als que ja s'han fet.
        # tanqui l'arxiu creat (com a input) i afegeixi a l'arxiu (a) tot allò nou que es generi.


        for line in input_file_for_CIRA_def: # dataset
            page_data = line.strip('\n').split('\t')
    #        page_data = line.strip('\n').split(',')
            page_id = str(page_data[1])
            page_title = page_data[0]
            page_title = urllib.unquote(page_title).decode('utf8')
            page_title=str(page_title)
            page_titles_cira_def[page_title] = page_id

        print ('En aquesta llengua hi tenim tants articles a '+dataset_name+': '+str(len(page_titles_cira_def)))


        input_file_name_already = data_dir + lang+'_'+dataset_name+'_articles_topics.csv'
        if os.path.isfile(input_file_name_already):
            page_titles_already = []
            input_file_already = open(input_file_name_already, 'r')
            input_file_already.readline()
            for line in input_file_already:
                page_data = line.strip('\n').split('\t')
                page_title = page_data[0]
 #                page_title = urllib.unquote(page_title).decode('utf8')
                page_title=str(page_title)
                page_titles_already.append(page_title)
            print ('Tenim tants articles ja amb topics: '+str(len(page_titles_already)))

            for title in page_titles_already:
                try:
                    page_titles_cira_def.pop(title)
                except:
                    print (title)

            print ('Tenim tants articles per fer encara: '+str(len(page_titles_cira_def)))


        output_file_name = data_dir + lang+'_'+dataset_name+'_articles_topics.csv'
        output_file = codecs.open(output_file_name, 'a', 'UTF-8')

        # CATEGORIES INVISIBLES: TREURE-LES.
        mysql_cur.execute("SELECT page_title FROM page p, page_props pro WHERE pro.pp_page=p.page_id AND pro.pp_propname LIKE 'hiddencat'")
        rows = mysql_cur.fetchall()
        hiddencategorylist = []
        for row in rows:
            valor = str(row[0])
            hiddencategorylist.append(valor)

        input_file_categories = data_dir + '/source/cira_topicalcoverage.csv'
        input_file_topic = open(input_file_categories, 'r')
        wordlist = []
        for line in input_file_topic: # CIRA DEF
            page_data = line.strip('\n').split('\t')
            if page_data[0]==lang:
                for item in page_data:
                    wordlist.append(item)
        wordlist.pop(0)
        print (wordlist)

        mothercategories_finalvalues = {}
        for i in wordlist: mothercategories_finalvalues[i]=0
        print (mothercategories_finalvalues)

        numarticle = 0
        for article_title in page_titles_cira_def.keys():
            numarticle = numarticle + 1
            print ("Anem per l'article número: "+str(numarticle)+' amb nom: '+article_title)

            articlecategorieslist = []
            mysql_cur.execute('SELECT cl_to FROM categorylinks WHERE cl_from=%s',(page_titles_cira_def[article_title],))
            rows = mysql_cur.fetchall()
            for row in rows:articlecategorieslist.append(str(row[0]))
            ocultesperarticle = list(set(articlecategorieslist).intersection(set(hiddencategorylist)))
            for oculta in ocultesperarticle: articlecategorieslist.remove(oculta)
            # aquí ja tenim les categories per cada article

            mothercategories_forarticle = {}
            for i in wordlist: mothercategories_forarticle[i]=0
#            print '# El nom de l\'article per veure és: '+article_title
#            print 'Les seves categories són: '
#            print articlecategorieslist

            #raw_input("\nPress Enter to continue...\n")
            for categoria in articlecategorieslist: # treballarem amb un arbre de categories a l'article base
                # print '\nLa categoria per pujar per arbre és: '+categoria
                categoriesjavistes = []
                categoriesjavistes = categoriesjavistes + hiddencategorylist
                current_categories = []
                current_categories.append(categoria)

                mothercategories_forcategory = {}
                salts = 0
                while current_categories: # que quedi alguna categoria a les 'future categories' que no s'hagi buscat abans.
                    # print '\n\n\n\n\n''ja vistes:'
                    # print categoriesjavistes
                    salts = salts + 1
                    page_asstring = ','.join( ['%s'] * len( current_categories ) )

                    mysql_cur.execute('SELECT cl_to FROM categorylinks INNER JOIN page ON cl_from=page_id WHERE page_namespace=14 AND page_title IN (%s)' % page_asstring, tuple(current_categories))
                    rows = mysql_cur.fetchall()
                    current_categories = []

                    for row in rows:
                        valor = str(row[0])
                        current_categories.append(valor)

    #               print '\n*** '+str(salts)+' Salt: '+'abans de treure les ja vistes:'
    #                print current_categories
                    current_categories = list(set(current_categories))

                    for element in list(set(current_categories).intersection(set(categoriesjavistes))):
    #                    print 'element coincident amb les ja vistes: '+ element
                        current_categories.remove(element)
    #                print 'després de treure les ja vistes:'
    #                print current_categories

                    # CONDICIONS
                    coincident = list(set(current_categories).intersection(set(mothercategories_finalvalues.keys())))
                    for a in coincident:
                        if a not in mothercategories_forcategory: mothercategories_forcategory[a]=salts # els salts a les categories mare que hem trobat per cada categoria a l'arrel-base de l'article.
                        # print "BINGOOOOOOO!!! La categoria mare amb la que es relaciona l\'article a partir de la seva categoria: "+categoria+' és: '+a+' amb aquest nombre de salts: '+str(salts)
                        current_categories.remove(a)
                    if len(coincident)!=0: break

                    categoriesjavistes = categoriesjavistes + current_categories

#                print 'per aquesta categoria '+categoria+' tenim que vincula amb les següents categories mare:'
#                print mothercategories_forcategory

    #            raw_input("\nPress Enter to continue...\n")

                if mothercategories_forcategory:
                    minkey = min(mothercategories_forcategory.iterkeys(), key=lambda k: mothercategories_forcategory[k])
                    minvalue = mothercategories_forcategory[minkey]
#                    print str(minkey)+';'+str(minvalue)
                    assignacions = []

                    for cat in mothercategories_forcategory.keys():
                        # print cat + ' amb el valor: '+ str(mothercategories_forcategory[cat])
                        if float(mothercategories_forcategory[cat]) == float(minvalue):
                            assignacions.append(cat)

                    numassig = len(assignacions)
#                    print 'el número d\'assignacions és: '+str(numassig)
                    repartiment=(float(1)/len(articlecategorieslist))/numassig
#                    print repartiment
                    for cat in mothercategories_forcategory.keys():
                        if mothercategories_forcategory[cat]==minvalue:
                            mothercategories_forarticle[cat]=mothercategories_forarticle[cat]+ repartiment
#                            print cat+' : '+str(repartiment)

    #            raw_input("\nPress Enter to continue...\n")

#            print 'per aquest article '+article_title+' tenim la següent distribució de valors per categories mare sense normalitzar: '
#            print mothercategories_forarticle

    #       normalització a nivell d'article
            totalvalor = sum(mothercategories_forarticle.values())
#            print 'valor total de l\'article amb categories: '+str(totalvalor)
            if totalvalor == 0: continue
            for cat in mothercategories_forarticle.keys():
                mothercategories_forarticle[cat] = float(mothercategories_forarticle[cat])/totalvalor

#            print 'per aquest article '+article_title+' tenim la següent distribució de valors per categories mare després de normalitzar: '
#            print mothercategories_forarticle

#            print 'això hauria de sumar 1: '+str(sum(mothercategories_forarticle.values()))

            string = ''
            for catvalue in wordlist:
                string = string + '\t'+catvalue+'\t'+str(mothercategories_forarticle[catvalue])
#            print 'aquest article té els següents valors: '+article_title+string+'\n'

#            raw_input('ESPERA!')
            output_file.write(article_title+string+'\n')

#            raw_input('ESPERA: SEGÜENT ARTICLE.')
    #        abocar a mothercategories_finalvalues
            for cat in mothercategories_forarticle.keys():
                mothercategories_finalvalues[cat]=mothercategories_finalvalues[cat]+mothercategories_forarticle[cat]
            # print 'portem un acumulat TOTAL de valors per les categories mare:'
            # print mothercategories_finalvalues

        output_file.close()
        sentence = lang +'\t'

    #   traiem els valors finals dels percentatges de les categories mare pel grup d'articles
        totalvalorgrup = sum(mothercategories_finalvalues.values())
        for cat in wordlist:
            mothercategories_finalvalues[cat]=str(100*round(float(mothercategories_finalvalues[cat])/totalvalorgrup,5))
            sentence = sentence + cat+'\t'+str(mothercategories_finalvalues[cat])+'\t'

        sentence = sentence + '\n'
        output_stats_file_topic = data_dir + "results/"+'_'+dataset_name+"_topical_coverage.csv"
        output_stats_topic = codecs.open(output_stats_file_topic, 'a', 'UTF-8')
        output_stats_topic.write(sentence)
        print (sentence)
        output_stats_topic.close()

   

def create_table_ccc_bridging_lists(count_covered_articles, languagecode_covered, languagecode_covering, percentage, list_name):

    # It creates a table for all languages.
    query = ('CREATE table if not exists vital_articles_lists ('+
    'languagecode_covered text, '+
    'languagecode_covering text, '+
    'percentage real, '+
    'number_articles integer, '+
    'measurement_date text, '+
    'list_name text, '+
    'PRIMARY KEY (languagecode_covered, languagecode_covering, group_type, reference, measurement_date));')
    cursor.execute(query)
    print ('Table vital_articles_lists has been created in case it did not exist.')
    measurementdate = datetime.datetime.utcnow().strftime("%Y%m%d");

    parameters = (languagecode_covering,languagecode_covered,percentage,count_covered_articles,list_name,measurementdate)
    print ('inserting...'+list_name+'.')
    cursor.execute('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,list_name,measurement_date) VALUES (?,?,?,?,?,?)', parameters)




### GENERATE TABLES VISUALIZATIONS.
#   ALL LANGUAGES
#   QUESTION: What is the extent of cultural context content in each language edition?
def generate_ccc_extent_all_languages_table(wiki_path):

    print ('* generate_ccc_extent_all_languages_table')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('ccc_data.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_extent_language WHERE measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_language) ORDER BY ccc_percent_wp DESC;'
    print (query)
    df = pd.read_sql_query(query, conn)
    df = df.set_index(['languagecode'])
    measurement_date = df.loc[df.index.values[0]]['measurement_date']

    rank_dict={}
    i=1
    for x in list(df.index.values):
        rank_dict[x]=i
        i=i+1
    df['Nº'] = pd.Series(rank_dict)

    df['Region']=languages.region
    df['Subregion']=languages.subregion

    WPlanguagearticle={}
    for x in df.index.values:
        WPlanguagearticle[x]='<a href="'+languages.loc[x]['WikipedialanguagearticleEnglish']+'">'+x+'</a>'
    df['Wiki'] = pd.Series(WPlanguagearticle)

    languagelink={}
    for x in df.index.values:
        languagelink[x]='<a href="https://'+x+'.wikipedia.org/wiki/">'+df.loc[x]['languagename']+'</a>'
    df['Language'] = pd.Series(languagelink)

    # Renaming the columns
    columns_dict = {'wp_number_articles':'WP','ccc_number_articles':'CCC','ccc_percent_wp':'CCC %','geolocated_number_articles':'CCC Geolocated','country_wd':'Country WD','location_wd':'Location WD', 'language_strong_wd':'Language Strong WD', 'created_by_wd':'Created by WD', 'part_of_wd':'Part of WD', 'keyword_title':'Keywords Title', 'category_crawling_territories':'Category Crawling Territories', 'language_weak_wd':'Language Weak WD', 'affiliation_wd':'Affiliation WD', 'has_part_wd':'Has Part WD', 'num_inlinks_from_CCC':'Inlinks from CCC','num_outlinks_to_CCC':'Outlinks from CCC','percent_inlinks_from_CCC':'Inlinks from CCC %', 'percent_outlinks_to_CCC':'Outlinks from CCC%', 'average_num_retrieval_strategies':'Avg. Num Retrieval Strategies', 'date_created':'Avg. Date Created', 'female':'Num. Female','male':'Num. Male', 'female_ccc':'Num. Female CCC','male_ccc':'Num. Male CCC','people_ccc_percent':'People CCC %','measurement_date':'Measurement Date'}

#    df = df.set_index(['Nº'])
    df=df.rename(columns=columns_dict)

#    # Choosing the final columns
    columns = ['Nº','Language','Wiki','WP','CCC','CCC %','Avg. Date Created','CCC Geolocated','Keywords Title','Num. Male CCC','Num. Female CCC','People CCC %','Subregion','Region']
    df = df[columns] # selecting the parameters to export

    os.makedirs(current_site_path, exist_ok=True)
    filename = 'ccc_extent_all_languages_table'+'.html'
    file_path = current_site_path + wiki_path + filename
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)

    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)

    pd.set_option('display.max_colwidth', old_width)

    print ('* dataframe and html file created.\n')

    # PENDING:
    #    em caldria complementar la funció table_ccc_extent_by_language per posar-hi la mitjana de % de compleció d'altres llengües, és a dir, el culture gap index.
    #    això ho puc saber mirant tots els articles que tinc que pertanyen a CCC dels altres entre tots els articles de CCC dels altres.

    return wikitext



#   QUESTION: What is the extent of Cultural Context Content in all language editions?
def generate_ccc_extent_visualization(languagecode, languagelist, wiki_path):
    print ('')
    # http://whgi.wmflabs.org/gender-by-language.html
    # semblant a aquesta.
#    gràfic de boles.
    location_png = '1'

    p.xaxis.axis_label = 'Percentage female biographies'
    p.yaxis.axis_label = 'Total biographies'
    p.yaxis[0].formatter = NumeralTickFormatter(format='0,0')

    source = ColumnDataSource(data=cutoff_plot.to_dict(orient='list'))
    p.circle('fem_per', 'total', size=12, line_color="black", fill_alpha=0.8,
             source=source)

    # label text showing language name
    p.text(x="fem_per", y="total", text="index", text_color="#333333",
           text_align="left", text_font_size="8pt",
           y_offset=-5, source=source)

    filename='ccc_extent_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path

    # agregació per REGIONS TAMBÉ!!! DOS GRÀFICS.



#   QUESTION: What is the extent of Cultural Context Content in the articles created during the last month?
def generate_ccc_creation_visualization_monthly(languageslist, languagelist, wiki_path):
    print ('')

    file_path = '1'

#    gràfic de boles.
#    pels grups seleccionats. top 20.

#    An on-going presentation of different visualizations of the CCC creation map in each language edition.
#    Presentació: De manera temporal (aquesta setmana), el nombre d'articles de CCC creats en absolut i relatiu per cada llengua. Gràfic amb totes les llengües.
#    Dades: Cal crear scripts que mitjançant els articles creats a cada setmana computin els resultats i els emmagatzemin els resultats en bases de dades.
#    Gràfic:
#    Puc utilitzar:     http://whgi.wmflabs.org/gender-by-language.html       https://bokeh.pydata.org/en/latest/docs/gallery/elements.html
#    Interfície:     Per c caldria pensar en el selector d'idioma.

#    LA LLENGUA EN QÜESTIÓ QUE PASSEM, LA COMPARAREM EN TRES GRÀFICS AMB:
#        grup de la mateixa mida
#        proximitat llengües
#        llengua contra les 20 primeres
    
    filename='ccc_extent_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: What is the composition of the entire Wikipedia project in terms of each language Cultural Context Content?
def generate_ccc_all_languages_composition(wiki_path):

    file_path = '1'
#    Cultures in Own Language. Formatge.
#    Calcular a la vegada el què ocupen a la pròpia WP... és a dir... amb la intenció de fer un formatge de com està compost d'altres cultures.
#    També es podria fer una 'topical coverage de cultures' de TOTES les viquipèdies a la vegada.
#    formatge/barres. la suma de share de ccc_gaps.

    # també es podria fer un context coverage de wikidata.

    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: How well each language edition covers the CCC of each other language edition?
def generate_ccc_culture_gap_table(wiki_path):
    # taula amb les cinc primeres llengües.
    wikitext=''
    html=1

    file_path = current_site_path + wiki_path + filename
    print (file_path)

    return wikitext


#   QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
def generate_ccc_culture_gap_index_visualization_monthly(languagecode, languageslist, wiki_path):
    print ('')

    # DIAGRAMA DE LÍNIES TEMPORAL. EVOLUCIÓ DEL PERCENTATGE.

    output_file("line.html")

    p = figure(plot_width=400, plot_height=400)

    # add a line renderer
    p.line([1, 2, 3, 4, 5], [6, 7, 2, 4, 5], line_width=2)
    show(p)

    filename = '1'
    # diagrama de línies per veure com estan cobrint el culture gap challenge les diferents llengües.
    # dos diagrames: la comunitat i l'algoritme.
    """
    LA LLENGUA EN QÜESTIÓ QUE PASSEM, LA COMPARAREM EN TRES GRÀFICS AMB:
    grup de la mateixa mida
    proximitat llengües
    llengua contra les 20 primeres
    """
    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
def generate_ccc_bridging_culture_gap_visualization_monthly(languagecode, wiki_path):
#    % per cada llengua.

    filename = languagecode+'_'+'ccc_bridging_culture_gap_visualization_monthly'+'.png'

#    centrat en les top 50.
#    % totes les llengües agregades.

#    An on-going presentation of different visualizations of the CCC bridging map in each language edition as articles that bridge the culture gap are created.
#    Presentació: De manera temporal (aquesta setmana), per una llengua a seleccionar, el nombre d'articles creats de CCC d'altres llengües. Gràfic per cada llengua.
#    Bridger.Dades: Cal crear scripts que mitjançant els articles creats a cada setmana computin els resultats i els emmagatzemin els resultats en bases de dades.
#    Gràfic: Puc utilitzar:
#    http://whgi.wmflabs.org/gender-by-language.html
#    https://bokeh.pydata.org/en/latest/docs/gallery/elements.html

#    LA LLENGUA EN QÜESTIÓ QUE PASSEM, LA COMPARAREM EN TRES GRÀFICS AMB: grup de la mateixa mida
#    proximitat llengües
#        llengua contra les 20 primeres
    file_path = current_site_path + wiki_path + filename

    return file_path



#   LANGUAGE BY LANGUAGE
    # project site page WCDO for each language
#   QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?
def generate_ccc_extent_qitem_table_by_language(languagecode, page_titles_qitems, qitems_page_titles_english, wiki_path):

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('ccc_data.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_extent_qitem WHERE measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem) AND languagecode = "'+languagecode+'" ORDER BY ccc_articles_qitem_percent_ccc DESC;'
#    print (query)
    df = pd.read_sql_query(query, conn)
    df = df.set_index(['qitem'])
    print ('This the number of territories for language '+languagecode+': '+str(len(df)))
    if len(df)==0: return

    measurement_date = df.loc[df.index.values[0]]['measurement_date']

    territories_adapted=territories.loc[languagecode]
    territories_adapted=territories.drop(territories[territories.index!=languagecode].index)
    territories_adapted=territories_adapted.set_index(['QitemTerritory'])
    territories_adapted=territories_adapted.rename(index={'QitemTerritory':'qitem'})

#    df['Territory Name (local)']=territories_adapted.territorynameNative
#    df['Territory Name']=territories_adapted.territoryname
    df['country']=territories_adapted.country
    df['ISO3166']=territories_adapted.ISO3166
    df['ISO3166-2']=territories_adapted.ISO31662
    df['Subregion']=territories_adapted.subregion
    df['Region']=territories_adapted.region

    territoryname={}
    for x in df.index.values:
        if x != None and x in qitems_page_titles_english: territoryname[x]='<a href="https://en.wikipedia.org/wiki/'+qitems_page_titles_english[x]+'">'+qitems_page_titles_english[x].replace('_',' ')+'</a>'
        elif x != None: territoryname[x]=territories.loc[territories['QitemTerritory'] == x].loc[languagecode]['territoryname']
        else: territoryname[x]='Not Assigned'
    df['Territory Name'] = pd.Series(territoryname)

    territoryname_local={}
    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
    for x in df.index.values:
        if x != None and x in qitems_page_titles: territoryname_local[x]='<a href="https://'+languagecode+'.wikipedia.org/wiki/'+qitems_page_titles[x]+'">'+qitems_page_titles[x].replace('_',' ')+'</a>'
        elif x != None: territoryname[x]=territories.loc[territories['QitemTerritory'] == x].loc[languagecode]['territorynameNative']
        else: territoryname_local[x]='Not Assigned'
    df['Territory Name (local)'] = pd.Series(territoryname_local)

    rank_dict={}
    i=1
    for x in list(df.index.values):
        rank_dict[x]=i
        i=i+1
    df['Nº'] = pd.Series(rank_dict)

    languagelink={}
    for x in df.index.values:
        if x != None: languagelink[x]='<a href="https://www.wikidata.org/wiki/'+x+'">'+x+'</a>'
        else: languagelink[x]=''
    df['Qitem'] = pd.Series(languagelink)

    # Renaming the columns
    columns_dict = {'wp_number_articles':'WP','ccc_articles_qitem':'CCC','ccc_articles_qitem_percent_ccc':'CCC %','geolocated_number_articles':'CCC Geolocated','country_wd':'Country WD','location_wd':'Location WD', 'language_strong_wd':'Language Strong WD', 'created_by_wd':'Created by WD', 'part_of_wd':'Part of WD', 'keyword_title':'Keywords Title', 'category_crawling_territories':'Category Crawling Territories', 'language_weak_wd':'Language Weak WD', 'affiliation_wd':'Affiliation WD', 'has_part_wd':'Has Part WD', 'num_inlinks_from_CCC':'Inlinks from CCC','num_outlinks_to_CCC':'Outlinks from CCC','percent_inlinks_from_CCC':'Avg. Percent Incoming Links from CCC', 'percent_outlinks_to_CCC':'Outlinks from CCC%', 'average_num_retrieval_strategies':'Avg. Num. Retrieval Strategies', 'date_created':'Avg. Date Created','measurement_date':'Measurement Date'}

#    df = df.set_index(['Nº'])
    df=df.rename(columns=columns_dict)

#    # Choosing the final columns
    columns = ['Nº','Territory Name','Territory Name (local)','Qitem','WP','CCC','CCC %','Avg. Date Created','Avg. Percent Incoming Links from CCC','CCC Geolocated','Keywords Title','Avg. Num. Retrieval Strategies','country','ISO3166','ISO3166-2','Subregion','Region']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')

    wikitext = '' # THIS NEEDS TO BE FILLED.

    os.makedirs(current_site_path + wiki_path, exist_ok=True)
    filename = languagecode +'_'+'ccc_extent_qitem_table_by_language_' + measurement_date + '.html'
    file_path = current_site_path + wiki_path + filename
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)

    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)

    pd.set_option('display.max_colwidth', old_width)

    print ('* dataframe and html file created.\n')

    return wikitext


#   QUESTION: What is the topical coverage of the articles of this Wikipedia language edition?   
def generate_ccc_topical_coverage_visualization(content_type, languagecode, wiki_path):
    if content_type == 'ccc':
        print (content_type)
    else:
        print (content_type)

    print ('')
    location_png =''

    fruits = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
    years = ["2015", "2016", "2017"]
    colors = ["#c9d9d3", "#718dbf", "#e84d60"]

    data = {'fruits' : fruits,
            '2015'   : [2, 1, 4, 3, 2, 4],
            '2016'   : [5, 3, 4, 2, 4, 6],
            '2017'   : [3, 2, 4, 4, 5, 3]}

    source = ColumnDataSource(data=data)

    p = figure(x_range=fruits, plot_height=350, title="Fruit Counts by Year",
               toolbar_location=None, tools="")

    renderers = p.vbar_stack(years, x='fruits', width=0.9, color=colors, source=source,
                             legend=[value(x) for x in years], name=years)

    for r in renderers:
        year = r.name
        hover = HoverTool(tooltips=[
            ("%s total" % year, "@%s" % year),
            ("index", "$index")
        ], renderers=[r])
        p.add_tools(hover)

    p.y_range.start = 0
    p.x_range.range_padding = 0.1
    p.xgrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"

    show(p)
#    T’ensenyaria graficament la distribució temàtica del CCC de la llengua seleccionada (i també d’altres).
#    Dades: Base de dades acumulat de CCC. 
#    Caldrà posar dades de les temàtiques a la base de dades.
#    Gràfic:     https://bokeh.pydata.org/en/latest/docs/gallery/bar_stacked.html
    filename = languagecode+'_'+content_type+'_topical_coverage_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: Which are the geolocated articles with most inlinks, pageviews, etc.?
#   QUESTION: How well does this language covers other language editions geolocated articles?
def generate_ccc_geolocated_articles_map_visualization(languagecode, wiki_path):
    print ('')

#    A monthly updated visual representation of the CCC geographical articles. (OPCIONAL-VALORABLE)
#    Presentació: T’ensenyaria gràficament el nombre d’articles per territori.
#    Dades: Base de dades acumulat de CCC amb geolocalització. 

#    https://developers.arcgis.com/python/sample-notebooks/html-table-to-pandas-data-frame-to-portal-item/
#    https://github.com/python-visualization/folium
#    https://bokeh.pydata.org/en/latest/docs/gallery/texas.html
#    https://stats.wikimedia.org/v2/#/es.wikipedia.org/reading/pageviews-by-country
#    http://geo.holoviews.org/Working_with_Bokeh.html
    filename = languagecode+'_'+content_type+'_geolocated_articles_map_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: How well the language editions cover the CCC of this language edition?
def generate_ccc_culture_gap_covered_visualization(languagecode, wiki_path):
    # diagrama de boles


    filename = languagecode+'_'+'ccc_culture_gap_covered_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path


#   QUESTION: How well this language edition cover the CCC of the other language editions?     
def generate_ccc_culture_gap_covering_visualization(languagecode, wiki_path):
#    això és utilitzar table_ccc_gaps amb funció share.
    # diagrama de boles. stacked bar o formatget.

    filename = languagecode+'_'+'ccc_culture_gap_covering_visualization'+'.png'
    file_path = current_site_path + wiki_path + filename

    return file_path



#   QUESTION: Which articles from each CCC should be available in other language editions?
# It creates a table and a list of top 100, top 1000 articles according to different criteria.
def generate_ccc_vital_articles_list(languagecode, languagecode_target, content_type, category, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns, page_titles_qitems, wiki_path):

    print ('Obtaining a prioritized article list based on these parameters:')
    print (languagecode, languagecode_target, content_type, category, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns)

    # DEFINE CONTENT TYPE
    # according to the content type, we make a query or another.
    print ('define the content type')
    if content_type[0] == 'ccc': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
    if content_type[0] == 'gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NOT NULL'
    if content_type[0] == 'kw': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
    if content_type[0] == 'ccc_not_gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NULL'
    if content_type[0] == 'ccc_main_territory': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'


    # DEFINE CATEGORY TO FILTER THE DATA (specific territory, specific topic)
    print ('define the specific category')
    if category != '':
        print ('We are usign this category to filter, either by main_territory or main_topic: '+category)
        if content_type[0] == 'ccc':
            query = query + ' AND main_territory = "'+str(category)+'";'
        if content_type[0] == 'topical':
            query = query + ' AND main_topic = "'+str(category)+'";'


    # DEFINE THE TIMEFRAME -> if it is necessary, it will admit two timestamps two be passed as parameters.
    print ('define the timeframe')
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    if time_frame == 'last_week':
        week_ago_timestamp=(datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created > '+str(week_ago_timestamp)
    if time_frame == 'last_month':
        month_ago_timestamp=(datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created > '+str(month_ago_timestamp)
    if time_frame == 'last_three_months':
        month_ago_timestamp=(datetime.date.today() - datetime.timedelta(days=3*30)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created > '+str(month_ago_timestamp)
    if time_frame == 'last_year':
        last_year_timestamp=(datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created > '+str(last_year_timestamp)
    if time_frame == 'last_five_years':
        last_five_years=(datetime.date.today() - datetime.timedelta(days=5*365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created > '+str(last_five_years)
    if time_frame == 'first_year':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = cursor.fetchone()[0]
        print (timestamp)
        first_year=(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S') + datetime.timedelta(days=365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_year)
    if time_frame == 'first_three_years':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = cursor.fetchone()[0]
        first_three_years=(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S') + datetime.timedelta(days=5*365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_three_years)
    if time_frame == 'first_five_years':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = cursor.fetchone()[0]
        first_five_years=(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S') + datetime.timedelta(days=5*365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_five_years)


    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    print (query)
    ccc_df = pd.read_sql_query(query, conn)
    print (ccc_df.columns.values)
    ccc_df = ccc_df.set_index(['qitem'])
    ccc_df = ccc_df.fillna(0)
    print ('this is the number of lines of the dataframe: '+str(len(ccc_df)))


    # FILTER ARTICLES IN CASE OF CONTENT TYPE
    if len(content_type)>1:
        if content_type[1] == 'people': query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.property="P31" AND sitelinks.langcode="'+languagecode+'wiki";'
        if content_type[1] == 'male': query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        if content_type[1] == 'female': query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        if content_type[1] == 'topical': query = ''

        conn3 = sqlite3.connect('wikidata.db'); cursor3 = conn3.cursor()
        topic_selected=[]
        print (query)
#        print (languagecode)
        for row in cursor3.execute(query): topic_selected.append(row[0])
        print (len(topic_selected))
        ccc_df = ccc_df.reindex(topic_selected)
        print ('this is the number of lines of the dataframe after the content filtering: '+str(len(ccc_df)))
        ccc_df = ccc_df.fillna(0)


    # FILTER THE LOWEST PART OF CCC
#    print ('filter the lowest part of CCC')
    # here we would filter a % of the dataframe according to the number of inlinks_from_CCC and/or num_retrieval_strategies.
    # ccc_df.drop(ccc_df.query('num_inlinks_ccc < 6').index)
#    percentage = 20
#    new_index=ccc_df['num_inlinks_from_CCC'].sort_values(ascending=False).index.tolist()
#    ccc_df = ccc_df.reindex(index = new_index)
#    ccc_df.drop(ccc_df.index[len(ccc_df)-int(len(ccc_df)/percentage):], inplace=True) # this is the percentage deleted

#    new_index=ccc_df['num_retrieval_strategies'].sort_values(ascending=False).index.tolist()
#    ccc_df = ccc_df.reindex(index = new_index)
#    ccc_df.drop(ccc_df.index[len(ccc_df)-int(len(ccc_df)/percentage):], inplace=True) # this is the percentage deleted  


    # RANK ARTICLES BY RELLEVANCE
    print ('rank articles by rellevance')
    articles_ranked = {}
    if rellevance_sense=='positive': # articles top priority of rellevance
        ascending=False
    if rellevance_sense=='negative': # articles for deletion (no one cares)
        ascending=True

    rellevance_measures = ['num_inlinks', 'num_outlinks', 'num_bytes', 'num_references', 'num_edits', 'num_editors', 'num_pageviews', 'num_wdproperty', 'num_interwiki', 'num_discussions', 'featured_article', 'num_inlinks_from_CCC', 'num_retrieval_strategies']
    rank_dict = {}
    for parameter in rellevance_rank.keys():
        if parameter in rellevance_measures:
            coefficient=rellevance_rank[parameter]
            ccc_ranked=ccc_df[parameter].sort_values(ascending=ascending).index.tolist()
            print ('parameter of rellevance: '+parameter+ ' with coefficient: '+str(coefficient))
            value = 1
            for x in ccc_ranked:
 #               print (x,ccc_df.loc[x]['page_title'],ccc_df.loc[x][parameter]); input('')
                if x in rank_dict:
                    rank_dict[x]=rank_dict[x]+value*coefficient
                else:
                    rank_dict[x]=value*coefficient
                value = value + 1
    rank = sorted(rank_dict, key=rank_dict.get)
#    print (rank[:100])

#    print ('Showing the top articles: ')
#    i=0
#    for y in rank:
#        print (ccc_df.loc[y]['page_title'],ccc_df.loc[y]['main_territory'],rank_dict[y],ccc_df.loc[y]['num_pageviews'])
#        i=i+1
#        if i == 500: break
#    input('')

  
    # GET TERRITORY REPRESENTATIVITY COEFFICIENTS
    # get the different territories for the language. a list.
    print ('calculate the representativity coefficients')
    representativity_coefficients = {}

    if representativity == 'none':
        representativity_coefficients['Any']=1

    if representativity == 'all_equal': # all equal. get all the qitems for the language code. divide the 
        try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems = [territories.loc[languagecode]['QitemTerritory']]
        coefficient=1/(len(qitems)+1)
        for x in qitems: representativity_coefficients[x]=coefficient
        representativity_coefficients[0]=coefficient

    if representativity == 'proportional_articles' or representativity == 'proportional_articles_compensation': # proportional to the number of articles for each territory. check data from: ccc_extent_by_qitem.
        conn2 = sqlite3.connect('ccc_data.db'); cursor2 = conn2.cursor()
        query = 'SELECT qitem, ccc_articles_qitem FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'" AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'");'
#        print (query)
        sum = 0 
        for row in cursor2.execute(query):
            main_territory = row[0]
            if main_territory == None: main_territory = 0
            representativity_coefficients[main_territory]=row[1]
            sum = sum + row[1]
        for x,y in representativity_coefficients.items():
            representativity_coefficients[x]=y/sum

        if representativity == 'proportional_articles_compensation':
            for x,y in representativity_coefficients.items():
                if y < 0.02:
                    representativity_coefficients[x]=0.02
                    diff = 0.02 - representativity_coefficients[x]
                    representativity_coefficients[0]=representativity_coefficients[0]-diff

    if representativity == 'proportional_ccc_rellevance': # proportional to the rellevance of each qitem.
        # check data from: ccc_extent_by_qitems. number of inlinks from CCC.
        total_inlinks = 0
        for qitem in ccc_df.index:
            inlinks = ccc_df.loc[qitem]['num_inlinks_from_CCC']; 
            main_territory = ccc_df.loc[qitem]['main_territory']
            if main_territory == 0: main_territory = 0

            if main_territory in representativity_coefficients:
                representativity_coefficients[main_territory]=representativity_coefficients[main_territory]+int(inlinks)
            else:
                representativity_coefficients[main_territory]=int(inlinks)
            total_inlinks = total_inlinks + inlinks
        for qitem in representativity_coefficients.keys(): representativity_coefficients[qitem]=representativity_coefficients[qitem]/total_inlinks # normalization

    if representativity == 'minimum':
        try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems = [territories.loc[languagecode]['QitemTerritory']]

        coefficient=0.02
        if coefficient > 1/len(qitems): coefficient = round(1/len(qitems),2)
        for x in qitems: representativity_coefficients[x]=coefficient

        rest=1-len(qitems)*coefficient
        representativity_coefficients['Any']=rest

    if category != '':
        representativity_coefficients={}
        representativity_coefficients[category]=1

    representativity_coefficients_sorted = sorted(representativity_coefficients, key=representativity_coefficients.get, reverse=False)
    print (representativity_coefficients)
    print (representativity_coefficients_sorted)
    sum = 0
    for x,y in representativity_coefficients.items(): sum = sum + y
    print (sum)

    # Get dictionary names
    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
    qitems_territories_names = {}
    for x in representativity_coefficients_sorted: 
        if x != 0 and x!= 'Any': qitems_territories_names[x]=qitems_page_titles[x]
    print (qitems_territories_names)

    if content_type[0] == 'ccc_main_territory':
        representativity_coefficients={}
        representativity_coefficients[representativity_coefficients_sorted[0]]=1


    # MAKE THE DATAFRAME
    # Creating the final dataframe with the representation for each territory
    print ('make the new dataframe')
    prioritized_list=[]
    articles_ranked=rank
    representativity_articles={}
    d=0
    i=1
    error='No errors.'
    number_windows = 1
    while len(articles_ranked)!=d and i<=number_windows:
        d = len(articles_ranked)
        for x in representativity_coefficients_sorted: representativity_articles[x]=int(window*representativity_coefficients[x]) # SET THE NEXT ITERATION OF ARTICLES TO prioritized_list={}
        print (representativity_articles)

        z=0
        for x,y in representativity_articles.items(): z=z+y
        print ('the window has: '+str(z))
        if window > z: representativity_articles['Any']=representativity_articles['Any']+(window-z)
        print ('Any incremented: '+str(representativity_articles['Any']))

#        input('')
        for x,y in representativity_articles.items(): # filling the window
            todelete = []
            for qitem in articles_ranked:
                main_territory = ccc_df.loc[qitem]['main_territory']
#                print (ccc_df.loc[qitem]['page_title'],main_territory,x)

                if main_territory == x or x == 'Any':
#                    input('')
                    if main_territory != 0: territory = qitems_territories_names[main_territory]
                    else: territory = 'None'


                    print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],
                    '\t\t\t'+str(list(rellevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[0]]),
                    '\t'+str(list(rellevance_rank.keys())[1])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[1]]),
#                    '\t'+str(list(rellevance_rank.keys())[2])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[2]]),
                    '\trank:'+str(rank_dict[qitem]),
                    qitem,territory,main_territory,x); #input('')

                    prioritized_list.append(qitem)
                    todelete.append(qitem)
                    i=i+1

                    y = y - 1 # countdown
                if y == 0 or y < 1:
#                    print ('* one type is filled: '+x)
                    break

            if len(todelete) == 0 or len(todelete)<=y:
                error = 'No articles for the territory: '+str(x)+' so we took articles from the top of the ranking to fill the gap.'
                if len(articles_ranked)>y:
                    for x in range(0,y):
                        qitem = articles_ranked[x]
                        prioritized_list.append(qitem)
                        todelete.append(qitem)
                        print (y,ccc_df.loc[qitem]['page_title'],rank_dict[qitem]); #input('')
                        y = y - 1 # countdown

            for qitem in todelete: articles_ranked.remove(qitem)

#        print ('* one window filled.')
    ccc_df=ccc_df.reindex(prioritized_list)
    print (error)
    print (len(ccc_df))
#    print (prioritized_list[:100])



##### --- ##### EXPORTING THE DATA AS HTML AND WIKIMARKUP IN THIS FUNCTION

    wiki_tables_dict = {}

    # ADDING NEW COLUMNS
    # Adding new columns with the IW address from the closest languages
    closestlanguages=[]
    langs = obtain_proximity_wikipedia_languages_lists(languagecode)[2][:5]
    for x in langs: closestlanguages.append(x+'wiki')
    print (closestlanguages)

    # Languages name
    lang_dict = {}
    for lang in langs:
        qitem=languages.loc[lang.split('wiki')[0]]['Qitem']
        lang_dict[lang.split('wiki')[0]]=qitems_page_titles[qitem]

    qitems_list=ccc_df.index.tolist()
    print ('adding extra wd_sitelinks')
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()
    page_asstring = ','.join( ['?'] * len( closestlanguages ) )
    articles_foreignlang = {}
    for qitem in qitems_list:
        for lang in closestlanguages:
            query = 'SELECT page_title FROM sitelinks WHERE sitelinks.langcode = ? AND qitem = ?'
            cursor2.execute(query,(lang,qitem))
            row = cursor2.fetchone()
            if row!=None:
                languagename = lang_dict[lang.split('wiki')[0]]
#                languagename = languages.loc[lang.split('wiki')[0]]['languagename']
                url = '<a href="https://'+lang.split('wiki')[0]+'.wikipedia.org/wiki/'+row[0].replace(' ','_')+'">'+languagename+'</a>'
#                print (url)
                if qitem not in articles_foreignlang:
                    articles_foreignlang[qitem]=url
                else:
                    articles_foreignlang[qitem]=articles_foreignlang[qitem]+', '+url
        if qitem not in articles_foreignlang: articles_foreignlang[qitem]=''
    ccc_df['Other Languages'] = pd.Series(articles_foreignlang)

    # Translated Title Proposal
    potential_titles = {}
    for qitem in qitems_list:
        query = 'SELECT page_title FROM sitelinks WHERE sitelinks.langcode = ? AND qitem = ?'
        cursor2.execute(query,(languagecode_target+'wiki',qitem))
        row = cursor2.fetchone()


        # TODO TODO TODO
        # NO!!! WE NEED REAL-TIME INFO. WE CANNOT USE INFO FROM THE LAST MONTH.
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO
        # TODO TODO TODO


        if row!=None:
            count_covered_articles=count_covered_articles+1
            page_title = row[0].replace(' ','_')
            url = '<a href="https://'+languagecode_target+'.wikipedia.org/wiki/'+page_title+'">'+page_title+'</a>'
        else:
            title = ccc_df.loc[qitem]['page_title'].replace('_',' ')
            r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+languagecode+"/"+languagecode_target+"/Apertium", data={'html': '<div>'+title+'</div>'}) # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
            if str(r) == 'Provider not supported': potential_title = ''
            potential_title = r.text.split('<div>')[1].split('</div>')[0].replace(' ','_')
            url = 'https://'+languagecode+'.wikipedia.org/w/index.php?title=Special:ContentTranslation&page='+title+'&from='+languagecode+'&to='+languagecode_target # target_url_translator
            url = 'https://'+languagecode_target+'.wikipedia.org/wiki/Special:ContentTranslation?page='+title+'&from='+languagecode+'&to='+languagecode_target+'&targettitle='+potential_title+'&campaign=interlanguagelink' # target_url_potential_title_translator
            url = 'https://'+languagecode_target+'.wikipedia.org/w/index.php?title='+potential_title+'&action=edit&redlink=1'
            url = '<a href="'+ url + '">'+potential_title+'</a>'
#            print (url)
        potential_titles[qitem]=url
    ccc_df['Title / Suggested Title'] = pd.Series(potential_titles)


    # INTRODUCING THE LIST CROSS-LANGUAGE AVAILABILITY TO THE TABLE: vital_articles_lists
    languagecode_covered = languagecode
    list_name=wiki_path.split('/')[2]
    create_table_ccc_bridging_lists(count_covered_articles, languagecode_covered, languagecode_covering, percentage, list_name)

    print ('* list introduced into the database vital_articles_lists from ccc_data')
    print (languagecode_covered, languagecode_covering, percentage, count_covered_articles, number_articles, list_name)


    # REFORMATTING THE DATASET INTO AN HTML TABLE
    # Ranking positions by main feature / Reindexing the dataframe
    i=0
    ccc_ranked_dict = {}
    main_value = sorted(rellevance_rank, key=rellevance_rank.get, reverse=True)[0]
    ccc_ranked=ccc_df[main_value].sort_values(ascending=ascending).index.tolist()
    for x in ccc_ranked:
        i = i + 1
        ccc_ranked_dict[x]=i
    ccc_df['Nº'] = pd.Series(ccc_ranked_dict)
    ccc_df=ccc_df.reindex(ccc_ranked)

    # Change editors format:
    ccc_df['num_editors'] = ccc_df.num_editors.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_editors'] = int(ccc_df.loc[x]['num_editors'])

    # Change edits format:
    ccc_df['num_edits'] = ccc_df.num_edits.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_edits'] = int(ccc_df.loc[x]['num_edits'])

    # Change discussion edits format:
    ccc_df['num_discussions'] = ccc_df.num_discussions.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_discussions'] = int(ccc_df.loc[x]['num_discussions'])

    # Change pageviews format:
    ccc_df['num_pageviews'] = ccc_df.num_pageviews.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_pageviews'] = int(ccc_df.loc[x]['num_pageviews'])

    # Change Retrieval strategies format:
    ccc_df['num_retrieval_strategies'] = ccc_df.num_retrieval_strategies.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_retrieval_strategies'] = int(ccc_df.loc[x]['num_retrieval_strategies'])

    # Change Inlinks from CCC format:
    ccc_df['num_inlinks_from_CCC'] = ccc_df.num_inlinks_from_CCC.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_inlinks_from_CCC'] = int(ccc_df.loc[x]['num_inlinks_from_CCC'])

    # Change Bytes format:
    ccc_df['num_bytes'] = ccc_df.num_bytes.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_bytes'] = int(ccc_df.loc[x]['num_bytes'])

    # Change pageviews format:
    ccc_df['num_references'] = ccc_df.num_references.astype(int)
    for x in ccc_ranked: ccc_df.at[x, 'num_references'] = int(ccc_df.loc[x]['num_references'])

    # Change date created format
    for x in ccc_ranked: ccc_df.at[x, 'date_created'] = time.strftime("%Y-%m-%d", time.strptime(ccc_df.loc[x]['date_created'], "%Y%m%d%H%M%S"))

    # Change page_title format
    for x in ccc_ranked: ccc_df.at[x, 'page_title'] = '<a href="https://'+languagecode+'.wikipedia.org/wiki/'+ccc_df.loc[x]['page_title']+'">'+ccc_df.loc[x]['page_title'].replace('_',' ')+'</a>'

    # Change featured articles
    ccc_df['featured_article'] = ccc_df.featured_article.astype(str)
    for x in ccc_ranked:
        if ccc_df.loc[x]['featured_article']=='0.0': ccc_df.at[x, 'featured_article'] = 'No'
        else: ccc_df.at[x, 'featured_article'] = 'Yes'

    # Change interwiki format:
    ccc_df['num_interwiki'] = ccc_df.num_interwiki.astype(int)
    ccc_df['num_interwiki'] = ccc_df.num_interwiki.astype(str)
    for x in ccc_ranked: ccc_df.at[x,'num_interwiki'] = '<a href="https://www.wikidata.org/wiki/'+x+'">'+ccc_df.loc[x]['num_interwiki']+'</a>'

    # Change properties format:
    ccc_df['num_wdproperty'] = ccc_df.num_wdproperty.astype(int)
    ccc_df['num_wdproperty'] = ccc_df.num_wdproperty.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_wdproperty'] = '<a href="https://www.wikidata.org/wiki/'+x+'">'+ccc_df.loc[x]['num_wdproperty']+'</a>'

    # Renaming the columns
    columns_dict = {'page_title':'Title','date_created':'Date Created','main_territory':'Associated Territory','num_bytes':'Bytes','num_discussions':'Discussion Edits','num_references':'External Links','num_inlinks':'Inlinks','num_outlinks':'Outlinks','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_wdproperty':'Wikidata Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_inlinks_from_CCC':'Inlinks from CCC','num_retrieval_strategies':'CCC Strategies'}
    ccc_df=ccc_df.rename(columns=columns_dict)

#    # Choosing the final columns
#    columns = ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki']

    for x in range(0,len(columns)): columns[x]=columns_dict[columns[x]]
    new_columns = ['Nº','Title']+columns+['Date Created','Inlinks from CCC','Other Languages','Title / Suggested Title']
    ccc_df = ccc_df[new_columns] # selecting the parameters to export

    # Filename Path
    if time_frame != '': time_frame = '_' + time_frame
    path = '/srv/wcdo/site/'+languagecode+'/'
    os.makedirs(path, exist_ok=True)

    file_path = path+languagecode+'/'+str(window) + '_' + "_".join(content_type)+'_vital_articles_' + columns[0].lower().replace(' ','_') + time_frame + '_' + languagecode + '_' + languagecode_target + '.html'
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)

    html_str=HTML(ccc_df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)

    pd.set_option('display.max_colwidth', old_width)

    print ('* wikimarkup and html file created.\n')


    return wiki_tables_dict



#   QUESTION: Which of these CCC articles is or should be available in this language edition?
def generate_ccc_all_vital_articles_lists_table(languagecode_covered, wiki_path):

#    comprovar les llistes introduïdes a create_table_ccc_bridging_lists. 
#    comptar els números. sumar els números (per ordenar-los per ranking)
#    comprovar que els articles existeixin al ccc_Xwiki a la base de dades real enwiki...etc..
    conn = sqlite3.connect('ccc_data.db'); cursor = conn.cursor()


    # CREATE THE DATAFRAME. create the columns.

    for languagecode in wikilanguagecodes:
        # for a given language_covering get the lists for the language covered.
        query = 'SELECT number_articles, percentage, list_name FROM vital_articles_lists ;'
        
        covering_language_SUM = 0
        for row in cursor.execute(query):
            list_name=row[2]
            number_articles = row[0]
            percentage = row[1]

            covering_language_SUM = covering_language_SUM + number_articles

        # recreate the links to the entire lists.
        url='Wikipedia_Cultural_Diversity_Observatory/'+languages.loc[languagecode]['Wikipedia'].replace(' ','_')+'/'+list_name


        # INTRODUCE IT TO A DATAFRAME (as a list.)


    # CREATE THE TABLE IN WIKIMARKUP AND HTML FROM THE DATAFRAME. si pot ser amb colorets de vermell a verd...
    wikitext=''


    # RANK ACCORDING TO covering_language_SUM


    # CREATE THE FILE IN HTML
    filename = languagecode+'_'+'ccc_all_vital_articles_lists_table'+'.html'
    file_path = current_site_path + wiki_path + filename
    
    return wikitext


# in this function we create the table language_territories_mapping.
def generate_ccc_language_territories_mapping_table(wiki_path):


    print ('')



### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

### PUBLISHING TO META/NOTIFYING THE WORK DONE.

# Publishes all the meta pages for all the languages in case they do not exist.
def publish_wcdo_first_time():

    site = pywikibot.Site("meta","meta")
    text_files_path = site_path + '/first_time_texts/'

    # HERE WE INTRODUCE FOR THE FIRST TIME:
    # a) TEXT
    # b) PICTURES TAGS (THAT ARE GOING TO BE UPDATED)
    # c) TABLES TAGS (THAT ARE GOING TO BE UPDATED VIA TRANSCLUSION)
    # not the actual pictures and the tables themselves.
    # We also introduce the categories.

    # CREATING THE MAIN CATEGORY
    category_page_name = 'Wikipedia_Cultural_Diversity_Observatory'
    categorypage = pywikibot.Category(site, category_page_name)
    categorypage.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text='[[Category:Parent]]')

    # ALL LANGUAGES PAGES
    # WCDO project site for all languages
    main_page_name = 'Wikipedia_Cultural_Diversity_Observatory'
    wiki_path = main_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+main_page_name+'.txt',"r").read()
    wikitext= 'this is a transclusion tag based on generate_ccc_all_languages_composition, generate_ccc_extent_visualization and generate_ccc_creation_visualization_monthly'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    list_ccc_extent_page_name = 'List_of_Wikipedias_by_Cultural_Context_Content'
    wiki_path = main_page_name + '/' + list_ccc_extent_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+list_ccc_extent_page_name+'.txt',"r").read()
    wikitext= 'this is a transclusion tag based on generate_ccc_extent_all_languages_table'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    wikitext = wikitext + '[[Category:Lists_of_Wikipedias]]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    culture_gap_page_name = 'Culture_Gap'
    wiki_path = main_page_name + '/' + culture_gap_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+culture_gap_page_name+'.txt',"r").read()
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_culture_gap_table'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    languages_territories_mapping_page_name = 'Language_Territories_Mapping'
    wiki_path = main_page_name + '/' + languages_territories_mapping_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+languages_territories_mapping_page_name+'.txt',"r").read()
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_language_territories_mapping_table'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    cultural_context_content_page_name = 'Cultural_Context_Content'
    wiki_path = main_page_name + '/' + cultural_context_content_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+cultural_context_content_page_name+'.txt',"r").read()
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_creation_visualization_monthly'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    get_involved_page_name = 'Get_Involved'
    wiki_path = main_page_name + '/' + get_involved_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+get_involved_page_name+'.txt',"r").read()
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_bridging_culture_gap_visualization_monthly'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



    for languagecode in wikilanguagecodes:

        # CREATING THE WCDO LANGUAGE CATEGORY
        category_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')+'_(WCDO)'
        categorypage = pywikibot.Category(site, category_page_name)
        categorypage.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text='[[Category:Wikipedia_Cultural_Diversity_Observatory]]') # introducing the parent

        # LANGUAGE BY LANGUAGE PAGES
        language_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')+'_(WCDO)'
        wiki_path = main_page_name + '/' + language_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)'+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_extent_qitem_table_by_language, generate_ccc_extent_visualization, generate_ccc_creation_visualization_monthly'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        wiki_path = main_page_name + '/' + language_page_name + '/' + culture_gap_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+culture_gap_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_culture_gap_covered_visualization, generate_ccc_culture_gap_covering_visualization'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        culture_gap_monthly_page_name = 'Culture_Gap_monthly'
        wiki_path = main_page_name + '/' + language_page_name + '/' + culture_gap_monthly_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+culture_gap_monthly_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_bridging_culture_gap_visualization_monthly, generate_ccc_culture_gap_index_visualization_monthly'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        vital_articles_lists_page_name = 'Vital_articles_lists'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+vital_articles_lists_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'       
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        topical_coverage_page_name = 'Topical_coverage'
        wiki_path = main_page_name + '/' + language_page_name + '/' + topical_coverage_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+topical_coverage_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_topical_coverage_visualization   generate_ccc_topical_coverage_visualization'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        geolocated_articles_page_name = 'Geolocated_articles'
        wiki_path = main_page_name + '/' + language_page_name + '/' + geolocated_articles_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+geolocated_articles_page_name+'.txt',"r").read()
        wikitext = 'this is the text and the transclusion tag based on   generate_ccc_geolocated_articles_map_visualization'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_Top_100_page_name = 'CCC_Vital_articles_Top_100'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_100_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_Top_100_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_Top_1000_page_name = 'CCC_Vital_articles_Top_1000'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_1000_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_Top_1000_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_featured_page_name = 'CCC_Vital_articles_featured'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_featured_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_featured_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_geolocated_page_name = 'CCC_Vital_articles_geolocated'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_geolocated_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_geolocated_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_keywords_page_name = 'CCC_Vital_articles_keywords'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_keywords_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_keywords_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_first_years_page_name = 'CCC_Vital_articles_first_years'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_first_years_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_first_years_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_last_quarter_page_name = 'CCC_Vital_articles_last_quarter'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_last_quarter_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_last_quarter_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_women_page_name = 'CCC_Vital_articles_women'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_women_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_women_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'       
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_men_page_name = 'CCC_Vital_articles_men'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_men_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_men_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_pageviews_page_name = 'CCC_Vital_articles_pageviews'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_pageviews_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_pageviews_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'       
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        CCC_Vital_articles_discussions_page_name = 'CCC_Vital_articles_discussions'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_discussions_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+CCC_Vital_articles_discussions_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on   generate_ccc_all_vital_articles_lists_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



# This is a function that includes all the calls to the functions in order to create the visualizations and apply them to each language.
def publish_wcdo_updates():
    site = pywikibot.Site('meta','meta')


    # HERE WE UPDATE
    # a) PICTURES UPLOADED
    # b) TABLES TEXT
    # not the actual text from the pages.


# ALL LANGUAGES

    top19 = obtain_proximity_wikipedia_languages_lists('en')[0]

    # WCDO PAGE
    main_page_name = 'Wikipedia_Cultural_Diversity_Observatory'
    wiki_path = main_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext=''
    # QUESTION: What is the composition of the entire Wikipedia project in terms of each language Cultural Context Content?
    file_path = generate_ccc_all_languages_composition(wiki_path)
    bot = upload.UploadRobot(url=[join_images_path("MP_sounds.png")],
                             description="pywikibot upload.py script test",
                             useFilename=None, keepFilename=True,
                             verifyDescription=True, aborts=set(),
                             ignoreWarning=True, targetSite=self.get_site())
    bot.run()
    # ONCE VERIFIED AND ADJUSTED, this must be copied to evry other image.
    # https://github.com/wikimedia/pywikibot/blob/master/scripts/upload.py

    # QUESTION: What is the extent of Cultural Context Content in all language editions?
    file_path = generate_ccc_extent_visualization('en',top19,wiki_path)

    bot = upload.UploadRobot(url=[join_images_path("MP_sounds.png")],
                             description="pywikibot upload.py script test",
                             useFilename=None, keepFilename=True,
                             verifyDescription=True, aborts=set(),
                             ignoreWarning=True, targetSite=self.get_site())
    bot.run()
    # ONCE VERIFIED AND ADJUSTED, this must be copied to evry other image.
    # https://github.com/wikimedia/pywikibot/blob/master/scripts/upload.py



    # LIST OF WIKIPEDIAS BY CCC
    list_ccc_extent_page_name = 'List_of_Wikipedias_by_Cultural_Context_Content'
    wiki_path = main_page_name + '/' + list_ccc_extent_page_name
    # QUESTION: What is the extent of cultural context content in each language edition?
    wiki_table = generate_ccc_extent_all_languages_table(wiki_path)

    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)



    # CULTURE GAP
    culture_gap_page_name = 'Culture_Gap'
    wiki_path = main_page_name + '/' + culture_gap_page_name
    culture_gap_page = pywikibot.Page(site, wiki_path)

    # QUESTION: How well each language edition covers the CCC of each other language edition?
    wiki_table = generate_ccc_culture_gap_table(wiki_path) #    taula amb les top 5 que més cobreix cada llengua.
    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

    # QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
    file_path = generate_ccc_culture_gap_index_visualization_monthly('en',top19,wiki_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



    # CULTURAL CONTEXT CONTENT
    cultural_context_content_page_name = 'Cultural_Context_Content'
    wiki_path = main_page_name + '/' + cultural_context_content_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext='Method and datasets'
    # QUESTION: What is the extent of Cultural Context Content in the articles created during the last month?
    file_path = generate_ccc_creation_visualization_monthly('en',top19,wiki_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



    # GET INVOLVED
    get_involved_page_name = 'Get_Involved'
    wiki_path = main_page_name + '/' + get_involved_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext='Method and datasets'
    # QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
    file_path = generate_ccc_bridging_culture_gap_visualization_monthly('en',wiki_path)

    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



    # LANGUAGE TERRITORIES MAPPING
    languages_territories_mapping_page_name = 'Language_Territories_Mapping'
    wiki_path = main_page_name + '/' + languages_territories_mapping_page_name
    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


# LANGUAGE BY LANGUAGE
    # project site page WCDO for each language
    for languagecode in wikilanguagecodes:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        qitems_page_titles_english = {v: k for k, v in load_dicts_page_ids_qitems('en')[0].items()}
        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)

        wikipedia_proximity_lists = obtain_proximity_wikipedia_languages_lists(languagecode)
        top19 = wikipedia_proximity_lists[0]
        upper9lower10 = wikipedia_proximity_lists[1]
        closest19 = wikipedia_proximity_lists[2]
      
        # LANGUAGE PAGE
        language_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')
        wiki_path = main_page_name + '/' + language_page_name
        language_page = pywikibot.Page(site, wiki_path)
        wikitext=''

        # QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?
        wikitext = generate_ccc_extent_qitem_table_by_language(languagecode,page_titles_qitems,qitems_page_titles_english,wiki_path)

        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        wikitext = 'updating the transclusion'
        language_page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
         
        # QUESTION: What is the extent of Cultural Context Content in this language edition compared to others?
        file_path = generate_ccc_extent_visualization(languagecode, wiki_path)
        language_page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # QUESTION: What is the extent of Cultural Context Content from the articles created during the last month?
        file_path = generate_ccc_creation_visualization_monthly(languagecode,closest19, wiki_path)
        language_page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



        # CULTURE GAP PAGE
        wiki_path = main_page_name + '/' + language_page_name + '/' + culture_gap_page_name
        culture_gap_page = pywikibot.Page(site, wiki_path)
        wikitext=''

        # QUESTION: How well the other language editions cover the CCC of this language edition?
        file_path = generate_ccc_culture_gap_covered_visualization(languagecode, wiki_path)
        culture_gap_page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # QUESTION: How well this language edition cover the CCC of the other language editions?     
        file_path = generate_ccc_culture_gap_covering_visualization(languagecode, wiki_path)
        culture_gap_page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



        # CULTURE GAP MONTHLY PAGE
        culture_gap_monthly_page_name = 'Culture_Gap_monthly'
        wiki_path = main_page_name + '/' + language_page_name + '/' + culture_gap_monthly_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext='Method and datasets'
        # QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
        file_path=generate_ccc_bridging_culture_gap_visualization_monthly(languagecode, wiki_path)
        # QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
        file_path=generate_ccc_culture_gap_index_visualization_monthly(languagecode,top19,wiki_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



        # VITAL ARTICLES LIST PAGE
        vital_articles_lists_page_name = 'Vital_articles_lists'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        wikitext = generate_ccc_all_vital_articles_lists_table(languagecode_covered, wiki_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



        # TOPICAL COVERAGE PAGE
        topical_coverage_page_name = 'Topical_coverage'
        wiki_path = main_page_name + '/' + language_page_name + '/' + topical_coverage_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext='Method and datasets'
        # QUESTION: What is the topical coverage of the articles of this Wikipedia language edition?     
        file_path = generate_topical_coverage_visualization(languagecode, wiki_path)
        # QUESTION: What is the topical coverage of the CCC of this Wikipedia language edition?
        file_path = generate_ccc_topical_coverage_visualization(languagecode, wiki_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


        # GEOLOCATED ARTICLES PAGE
        geolocated_articles_page_name = 'Geolocated_articles'
        wiki_path = main_page_name + '/' + language_page_name + '/' + geolocated_articles_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext='Method and datasets'
        # QUESTION: Which are the geolocated articles with most inlinks, pageviews, etc.?
        # QUESTION: How well does this language covers other language editions geolocated articles?
        file_path = generate_ccc_geolocated_articles_map_visualization(languagecode, wiki_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


        # RECOMMENDATION LISTS: VITAL ARTICLES
        # QUESTION: Which of these CCC articles is or should be available in this language edition?
        CCC_Vital_articles_Top_100_page_name = 'CCC_Vital_articles_Top_100'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_100_page_name

        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)






        # THESE generate_ccc_vital_articles_list FUNCTIONS ARE NOT PROPERLY CONFIGURED.





        CCC_Vital_articles_Top_1000_page_name = 'CCC_Vital_articles_Top_1000'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_1000_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_featured_page_name = 'CCC_Vital_articles_featured'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_featured_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_geolocated_page_name = 'CCC_Vital_articles_geolocated'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_geolocated_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_keywords_page_name = 'CCC_Vital_articles_keywords'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_keywords_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_first_years_page_name = 'CCC_Vital_articles_first_years'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_first_years_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_last_quarter_page_Name = 'CCC_Vital_articles_last_quarter'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_last_quarter_page_Name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_women_page_name = 'CCC_Vital_articles_women'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_women_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_men_page_name = 'CCC_Vital_articles_men'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_men_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_pageviews_page_name = 'CCC_Vital_articles_pageviews'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_pageviews_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        CCC_Vital_articles_discussions_page_name = 'CCC_Vital_articles_discussions'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_discussions_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)


#        BIGGEST SINGLE TERRITORY
#        CCC_Vital_articles_top_territory_page_name = 'CCC_Vital_articles_top_territory'
#        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_top_territory_page_name
#        table_path = wiki_path + '/Table'
#        page = pywikibot.Page(site, table_path)
#        wiki_tables_dict = generate_ccc_vital_articles_list(languagecode, 'ca', ['ccc_main_territory'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
#        wikitext = wiki_tables_dict[languagecode]
#        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
#        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)





# TOOLS for publishing in Wikipedia (meta, language editions, commons, etc.)
def upload_publish_image(filename, description, url):

# upload pictures to a wikipedia
#https://phabricator.wikimedia.org/diffusion/PWBO/browse/master/upload.py

    print ('')


def upload_publish_table(html, page):
    wikitext = pywikibot.convertTable(html)
    page = pywikibot.Page(site, page)

    page.save(summary="Updating Table", watch=None, minor=False,
                    botflag=False, force=False, async=False, callback=None,
                    apply_cosmetic_changes=None, text=text)


def update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict):
    for x, y in wiki_tables_dict.items():
        table_path = table_path + x # x = '/en'
        print (x, y)
        page = pywikibot.Page(site, table_path)
        page.save(summary="updating the table for language: "+x, watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=y)




#######################################################################################


def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

def finish_email():
    try:
        sys.stdout=None; send_email_toolaccount('WCDO: CCC completed successfuly', open('ccc_selection.out', 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount('WCDO: CCC aborted because of an error', open('ccc_selection.out', 'r').read()+'err')


#######################################################################################


### MAIN:
if __name__ == '__main__':

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    site_path = '/srv/wcdo/site/'
    data_path = '/srv/wcdo/data/'
    current_data_path = site_path + year_month + '/'
    current_site_path = data_path + year_month + '/'

    sys.stdout = Logger()

    # Import the language-territories mappings
    territories = load_languageterritories_mapping()

    # Import the Wikipedia languages characteristics / UPGRADE CASE: in case of extending the project to WMF SISTER PROJECTS, a) this should be extended with other lists for Wikimedia sister projects b) along with more get functions in the MAIN for each sister project.
    languages = load_wiki_projects_information();
    extract_check_new_wiki_projects();

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
    wikilanguagecodes = languages.index.tolist() 

    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if establish_mysql_connection_read(a)==None: wikilanguagecodes.remove(a)

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = load_wikipedia_language_editions_numberofarticles()

    # Final Wikipedia languages to process
#    wikilanguagecodes = obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
    wikilanguagecodes = ['en']

    print ('\n* Starting the WCDO CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* WCDO CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    finish_email()


