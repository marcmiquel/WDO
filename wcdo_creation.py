# -*- coding: utf-8 -*-

# time
import time
import datetime
# system
import os
import sys
import re
from IPython.display import HTML
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import codecs
# requests and others
import requests
import urllib
import colour
# data
import pandas as pd
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
        self.log = open("wcdo_creation.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass

# MAIN
def main():


#    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/List_of_Wikipedias_by_Cultural_Context_Content'
#    wikitext = generate_ccc_extent_all_languages_table(wiki_path)


#    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/Language_Territories_Mapping'
#    wikitext = generate_ccc_language_territories_mapping_table(wiki_path)


    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/Culture_Gap_(spread)'
    wikitext = generate_ccc_culture_gap_table(wiki_path, 'spread')

#    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/Culture_Gap_(coverage)'
#    wikitext = generate_ccc_culture_gap_table(wiki_path, 'coverage')

    file = open('testfile.txt','w') 
    file.write(wikitext)
    file.close()


"""
# MAIN
######################## WCDO CREATION SCRIPT ######################## 

# (A) -> STATS DATA PHASE / CREATE CCC OUTPUT DATA FOR DISSEMINATION (wcdo_data.db / tables: ccc_extent_language, ccc_extent_qitem, ccc_gaps, vital_articles_lists_gaps, ccc_indexs, ccc_bridges)
    create_db_table_ccc_extent_by_language()
    create_db_table_ccc_extent_by_qitem()
    create_db_table_ccc_gaps()

    create_db_table_ccc_allwiki()
    create_db_table_ccc_project_composition()
    create_db_table_ccc_bridging_last_month()
    create_db_table_ccc_topical_coverage()

# (B) -> PUBLISHING FORMATTED DATA PHASE (TABLES AND GRAPHS)
    publish_wcdo_updates()

# (C) -> NOTIFICATION:
    send_email_toolaccount('CCC created successfuly', '')
"""


"""
    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/Catalan_Wikipedia'
    generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc','female'], '', 80, '', {'num_edits': 1}, 'positive', 100, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_women')


    qitems_page_titles_english = {v: k for k, v in load_dicts_page_ids_qitems('en')[0].items()}
    wiki_path = 'Wikipedia_Cultural_Diversity_Observatory/English_Wikipedia'
    wikitext = generate_ccc_extent_qitem_table_by_language(languagecode,page_titles_qitems,qitems_page_titles_english,wiki_path)
    print (wikitext)
"""






# DATABASE AND DATASETS MAINTENANCE FUNCTIONS (CCC AND WIKIDATA)
################################################################

# Loads language_territories_mapping_quality.csv file
def load_languageterritories_mapping():
# READ FROM STORED FILE:
    territories = pd.read_csv('language_territories_mapping_quality.csv',sep='\t',na_filter = False)
    territories = territories[['territoryname','territorynameNative','QitemTerritory','languagenameEnglishethnologue','WikimediaLanguagecode','demonym','demonymNative','ISO3166','ISO31662','regional','country','indigenous','languagestatuscountry','officialnationalorregional']]
    territories = territories.set_index(['WikimediaLanguagecode'])

    territorylanguagecodes = territories.index.tolist()
    for n, i in enumerate(territorylanguagecodes): territorylanguagecodes[n]=i.replace('-','_')
    territories.index = territorylanguagecodes
    territories=territories.rename(index={'be_tarask': 'be_x_old'})
    territories=territories.rename(index={'nan': 'zh_min_nan'})

    ISO3166=territories['ISO3166'].tolist()
    regions = pd.read_csv('wikipedia_country_regions.csv',sep=',',na_filter = False)
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



# Loads wikipedia_language_editions.csv file
def load_wiki_projects_information():
    # in case of extending the project to other WMF sister projects, it would be necessary to revise these columns and create a new file where a column would specify whether it is a language edition, a wikictionary, etc.

# READ FROM STORED FILE:
    languages = pd.read_csv('wikipedia_language_editions.csv',sep='\t',na_filter = False)
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

    if not os.path.isfile('ccc_current.db'): return
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


# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
        return mysql_con_read
    except:
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')


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

    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
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

# COMMAND LINE: sqlite3 -header -csv wcdo_data.db "SELECT * FROM ccc_extent_language;" > ccc_extent_language.csv
def create_db_table_ccc_extent_by_language():
    functionstartTime = time.time()
    print ('* create_db_table_ccc_extent_by_language')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists ccc_extent_language ('+
    'languagecode text, '+
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
    print ('* create_db_table_ccc_extent_by_language Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# It creates a table to account for the number of articles obtained for each qitem and for each strategy.
# COMMAND LINE: sqlite3 -header -csv wcdo_data.db "SELECT * FROM ccc_extent_qitem;" > ccc_extent_qitem.csv
def create_db_table_ccc_extent_by_qitem():
    functionstartTime = time.time()
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()

    print ('* create_db_table_ccc_extent_by_qitem')

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
    print ('* All languages CCC extent qitems have been calculated and inserted.')
    print ('* create_db_table_ccc_extent_by_qitem Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# It creates a table for the culture gap total wp, keywords, geolocated, ccc, etc.
# COMMAND LINE: sqlite3 -header -csv wcdo_data.db "SELECT * FROM ccc_gaps;" > ccc_gaps.csv
def create_db_table_ccc_gaps(): # això es podria fer amb Qitems / o amb llengües. -> pensar si guardar el nombre absolut d'articles també.
    functionstartTime = time.time()

    print ('* create_db_table_ccc_gaps')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()

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

        groups = ['wp','ccc','gl','kw']
        for group_type in groups: # WE ARE TESTING. In the final version the tables should be without the _current.
            if group_type == 'ccc': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where ccc_binary = 1'
            if group_type == 'kw': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where keyword_title IS NOT NULL'
            if group_type == 'gl': query = 'SELECT page_id FROM ccc_'+languagecode+'wiki where geocoordinates IS NOT NULL'
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
                if percentage > 100: print ('ALERT: languagecovering: '+languagecode_covering+'. languagecovered: '+languagecode_covered+'. number articles: '+str(number_articles)+'. total articles: '+str(total_articles)+'. article group:'+group_type); percentage = 100
                parameters_gap.append((languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'gap',measurementdate))


                percentage = round(100*number_articles/wikipedialanguage_numberarticles[languagecode_covering],3)
                parameters_share.append((languagecode_covering,languagecode_covered,percentage,number_articles,group_type,'share',measurementdate))

            print ('inserting...'+group_type+'.')
            cursor2.executemany('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,group_type,reference,measurement_date) VALUES (?,?,?,?,?,?,?)', parameters_gap)
            conn2.commit()
            cursor2.executemany('INSERT INTO ccc_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,group_type,reference,measurement_date) VALUES (?,?,?,?,?,?,?)', parameters_share)
            conn2.commit()
#            print (parameters_gap)
#            print (parameters_share)
#            input('')

    print ('All languages CCC gap have been calculated and inserted.')
    print ('* create_db_table_ccc_gaps Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_db_table_ccc_lists_gaps(count_covered_articles, languagecode_covered, languagecode_covering, percentage, list_name):
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    # It creates a table for all languages.
    query = ('CREATE table if not exists vital_articles_lists_gaps ('+
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
    print ('inserting...'+list_name+' for language covering: '+languagecode_covering+' and language covered: '+languagecode_covered)
    cursor.execute('INSERT INTO vital_articles_lists_gaps (languagecode_covering,languagecode_covered,percentage,number_articles,list_name,measurement_date) VALUES (?,?,?,?,?,?)', parameters)


def create_db_table_ccc_indexs(language, index_name, value):
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");
    # IT CAN BE FROM:
    # * generate_ccc_culture_gap_table
    # relative coverage index, total coverage index, covered articles sum
    # relative spread index, total spread index, spread articles sum

    # * generate_ccc_all_vital_articles_lists_table
    # lists coverage index
    # covered list articles sum
    # lists spread index
    # spread list articles sum

    query = ('CREATE table if not exists ccc_indexs ('+
    'language text, '+
    'index_name text, '+
    'value real, '+
    'measurement_date text, '+
    'PRIMARY KEY (language, index_name, measurement_date));')
    cursor.execute(query)
#    print ('Table ccc_indexs has been created in case it did not exist.')
    measurementdate = datetime.datetime.utcnow().strftime("%Y%m%d");

    parameters = (language, index_name, value, measurement_date)
    print ('inserting...'+index_name+' for language: '+language)
    cursor.execute('INSERT INTO ccc_indexs (language, index_name, value, measurement_date) VALUES (?,?,?,?)', parameters)


# It creates a table to account for the number of articles created for each language to bridge other CCC during the last fifteen days (i.e. How much effort do editors dedicate to bridging CCC from other languages?).
def create_db_table_ccc_bridging_last_month(languagecode):

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wikidata.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect('wcdo_data.db'); cursor3 = conn3.cursor()

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
        query = "SELECT page_title FROM revision INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 AND rev_timestamp < "+ (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S') # this is a week ago.
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


def create_db_table_ccc_allwiki():
    functionstartTime = time.time()
    print ('* create_db_table_ccc_allwiki')

    # create the db
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()   
    cursor.execute("CREATE TABLE IF NOT EXISTS ccc_allwiki (languagecode text,  qitem text, PRIMARY KEY (languagecode, qitem))")
    for languagecode in wikilanguagecodes:
        query = 'INSERT INTO ccc_allwiki SELECT languagecode, qitem FROM ccc_'+languagecode+'wiki;'
        cursor.execute(query)
    conn.commit()

    print ('CCC ccc_allwiki table has been filledwith all the qitems.')
    print ('* create_db_table_ccc_allwiki Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_db_table_ccc_project_composition():
    functionstartTime = time.time()
    print ('* create_db_table_ccc_entire_project')

    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect('wikidata.db'); cursor3 = conn3.cursor()

    query = ('CREATE table if not exists ccc_entire_project ('+
    'entity_name text, '+ # name
    'type text, '+ # language, region, subregion
    'percentage real, '+ #
    'measurement_date real, '+#
    'PRIMARY KEY (entity_name, type, measurement_date)')

    cursor2.execute(query)
    print ('\nTable ccc_entire_project has just been created in case it did not exist.')
    print ('Filling it with languages, regions and subregions.')


    query_parameters = ','.join( ['?'] * len( languagelist ) )


    # 1. Formatges de tot el projecte Wikipedia amb totes les llengües (CCC) i el què ocupen (repetint articles).

    # 2. Formatges de tot el projecte Wikipedia amb totes les llengües (CCC) i el què ocupen (sense repetir articles, és a dir, tot únic).

    # 3. Formatges de tot Wikidata.


    wikilanguagecodes

    query = 'SELECT COUNT(*) FROM ccc_allwiki;'


    # JUGAR AMB ELS COUNTS, AMB ELS DISTINCTS I AMB ELS PARÀMETRES PER PASSAR LES LLISTES DE LLENGÜES.

    # el mateix però amb regions, el mateix però amb subregions.
    ###
    regionlist = languages.region.unique()
    subregionlist = languages.subregion.unique()


    for subregion in subregionlist:
        languages_of_region = languages[languages['subregion'].str.contains(subregion)]


    for region in regionlist: 
        languages_of_subregion = languages.loc[languages['region'] == region]

    print ('* create_db_table_ccc_project_composition Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# It creates a table for the topical coverage of each Wikipedia language edition.
# hauria de fer l'alineament de categories disponible i consultar-lo per si algú em pot dir que està malament?
def create_db_table_ccc_topical_coverage(lang): # AIXÒ NO HAURIA D'ANAR AQUÍ
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

   

### GENERATE TABLES VISUALIZATIONS.
#   ALL LANGUAGES
#   QUESTION: What is the extent of cultural context content in each language edition?
def generate_ccc_extent_all_languages_table(wiki_path):

    print ('* generate_ccc_extent_all_languages_table')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_extent_language WHERE measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_language) ORDER BY ccc_percent_wp DESC;'
#    print (query)
    df = pd.read_sql_query(query, conn)
    df = df.set_index(['languagecode'])
    measurement_date = df.loc[df.index.values[0]]['measurement_date']

    # FORMAT THE DATA
    rank_dict={}
    i=1
    for x in list(df.index.values):
        rank_dict[x]=i
        i=i+1
    df['Nº'] = pd.Series(rank_dict)

    df['wp_number_articles'] = df.wp_number_articles.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'wp_number_articles'] = '{:,}'.format(int(df.loc[x]['wp_number_articles']))

    df['ccc_number_articles'] = df.ccc_number_articles.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'ccc_number_articles'] = ' '+str('{:,}'.format(int(df.loc[x]['ccc_number_articles'])))+' '+'<small>('+str(df.loc[x]['ccc_percent_wp'])+'%)</small>'


    df['geolocated_number_articles'] = df.geolocated_number_articles.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'geolocated_number_articles'] = ' '+str('{:,}'.format(int(df.loc[x]['geolocated_number_articles'])))+' '+'<small>('+str(round(df.loc[x]['geolocated_percent_wp'],2))+'%)</small>'

    df['keyword_title'] = df.keyword_title.astype(str)
    for x in df.index.values.tolist(): 
        df.at[x, 'keyword_title'] = ' '+str('{:,}'.format(int(df.loc[x]['keyword_title'])))+' '+'<small>('+str(round(100*int(df.loc[x]['keyword_title'])/wikipedialanguage_numberarticles[x],2))+'%)</small>'

    df['male_ccc'] = df.male_ccc.astype(str)
    df['female_ccc'] = df.female_ccc.astype(str)
    df['people_ccc_percent'] = df.people_ccc_percent.astype(str)

    female_male_CCC={}
    for x in df.index.values.tolist():
        sumpeople = int(df.loc[x]['male_ccc'])+int(df.loc[x]['female_ccc'])
        if sumpeople != 0:
            female_male_CCC[x] = str(round(100*int(df.loc[x]['female_ccc'])/sumpeople,1))+'%\t-\t'+str(round(100*int(df.loc[x]['male_ccc'])/sumpeople,1))+'%'

            df.at[x, 'people_ccc_percent'] = ' '+str('{:,}'.format(sumpeople)+' '+'<small>('+str(df.loc[x]['people_ccc_percent']))+'%)</small>'

            df.at[x, 'male_ccc'] = str(round(100*int(df.loc[x]['male_ccc'])/sumpeople,2))
            df.at[x, 'female_ccc'] = str(round(100*int(df.loc[x]['female_ccc'])/sumpeople,2))
        else:
            female_male_CCC[x] = '0.0%'+'\t-\t'+'0.0%'


    df['female-male_ccc'] = pd.Series(female_male_CCC)


    df['Region']=languages.region
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

    df['Subregion']=languages.subregion
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

    # PREPARING THE DATAFRAME
    # Renaming the columns
    columns_dict = {'wp_number_articles':'Articles','ccc_number_articles':'CCC (%)','geolocated_number_articles':'CCC GL (%)','country_wd':'Country WD','location_wd':'Location WD', 'language_strong_wd':'Language Strong WD', 'created_by_wd':'Created by WD', 'part_of_wd':'Part of WD', 'keyword_title':'KW Title (%)', 'category_crawling_territories':'Category Crawling Territories', 'language_weak_wd':'Language Weak WD', 'affiliation_wd':'Affiliation WD', 'has_part_wd':'Has Part WD', 'num_inlinks_from_CCC':'Inlinks from CCC','num_outlinks_to_CCC':'Outlinks from CCC','percent_inlinks_from_CCC':'Inlinks from CCC %', 'percent_outlinks_to_CCC':'Outlinks from CCC%', 'average_num_retrieval_strategies':'Avg. Num Retrieval Strategies', 'date_created':'Avg. Date Created', 'female':'Num. Female','male':'Num. Male', 'female_ccc':'CCC Female %','male_ccc':'CCC Male %', 'female-male_ccc':'CCC Female-Male %','people_ccc_percent':'CCC People (%)','measurement_date':'Measurement Date'}
    df=df.rename(columns=columns_dict)



    # HTML
    WPlanguagearticle={}
    for x in df.index.values: WPlanguagearticle[x]='<a href="'+languages.loc[x]['WikipedialanguagearticleEnglish']+'">'+x.replace('_','-')+'</a>'
    df['Wiki'] = pd.Series(WPlanguagearticle)

    languagelink={}
    for x in df.index.values: languagelink[x]='<a href="https://'+x+'.wikipedia.org/wiki/">'+languages.loc[x]['languagename']+'</a>'
    df['Language'] = pd.Series(languagelink)

    # Choosing the final columns
    columns = ['Nº','Language','Wiki','Articles','CCC (%)','CCC GL (%)','KW Title (%)','CCC People (%)','CCC Female-Male %','Region']

    df = df[columns] # selecting the parameters to export

    os.makedirs(current_web_path + wiki_path, exist_ok=True)
    filename = 'ccc_extent_all_languages_table'+'.html'
    file_path = current_web_path + wiki_path + '/' + filename
#    print (file_path)

    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)

    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)

    pd.set_option('display.max_colwidth', old_width)


    # Wikitext
    WPlanguagearticle={}
    for x in df.index.values: WPlanguagearticle[x]='[[:'+x.replace('_','-')+':|'+x.replace('_','-')+']]'
    df['Wiki'] = pd.Series(WPlanguagearticle)
    languagelink={}
    for x in df.index.values:
#        print (languages.loc[x]['WikipedialanguagearticleEnglish'].split('/'))
        languagelink[x]='[[w:'+languages.loc[x]['WikipedialanguagearticleEnglish'].split('/')[4].replace('_',' ')+'|'+languages.loc[x]['languagename']+']]'
    df['Language'] = pd.Series(languagelink)

    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'


    dict_data_type = {'CCC (%)':'data-sort-type="number"|','CCC GL (%)':'data-sort-type="number"|','KW Title (%)':'data-sort-type="number"|','CCC People (%)':'data-sort-type="number"|','CCC Female-Male %':'data-sort-type="number"|'}

    header_string = '!'
    for x in range(0,len(df_columns_list)):
        if x == len(df_columns_list)-1: add = ''
        else: add = '!!'
        data_type = ''
        if df_columns_list[x] in dict_data_type: data_type = ' '+dict_data_type[df_columns_list[x]]
        header_string = header_string + data_type + df_columns_list[x] + add

    header_string = header_string + '\n'


    rows = ''
    for row in df_rows:
        midline = '|-\n'
        row_string = '|'
        for x in range(0,len(row)):
            if x == len(row)-1: add = ''
            else: add = '||'
            value = row[x]
            row_string = row_string + str(value) + add # here is the value

            # here we might add colors.
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    print ('* dataframe and html file created.\n')

    return wikitext


    # project site page WCDO for each language
#   QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?
def generate_ccc_extent_qitem_table_by_language(languagecode, page_titles_qitems, qitems_page_titles_english, wiki_path):

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    query = 'SELECT * FROM ccc_extent_qitem WHERE measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem) AND languagecode = "'+languagecode+'" ORDER BY ccc_articles_qitem_percent_ccc DESC;'
#    print (query)
    df = pd.read_sql_query(query, conn)
    df = df.set_index(['qitem'])
    print ('This the number of territories for language '+languagecode+': '+str(len(df)))
    if len(df)==0: return

    measurement_date = df.loc[df.index.values[0]]['measurement_date']

    # FORMAT THE DATA
    territories_adapted=territories.loc[languagecode]
    territories_adapted=territories.drop(territories[territories.index!=languagecode].index)
    territories_adapted=territories_adapted.set_index(['QitemTerritory'])
    territories_adapted=territories_adapted.rename(index={'QitemTerritory':'qitem'})

#    df['Territory Name (local)']=territories_adapted.territorynameNative
#    df['Territory Name']=territories_adapted.territoryname
    df['country']=territories_adapted.country
#    df['ISO3166']=territories_adapted.ISO3166
#    df['ISO3166-2']=territories_adapted.ISO31662
    df['Subregion']=territories_adapted.subregion
    df['Region']=territories_adapted.region

#    df['wp_number_articles'] = df.wp_number_articles.astype(str)
#    for x in df.index.values.tolist(): df.at[x, 'wp_number_articles'] = '{:,}'.format(int(df.loc[x]['wp_number_articles']))

    df['ccc_articles_qitem'] = df.ccc_articles_qitem.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'ccc_articles_qitem'] = ' '+str('{:,}'.format(int(df.loc[x]['ccc_articles_qitem'])))+' '+'<small>('+str(df.loc[x]['ccc_articles_qitem_percent_ccc'])+'%)</small>'


    df['geolocated_number_articles'] = df.geolocated_number_articles.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'geolocated_number_articles'] = '{:,}'.format(int(df.loc[x]['geolocated_number_articles']))

    df['keyword_title'] = df.keyword_title.astype(str)
    for x in df.index.values.tolist(): df.at[x, 'keyword_title'] = '{:,}'.format(int(df.loc[x]['keyword_title']))

    rank_dict={}
    i=1
    for x in list(df.index.values):
        rank_dict[x]=i
        i=i+1
    df['Nº'] = pd.Series(rank_dict)

    # PREPARING THE DATAFRAME
    # Renaming the columns
    columns_dict = {'wp_number_articles':'WP','ccc_articles_qitem':'CCC','ccc_articles_qitem_percent_ccc':'CCC %','geolocated_number_articles':'CCC GL','country_wd':'Country WD','location_wd':'Location WD', 'language_strong_wd':'Language Strong WD', 'created_by_wd':'Created by WD', 'part_of_wd':'Part of WD', 'keyword_title':'KW Title', 'category_crawling_territories':'Category Crawling Territories', 'language_weak_wd':'Language Weak WD', 'affiliation_wd':'Affiliation WD', 'has_part_wd':'Has Part WD', 'num_inlinks_from_CCC':'Inlinks from CCC','num_outlinks_to_CCC':'Outlinks from CCC','percent_inlinks_from_CCC':'Avg. Percent Incoming Links from CCC', 'percent_outlinks_to_CCC':'Outlinks from CCC%', 'average_num_retrieval_strategies':'Avg. Num. Retrieval Strategies', 'date_created':'Avg. Date Created','measurement_date':'Measurement Date'}
    df=df.rename(columns=columns_dict)

    # HTML
    languagelink={}
    for x in df.index.values:
        if x != None: languagelink[x]='<a href="https://www.wikidata.org/wiki/'+x+'">'+x+'</a>'
        else: languagelink[x]=''
    df['Qitem'] = pd.Series(languagelink)

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

#    # Choosing the final columns
    columns = ['Nº','Territory Name','Territory Name (local)','Qitem','CCC','CCC Geolocated','Keywords Title','country','Subregion']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    os.makedirs(current_web_path + wiki_path, exist_ok=True)
    filename = languagecode +'_'+'ccc_extent_qitem_table_by_language_' + measurement_date + '.html'
    file_path = current_web_path + wiki_path + '/' + filename
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)
    pd.set_option('display.max_colwidth', old_width)


    # Wikitext
    languagelink={}
    for x in df.index.values:
        if x != None: languagelink[x]='[[wikidata:'+x+'|'+x+']]'
        else: languagelink[x]=''
    df['Qitem'] = pd.Series(languagelink)

    territoryname={}
    for x in df.index.values:
        if x != None and x in qitems_page_titles_english: 
            territoryname[x]='[[w:'+qitems_page_titles_english[x]+'|'+qitems_page_titles_english[x].replace('_',' ')+']]'
        elif x != None: territoryname[x]=territories.loc[territories['QitemTerritory'] == x].loc[languagecode]['territoryname']
        else: territoryname[x]='Not Assigned'
    df['Territory Name'] = pd.Series(territoryname)

    territoryname_local={}
    qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}
    print (df.index.values)
    for x in df.index.values:
        print (x)
        if x != None and x in qitems_page_titles:
            try: territoryname_local[x]='[{{fullurl:'+languagecode+':'+qitems_page_titles[x]+'}} '+territories.loc[territories['QitemTerritory'] == x].loc[languagecode]['territorynameNative']+']'
            except: print ('error.'); continue
        elif x != None: territoryname[x]=territories.loc[territories['QitemTerritory'] == x].loc[languagecode]['territorynameNative']
        else: territoryname_local[x]='Not Assigned'
    df['Territory Name (local)'] = pd.Series(territoryname_local)

    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'CCC':'data-sort-type="number"|'}

    header_string = '!'
    for x in range(0,len(df_columns_list)):
        if x == len(df_columns_list)-1: add = ''
        else: add = '!!'
        data_type = ''
        if df_columns_list[x] in dict_data_type: data_type = ' '+dict_data_type[df_columns_list[x]]
        header_string = header_string + data_type + df_columns_list[x] + add

    header_string = header_string + '\n'

    rows = ''
    for row in df_rows:
        midline = '|-\n'
        row_string = '|'
        for x in range(0,len(row)):
            if x == len(row)-1: add = ''
            else: add = '||'
            value = row[x]
            row_string = row_string + str(value) + add # here is the value

            # here we might add colors.
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    print ('* dataframe and html file created.\n')

    return wikitext



def generate_ccc_culture_gap_table(wiki_path, spread_coverage):

    print ('* generate_ccc_culture_gap_table')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    if spread_coverage == 'spread': # SPREAD

        #   QUESTION: How well each language edition CCC is spread in other language editions?
        # TABLE COLUMN (spread):
        # language, CCC%, RANKING TOP 5, relative spread index, total spread index, spread articles sum.
        # relative spread index: the average of the percentages it occupies in other languages.
        # total spread index: the overall percentage of spread of the own CCC articles. (sum of x-lang CCC in every language / sum of all articles in every language)
        # spread articles sum: the number of articles from this language CCC in all languages.

        conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

        language_dict = {}
        for languagecode in wikilanguagecodes:
            print (languagecode)
            query = "SELECT ccc_percent_wp FROM ccc_extent_language WHERE languagecode = ?"
            cursor.execute(query,(languagecode,))
            ccc_percent_wp=cursor.fetchone()[0]

            query = "SELECT languagecode_covering, number_articles, percentage FROM ccc_gaps WHERE languagecode_covered='"+languagecode+"' AND group_type='ccc' AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_gaps WHERE group_type='ccc') AND reference='share' ORDER BY percentage DESC"

            ranking = 5
            i = 1
            spread_articles_sum=0
            sum_percentage=0
            row_dict = {}
            total_number_articles=0
            for row in cursor.execute(query):
                languagecode_covering=row[0]
                number_articles=row[1]
                percentage=row[2]
                spread_articles_sum += number_articles
                sum_percentage += percentage
                total_number_articles += wikipedialanguage_numberarticles[languagecode_covering]

                if i <= ranking:
                    value = languagecode_covering + '<small>('+str(percentage)+'%)</small>'
                    row_dict[str(i)]=value
                i+=1

            relative_spread_index = round(sum_percentage/i,2)
            if total_number_articles!=0: total_spread_index = round(spread_articles_sum/total_number_articles,2)
            else: total_spread_index=0

            row_dict['language']=languages.loc[languagecode]['languagename']
            row_dict['CCC%']=ccc_percent_wp
            row_dict['relative_spread_index']=relative_spread_index
            row_dict['total_spread_index']=total_spread_index
            row_dict['spread_articles_sum']=spread_articles_sum

            language_dict[languagecode]=row_dict

            create_db_table_ccc_indexs(languagecode, 'relative_spread_index', relative_spread_index)
            create_db_table_ccc_indexs(languagecode, 'total_spread_index', total_spread_index)
            create_db_table_ccc_indexs(languagecode, 'spread_articles_sum', spread_articles_sum)

        column_list_dict = {'language':'Language', 'CCC%':'CCC %','1':'1','2':'2','3':'3','4':'4','5':'5','relative_spread_index':'Relative Spread Idx','total_spread_index':'Total Spread Idx','spread_articles_sum':'Spread Articles Sum'}

        column_list = ['Language','CCC %','1','2','3','4','5','Relative Spread Idx','Total Spread Idx','Spread Articles Sum']

        # HTML (from dataframe)
        df=pd.DataFrame.from_dict(language_dict,orient='index')

        df=df.rename(columns=column_list_dict)

        df = df[column_list] # selecting the parameters to export
        df = df.fillna('')

        df_columns_list = df.columns.values.tolist()
        df_rows = df.values.tolist()


        # WIKITEXT
        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        dict_data_type = {'CCC':'data-sort-type="number"|'}

        header_string = '!'
        for x in range(0,len(column_list)):
            if x == len(column_list)-1: add = ''
            else: add = '!!'
            data_type = ''
            if df_columns_list[x] in dict_data_type: data_type = ' '+dict_data_type[df_columns_list[x]]

            header_string = header_string + data_type + column_list[x] + add
        header_string = header_string + '\n'

        rows = ''
        for row in df_rows:
            midline = '|-\n'
            row_string = '|'

            for x in range(0,len(row)):
                if x == len(row)-1: add = ''
                else: add = '||'
                value = row[x]
                if value == '': value = 0
                print (value)
                print ('ah')

                color = ''
                if x > 1 and x < 7:
                    scale = 10
                    color1 = '#cc0000' # red
                    color2 = '#339933' # green
                    colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, int(value))
                    color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.

                    row_string = row_string + color + str(value) + add # here is the value
                  
            row_string = midline + row_string + '\n'
            rows = rows + row_string
        closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string



    else: # COVERAGE
        
        # QUESTION: How well each language edition covers the CCC of each other language edition?
        # TABLE COLUMNS (coverage):
        # language, CCC%, RANKING TOP 5, relative coverage index, total coverage index, covered articles sum.
        # relative coverage index: the average percentage of the coverage of other languages.
        # total coverage index: the overall percentage of coverage of other CCC articles.
        # covered articles sum: the number of articles from other languages CCC.

        ccc_articles_dict={}
        for languagecode in wikilanguagecodes:
            query = "SELECT ccc_number_articles FROM ccc_extent_language WHERE languagecode = ?;"
            cursor.execute(query,(languagecode,))
            ccc_number_articles=cursor.fetchone()[0]
            ccc_articles_dict[languagecode]=ccc_number_articles


        for languagecode in wikilanguagecodes:
            query = "SELECT languagecode_covered, number_articles, percentage FROM ccc_gaps WHERE languagecode_covering='"+languagecode+"' AND group_type='ccc' AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_gaps WHERE group_type='ccc') AND reference='gap' ORDER BY percentage DESC";

            ranking = 5
            i = 1
            coverage_articles_sum=0
            sum_percentage=0
            row_dict = {}
            for row in cursor.execute(query):               
                languagecode_covered=row[0]
                number_articles=row[1]
                percentage=row[2]
                coverage_articles_sum += number_articles
                sum_percentage += percentage
                total_number_articles += ccc_articles_dict[languagecode_covered]

                if i <= ranking:
                    value = languagecode_covered + '<small>('+str(percentage)+'%)</small>'
                    row_dict[str(i)]=value
                i+=1

            relative_coverage_index = round(sum_percentage/i,2)
            total_coverage_index = round(spread_articles_sum/total_number_articles,2)

            row_dict['language']=languages.loc[languagecode]['languagename']
            row_dict['WP articles']=wikipedialanguage_numberarticles[languagecode]
            row_dict['relative_coverage_index']=relative_coverage_index
            row_dict['total_coverage_index']=total_coverage_index
            row_dict['coverage_articles_sum']=coverage_articles_sum

            language_dict[languagecode]=row_dict

            create_db_table_ccc_indexs(languagecode, 'relative_coverage_index', relative_coverage_index)
            create_db_table_ccc_indexs(languagecode, 'total_coverage_index', total_coverage_index)
            create_db_table_ccc_indexs(languagecode, 'coverage_articles_sum', coverage_articles_sum)


        columns_list_dict = {'language':'Language', 'WP articles':'WP articles','1':'1','2':'2','3':'3','4':'4','5':'5','relative_coverage_index':'Relative Coverage Idx','total_coverage_index':'Total Coverage Idx','coverage_articles_sum':'Coverage Articles Sum'}
        column_list = ['Language','WP articles','1','2','3','4','5','Relative Coverage Idx.','Total Coverage Idx.','Coverage Articles Sum']

        # HTML (from dataframe)
        df=pd.DataFrame([language_dict])
        df=df.rename(columns=columns_list_dict)
        df = df[column_list] # selecting the parameters to export
        df = df.fillna('')

        # WIKITEXT
        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        dict_data_type = {'CCC':'data-sort-type="number"|'}

        header_string = '!'
        for x in range(0,len(column_list)):
            if x == len(column_list)-1: add = ''
            else: add = '!!'
            data_type = ''

            header_string = header_string + data_type + column_list[x] + add
        header_string = header_string + '\n'


        rows = ''
        for languagecode in sorted(language_dict.items(), key = lambda x: x[0], reverse=False):

            row = []
            row_dict = language_dict[languagecode]
            for x in column_list: 
                row.append(row_dict[x])
                midline = '|-\n'
                row_string = '|'

                for x in range(0,len(row)):
                    if x == len(row)-1: add = ''
                    else: add = '||'
                    value = row[x]

                    color = ''
                    if x > 1 and x < 7:
                        scale = 10
                        color1 = '#cc0000' # red
                        color2 = '#339933' # green
                        colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, value)
                        color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.

                    row_string = row_string + color + str(value) + add # here is the value
                  
                row_string = midline + row_string + '\n'
                rows = rows + row_string
            closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string


    # CREATE THE FILE IN HTML
    filename = languagecode+'_'+spread_coverage+'_'+'ccc_all_vital_articles_lists_table'+'.html'
    file_path = current_web_path + wiki_path + filename
    os.makedirs(current_web_path + wiki_path, exist_ok=True)
    file_path = current_web_path + wiki_path + '/' + filename
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)
    pd.set_option('display.max_colwidth', old_width)

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext




def generate_ccc_all_vital_articles_lists_table(languagecode, wiki_path, spread_coverage):

    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    if spread_coverage == 'spread': # SPREAD

        # QUESTION: Which of these CCC articles is or should be available in other language editions?
        # SPREAD: how well the lists of this language are spread into other languages?
        # TABLE COLUMN (spread).
        # Language Covering, Wiki, Editors 1000, Editors 100, Featured, Geolocated, Keywords, First 3Y, Last 3M, Women, Men, Pageviews, Talk Edits, Lists Spread Index, Spread articles sum.
        # Lists Spread Index is the percentage of articles from this language lists spread in other languages. It must be computed here.

        language_covered = languagecode

        query = "SELECT list_name, languagecode_covering, percentage FROM vital_articles_lists_gaps WHERE languagecode_covered=? AND measurement_date IN (SELECT MAX(measurement_date) FROM vital_articles_lists_gaps) GROUP BY list_name DESC ORDER BY languagecode_covering";

        row_dict = {}
        language_dict = {}
        current_languagecode_covering = ''
        for row in cursor.execute(query,(language_covered,)):
            languagecode_covering = row[1]

            if current_languagecode_covering!=languagecode_covering and current_languagecode_covering!='':
                spread_list_articles_sum=0
                for x,y in row_dict.items(): spread_list_articles_sum += y
                list_spread_index = round(spread_list_articles_sum/1000,2) # if we took into account the list of editors 1000, the number would be 2000.

                row_dict['languagename']=languages.loc[languagecode_covering]['languagename']
                row_dict['Wiki']=languagecode_covering
                row_dict['list_spread_index']=list_spread_index
                row_dict['spread_list_articles_sum']=spread_list_articles_sum

                language_dict[languagecode_covering]=row_dict
                row_dict={}

                create_db_table_ccc_indexs(language_covered, 'list_spread_index', list_spread_index)
                create_db_table_ccc_indexs(language_covered, 'spread_list_articles_sum', spread_list_articles_sum)

            percentage = row[2]
            list_name = row[0]
            row_dict[list_name]=percentage

            current_languagecode_covering = languagecode_covering


        column_list_dict = {'languagename':'Language','Wiki':'Wiki','CCC_Vital_articles_Top_1000':'Editors 1000', 'CCC_Vital_articles_Top_100':'Editors 100', 'CCC_Vital_articles_featured':'Featured', 'CCC_Vital_articles_geolocated':'Geolocated', 'CCC_Vital_articles_keywords':'Keywords', 'CCC_Vital_articles_first_years':'First 3Y', 'CCC_Vital_articles_last_quarter':'Last 3M', 'CCC_Vital_articles_women':'Women', 'CCC_Vital_articles_men':'Men', 'CCC_Vital_articles_pageviews':'Pageviews', 'CCC_Vital_articles_discussions':'Talk Edits','list_spread_index':'List Spread Idx.','spread_list_articles_sum':'Sum Spread Articles'}

        column_list = ['languagename','Wiki','CCC_Vital_articles_Top_1000','CCC_Vital_articles_Top_100','CCC_Vital_articles_featured','CCC_Vital_articles_geolocated','CCC_Vital_articles_keywords','CCC_Vital_articles_first_years','CCC_Vital_articles_last_quarter','CCC_Vital_articles_women','CCC_Vital_articles_men','CCC_Vital_articles_pageviews','CCC_Vital_articles_discussions','list_spread_index','spread_list_articles_sum']

        # HTML (from dataframe)
        df=pd.Dataframe.from_dict(language_dict) # I don't know what is the index here...
        df=df.rename(columns=columns_dict)
        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        # WIKITEXT
        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        dict_data_type = {'CCC':'data-sort-type="number"|'}

        header_string = '!'
        for x in range(0,len(column_list)):
            if x == len(column_list)-1: add = ''
            else: add = '!!'
            data_type = ''

            header_string = header_string + data_type + column_list_dict[column_list[x]] + add
        header_string = header_string + '\n'


        rows = ''
        for languagecode in sorted(language_dict.items(), key = lambda x: x[0], reverse=False):

            row = []
            row_dict = language_dict[languagecode]
            for x in column_list: 
                row.append(row_dict[x])
                midline = '|-\n'
                row_string = '|'

                for x in range(0,len(row)):
                    if x == len(row)-1: add = ''
                    else: add = '||'
                    value = row[x]
                    if value == '': value = 0

                    # MISSING:
#                    all vitat_articles_list_table falta posar els links per cada número a la llengua concreta.

                    color = ''
                    if x > 1 and x < 13:
                        scale = 10
                        color1 = '#cc0000' # red
                        color2 = '#339933' # green
                        colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, value)
                        color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.

                    row_string = row_string + color + str(value) + add # here is the value
                  
                row_string = midline + row_string + '\n'
                rows = rows + row_string
            closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string


    else: # COVERAGE
        language_covering = languagecode

        # QUESTION: How well does this language edition cover the lists of CCC articles from other language editions?
        # COVERAGE: how well this language covers the lists from other languages?
        # TABLE COLUMN (coverage).
        # Language Covered, Wiki, Editors 100, Editors 1000, Featured, Geolocated, Keywords, First 3Y, Last 3M, Women, Men, Pageviews, Talk Edits, Lists Coverage Index, Covered articles sum.
        # Lists Coverage Index is the percentage of articles covered from these lists.

        query = "SELECT list_name, languagecode_covered, percentage FROM vital_articles_lists_gaps WHERE languagecode_covering=? AND measurement_date IN (SELECT MAX(measurement_date) FROM vital_articles_lists_gaps) GROUP BY list_name DESC ORDER BY languagecode_covered";

        row_dict = {}
        language_dict = {}
        current_languagecode_covered = ''
        for row in cursor.execute(query,(language_covering,)):
            languagecode_covered = row[1]

            if current_languagecode_covered!=languagecode_covered and current_languagecode_covered!='':
                covered_list_articles_sum=0
                for x,y in row_dict.items(): covered_list_articles_sum += y
                list_coverage_index = round(covered_list_articles_sum/1000,2) # if we took into account the list of editors 1000, the number would be 2000.
                row_dict['languagename']=languages.loc[language_covered]['languagename']
                row_dict['Wiki']=language_covered
                row_dict['list_coverage_index']=list_coverage_index
                row_dict['covered_list_articles_sum']=covered_list_articles_sum

                language_dict[languagecode_covering]=row_dict
                row_dict={}

                create_db_table_ccc_indexs(language_covering, 'list_coverage_index', list_coverage_index)
                create_db_table_ccc_indexs(language_covering, 'covered_list_articles_sum', covered_list_articles_sum)

            percentage = row[2]
            list_name = row[0]
            row_dict[list_name]=percentage

            current_languagecode_covered = languagecode_covered


        column_list_dict = {'languagename':'Language','Wiki':'Wiki','CCC_Vital_articles_Top_1000':'Editors 1000', 'CCC_Vital_articles_Top_100':'Editors 100', 'CCC_Vital_articles_featured':'Featured', 'CCC_Vital_articles_geolocated':'Geolocated', 'CCC_Vital_articles_keywords':'Keywords', 'CCC_Vital_articles_first_years':'First 3Y', 'CCC_Vital_articles_last_quarter':'Last 3M', 'CCC_Vital_articles_women':'Women', 'CCC_Vital_articles_men':'Men', 'CCC_Vital_articles_pageviews':'Pageviews', 'CCC_Vital_articles_discussions':'Talk Edits','list_coverage_index':'List Coverage Idx.','covered_list_articles_sum':'Sum Covered Articles'}

        column_list = ['languagename','Wiki','CCC_Vital_articles_Top_1000','CCC_Vital_articles_Top_100','CCC_Vital_articles_featured','CCC_Vital_articles_geolocated','CCC_Vital_articles_keywords','CCC_Vital_articles_first_years','CCC_Vital_articles_last_quarter','CCC_Vital_articles_women','CCC_Vital_articles_men','CCC_Vital_articles_pageviews','CCC_Vital_articles_discussions','list_coverage_index','covered_list_articles_sum']

        # HTML (from dataframe)
        df=pd.Dataframe.from_dict(language_dict)
        df=df.rename(columns=columns_dict)
        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        # WIKITEXT
        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        dict_data_type = {'CCC':'data-sort-type="number"|'}

        header_string = '!'
        for x in range(0,len(column_list)):
            if x == len(column_list)-1: add = ''
            else: add = '!!'
            data_type = ''

            header_string = header_string + data_type + column_list_dict[column_list[x]] + add
        header_string = header_string + '\n'

        rows = ''
        for languagecode in sorted(language_dict.items(), key = lambda x: x[0], reverse=False):

            row = []
            languagename = languages.loc[languagecode]['languagename']
            row.append(languagename)
            row.append(langaugecode)

            row_dict = language_dict[languagecode]
            for x in column_list: 
                row.append(row_dict[x])
                midline = '|-\n'
                row_string = '|'

                for x in range(0,len(row)):
                    if x == len(row)-1: add = ''
                    else: add = '||'
                    value = row[x]
                    if value == '': value = 0

                    # MISSING:
#                    all vitat_articles_list_table falta posar els links per cada número a la llengua concreta.

                    color = ''
                    if x > 1 and x < 13:
                        scale = 10
                        color1 = '#cc0000' # red
                        color2 = '#339933' # green
                        colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, value)
                        color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.

                    row_string = row_string + color + str(value) + add # here is the value
                  
                row_string = midline + row_string + '\n'
                rows = rows + row_string
            closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string

    # CREATE THE FILE IN HTML
    filename = languagecode+'_'+spread_coverage+'_'+'ccc_all_vital_articles_lists_table'+'.html'
    file_path = current_web_path + wiki_path + filename
    os.makedirs(current_web_path + wiki_path, exist_ok=True)
    file_path = current_web_path + wiki_path + '/' + filename
    print (file_path)

    # Exporting to HTML
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    html_str=HTML(df.to_html(index=False,escape=False))
    html = html_str.data
    with open(file_path, 'w', encoding='utf-8') as f: f.write(html)
    pd.set_option('display.max_colwidth', old_width)

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string
    
    return wikitext


#   QUESTION: Which articles from each CCC should be available in other language editions?
# It creates a table and a list of top 100, top 1000 articles according to different criteria.
def generate_ccc_vital_articles_list_table(languagecode, languagecode_target, content_type, category, percentage_filtered, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns, page_titles_qitems, wiki_path, list_name):

#    list_name=wiki_path.split('/')[2]
    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('\n\n* generate_ccc_vital_articles_list_table')
    print (list_name)
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
        timestamp = str(cursor.fetchone()[0])
        print (timestamp)
        first_three_years=(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S') + datetime.timedelta(days=3*365)).strftime('%Y%m%d%H%M%S')
        print (first_three_years)
        query = query + ' AND date_created < '+str(first_three_years)


    if time_frame == 'first_five_years':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = str(cursor.fetchone()[0])
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
    print ('filter and save the first '+str(percentage_filtered)+'% of the CCC articles in terms of number of strategies and inlinks from CCC.')

    ranked_saved_1=ccc_df['num_inlinks_from_CCC'].sort_values(ascending=False).index.tolist()
    ranked_saved_1=ranked_saved_1[:int(percentage_filtered*len(ranked_saved_1)/100)]

    ranked_saved_2=ccc_df['num_retrieval_strategies'].sort_values(ascending=False).index.tolist()
    ranked_saved_2=ranked_saved_2[:int(percentage_filtered*len(ranked_saved_2)/100)]

    intersection = list(set(ranked_saved_1)&set(ranked_saved_2))

    ccc_df = ccc_df.reindex(index = intersection)
    print ('There are now: '+str(len(ccc_df))+' articles.')


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
        conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()
        query = 'SELECT qitem, ccc_articles_qitem FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'" AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'");'
#        print (query)
        sum = 0 
        for row in cursor2.execute(query):
            main_territory = row[0]
            if main_territory == None: main_territory = 'Any'
            representativity_coefficients[main_territory]=row[1]
            sum = sum + row[1]
        for x,y in representativity_coefficients.items():
            representativity_coefficients[x]=y/sum

        if representativity == 'proportional_articles_compensation':
            for x,y in representativity_coefficients.items():
                if y < 0.02:
                    diff = 0.02 - representativity_coefficients[x]
                    representativity_coefficients[x]=0.02
                    representativity_coefficients['Any']=representativity_coefficients['Any']-diff

    if representativity == 'proportional_ccc_rellevance': # proportional to the rellevance of each qitem.
        # check data from: ccc_extent_by_qitems. number of inlinks from CCC.
        total_inlinks = 0
        for qitem in ccc_df.index:
            inlinks = ccc_df.loc[qitem]['num_inlinks_from_CCC']; 
            main_territory = ccc_df.loc[qitem]['main_territory']
            if main_territory == 0: main_territory = 'Any'

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
        print ('Any has: '+str(representativity_articles['Any']))
        if window > z: representativity_articles['Any']=representativity_articles['Any']+(window-z)
        z=0
        for x,y in representativity_articles.items(): z=z+y        
        print ('the window has: '+str(z))
        print ('Any has: '+str(representativity_articles['Any']))

        for x in sorted(representativity_articles,reverse=True):
            y = representativity_articles[x]
            print (x,y)
            if y == 0: continue

            todelete = []
            for qitem in articles_ranked:
                main_territory = ccc_df.loc[qitem]['main_territory']
#                print (ccc_df.loc[qitem]['page_title'],main_territory,x)

                if main_territory == x or x == 'Any':
#                    input('')
#                    print (main_territory)

                    if main_territory != 0 and main_territory in qitems_territories_names: territory = qitems_territories_names[main_territory]
                    else: territory = 'None'

                    print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],
                    '\t\t\t\t\t'+str(list(rellevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[0]]),
#                    '\t'+str(list(rellevance_rank.keys())[1])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[1]]),
#                    '\t'+str(list(rellevance_rank.keys())[2])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[2]]),
                    '\trank:'+str(rank_dict[qitem]),
                    qitem,territory,main_territory,x); #input('')

                    prioritized_list.append(qitem)
                    todelete.append(qitem)
                    i=i+1
                    y = y - 1 # countdown

                if y == 0 or y < 1:
                    print ('* one type is filled: '+x)
                    break

            print ('. articles_ranked iteration .')

            if len(todelete) == 0 or len(todelete)<=y:
                error = 'No articles for the territory: '+str(x)+' so we took articles from the top of the ranking to fill the gap.'
                if len(articles_ranked)>y:
                    for x in range(0,y):
                        i=i+1
                        qitem = articles_ranked[x]
                        prioritized_list.append(qitem)
                        todelete.append(qitem)
                        print (y,ccc_df.loc[qitem]['page_title'],rank_dict[qitem]); #input('')

            for qitem in todelete: articles_ranked.remove(qitem)

#        print ('* one window filled.')
    ccc_df=ccc_df.reindex(prioritized_list)
#    print (error)
    print (len(ccc_df))
#    print (prioritized_list[:100])



##### CHECKING ARTICLES CROSS-LANGUAGE AVAILABILITY AND INTRODUCING THE ARTICLES INTO THE TABLE CCC_GAPS #####
    
    articles_lang_dict={}
    lang_title_dict={}
    count_covered_articles_langs={}
    mysql_con_read = establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    qitems = prioritized_list
    for qitem in qitems:
        page_id=ccc_df.loc[qitem]['page_id']
        query = "SELECT ll_lang, ll_title FROM langlinks WHERE ll_from=%s"
        mysql_cur_read.execute(query,(page_id,))
        result = mysql_cur_read.fetchall()
        for row in result:
            lang = row[0].decode('utf-8')
            title = str(row[1].decode('utf-8')).replace('_',' ')
            lang_title_dict[lang]=title
            if lang in count_covered_articles_langs: count_covered_articles_langs[lang]=count_covered_articles_langs[lang]+1
            else: count_covered_articles_langs[lang]=1
        articles_lang_dict[qitem]=lang_title_dict


    for lang,count in count_covered_articles_langs.items():
        create_db_table_ccc_lists_gaps(count, languagecode, lang, round(100*count/window,2), list_name)
        print ((languagecode, lang, round(100*count/window,2), count, list_name))
    print ('* list introduced into vital_articles_lists at the database wcdo_data.db')


##### EXPORTING THE DATA AS HTML AND WIKIMARKUP #####

    # REFORMATTING THE DATASET
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
    ccc_df['num_editors'] = ccc_df.num_editors.astype(int); ccc_df['num_editors'] = ccc_df.num_editors.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_editors'] = '{:,}'.format(int(ccc_df.loc[x]['num_editors']))

    # Change edits format:
    ccc_df['num_edits'] = ccc_df.num_edits.astype(int); ccc_df['num_edits'] = ccc_df.num_edits.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_edits'] = '{:,}'.format(int(ccc_df.loc[x]['num_edits']))

    # Change discussion edits format:
    ccc_df['num_discussions'] = ccc_df.num_discussions.astype(int); ccc_df['num_discussions'] = ccc_df.num_discussions.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_discussions'] = '{:,}'.format(int(ccc_df.loc[x]['num_discussions']))

    # Change pageviews format:
    ccc_df['num_pageviews'] = ccc_df.num_pageviews.astype(int); ccc_df['num_pageviews'] = ccc_df.num_pageviews.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_pageviews'] = '{:,}'.format(int(ccc_df.loc[x]['num_pageviews']))

    # Change Retrieval strategies format:
    ccc_df['num_retrieval_strategies'] = ccc_df.num_retrieval_strategies.astype(int); ccc_df['num_retrieval_strategies'] = ccc_df.num_retrieval_strategies.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_retrieval_strategies'] = int(ccc_df.loc[x]['num_retrieval_strategies'])

    # Change Inlinks from CCC format:
    ccc_df['num_inlinks_from_CCC'] = ccc_df.num_inlinks_from_CCC.astype(int); ccc_df['num_inlinks_from_CCC'] = ccc_df.num_inlinks_from_CCC.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_inlinks_from_CCC'] = '{:,}'.format(int(ccc_df.loc[x]['num_inlinks_from_CCC']))

    # Change Bytes format:
    ccc_df['num_bytes'] = ccc_df.num_bytes.astype(int); ccc_df['num_bytes'] = ccc_df.num_bytes.astype(str); 
    for x in ccc_ranked: ccc_df.at[x, 'num_bytes'] = '{:,}'.format(int(ccc_df.loc[x]['num_bytes']))

    # Change references format:
    ccc_df['num_references'] = ccc_df.num_references.astype(int); ccc_df['num_references'] = ccc_df.num_references.astype(str)
    for x in ccc_ranked: ccc_df.at[x, 'num_references'] = '{:,}'.format(int(ccc_df.loc[x]['num_references']))

    # Change date created format
    for x in ccc_ranked: ccc_df.at[x, 'date_created'] = time.strftime("%Y-%m-%d", time.strptime(ccc_df.loc[x]['date_created'], "%Y%m%d%H%M%S"))

    # Change featured articles
    ccc_df['featured_article'] = ccc_df.featured_article.astype(str)
    for x in ccc_ranked:
        if ccc_df.loc[x]['featured_article']=='0.0': ccc_df.at[x, 'featured_article'] = 'No'
        else: ccc_df.at[x, 'featured_article'] = 'Yes'

    # Adding new columns with the IW address from the closest languages
    closestlanguages = obtain_proximity_wikipedia_languages_lists(languagecode)[2][:5]
    articles_foreignlang_html = {}
    articles_foreignlang_wikitext = {}
    for qitem in ccc_df.index.tolist():
        for lang in closestlanguages:
            page_title=articles_lang_dict[qitem][lang]
            if page_title!=None:

                url = '<a href="https://'+lang+'.wikipedia.org/wiki/'+page_title.replace(' ','_')+'">'+lang+'</a>'
                if qitem not in articles_foreignlang_html: articles_foreignlang_html[qitem]=url
                else: articles_foreignlang_html[qitem]=articles_foreignlang_html[qitem]+', '+url

                url = '<a href="https://'+lang+'.wikipedia.org/wiki/'+page_title.replace(' ','_')+'">'+lang+'</a>'
                if qitem not in articles_foreignlang_wikitext: articles_foreignlang_wikitext[qitem]=url
                else: articles_foreignlang_wikitext[qitem]=articles_foreignlang_wikitext[qitem]+', '+url

        if qitem not in articles_foreignlang_html: articles_foreignlang_html[qitem]=''
        if qitem not in articles_foreignlang_wikitext: articles_foreignlang_wikitext[qitem]=''


    # CREATING ALL THE HTML i WIKITEXT FOR EVERY LANGUAGE
    wiki_tables_dict = {}
    for languagecode_target in wikilanguagecodes:


        # GETTING THE TRANSLATION LINK
        potential_titles_html = {}
        potential_titles_wikitext = {}
        for qitem in prioritized_list:
            if languagecode_target in articles_lang_dict[qitem]:
                translated_title = articles_lang_dict[qitem][languagecode_target]
                url = '<a href="https://'+languagecode_target+'.wikipedia.org/wiki/'+translated_title.replace(' ','_')+'">'+translated_title+'</a>'
                wt_url ='[['+languagecode_target+':'+translated_title+'|'+translated_title+']]'

            else:
                title=ccc_df.loc[qitem]['page_title'] # local title
                r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+languagecode+"/"+languagecode_target+"/Apertium", data={'html': '<div>'+title+'</div>'}) # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                if str(r) == 'Provider not supported': translated_title = ''

                translated_title = r.text.split('<div>')[1].split('</div>')[0].replace(' ','_')
                url = 'https://'+languagecode+'.wikipedia.org/w/index.php?title=Special:ContentTranslation&page='+title.replace('_',' ')+'&from='+languagecode+'&to='+languagecode_target # target_url_translator

                url = 'https://'+languagecode_target+'.wikipedia.org/wiki/Special:ContentTranslation?page='+title.replace('_',' ')+'&from='+languagecode+'&to='+languagecode_target+'&targettitle='+translated_title+'&campaign=interlanguagelink' # target_url_potential_title_translator

                url = 'https://'+languagecode_target+'.wikipedia.org/w/index.php?title='+translated_title+'&action=edit&redlink=1'

                url = '<a href="'+ url + '" style="color:#ba0000">'+translated_title+'</a>'

                wt_url ='[['+languagecode_target+':'+translated_title+'|<span style="color:#ba0000">'+translated_title+'</span>]]'

            potential_titles_html[qitem]=url
            potential_titles_wikitext[qitem]=wt_url


        # HTML
        ccc_df['Other Languages'] = pd.Series(articles_foreignlang_html)
        ccc_df['Title / Suggested Title'] = pd.Series(potential_titles_html)

        # Change page_title format
        for x in ccc_ranked: ccc_df.at[x, 'page_title'] = '<a href="https://'+languagecode+'.wikipedia.org/wiki/'+ccc_df.loc[x]['page_title']+'">'+ccc_df.loc[x]['page_title'].replace('_',' ')+'</a>'

        # Change interwiki format:
        ccc_df['num_interwiki'] = ccc_df.num_interwiki.astype(int)
        ccc_df['num_interwiki'] = ccc_df.num_interwiki.astype(str)
        for x in ccc_ranked: ccc_df.at[x,'num_interwiki'] = '<a href="https://www.wikidata.org/wiki/'+x+'">'+ccc_df.loc[x]['num_interwiki']+'</a>'

        # Change properties format:
        ccc_df['num_wdproperty'] = ccc_df.num_wdproperty.astype(int)
        ccc_df['num_wdproperty'] = ccc_df.num_wdproperty.astype(str)
        for x in ccc_ranked: ccc_df.at[x, 'num_wdproperty'] = '<a href="https://www.wikidata.org/wiki/'+x+'">'+ccc_df.loc[x]['num_wdproperty']+'</a>'

        # Renaming the columns
        columns_dict = {'page_title':'Title','date_created':'Date Created','main_territory':'Associated Territory','num_bytes':'Bytes','num_discussions':'Discussion Edits','num_references':'External Links','num_inlinks':'Inlinks','num_outlinks':'Outlinks','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_wdproperty':'WD Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_inlinks_from_CCC':'Inlinks from CCC','num_retrieval_strategies':'CCC Strategies'}
        ccc_df=ccc_df.rename(columns=columns_dict)

        for x in range(0,len(columns)): columns[x]=columns_dict[columns[x]]
        new_columns = ['Nº','Title']+columns+['Date Created','Inlinks from CCC','Other Languages','Title / Suggested Title']
        ccc_df = ccc_df[new_columns] # selecting the parameters to export

        os.makedirs(current_web_path+'/'+wiki_path+'/'+ list_name, exist_ok=True)
        file_path = current_web_path+'/'+wiki_path+'/'+ list_name +'/'+ list_name + languagecode + '_' +languagecode_target + '.html'
        print (file_path)

        # converting and exporting
        old_width = pd.get_option('display.max_colwidth')
        pd.set_option('display.max_colwidth', -1)
        html_str=HTML(ccc_df.to_html(index=False,escape=False))
        html = html_str.data
        with open(file_path, 'w', encoding='utf-8') as f: f.write(html)
        pd.set_option('display.max_colwidth', old_width)


        # WIKITEXT
        ccc_df['Other Languages'] = pd.Series(articles_foreignlang_wikitext)
        ccc_df['Title / Suggested Title'] = pd.Series(potential_titles_wikitext)

        # Change page_title format
        for x in ccc_ranked: ccc_df.at[x, 'page_title'] = '['+languagecode+':'+ccc_df.loc[x]['page_title']+'|'+ccc_df.loc[x]['page_title'].replace('_',' ')+']'

        # Change interwiki format:
        for x in ccc_ranked: ccc_df.at[x,'Interwiki Links'] = '[[wikidata:'+x+'|'+ccc_df.loc[x]['Interwiki Links']+']]'

        # Change properties format:
        for x in ccc_ranked: ccc_df.at[x, 'WD Properties'] = '[[wikidata:'+x+'|'+ccc_df.loc[x]['WD Properties']+']]'

        df_columns_list = ccc_df.columns.values.tolist()
        df_rows = ccc_df.values.tolist()

        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        header_string = '!'
        for x in range(0,len(df_columns_list)):
            if x == len(df_columns_list)-1: add = ''
            else: add = '!!'
            
            header_string = header_string + df_columns_list[x] + add
        header_string = header_string + '\n'

        rows = ''
        for row in df_rows:
            midline = '|-\n'
            row_string = '|'
            for x in range(0,len(row)):
                if x == len(row)-1: add = ''
                else: add = '||'
                value = row[x]
                row_string = row_string + str(value) + add # here is the value

                # here we might add colors.
            row_string = midline + row_string + '\n'
            rows = rows + row_string
        closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string
        wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
        wikitext += wiki_table_string

        print ('here we have another wikitext for language '+languagecode_target+':')

        print (str(wikitext))
        wiki_tables_dict[languagecode_target]=wikitext
        input('')

    print ('* generate_ccc_vital_articles_list_table Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    return wiki_tables_dict



# in this function we create the table language_territories_mapping.
def generate_ccc_language_territories_mapping_table(wiki_path):

    df = pd.read_csv('language_territories_mapping_quality.csv',sep='\t',na_filter = False)
    df = df[['territoryname','territorynameNative','QitemTerritory','languagenameEnglishethnologue','WikimediaLanguagecode','demonym','demonymNative','ISO3166','ISO31662','regional','country','indigenous','languagestatuscountry','officialnationalorregional']]

    territorylanguagecodes_original = list(df.WikimediaLanguagecode.values)
    indexs = df.index.values.tolist()

    df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('-','_')
    df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('be_tarask', 'be_x_old')
    df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('nan', 'zh_min_nan')

    languagenames={}
    updated_langcodes = list(df.WikimediaLanguagecode.values)
    for x in range(0,len(territorylanguagecodes_original)):
        curindex = indexs[x]
        languagenames[curindex]=languages.loc[updated_langcodes[x]]['languagename']
    df['Language Name'] = pd.Series(languagenames)

    languagecodes={}
    for x in range(0,len(territorylanguagecodes_original)):
        curindex = indexs[x]
        curlanguagecode = territorylanguagecodes_original[x]
        languagecodes[curindex]=curlanguagecode
    df['WikimediaLanguagecode'] = pd.Series(languagecodes)



#    languagenames_local={}
#    for languagecode in territorylanguagecodes:
#        languagenames_local[languagecode]=languages.loc[languagecode]['languagename']
#    df['Language Local'] = pd.Series(languagenames_local)

    df = df.reset_index()
    df = df.fillna('')

    qitems={}
    indexs = df.index.tolist()
    qitems_list = list(df.QitemTerritory.values)
    for x in range(0,len(qitems_list)):
        curqitem = qitems_list[x]
        curindex = indexs[x]
#        print (curqitem, curindex)

        if curqitem != None: qitems[curindex]='[[wikidata:'+curqitem+'|'+curqitem+']]'
        else: qitems[curindex]=''
    df['Qitems'] = pd.Series(qitems)



    columns = ['Language Name','WikimediaLanguagecode','Qitems','territorynameNative','demonymNative','ISO3166','ISO31662']
#    columns = ['Language Name','WikimediaLanguagecode','Qitems','territoryname','territorynameNative','demonymNative','ISO3166','ISO31662','country']
    df = df[columns] # selecting the parameters to export
    print (df.head())

    columns_dict = {'Language Name':'Language','WikimediaLanguagecode':'Wiki','Qitems':'WD Qitem','territoryname':'Territory','territorynameNative':'Territory (Local)','demonymNative':'Demonyms (Local)','ISO3166':'ISO 3166', 'ISO3662':'ISO 3166-2','country':'Country'}
    df=df.rename(columns=columns_dict)

    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    header_string = '!'
    for x in range(0,len(df_columns_list)):
        if x == len(df_columns_list)-1: add = ''
        else: add = '!!'
        header_string = header_string + df_columns_list[x] + add

    header_string = header_string + '\n'

    rows = ''
    for row in df_rows:
        midline = '|-\n'
        row_string = '|'
        for x in range(0,len(row)):
            if x == len(row)-1: add = ''
            else: add = '||'
            value = row[x]
            row_string = row_string + str(value) + add # here is the value

            # here we might add colors. -> it would be nice to make a different colour for each language background, so it would be easy to see when one starts and another finishes.
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    print ('* dataframe and html file created.\n')

    return wikitext


#   QUESTION: What is the extent of Cultural Context Content in all language editions?
def generate_ccc_extent_visualization(languagecode, languagelist, wiki_path):

    print ('* generate_ccc_extent_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    query = 'SELECT * FROM ccc_extent_language'


    print ('')
    # http://whgi.wmflabs.org/gender-by-language.html
    # semblant a aquesta.
#    gràfic de boles.

    p.xaxis.axis_label = 'Total number of CCC articles'
    p.yaxis.axis_label = 'Percentage of CCC'
    p.yaxis[0].formatter = NumeralTickFormatter(format='0,0')

    source = ColumnDataSource(data=cutoff_plot.to_dict(orient='list'))
    p.circle('fem_per', 'total', size=12, line_color="black", fill_alpha=0.8,
             source=source)

    # label text showing language name
    p.text(x="fem_per", y="total", text="index", text_color="#333333",
           text_align="left", text_font_size="8pt",
           y_offset=-5, source=source)

    filename='ccc_extent_visualization'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: What is the extent of Cultural Context Content in the articles created during the last month?
def generate_ccc_creation_monthly_visualization(languageslist, languagelist, wiki_path):

    print ('* generate_ccc_creation_monthly_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()

    query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
    month_ago_timestamp=(datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S')
    query = query + ' AND date_created > '+str(month_ago_timestamp)

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
    file_path = current_web_path + wiki_path + filename

    return file_path



#   QUESTION: What is the composition of the entire Wikipedia project in terms of each language Cultural Context Content?
def generate_ccc_overall_wikipedia_project_composition_visualization(wiki_path):
    print ('* generate_ccc_overall_wikipedia_project_composition_visualization')

    # el gràfic seria semblant a ccc_extent_visualization()

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    # obtain data from table: ccc_entire_project


    # Formatges de tot el projecte Wikipedia amb totes les llengües CCC i el què ocupen (repetint articles).
    # Formatges de tot el projecte Wikipedia amb totes les llengües CCC i el què ocupen (sense repetir articles, és a dir, tot nou)
    # Formatges de tot Wikidata amb el què ocupen les llengües CCC.

    # El mateix per subregions. (x3)
    # El mateix per regions. (x3)


    # This should be obtained from ccc_entire_project table from wcdo_data.db

    filename=''

    file_path1 = current_web_path + wiki_path + filename
    file_path2 = current_web_path + wiki_path + filename
    file_path3 = current_web_path + wiki_path + filename


    return file_path1,file_path2,file_path3



#   QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
def generate_ccc_culture_gap_index_monthly_visualization(languagecode, languageslist, wiki_path):


    print ('* generate_ccc_culture_gap_index_monthly_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_indexs.

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
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
def generate_ccc_bridging_culture_gap_monthly_visualization(languagecode, wiki_path):
#    % per cada llengua.

    print ('* generate_ccc_bridging_culture_gap_monthly_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_bridges.


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
    file_path = current_web_path + wiki_path + filename

    return file_path






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
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: Which are the geolocated articles with most inlinks, pageviews, etc.?
#   QUESTION: How well does this language covers other language editions geolocated articles?
def generate_ccc_geolocated_articles_map_visualization(languagecode, wiki_path):

    print ('* generate_ccc_geolocated_articles_map_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_langwiki with geolocated not null.

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
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: How well the language editions cover the CCC of this language edition?
def generate_ccc_culture_gap_covered_visualization(languagecode, wiki_path):
    # diagrama de boles

    print ('* generate_ccc_culture_gap_covered_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_gaps.


    filename = languagecode+'_'+'ccc_culture_gap_covered_visualization'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: How well this language edition cover the CCC of the other language editions?     
def generate_ccc_culture_gap_covering_visualization(languagecode, wiki_path):
#    això és utilitzar table_ccc_gaps amb funció share.
    # diagrama de boles. stacked bar o formatget.

    print ('* generate_ccc_culture_gap_covered_visualization')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_gaps.


    filename = languagecode+'_'+'ccc_culture_gap_covering_visualization'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path


#   QUESTION: How useful has this project been to the entire Wikipedia project?
def generate_ccc_overall_measures_evolution_visualizations():

    print ('* generate_ccc_overall_measures_evolution_visualizations')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_indexs i ccc_gaps.

    # CALCULATE INCREMENTS FROM PROJECT-START AND CURRENT.

#    per llengües:
#    culture_gap_total -> from ccc_indexs.
#    total coverage index, covered articles sum.
#    culture_gap_lists -> from ccc_indexs.
#    lists coverage index, covered articles sum.
#    número de punts que ha crescut cada llengua quant a index i quant a articles coberts, en total o de llistes.
#    d'aquí en fem un gràfic amb: 
#    a) els que han crescut més a llistes | axis y: creixement index, axis x: creixement covered articles sum.
#    b) els que han crescut més en total | axis y: creixement index, axis x: creixement covered articles sum.
    filepath1=''


    # OBTAIN LIST OF LANGUAGES FROM X
    regionlist = languages.region.unique()
    subregionlist = languages.subregion.unique()


    for subregion in subregionlist:
        languages_of_region = languages[languages['subregion'].str.contains(subregion)]


    for region in regionlist: 
        languages_of_subregion = languages.loc[languages['region'] == region]


#    per regions i subregions:
#    p.e. cas: m'agradaria saber si ha duplicat o no la presència l'àfrica des de l'inici del projecte.
#    covered articles sum (total), covered articles sum (lists).
#    presència de les regions i subregions del món al projecte sencer. Això 
    filepath2=''

#    per tot el projecte:
#    covered articles sum (total), covered articles sum (lists).
#    nombre d'articles creats en total, nombre d'articles creats a les llistes.
#    frase: language, inc. covered articles (lists), inc. covered articles (sum).
    wikitext=''

    return wikitext, filepath1, filepath2





### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

### PUBLISHING TO META/NOTIFYING THE WORK DONE.

# Publishes all the meta pages for all the languages in case they do not exist.
def publish_wcdo_first_time():

    site = pywikibot.Site("meta","meta")
    text_files_path = web_path + '/first_time_texts/'

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
    wikitext= 'this is a transclusion tag based on generate_ccc_overall_wikipedia_project_composition_visualization, generate_ccc_extent_visualization and generate_ccc_creation_monthly_visualization'
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

    culture_gap_spread_page_name = 'Culture_Gap_(spread)'
    wiki_path = main_page_name + '/' + culture_gap_spread_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+culture_gap_spread_page_name+'.txt',"r").read()
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_culture_gap_table'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    culture_gap_coverage_page_name = 'Culture_Gap_(coverage)'
    wiki_path = main_page_name + '/' + culture_gap_coverage_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+culture_gap_coverage_page_name+'.txt',"r").read()
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
    wikitext= 'this is the text and the transclusion tag based on generate_ccc_creation_monthly_visualization'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    get_involved_page_name = 'Get_Involved'
    wiki_path = main_page_name + '/' + get_involved_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+get_involved_page_name+'.txt',"r").read()
    wikitext= 'this is the text'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    entire_wikipedia_CCC_Composition_project_page_name = 'Entire_Wikipedia_CCC_Composition'
    wiki_path = main_page_name + '/' + entire_wikipedia_CCC_Composition_project
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+entire_wikipedia_CCC_Composition_project_page_name+'.txt',"r").read()
    wikitext= 'this is the text'
    wikitext = wikitext + '[[Category:'+category_page_name+']]'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

    project_measures_name = 'Project_Measures'
    wiki_path = main_page_name + '/' + project_measures_name
    page = pywikibot.Page(site, wiki_path)
    wikitext = open(text_files_path+project_measures_name+'.txt',"r").read()
    wikitext= 'this is the text'
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
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_extent_qitem_table_by_language, generate_ccc_extent_visualization, generate_ccc_creation_monthly_visualization'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        culture_gap_page_name = 'Culture_Gap'
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
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_bridging_culture_gap_monthly_visualization, generate_ccc_culture_gap_index_monthly_visualization'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        vital_articles_lists_coverage_page_name = 'Vital_articles_lists_(coverage)'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_coverage_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+vital_articles_lists_coverage_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_vital_articles_list_tables_table'
        wikitext = wikitext + '[[Category:'+category_page_name+']]'       
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        vital_articles_lists_spread_page_name = 'Vital_articles_lists_(spread)'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_spread_page_name
        page = pywikibot.Page(site, wiki_path)
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+vital_articles_lists_spread_page_name+'.txt',"r").read()
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_vital_articles_list_tables_table'
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
        wikitext= 'this is the text and the transclusion tag based on generate_ccc_all_vital_articles_lists_table'
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
    file_path = generate_ccc_overall_wikipedia_project_composition_visualization(wiki_path)
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


    # CULTURE GAP SPREAD
    culture_gap_spread_page_name = 'Culture_Gap_(spread)'
    wiki_path = main_page_name + '/' + culture_gap_spread_page_name
    culture_gap_spread_page_name = pywikibot.Page(site, wiki_path)
    # QUESTION: How well each language edition covers the CCC of each other language edition?
    wiki_table = generate_ccc_culture_gap_table(wiki_path, 'spread') #    taula amb les top 5 que més cobreix cada llengua.
    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # CULTURE GAP COVERAGE
    culture_gap_spread_page_name = 'Culture_Gap_(coverage)'
    wiki_path = main_page_name + '/' + culture_gap_spread_page_name
    culture_gap_spread_page_name = pywikibot.Page(site, wiki_path)
    # QUESTION: How well this language edition covers the CCC of each other language edition?
    wiki_table = generate_ccc_culture_gap_table(wiki_path, 'coverage') #    taula amb les top 5 que més cobreix cada llengua.
    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # LANGUAGE TERRITORIES MAPPING
    languages_territories_mapping_page_name = 'Language_Territories_Mapping'
    wiki_path = main_page_name + '/' + languages_territories_mapping_page_name
    table_path = wiki_path + '/Table'
    page = pywikibot.Page(site, table_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # CULTURAL CONTEXT CONTENT
    cultural_context_content_page_name = 'Cultural_Context_Content'
    wiki_path = main_page_name + '/' + cultural_context_content_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext='Method and datasets'
    # QUESTION: What is the extent of Cultural Context Content in the articles created during the last month?
    file_path = generate_ccc_creation_monthly_visualization('en',top19,wiki_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # GET INVOLVED
    get_involved_page_name = 'Get_Involved'
    wiki_path = main_page_name + '/' + get_involved_page_name
    page = pywikibot.Page(site, wiki_path)
    wikitext='Method and datasets'
    # QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
    file_path=generate_ccc_bridging_culture_gap_monthly_visualization(languagecode, wiki_path)
    # QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
    file_path=generate_ccc_culture_gap_index_monthly_visualization(languagecode,top19,wiki_path)
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # PROJECT MEASURES
    entire_wikipedia_CCC_Composition_page_name = 'Entire_Wikipedia_CCC_Composition'
    wiki_path = main_page_name + '/' + entire_wikipedia_CCC_Composition_page_name
    page = pywikibot.Page(site, wiki_path)
    file_path = generate_ccc_overall_measures_evolution_visualizations()
    wikitext='Method and datasets'
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # PROJECT MEASURES
    project_measures_page_name = 'Project_Measures'
    wiki_path = main_page_name + '/' + project_measures_page_name
    page = pywikibot.Page(site, wiki_path)
    file_path = generate_ccc_overall_measures_evolution_visualizations()
    wikitext='Method and datasets'



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
        file_path = generate_ccc_creation_monthly_visualization(languagecode,closest19, wiki_path)
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
        file_path=generate_ccc_bridging_culture_gap_monthly_visualization(languagecode, wiki_path)
        # QUESTION: What is the evolution of the Culture Gap Index evolve for each Wikipedia language edition?
        file_path=generate_ccc_culture_gap_index_monthly_visualization(languagecode,top19,wiki_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


        # VITAL ARTICLES (SPREAD) LIST PAGE
        vital_articles_lists_spread_page_name = 'Vital_articles_lists_(spread)'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_spread_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        wikitext = generate_ccc_all_vital_articles_lists_table(languagecode_covered, wiki_path, 'spread')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


        # VITAL ARTICLES (COVERAGE) LIST PAGE
        vital_articles_lists_coverage_page_name = 'Vital_articles_lists_(coverage)'
        wiki_path = main_page_name + '/' + language_page_name + '/' + vital_articles_lists_coverage_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        wikitext = generate_ccc_all_vital_articles_lists_table(languagecode_covered, wiki_path, 'coverage')
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




        # THESE generate_ccc_vital_articles_list_table FUNCTIONS ARE NOT PROPERLY CONFIGURED.

        # RECOMMENDATION LISTS: VITAL ARTICLES
        # QUESTION: Which of these CCC articles is or should be available in this language edition?

        # EDITORS - 100
        CCC_Vital_articles_Top_100_page_name = 'CCC_Vital_articles_Top_100'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_100_page_name

        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, '', {'num_editors': 1}, 'positive', 100, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_Top_100')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # EDITORS - 1000
        CCC_Vital_articles_Top_1000_page_name = 'CCC_Vital_articles_Top_1000'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_Top_1000_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, '', {'num_editors': 1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_pathm, 'CCC_Vital_articles_Top_1000')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # LONG, CITING AND FEATURED
        CCC_Vital_articles_featured_page_name = 'CCC_Vital_articles_featured'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_featured_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, '', {'featured_article': 0.8, 'num_references':0.1, 'num_bytes':0.1}, 'positive', 100, 'proportional_articles', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_featured')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # GL MOST INLINKED FROM CCC
        CCC_Vital_articles_geolocated_page_name = 'CCC_Vital_articles_geolocated'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_geolocated_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['gl'], '', 80, '', {'num_inlinks_from_CCC': 1}, 'positive', 100, 'proportional_articles', ['num_inlinks_from_CCC','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_geolocated')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)


       # KEYWORDS ON TITLE WITH MOST BYTES
        CCC_Vital_articles_keywords_page_name = 'CCC_Vital_articles_keywords'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_keywords_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['kw'], '', 80, '', {'num_bytes': 1}, 'positive', 100, 'proportional_articles', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_keywords')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # MOST EDITED AND CREATED DURING FIRST THREE YEARS
        CCC_Vital_articles_first_years_page_name = 'CCC_Vital_articles_first_years'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_first_years_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, 'first_three_years', {'num_edits': 1}, 'positive', 100, 'proportional_articles', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_first_years')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # MOST EDITED AND CREATED DURING LAST THREE MONTHS
        CCC_Vital_articles_last_quarter_page_Name = 'CCC_Vital_articles_last_quarter'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_last_quarter_page_Name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, 'last_three_months', {'num_edits': 1}, 'positive', 100, 'proportional_articles', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_last_three_months')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # MOST EDITED WOMEN BIOGRAPHY
        CCC_Vital_articles_women_page_name = 'CCC_Vital_articles_women'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_women_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc','female'], '', 80, '', {'num_edits': 1}, 'positive', 100, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_women')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)


        # MOST EDITED MEN BIOGRAPHY
        CCC_Vital_articles_men_page_name = 'CCC_Vital_articles_men'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_men_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc','male'], '', 80, '', {'num_edits': 1}, 'positive', 100, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_men')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)


        # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
        CCC_Vital_articles_pageviews_page_name = 'CCC_Vital_articles_pageviews'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_pageviews_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, '', {'num_pageviews':1}, 'positive', 100, 'proportional_articles', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_pageviews')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)

        # MOST DISCUSSED (EDITS DISCUSSIONS)
        CCC_Vital_articles_discussions_page_name = 'CCC_Vital_articles_discussions'
        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_discussions_page_name
        table_path = wiki_path + '/Table'
        page = pywikibot.Page(site, table_path)
        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc'], '', 80, '', {'num_discussions': 1}, 'positive', 100, 'proportional_articles', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path, 'CCC_Vital_articles_discussions')
        wikitext = wiki_tables_dict[languagecode]
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        update_vital_articles_transclusion_tables(languagecode, table_path, wiki_tables_dict)


#        BIGGEST SINGLE TERRITORY
#        CCC_Vital_articles_top_territory_page_name = 'CCC_Vital_articles_top_territory'
#        wiki_path = main_page_name + '/' + language_page_name + '/' + CCC_Vital_articles_top_territory_page_name
#        table_path = wiki_path + '/Table'
#        page = pywikibot.Page(site, table_path)
#        wiki_tables_dict = generate_ccc_vital_articles_list_table(languagecode, 'ca', ['ccc_main_territory'], '', '', {'num_editors': 0.9,'num_inlinks_from_CCC': 0.1}, 'positive', 1000, 'minimum', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, wiki_path)
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



def get_hexcolorrange(color1, color2, scale, value_min, value_max, actualvalue):   
    interval=int((value_max - value_min)/scale)
    index=int(actualvalue/interval)
    colors=list(colour.Color(color1).range_to(colour.Color(color2), scale))
    choosencolor = colors[index].hex
    return choosencolor


#######################################################################################


def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

def finish_email():
    try:
        sys.stdout=None; send_email_toolaccount('WCDO CREATION', open('wcdo_creation.out', 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount('WCDO CREATION aborted because of an error', open('wcdo_creation.out', 'r').read()+'err')


#######################################################################################


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    web_path = '/srv/wcdo/site/web/'
    current_web_path = web_path + year_month + '/'

    # Import the language-territories mappings
    territories = load_languageterritories_mapping()

    # Import the Wikipedia languages characteristics / UPGRADE CASE: in case of extending the project to WMF SISTER PROJECTS, a) this should be extended with other lists for Wikimedia sister projects b) along with more get functions in the MAIN for each sister project.
    languages = load_wiki_projects_information();

    wikilanguagecodes = languages.index.tolist() 

    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if establish_mysql_connection_read(a)==None: wikilanguagecodes.remove(a)

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = load_wikipedia_language_editions_numberofarticles()

    # Final Wikipedia languages to process
    print (wikilanguagecodes)
#    wikilanguagecodes = obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
#    wikilanguagecodes = ['ca']

    print ('\n* Starting the WCDO CREATION CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* WCDO CREATION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    finish_email()