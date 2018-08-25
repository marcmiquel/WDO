# -*- coding: utf-8 -*-

# time
import time
import datetime
from dateutil import relativedelta
# system
import os
import sys
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# data
import pandas as pd


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

    # CREATE DATABASES
    print ('Creating the databases.')
    create_intersections_db()
    create_increments_db()
    create_ccc_vital_articles_lists_db()

    # GENERATE MAIN STATS
    print ('Generating the main stats.')
    generate_all_articles_intersections()
    generate_last_month_articles_intersections()
    delete_last_iteration_increments()

    # GENERATE CCC VITAL ARTICLES LISTS
    print ('Generating the CCC Vital articles lists.')
    for languagecode in wikilanguagecodes_real:
        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)

        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        countries = load_countries_from_language(languagecode)
        countries.append('')
        print ('these are the countries from this language:')
        print (countries)
        length = 1000
        for country in countries:
            # for the wiki_path
            if country != '': 
                country_name = territories.loc[territories['ISO3166'] == country].loc[languagecode]['country']
                if isinstance(country_name, str) != True: country_name=list(country_name)[0]
                country = country_name
            else: country = ''

            # category
            if country != '': 
                qitems_list = load_language_territories_from_country_language(country, languagecode)
                category = qitems_list
            else: category = ''

            # EDITORS
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, '', {'num_editors': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_editors')

            # FEATURED, LONG AND CITED
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, '', {'featured_article': 0.8, 'num_references':0.1, 'num_bytes':0.1}, 'positive', length, 'none', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_featured')

            # GL MOST INLINKED FROM CCC
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['gl'], category, 80, '', {'num_inlinks_from_CCC': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_geolocated')

            # KEYWORDS ON TITLE WITH MOST BYTES
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['kw'], category, 80, '', {'num_bytes': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_keywords')    

            # MOST EDITED WOMEN BIOGRAPHY
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc','female'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_women')

            # MOST EDITED MEN BIOGRAPHY
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc','male'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_men')

            # MOST EDITED AND CREATED DURING FIRST THREE YEARS
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, 'first_three_years', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_created_first_three_years')

            # MOST EDITED AND CREATED DURING LAST YEAR
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, 'last_year', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_created_last_year')

            # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, '', {'num_pageviews':1}, 'positive', length, 'none', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_pageviews')

            # MOST DISCUSSED (EDITS DISCUSSIONS)
            generate_intersections_with_ccc_vital_articles_lists(languagecode, 'ca', ['ccc'], category, 80, '', {'num_discussions': 1}, 'positive', length, 'none', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'CCC_Vital_articles_discussions')

    # FINISH
    delete_last_iteration_ccc_vital_articles_lists()


######################################################################

"""
# MAIN
######################## STATS GENERATION SCRIPT ######################## 


"""


# DATABASE AND DATASETS MAINTENANCE FUNCTIONS (CCC AND WIKIDATA)
################################################################

# Loads language_territories_mapping.csv file
def load_languageterritories_mapping():
# READ FROM STORED FILE:
    territories = pd.read_csv(databases_path + 'language_territories_mapping.csv',sep='\t',na_filter = False)
    territories = territories[['territoryname','territorynameNative','QitemTerritory','languagenameEnglishethnologue','WikimediaLanguagecode','demonym','demonymNative','ISO3166','ISO31662','regional','country','indigenous','languagestatuscountry','officialnationalorregional']]
    territories = territories.set_index(['WikimediaLanguagecode'])

    territorylanguagecodes = territories.index.tolist()
    for n, i in enumerate(territorylanguagecodes): territorylanguagecodes[n]=i.replace('-','_')
    territories.index = territorylanguagecodes
    territories=territories.rename(index={'be_tarask': 'be_x_old'})
    territories=territories.rename(index={'nan': 'zh_min_nan'})

    ISO3166=territories['ISO3166'].tolist()
    regions = pd.read_csv(databases_path + 'country_regions.csv',sep=',',na_filter = False)
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
    languages = pd.read_csv(databases_path + 'wikipedia_language_editions.csv',sep='\t',na_filter = False)
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
#    languages.to_csv(databases_path + filename+'.csv',sep='\t')

    return languages


def load_wikipedia_language_editions_numberofarticles():
    wikipedialanguage_numberarticles = {}

    if not os.path.isfile(databases_path + 'ccc_current.db'): return
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    # Obtaining CCC for all WP
    for languagecode in wikilanguagecodes:
        query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki;'
        cursor.execute(query)
        wikipedialanguage_numberarticles[languagecode]=cursor.fetchone()[0]

    return wikipedialanguage_numberarticles


def load_countries_from_language(languagecode):
    country_territories = {}
    countries=territories.loc[languagecode].ISO3166
    if isinstance(countries,str): countries = [countries]
    else: countries = list(set(countries))

    return countries


def load_territories_from_language_country(ISO3166, languagecode):

    qitems = territories.loc[territories['ISO3166'] == ISO3166].loc[languagecode]['QitemTerritory']
    if isinstance(qitems,str): qitems = [qitems]
    else: qitems = list(qitems)
    
    return qitems



def load_iso_3166_to_geographical_regions():

    country_regions = pd.read_csv(databases_path + 'country_regions.csv', sep=',')
    country_regions = country_regions[['alpha-2','name','region','sub-region']]
    country_regions = country_regions.rename(columns={'sub-region': 'subregion'})
    country_regions = country_regions.set_index(['alpha-2'])

    country_names = country_regions.name.to_dict()
    regions = country_regions.region.to_dict()
    subregions = country_regions.subregion.to_dict()

    print (country_names)
    
    return country_names, regions, subregions


def load_dicts_page_ids_qitems(languagecode):
    page_titles_qitems = {}
    page_titles_page_ids = {}

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
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


# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
        return mysql_con_read
    except:
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')


# It returns three lists with 20 languages according to different criteria.
def obtain_proximity_wikipedia_languages_lists(languagecode):

    # BIGGEST
    wikipedialanguage_numberarticles_sorted = sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)

    top19 = wikipedialanguage_numberarticles_sorted[:19]
    if languagecode in top19: top19.append(wikipedialanguage_numberarticles_sorted[20])
    else: top19.append(languagecode)

    # MIDDLE ONES
    i=wikipedialanguage_numberarticles_sorted.index(languagecode)
    upper9lower10 = wikipedialanguage_numberarticles_sorted[i-9:i]+wikipedialanguage_numberarticles_sorted[i:i+11]

    # CLOSEST
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()
    query = 'SELECT rel_value, entity_2 FROM wcdo_intersections WHERE entity_1 = "'+languagecode+'" AND entity_1_descriptor = "ccc" AND entity_2_descriptor = "wp";'
    for row in cursor.execute(query):
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









# INTERSECTIONS AND INCREMENTS
# COMMAND LINE: sqlite3 -header -csv wcdo_data.db "SELECT * FROM create_intersections_db;" > create_intersections_db.csv
def create_intersections_db():
    functionstartTime = time.time()
    print ('* create_intersections_db')

    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    query = ('CREATE table if not exists wcdo_intersections ('+
    'intersection_id integer primary key autoincrement, '+
    'content text not null, '+
    'entity_1 text not null, '+
    'entity_1_descriptor text, '+

    'entity_2 text, '+
    'entity_2_descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+
    'measurement_date text);')
    cursor.execute(query)

    print ('* create_intersections_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def create_increments_db():
    functionstartTime = time.time()
    print ('* create_increments_db')

    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()


    query = ('CREATE table if not exists wcdo_increments ('+
    'cur_intersection_id integer, '+
    'abs_increment integer,'+
    'rel_increment float,'+
    'period text,'+

    'PRIMARY KEY (cur_intersection_id, period);')

    cursor.execute(query)

    print ('* create_increments_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



# LISTS
def create_ccc_vital_articles_lists_db():
    functionstartTime = time.time()
    print ('* create_ccc_vital_articles_lists_db')


    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    query = ('CREATE table if not exists wcdo_lists_article_features ('+
    'langcode_original text, '+
    'qitem text,'+

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
    'measurement_date text,'+

    'PRIMARY KEY (qitem, langcode_original, measurement_date));')
    cursor.execute(query)

    query = ('CREATE table if not exists ccc_vital_lists_rankings ('+
    'langcode_original text, '+
    'qitem text,'+
    'position integer,'+
    'list_name text,'+
    'measurement_date text,'+

    'PRIMARY KEY (qitem, langcode_original, list_name, measurement_date);')
    cursor.execute(query)

    query = ('CREATE table if not exists wcdo_lists_article_page_titles ('+
    'langcode_original text, '+
    'qitem text,'+
    'langcode_target text,'+
    'page_title_original text,'+
    'page_title_target text,'+ 
    'generation_method text,'+ # page_title_target can either be the REAL (from sitelinks wikitada), the label proposal (from labels wikitada) or translated (content translator tool).
    'measurement_date text,'+

    'PRIMARY KEY (qitem, langcode_target, page_title_target, measurement_date));')
    cursor.execute(query)

    print ('* create_ccc_vital_articles_lists_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_all_articles_intersections():
    functionstartTime = time.time()
    print ('* generate_all_articles_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    query_insert = 'INSERT INTO wcdo_intersections (content,entity_1, entity_1_descriptor, entity_2, entity_2_descriptor, abs_value, rel_value, rel_reference, measurement_date) VALUES (?,?,?,?,?,?,?,?);'

    # LANGUAGE EDITIONS
    for languagecode_1 in wikilanguagecodes:
        wpnumberofarticles_1=wikipedialanguage_numberarticles[languagecode_2]

        for languagecode_2 in wikilanguagecodes:
            wpnumberofarticles_2=wikipedialanguage_numberarticles[languagecode_2]

            query = 'SELECT COUNT(qitem) FROM sitelinks WHERE langcode ='+languagecode_1+'wiki AND langcode ='+languagecode_2+'wiki;'
            cursor.execute(query)
            article_count = cursor.fetchone()[0]

            values = ('articles',languagecode_1,'wp',languagecode_2,'wp',article_count,100*article_count/wpnumberofarticles_1,measurement_date)
            cursor3.execute(query_insert,values); conn3.commit()
            cursor3.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1, 'wp', languagecode_2, 'wp', measurement_date)

            values = ('articles',languagecode_2,'wp',languagecode_1,'wp',article_count,100*article_count/wpnumberofarticles_2,measurement_date)
            cursor3.execute(query_insert,values); conn3.commit()
            cursor3.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1, 'wp', languagecode_2, 'wp', measurement_date)

        # entire wp
        query = 'SELECT count(page_title) FROM ccc_'+languagecode_1+'wiki WHERE num_interwiki = 0'
        cursor.execute(query)
        zero_ill_wp_count = cursor.fetchone()[0]

        values = ('articles',languagecode_1,'wp',languagecode_1,'zero_ill',zero_ill_wp_count,100*zero_ill_wp_count/wpnumberofarticles, measurement_date)
        cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode_1,'wp',languagecode_1,'zero_ill', measurement_date)


        query = 'SELECT count(*) FROM ccc_'+languagecode_1+'wiki WHERE qitem='' or qitem IS NULL'
        cursor.execute(query)
        null_qitem_count = cursor.fetchone()[0]

        values = ('articles',languagecode_1,'wp',languagecode_1,'null_qitems',null_qitem_count,100*null_qitem_count/wpnumberofarticles, measurement_date)
        cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode_1,'wp',languagecode_1,'null_qitems', measurement_date)

    print ('languagecode_1, wp, languagecode_2, wp,'+measurement_date)
    print ('languagecode_2, wp, languagecode_1, wp,'+measurement_date)

    print ('languagecode, wp, languagecode, zero_ill,'+measurement_date)
    print ('languagecode, wp, languagecode, null_qitems,'+measurement_date)


    # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        query = 'SELECT COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;';
        cursor.execute(query)
        row = cursor.fetchone()

        ccc_count = row[0]
        no_own_ccc_created_count = wpnumberofarticles - ccc_count
        ccc_geolocated_count = row[1]
        ccc_keywords_count = row[2]

        values = ('articles',languagecode,'wp',languagecode,'ccc',ccc_keywords_count,100*ccc_keywords_count/wpnumberofarticles,measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'ccc', measurement_date)

        values = ('articles',languagecode,'wp',languagecode,'no_language_ccc',no_own_ccc_created_count,100*no_own_ccc_created_count/wpnumberofarticles,measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'no_language_ccc', measurement_date)

        values = ('articles',languagecode,'wp',languagecode,'ccc_geolocated',ccc_geolocated_count,100*ccc_geolocated_count/wpnumberofarticles,'covering',measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'ccc_geolocated', measurement_date)

        values = ('articles',languagecode,'wp',languagecode,'ccc_keywords',ccc_keywords_count,100*ccc_keywords_count/wpnumberofarticles,'covering',measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'ccc_keywords', measurement_date)

        values = ('articles',languagecode,'ccc',languagecode,'ccc_keywords',ccc_keywords_count,100*ccc_keywords_count/ccc_count,measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'ccc_keywords', measurement_date)

        values = ('articles',languagecode,'ccc',languagecode,'ccc_geolocated',ccc_geolocated_count,100*ccc_geolocated_count/ccc_count,measurement_date)
        cursor2.execute(query_insert,values); conn2.commit()
        cursor2.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'ccc_geolocated', measurement_date)


        # zero ill
        query = 'SELECT count(page_title) FROM ccc_'+languagecode+'wiki WHERE num_interwiki = 0 AND ccc_binary=1'
        cursor.execute(query)
        zero_ill_ccc_count = cursor.fetchone()[0]

        values = ('articles',languagecode,'ccc',languagecode,'zero_ill',zero_ill_ccc_count,100*zero_ill_ccc_count/ccc_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode,'ccc',languagecode,'zero_ill',measurement_date)

        # MAIN TERRITORIES
        query = 'SELECT main_territory, COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 GROUP BY main_territory;';
        for row in cursor.execute(query):
            main_territory=row[0]
            if main_territory == '' or main_territory == None:
                main_territory = 'Not Assigned'
            ccc_articles_count=row[1]
            ccc_geolocated_count=row[2]
            ccc_keywords_count=row[3]

            values = ('articles',languagecode,'ccc','ccc',main_territory,ccc_articles_count,100*ccc_articles_count/ccc_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'ccc','ccc',main_territory, measurement_date)

            values = ('articles',languagecode,'ccc','ccc_geolocated',main_territory,ccc_geolocated_count,100*ccc_geolocated_count/ccc_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'ccc','ccc_geolocated',main_territory, measurement_date)

            values = ('articles',languagecode,'ccc','ccc_keywords',main_territory,ccc_keywords_count,100*ccc_keywords_count/ccc_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'ccc','ccc_keywords',main_territory, measurement_date)


    print ('languagecode, wp, languagecode, ccc,'+measurement_date)
    print ('languagecode, wp, languagecode, no_language_ccc,'+measurement_date)
    print ('languagecode, wp, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, wp, languagecode, ccc_keywords,'+measurement_date)

    print ('languagecode, ccc, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, ccc, languagecode, ccc_keywords,'+measurement_date)

    print ('languagecode, ccc, languagecode, zero_ill,'+measurement_date)

    print ('languagecode, ccc, ccc, qitem,'+measurement_date)
    print ('languagecode, ccc, ccc_geolocated, qitem,'+measurement_date)
    print ('languagecode, ccc, ccc_keywords, qitem,'+measurement_date)


    # WIKIDATA AND CCC
    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks;'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor.fetchone()[0]

    ccc_qitems = {}
    for languagecode in wikilanguagecodes:
        ccc_count = 0
        query = 'SELECT qitem FROM ccc_'+languagecode+' WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            ccc_count+=1
            ccc_qitems[qitem]=''

        values = ('articles','wikidata_article_qitems',,languagecode,'ccc',ccc_count,100*ccc_count/wikidata_article_qitems_count,measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','wikidata_article_qitems,',languagecode,'ccc',ccc_count, measurement_date)


    # LANGUAGES AND LANGUAGES CCC
    language_ccc_count = {}
    for languagecode_1 in wikilanguagecodes:
        all_ccc_articles_count_total=0
        all_ccc_articles_count=0
        all_ccc_rel_value_ccc_total =0  
        all_ccc_rel_value_wp_total =0
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode_1]

        for languagecode_2 in wikilanguagecodes:
            query = 'SELECT COUNT(ccc_binary), COUNT(ccc_keywords), COUNT(ccc_geolocated) FROM ccc_'+languagecode_2+'wiki WHERE ccc_binary=1;'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_articles_count_total = row[0]
            ccc_keywords_count_total = row[1]
            ccc_geolocated_count_total = row[2]
            all_ccc_articles_count_total+=ccc_articles_count_total
            language_ccc_count[languagecode_2]=ccc_articles_count_total

            query = 'SELECT COUNT (*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1;'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_articles_count = row[0]
            ccc_keywords_count = row[1]
            ccc_geolocated_count = row[2]

            all_ccc_articles_count+=ccc_articles_count
            all_ccc_rel_value_ccc_total+=100*ccc_articles_count/ccc_articles_count_total # for CCC% covered by all language editions.
            all_ccc_rel_value_wp_total+=100*ccc_articles_count/wpnumberofarticles # for CCC% impact in all language editions


            ## spread
            values = ('articles',languagecode_2,'ccc',languagecode_1,'wp',ccc_articles_count,100*ccc_articles_count/ccc_articles_count_total,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_2, 'ccc', languagecode_1, 'wp', measurement_date)

            values = ('articles',languagecode_2,'ccc_keywords',languagecode_1,'wp',ccc_keywords_count,100*ccc_keywords_count/ccc_keywords_count_total,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_2, 'ccc_keywords', languagecode_1, 'wp', measurement_date)

            values = ('articles',languagecode_2,'ccc_geolocated',languagecode_1,'wp',ccc_geolocated_count,100*ccc_geolocated_count/ccc_geolocated_count_total,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_2, 'ccc_geolocated', languagecode_1, 'wp', measurement_date)

            ## gap
            values = ('articles',languagecode_1,'wp',languagecode_2,'ccc',ccc_articles_count,100*ccc_articles_count/wpnumberofarticles,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1, 'wp', languagecode_2, 'ccc', measurement_date)

            values = ('articles',languagecode_1,'wp',languagecode_2,'ccc_keywords',ccc_keywords_count,100*ccc_keywords_count/wpnumberofarticles,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1, 'wp', languagecode_2, 'ccc_keywords', measurement_date)

            values = ('articles',languagecode_1,'wp',languagecode_2,'ccc_geolocated',ccc_geolocated_count,100*ccc_geolocated_count/wpnumberofarticles,measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1, 'wp', languagecode_1, 'ccc_geolocated', measurement_date)


        ### all articles
        # how well this language edition covered all CCC articles?
        values = ('articles','all_ccc_articles','',languagecode_1,'wp',all_ccc_articles_count,100*all_ccc_articles_count/all_ccc_articles_count_total, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','all_ccc_articles','',languagecode_1,'wp', measurement_date)

        # what is the extent of all ccc articles in this language edition?
        values = ('articles',languagecode_1,'wp','all_ccc_articles','',all_ccc_articles_count,100*all_ccc_articles_count/wpnumberofarticles, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','all_ccc_articles','',languagecode_1,'wp', measurement_date)

        # what is the extent of this language ccc in all the languages ccc?
        values = ('articles','all_ccc_articles','',languagecode_1,'ccc',all_ccc_articles_count,100*all_ccc_articles_count/language_ccc_count[languagecode_1], measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','all_ccc_articles','',languagecode_1,'ccc', measurement_date)


        ### all articles in average
        # how well this language edition covered all CCC articles in average?
        all_ccc_rel_value_ccc_total_avg=all_ccc_rel_value_ccc_total/len(wikilanguagecodes)
        all_ccc_abs_value_avg=all_ccc_articles_count/len(wikilanguagecodes)
        values = ('articles','all_ccc_avg','',languagecode_1,'wp',all_ccc_abs_value_avg,all_ccc_rel_value_ccc_total_avg, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','all_ccc_avg', '',languagecode_1, 'wp', measurement_date)

        # what is the average extent of this language ccc in all the languages ccc?
        all_ccc_rel_value_ccc_total_avg=all_ccc_rel_value_wp_total/len(wikilanguagecodes)
        all_ccc_abs_value_avg=all_ccc_articles_count/len(wikilanguagecodes)
        values = ('articles',languagecode_1,'ccc', 'all_wp_avg', , all_ccc_rel_value_ccc_total_avg, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode_1, 'ccc', 'all_wp_avg', , measurement_date)

    print ('languagecode_2, ccc, languagecode_1, wp'+ measurement_date)
    print ('languagecode_2, ccc_keywords, languagecode_1, wp'+ measurement_date)
    print ('languagecode_2, ccc_geolocated, languagecode_1, wp'+ measurement_date)

    print ('languagecode_1, wp, languagecode_2, ccc,'+ measurement_date)
    print ('languagecode_1, wp, languagecode_2, ccc_keywords,'+ measurement_date)
    print ('languagecode_1, wp, languagecode_2, ccc_geolocated,'+ measurement_date)

    print ('all_ccc_articles, , languagecode_1, wp'+ measurement_date)
    print ('languagecode_1, wp, all_ccc_articles, ,'+ measurement_date)
    print ('all_ccc_articles, , languagecode_1, ccc'+ measurement_date)

    print ('all_ccc_avg, ,languagecode_1, wp'+ measurement_date)
    print ('languagecode_1, wp, all_wp_avg, ,'+ measurement_date)


    # between languages ccc
    for languagecode_1 in wikilanguagecodes:
        for languagecode_2 in wikilanguagecodes:
            query = 'SELECT COUNT (*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND ccc_'+languagecode_1+'wiki.ccc_binary = 1;'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_coincident_articles_count = row[0]

            values = ('articles',languagecode_1,'ccc',languagecode_2,'ccc',ccc_coincident_articles_count,100*ccc_coincident_articles_count/language_ccc_count[languagecode_1],measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode_1,'ccc',languagecode_2,'ccc', measurement_date)

    print ('languagecode_1, ccc, languagecode_2, ccc,'+ measurement_date)



    # WORK TO DO: THIS SHOULD BE LIMITED TO 100 ARTICLES PER LIST.
    # CCC VITAL ARTICLES LISTS
    lists = ['CCC_Vital_articles_editors', 'CCC_Vital_articles_featured', 'CCC_Vital_articles_geolocated', 'CCC_Vital_articles_keywords', 'CCC_Vital_articles_women', 'CCC_Vital_articles_men', 'CCC_Vital_articles_created_first_three_years', 'CCC_Vital_articles_created_last_year', 'CCC_Vital_articles_pageviews', 'CCC_Vital_articles_discussions']

    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode_1]

        all_ccc_vital_articles_count = 0
        for list_name in lists:
            lists_qitems = []
            query = 'SELECT qitem FROM ccc_vital_lists_rankings WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_vital_lists_rankings);'

            for row in cursor2.execute(query): lists_qitems.append(row[0])
    #           lists_qitems_count=len(lists_qitems)
            all_ccc_vital_articles_count+=len(lists_qitems)

            page_asstring = ','.join( ['?'] * len( lists_qitems ) )
            ccc_list_count =
            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%);' % page_asstring
            cursor.execute(query, (lists_qitems))
            ccc_list_coincident_count = cursor.fetchone()[0]
            all_ccc_vital_articles_coincident_count+=ccc_list_coincident_count

            rel_value = 100*ccc_list_coincident_count/wpnumberofarticles
            values = ('articles','ccc_vital_articles_lists',list_name,'wp',languagecode,ccc_list_coincident_count,100*ccc_list_coincident_count/wpnumberofarticles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','ccc_vital_articles_lists',list_name, measurement_date)

        # all CCC Vital articles lists
        values = ('articles','ccc','all_ccc_vital_articles',languagecode,'wp',all_ccc_vital_articles_coincident_count,100*all_ccc_vital_articles_coincident_count/all_ccc_vital_articles_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','ccc_vital_articles_lists',list_name, measurement_date)

        values = ('articles',languagecode,'wp','ccc','all_ccc_vital_articles',all_ccc_vital_articles_coincident_count,100*all_ccc_vital_articles_coincident_count/wpnumberofarticles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','ccc_vital_articles_lists',list_name, measurement_date)


    # PEOPLE SEGMENTS (PEOPLE, MALE, FEMALE)
    gender = {'Q6581097':'male','Q6581072':'female'}
    gender_name_count_total = {}
    people_count_total = 0
    query = 'SELECT qitem2, COUNT(*) FROM people_properties GROUP BY qitem2;'
    cursor.execute(query)
    for row in cursor.execute(query):
        gender_name_count_total[gender[row[0]]]=row[1]
        people_count_total += row[1]
    gender_name_count_total['people']=people_count_total

    values = ('articles','wikidata_article_qitems',,'wikidata_article_qitems','people',gender_name_count_total['people'],100*gender_name_count_total['people']/wikidata_article_qitems_count, measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems',,'wikidata_article_qitems','people', measurement_date)

    values = ('articles','wikidata_article_qitems','people','wikidata_article_qitems','male',gender_name_count_total['male'],100*gender_name_count_total['male']/gender_name_count_total['people'], measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems',,'wikidata_article_qitems','male', measurement_date)

    values = ('articles','wikidata_article_qitems',,'wikidata_article_qitems','female',gender_name_count_total['female'],100*gender_name_count_total['female']/gender_name_count_total['people'], measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems',,'wikidata_article_qitems','female', measurement_date)

    print ('wikidata_article_qitems, , wikidata_article_qitems, people, '+measurement_date)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, female, '+measurement_date)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, male, '+measurement_date)


    # zero ill: people
    query = 'SELECT COUNT(*) FROM sitelinks INNER JOIN people_properties ON sitelins.qitem = people_properties.qitem2 GROUP BY sitelinks.qitem HAVING (COUNT(qitem) = 1);'
    cursor.execute(query)
    zero_ill_people_count = cursor.fetchone()[0]

    values = ('articles','wikidata_article_qitems','people','wikidata_article_qitems','zero_ill',zero_ill_people_count,100*zero_ill_people_count/gender_name_count_total['people'], measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems','people','wikidata_article_qitems','zero_ill', measurement_date)

    print ('wikidata_article_qitems, people, wikidata_article_qitems, zero_ill, '+measurement_date)

    # zero ill: male
    query = 'SELECT COUNT(*) FROM sitelinks INNER JOIN people_properties ON sitelins.qitem = people_properties.qitem2 AND people_properties.qitem2 = "Q6581097" GROUP BY sitelinks.qitem HAVING (COUNT(qitem) = 1);'
    cursor.execute(query)
    zero_ill_male_count = cursor.fetchone()[0]

    values = ('articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill',zero_ill_male_count,100*zero_ill_male_count/gender_name_count_total['people'], measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill', measurement_date)

    print ('wikidata_article_qitems, male, wikidata_article_qitems, zero_ill, '+measurement_date)


    # zero ill: female
    query = 'SELECT COUNT(*) FROM sitelinks INNER JOIN people_properties ON sitelins.qitem = people_properties.qitem2 AND people_properties.qitem2 = "Q6581072" GROUP BY sitelinks.qitem HAVING (COUNT(qitem) = 1);'
    cursor.execute(query)
    zero_ill_female_count = cursor.fetchone()[0]

    values = ('articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill',zero_ill_female_count,100*zero_ill_female_count/gender_name_count_total['people'], measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems','female','wikidata_article_qitems','zero_ill', measurement_date)

    print ('wikidata_article_qitems, female, wikidata_article_qitems, zero_ill, '+measurement_date)


    # languages
    languages_people_count={}
    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        query = 'SELECT qitem2, COUNT(*) FROM people_properties INNER JOIN sitelinks ON people_properties.qitem = sitelinks.qitem WHERE langcode='+languagecode+'wiki GROUP BY qitem2'
        gender_name_count = {}
        people_count = 0
        for row in cursor3.execute(query):
            gender_name_count[gender[row[0]]]=row[1]
            people_count += row[1]
        gender_name_count['people']=people_count

        languages_people_count[languagecode]=gender_name_count

        for gender_name, gender_count in gender_name_count.items():
            values = ('articles',languagecode,'wp',languagecode,gender_name,gender_count,100*gender_count/wpnumberofarticles,measurement_date)
            cursor3.execute(query_insert,values); conn3.commit()
            cursor3.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, gender_name, measurement_date)

            values = ('articles',languagecode, gender_name, languagecode, 'wp', gender_count,100*gender_count/gender_name_count_total,measurement_date)
            cursor3.execute(query_insert,values); conn3.commit()
            cursor3.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles','wikidata_article_qitems', gender_name, languagecode, 'wp', gender_count, measurement_date)

    print ('languagecode, wp, languagecode, male,'+measurement_date)
    print ('languagecode, wp, languagecode, female,'+measurement_date)
    print ('languagecode, wp, languagecode, people,'+measurement_date)

    print ('languagecode, male, wikidata_article_qitems, male,'+measurement_date)
    print ('languagecode, female, wikidata_article_qitems, female, '+measurement_date)
    print ('languagecode, people, wikidata_article_qitems, people, '+measurement_date)


    # PEOPLE SEGMENTS AND CCC
    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            qitems.append(row[0])

        # male
        male=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query):
            male.append(row[0])
        malecount=len(male)
        male_ccc = set(male).intersection(set(qitems))
        male_ccc_count=len(male_ccc)

        values = ('articles',languagecode, 'male', languagecode, 'ccc', male_ccc_count, 100*male_ccc_count/malecount,measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'male', languagecode, 'ccc', measurement_date)

        values = ('articles',languagecode, 'ccc', languagecode, 'male', male_ccc_count,100*male_ccc_count/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'male', measurement_date)


        # female
        female=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female_ccc = set(female).intersection(set(qitems))
        female_ccc_count=len(female)

        values = ('articles',languagecode, 'female', languagecode, 'ccc', female_ccc_count, 100*female_ccc_count/femalecount,measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'female', languagecode, 'ccc', measurement_date)

        values = ('articles',languagecode, 'ccc', languagecode, 'female', female_ccc_count,100*female_ccc_count/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'female', measurement_date)


        # people
        people_count=femalecount+malecount
        ccc_peoplecount=male_ccc_count+female_ccc_count
        values = ('articles',languagecode, 'people', languagecode, 'ccc', ccc_peoplecount, 100*ccc_peoplecount/people_count,measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'people', languagecode, 'ccc', measurement_date)

        values = ('articles',languagecode, 'ccc', languagecode, 'people', ccc_peoplecount,100*ccc_peoplecount/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'people', measurement_date)

        values = ('articles',languagecode, 'wp', languagecode, 'ccc_people', ccc_peoplecount,100*ccc_peoplecount/wpnumberofarticles[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'ccc_people', measurement_date)

    print ('languagecode, male, languagecode, ccc,'+measurement_date)
    print ('languagecode, female, languagecode, ccc,'+measurement_date)
    print ('languagecode, people, languagecode, ccc,'+measurement_date)

    print ('languagecode, ccc, languagecode, male,'+measurement_date)
    print ('languagecode, ccc, languagecode, female,'+measurement_date)
    print ('languagecode, ccc, languagecode, people,'+measurement_date)

    print ('languagecode, wp, languagecode, ccc_people,'+measurement_date)


    # GEOLOCATED SEGMENTS (COUNTRIES, SUBREGIONS, REGIONS)
    country_names, regions, subregions = load_iso_3166_to_geographical_regions() # iso 3166 to X
    query = 'SELECT iso3166, COUNT(DISTINCT qitem) FROM geolocated_property GROUP BY iso3166'
    iso3166_qitems = {}
    geolocated_items_count_total = 0
    for row in cursor3.execute(query):
        iso3166_qitems[row[0]]=row[1]
        geolocated_items_count_total+=row[1]

    values = ('articles','wikidata_article_qitems',,'wikidata_article_qitems','geolocated',geolocated_items_count_total,100*geolocated_items_count_total/wikidata_article_qitems_count, measurement_date)
    cursor.execute(query_insert,values); conn.commit(); cursor.execute('SELECT last_insert_rowid()')
    intersection_id = cursor.fetchone()[0]
    generate_increments(intersection_id,'articles','wikidata_article_qitems',,'wikidata_article_qitems','geolocated', measurement_date)

    print ('wikidata_article_qitems, , wikidata_article_qitems, geolocated, '+measurement_date)


    query = 'SELECT iso3166, COUNT(DISTINCT qitem) FROM geolocated_property WHERE qitem IN (SELECT qitem FROM sitelinks HAVING (COUNT(qitem) = 1)) GROUP BY iso3166'
    iso3166_qitems_zero_ill = {}
    geolocated_items_zero_ill_count_total = 0
    for row in cursor3.execute(query):
        iso3166_qitems_zero_ill[row[0]]=row[1]
        geolocated_items_zero_ill_count_total+=row[1]

    regions_count_total={}
    subregions_count_total={}
    for iso3166_code, iso3166_count in iso3166_qitems.items():
        if iso3166_code not in regions_count_total: regions_count_total[regions[iso3166_code]]=iso3166_count
        else: regions_count_total[regions[iso3166_code]]+=iso3166_count

        if iso3166_code not in subregions_count_total: subregions_count_total[regions[iso3166_code]]=iso3166_count
        else: subregions_count_total[regions[iso3166_code]]+=iso3166_count

        # countries
        values = ('articles','wikidata_article_qitems','geolocated','countries',iso3166_code,iso3166_count,100*iso3166_count/geolocated_items_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','countries',iso3166_code, measurement_date)

    # subregions
    for subregion_name, subregion_count in subregions_count_total.items():
        values = ('articles','wikidata_article_qitems','geolocated','subregions',subregion_name,subregion_count,100*subregion_count/geolocated_items_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','subregions',subregion_name, measurement_date)

    # regions
    for region_name, region_count in regions_count_total.items():
        values = ('articles','wikidata_article_qitems','geolocated','regions',region_name,region_count,100*region_count/geolocated_items_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','regions',region_name,region_count, measurement_date)

    print ('wikidata_article_qitems, geolocated, countries, iso3166,'+measurement_date)
    print ('wikidata_article_qitems, geolocated, subregions, subregion_name'+measurement_date)
    print ('wikidata_article_qitems, geolocated, regions, region_name'+measurement_date)


    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        geolocated_articles_count = 0
        iso3166_articles = {}
        query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki GROUP BY iso3166'
        cursor.execute(query)
        for row in cursor.execute(query):
            iso3166_articles[row[0]]=row[1]
            geolocated_articles_count+=row[1]

        values = ('articles','wikidata_article_qitems','geolocated',languagecode,'geolocated',geolocated_articles_count,100*geolocated_articles_count/geolocated_items_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode,'wp','wikidata_article_qitems','geolocated', measurement_date)

        values = ('articles',languagecode,'wp','wikidata_article_qitems','geolocated',geolocated_articles_count,100*geolocated_articles_count/wikipedialanguage_numberarticles, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode,'wp','wikidata_article_qitems','geolocated', measurement_date)


        regions_count={}
        subregions_count={}
        for iso3166_code, iso3166_count in iso3166_articles.items():
            if iso3166_code not in regions_count: regions_count[regions[iso3166_code]]=iso3166_code
            else: regions_count[regions[iso3166_code]]+=iso3166_code

            if iso3166_code not in subregions_count: subregions_count[regions[iso3166_code]]=iso3166_code
            else: subregions_count[regions[iso3166_code]]+=iso3166_code

                # countries
                values = ('articles',languagecode,'geolocated','countries',iso3166_code,iso3166_count,100*iso3166_count/geolocated_articles_count, measurement_date)
                cursor.execute(query_insert,values); conn.commit()
                cursor.execute('SELECT last_insert_rowid()')
                intersection_id = cursor.fetchone()[0]
                generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','countries',iso3166_code, measurement_date)

                # countries
                values = ('articles','countries',iso3166_code,languagecode,'geolocated',iso3166_count,100*iso3166_count/iso3166_qitems[iso3166], measurement_date)
                cursor.execute(query_insert,values); conn.commit()
                cursor.execute('SELECT last_insert_rowid()')
                intersection_id = cursor.fetchone()[0]
                generate_increments(intersection_id,'articles','countries',iso3166_code,'countries',iso3166_code, measurement_date)

        # subregions
        for subregion_name, subregion_count in subregions_count_total.items():
            values = ('articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,100*subregion_count/geolocated_articles_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','subregions',subregion_name, measurement_date)

            values = ('articles','subregions', subregion_name, languagecode, 'geolocated', subregion_count,100*subregion_count/subregions_count[subregion_name], measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles','subregions', subregion_name, languagecode, 'geolocated', measurement_date)

        # regions
        for region_name, region_count in regions_count_total.items():
            values = ('articles',languagecode,'geolocated','regions',region_name,region_count,100*region_count/geolocated_articles_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles','wikidata_article_qitems','geolocated','regions',region_name,region_count, measurement_date)

            values = ('articles','regions',region_name,languagecode,'geolocated',region_count,100*region_count/region_count[region_name], measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles','regions',region_name,languagecode,'geolocated',region_count, measurement_date)

    print ('wikidata_article_qitems, geolocated, languagecode, geolocated'+measurement_date)
    print ('languagecode, wp, wikidata_article_qitems, geolocated'+measurement_date)

    print ('languagecode, geolocated, countries, iso3166,'+measurement_date)
    print ('languagecode, geolocated, subregions, iso3166'+measurement_date)
    print ('languagecode, geolocated, regions, iso3166'+measurement_date)

    print ('countries, iso3166, languagecode, geolocated,'+measurement_date)
    print ('subregions, subregion_name, languagecode, geolocated,'+measurement_date)
    print ('regions, region_name, languagecode, geolocated,'+measurement_date)

    print ('* generate_all_articles_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_last_month_articles_intersections():


    functionstartTime = time.time()
    print ('* generate_all_articles_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    query_insert = 'INSERT INTO wcdo_intersections (content, entity_1, entity_1_descriptor, entity_2, entity_2_descriptor, abs_value, rel_value, rel_reference, measurement_date) VALUES (?,?,?,?,?,?,?,?);'

    for languagecode in wikilanguagecodes:
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE date_created < '+ (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S')
        for row in cursor.execute(query):
            qitems.append(row[0])
        created_articles_count = len(qitems)

        # ALL ARTICLES
        values = ('articles',languagecode, 'wp', languagecode, 'last_month_articles', created_articles_count, 100*created_articles_count/wpnumberofarticles, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'wp', languagecode, 'last_month_articles', measurement_date)

        # CCC
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE date_created < '+ (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S' + 'AND ccc_binary=1')
        cursor.execute(query)
        ccc_articles_created_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;')
        ccc_articles_count = cursor.fetchone()[0]

        values = ('articles',languagecode, 'ccc', languagecode, 'last_month_articles', ccc_articles_created_count, 100*ccc_articles_created_count/ccc_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc', languagecode, 'last_month_articles', measurement_date)

        values = ('articles',languagecode, 'last_month_articles',languagecode, 'ccc', ccc_articles_created_count, 100*ccc_articles_created_count/created_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles',languagecode, 'ccc' measurement_date)

        # CCC geolocated
        cursor.execute('SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND ccc_geolocated=1;' + 'AND ccc_binary=1')
        ccc_geolocated_articles_count = cursor.fetchone()[0]

        values = ('articles',languagecode, 'ccc_geolocated', languagecode, 'last_month_articles', ccc_geolocated_articles_count, 100*ccc_geolocated_articles_count/ccc_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc_geolocated', languagecode, 'last_month_articles', measurement_date)

        values = ('articles',languagecode, 'last_month_articles',languagecode, 'ccc_geolocated', ccc_geolocated_articles_count, 100*ccc_geolocated_articles_count/created_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles',languagecode, 'ccc_geolocated' measurement_date)


        # CCC keywords
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_keywords IS NOT NULL ' + 'AND ccc_binary=1'
        cursor.execute(query)
        ccc_keywords_articles_count = cursor.fetchone()[0]

        values = ('articles',languagecode, 'ccc_keywords', languagecode, 'last_month_articles', ccc_keywords_articles_count, 100*ccc_keywords_articles_count/ccc_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'ccc_keywords', languagecode, 'last_month_articles', measurement_date)

        values = ('articles',languagecode, 'last_month_articles',languagecode, 'ccc_keywords', ccc_keywords_articles_count, 100*ccc_keywords_articles_count/created_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles',languagecode, 'ccc_geolocated' measurement_date)


        # No own CCC
        no_own_ccc_created_count = created_articles_count - ccc_articles_created_count

        values = ('articles',languagecode, 'no_language_ccc', languagecode, 'last_month_articles', ccc_keywords_articles_count, 100*ccc_keywords_articles_count/ccc_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'no_language_ccc', languagecode, 'last_month_articles', measurement_date)

        values = ('articles',languagecode, 'last_month_articles',languagecode, 'no_language_ccc', no_own_ccc_created_count, 100*no_own_ccc_created_count/created_articles_count, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles',languagecode, 'no_language_ccc' measurement_date)
        

        # Other Langs CCC
        for languagecode_2 in wikilanguagecodes:
            query = 'SELECT COUNT (*) FROM ccc_'+languagecode_1+'wiki INNER JOIN ccc_'+languagecode_2+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND ccc_'+languagecode_1+'wiki.date_created < '+ (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S')

            cursor.execute(query)
            ccc_articles_created_count = cursor.fetchone()[0]

            values = ('articles',languagecode, 'last_month_articles',languagecode_2, 'ccc', ccc_articles_created_count, 100*ccc_articles_created_count/created_articles_count, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode, 'last_month_articles',languagecode, 'ccc' measurement_date)


        # PEOPLE
        # male
        male=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query):
            male.append(row[0])
        malecount=len(male)
        male = set(male).intersection(set(qitems))
        last_month_articles_male_count=len(male)

        values = ('articles',languagecode, 'last_month_articles', languagecode, 'male', last_month_articles_male_count,100*last_month_articles_male_count/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles', languagecode, 'male', measurement_date)


        # female
        female=[]
        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor3.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female = set(female).intersection(set(qitems))
        last_month_female_count=len(female)

        values = ('articles',languagecode, 'last_month_articles', languagecode, 'female', last_month_articles_male_count,100*last_month_articles_male_count/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles', languagecode, 'female', measurement_date)


        # people
        people_count=femalecount+malecount
        ccc_peoplecount=last_month_articles_male_count+last_month_female_count

        values = ('articles',languagecode, 'last_month_articles', languagecode, 'people', last_month_articles_male_count,100*ccc_peoplecount/language_ccc_count[languagecode],measurement_date)
        cursor3.execute(query_insert,values); conn3.commit()
        cursor3.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode, 'last_month_articles', languagecode, 'people', measurement_date)


        # GEOLOCATED SEGMENTS
        geolocated_articles_count = 0
        iso3166_articles = {}
        query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki AND ccc_'+languagecode_1+'wiki.date_created < '+ (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y%m%d%H%M%S') + ' GROUP BY iso3166'
        cursor.execute(query)
        for row in cursor.execute(query):
            iso3166_articles[row[0]]=row[1]
            geolocated_articles_count+=row[1]

        values = ('articles',languagecode,'last_month_articles','wikidata_article_qitems','geolocated',geolocated_articles_count,100*geolocated_articles_count/last_month_articles, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles',languagecode,'last_month_articles','wikidata_article_qitems','geolocated', measurement_date)

        regions_count={}
        subregions_count={}
        for iso3166_code, iso3166_count in iso3166_articles.items():
            if iso3166_code not in regions_count: regions_count[regions[iso3166_code]]=iso3166_code
            else: regions_count[regions[iso3166_code]]+=iso3166_code

            if iso3166_code not in subregions_count: subregions_count[regions[iso3166_code]]=iso3166_code
            else: subregions_count[regions[iso3166_code]]+=iso3166_code

                # countries
                values = ('articles',languagecode,'last_month_articles','countries',iso3166_code,iso3166_count,100*iso3166_count/last_month_articles, measurement_date)
                cursor.execute(query_insert,values); conn.commit()
                cursor.execute('SELECT last_insert_rowid()')
                intersection_id = cursor.fetchone()[0]
                generate_increments(intersection_id,'articles',languagecode,'last_month_articles','countries',iso3166_code, measurement_date)

        # subregions
        for subregion_name, subregion_count in subregions_count_total.items():
            values = ('articles',languagecode,'last_month_articles','subregions',subregion_name,subregion_count,100*subregion_count/last_month_articles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','subregions',subregion_name, measurement_date)

        # regions
        for region_name, region_count in regions_count_total.items():
            values = ('articles',languagecode,'last_month_articles','regions',region_name,region_count,100*region_count/last_month_articles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',anguagecode,'last_month_articles','regions',region_name,region_count, measurement_date)

    
        # CCC VITAL ARTICLES LISTS
        lists = ['CCC_Vital_articles_editors', 'CCC_Vital_articles_featured', 'CCC_Vital_articles_geolocated', 'CCC_Vital_articles_keywords', 'CCC_Vital_articles_women', 'CCC_Vital_articles_men', 'CCC_Vital_articles_created_first_three_years', 'CCC_Vital_articles_created_last_year', 'CCC_Vital_articles_pageviews', 'CCC_Vital_articles_discussions']

        lists_count = 0
        for list_name in lists:
            lists_qitems = []
            query = 'SELECT qitem FROM ccc_vital_lists_rankings WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_vital_lists_rankings);'

            for row in cursor.execute(query): lists_qitems.append(row[0])
#           lists_qitems_count=len(lists_qitems)
            coincident_qitems = set(lists_qitems).intersection(set(qitems))
            last_month_list_count=len(coincident_qitems)
            lists_count+=last_month_list_count

            values = ('articles',languagecode,'last_month_articles','ccc_vital_articles_lists',list_name,last_month_list_count,100*last_month_list_count/last_month_articles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','ccc_vital_articles_lists',list_name, measurement_date)

        # all CCC Vital articles lists
        values = ('articles',languagecode,'last_month_articles','ccc','all_ccc_vital_articles',last_month_list_count,100*last_month_list_count/last_month_articles, measurement_date)
            cursor.execute(query_insert,values); conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            intersection_id = cursor.fetchone()[0]
            generate_increments(intersection_id,'articles',languagecode,'last_month_articles','all_ccc_vital_articles',list_name, measurement_date)


    print ('languagecode, wp, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, ccc, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, ccc,'+measurement_date)
    print ('languagecode, ccc_geolocated, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, ccc_keywords, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, last_month_articles,languagecode, ccc_keywords,'+measurement_date)
    print ('languagecode, no_language_ccc, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, no_language_ccc,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode_2, ccc,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, male,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, female,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, people,'+measurement_date)
    print ('languagecode, last_month_articles, wikidata_article_qitems, geolocated,'+measurement_date)
    print ('languagecode, last_month_articles, countries, iso3166,'+measurement_date)
    print ('languagecode, last_month_articles, subregions, subregion_name,'+measurement_date)
    print ('languagecode, last_month_articles, regions, region_name,'+measurement_date)
    print ('languagecode, last_month_articles, ccc_vital_articles_lists, ,'+measurement_date)
    print ('languagecode, last_month_articles, ccc, all_ccc_vital_articles,'+measurement_date)

    print ('* generate_last_month_articles_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




def generate_pageviews_intersections():


    functionstartTime = time.time()
    print ('* generate_all_articles_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    query_insert = 'INSERT INTO wcdo_intersections (content, entity_1, entity_1_descriptor, entity_2, entity_2_descriptor, abs_value, rel_value, rel_reference, measurement_date) VALUES (?,?,?,?,?,?,?,?);'

    for languagecode in wikilanguagecodes:
        print ('')



def generate_increments(cur_intersection_id, entity_1, entity_1_descriptor, entity_2, entity_2_descriptor, measurement_date):

    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    query = 'SELECT abs_value, rel_value, measurement_date FROM wcdo_intersections WHERE entity_1 = ?, entity_1_descriptor=? entity_2=? entity_2_descriptor=? ORDER BY measurement_date DESC'

    measurement_dates = []
    abs_values = []
    rel_values = []

    for row in cursor.execute(query):
        measurement_dates.append(row[0])
        abs_values.append(row[1])
        rel_values.append(row[2])

    current_date = datetime.strptime(measurement_dates[0], '%Y%m%d')
    current_abs_value = abs_values[0]
    current_rel_value = rel_values[0]

    query = 'INSERT INTO wcdo_increments (cur_intersection_id, abs_increment, rel_increment, period) VALUES (?,?,?,?)'

    for x in range(1,len(measurement_dates)):
        r = relativedelta.relativedelta(current_date, old_date)
        month_count = r.months
        period = ''
        
        if month_count ==1: period = 'month'
        if month_count ==3: period = 'quarter' 
        if month_count ==6: period = 'semester'
        if month_count ==12: period = 'year'
        if month_count ==60: period = 'five years'

        if period != '':
            abs_increment = current_abs_value-abs_values[x]
            rel_increment = current_rel_value-rel_values[x]
            values = ('articles',cur_intersection_id,abs_increment,rel_increment,period)
            cursor.execute(query,values); conn.commit()



def generate_ccc_vital_articles_lists(languagecode, languagecode_target, content_type, category, percentage_filtered, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns, page_titles_qitems, country, list_name):

    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('\n\n* make_table_ccc_vital_articles_list')
    print (list_name)
    print ('Obtaining a prioritized article list based on these parameters:')
    print (languagecode, languagecode_target, content_type, category, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns)

    # Databases connections
    conn = sqlite3.connect('ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect('wcdo_data.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect('wikidata.db'); cursor3 = conn3.cursor()

    # DEFINE CONTENT TYPE
    # according to the content type, we make a query or another.
    print ('define the content type')
    if content_type[0] == 'ccc': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
    if content_type[0] == 'gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NOT NULL'
    if content_type[0] == 'kw': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
    if content_type[0] == 'ccc_not_gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NULL'
    if content_type[0] == 'ccc_main_territory': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'


    # DEFINE CATEGORY TO FILTER THE DATA (specific territory, specific topic)
    print ('define the specific category.')
    if category != '':
        print ('We are usign these categories to filter the content (either topics or territories).')
        print (category)

        if isinstance(category,str): query = query + ' AND (main_territory = "'+str(category)+'");'
        else:
            query = query + ' AND ('
            for cat in category:
                query = query + 'main_territory = "'+str(cat)+'"'
                if (category.index(cat)+1)!=len(category): query = query + ' OR '
            query = query + ')'


    # DEFINE THE TIMEFRAME -> if it is necessary, it will admit two timestamps two be passed as parameters.
    print ('define the timeframe')
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

#    print (ccc_df.page_title.values)
#    print (ccc_df.index.values)

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
        for row in cursor3.execute(query):
            if row[0] in ccc_df.index:
                topic_selected.append(row[0])
        print (len(topic_selected))
        ccc_df = ccc_df.reindex(topic_selected)
        print ('this is the number of lines of the dataframe after the content filtering: '+str(len(ccc_df)))
        ccc_df = ccc_df.fillna(0)


    # FILTER THE LOWEST PART OF CCC
    if len(ccc_df)>2*window:
        print ('filter and save the first '+str(percentage_filtered)+'% of the CCC articles in terms of number of strategies and inlinks from CCC.')

        ranked_saved_1=ccc_df['num_inlinks_from_CCC'].sort_values(ascending=False).index.tolist()
        ranked_saved_1=ranked_saved_1[:int(percentage_filtered*len(ranked_saved_1)/100)]

        ranked_saved_2=ccc_df['num_retrieval_strategies'].sort_values(ascending=False).index.tolist()
        ranked_saved_2=ranked_saved_2[:int(percentage_filtered*len(ranked_saved_2)/100)]

        intersection = list(set(ranked_saved_1)&set(ranked_saved_2))

        ccc_df = ccc_df.reindex(index = intersection)
        print ('There are now: '+str(len(ccc_df))+' articles.')
    else:
        print ('Not enough articles to filter.')
#    if (len(ccc_df)<len(territories.loc[languagecode]['QitemTerritory'])): return

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

    # in case there are specific territories
    if isinstance(category,list):
        print ('representativity coefficients filtered by only these categories:')
        print (category)

    if representativity == 'none':
        representativity_coefficients['Any']=1

    if representativity == 'all_equal': # all equal. get all the qitems for the language code. divide the 
        try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems = [territories.loc[languagecode]['QitemTerritory']]
        if isinstance(category,list) and category[0][0] == 'Q': qitems = list(set.intersection(set(qitems),set(category)))

        coefficient=1/(len(qitems)+1)
        for x in qitems: representativity_coefficients[x]=coefficient
        representativity_coefficients[0]=coefficient

    if representativity == 'proportional_articles' or representativity == 'proportional_articles_compensation': # proportional to the number of articles for each territory. check data from: ccc_extent_by_qitem.
        query = 'SELECT qitem, ccc_articles_qitem FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'" AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'");'
#        print (query)
        sum = 0
        for row in cursor2.execute(query):
            main_territory = row[0]
            if isinstance(category,list) and main_territory not in category: continue
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
            if isinstance(category,list) and qitem in category: continue

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
        if len(category)!=0: qitems = category

        coefficient=0.02
        if coefficient > 1/len(qitems): coefficient = round(1/len(qitems),2)
        for x in qitems: representativity_coefficients[x]=coefficient

        rest=1-len(qitems)*coefficient
        representativity_coefficients['Any']=rest

#    if category != '':
#        representativity_coefficients={}
#        representativity_coefficients[category]=1

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
        if x != 0 and x!= 'Any' and x in qitems_page_titles: qitems_territories_names[x]=qitems_page_titles[x]
    print (qitems_territories_names)

    if content_type[0] == 'ccc_main_territory':
        representativity_coefficients={}
        representativity_coefficients[representativity_coefficients_sorted[0]]=1


    # MAKE THE DATAFRAME
    # Creating the final dataframe with the representation for each territory
    print ('make the new dataframe')
    selectionTime = time.time()
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

        if 'Any' in representativity_articles:
            print ('Any has: '+str(representativity_articles['Any']))
            if window > z: representativity_articles['Any']=representativity_articles['Any']+(window-z)
            z=0
            for x,y in representativity_articles.items(): z=z+y        
            print ('the window has: '+str(z))
            print ('Any has: '+str(representativity_articles['Any']))
        else:
            print ('There is not "Any" group of articles to fill.')

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

                    print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,
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
                        if qitem not in prioritized_list: prioritized_list.append(qitem)
                        todelete.append(qitem)
                        print (y,ccc_df.loc[qitem]['page_title'],rank_dict[qitem]); #input('')

            for qitem in todelete: 
                try: articles_ranked.remove(qitem)
                except: pass
    
#        print ('* one window filled.')
    ccc_df=ccc_df.reindex(prioritized_list)
#    print (error)
    print (len(ccc_df))
#    print (prioritized_list[:100])
    print ('selection completed after: ' + str(datetime.timedelta(seconds=time.time() - selectionTime)))


    # https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples

    # INSERT FEATURES
    measurement_date_dict={}
    for x in ccc_df.index.values: measurement_date_dict[x]=measurement_date
    ccc_df['measurement_date'] = pd.Series(measurement_date_dict)

    all_columns = ['num_inlinks', 'num_outlinks', 'num_bytes', 'num_references', 'num_edits', 'num_editors', 'num_discussions', 'num_pageviews', 'num_wdproperty', 'num_interwiki', 'featured_article']

    for x in all_columns:
        if x not in columns:
            column_dict = {}
            for y in ccc_df.index.values: column_dict[y]=None
            ccc_df[x] = pd.Series(column_dict)

    formatted_columns = ['qitem'] + all_columns + ['measurement_date']

    subset = ccc_df[formatted_columns]
    tuples = [tuple(x) for x in subset.values]


    query = 'INSERT OR IGNORE INTO wcdo_lists_article_features (qitem, num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, featured_article, measurement_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);';
    cursor2.executemany(query,tuples)
    query = 'UPDATE wcdo_lists_article_features (qitem, num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, featured_article, measurement_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);';
    cursor2.executemany(query,tuples)
    conn2.commit()


    # INSERT RANKING
    langcode_original_dict={}
    for x in ccc_df.index.values: langcode_original_dict[x]=languagecode
    ccc_df['langcode_original'] = pd.Series(langcode_original_dict)

    list_name_dict={}
    for x in ccc_df.index.values: list_name_dict[x]=list_name
    ccc_df['langcode_original'] = pd.Series(list_name_dict)

    ccc_df=ccc_df.reset_index()
    ccc_df=ccc_df.reset_index()
    df.rename(columns={'index': 'position'}, inplace=True)

    formatted_columns = ['position','langcode_original','qitem','list_name','measurement_date']

    subset = ccc_df[formatted_columns]
    tuples = [tuple(x) for x in subset.values]

    query = 'INSERT OR IGNORE INTO ccc_vital_lists_rankings (position, langcode_original, qitem, list_name, measurement_date) VALUES (?,?,?,?,?);';
    cursor2.executemany(query,tuples)
    conn2.commit()


    # CHECKING ARTICLES CROSS-LANGUAGE AVAILABILITY; INTRODUCING THE ARTICLES TITLES AND INTERSECTIONS
    ccc_vital_articles_availability_count={}
    for languagecode in wikilanguagecodes: ccc_vital_articles_availability_count[languagecode]=0
    
    if country == '': 
        list_origin = languagecode
        origin = 'languagecode'
    else: 
        list_origin = country + '_(' + languagecode + ')'
        origin = 'iso3166_(languagecode)'

    qitems = prioritized_list
    langs_missing_qitems = {}
    for langcode in wikilanguagecodes: langs_missing_qitems[langcode]=[]

    for qitem in qitems:
        remaining_lags = wikilanguagecodes
        parameters = []

        query = 'SELECT page_title, langcode FROM sitelinks WHERE qitem=?'
        for row in cursor3.execute(query):
            original_page_title = ccc_df.loc[qitem]['page_title']
            page_title = row[0].replace(' ','_')
            langcode = row[1].split('wiki')[0]
            remaining_lags.remove(langcode)

            parameters.append((languagecode,qitem,langcode,original_page_title,page_title,'sitelinks',measurement_date))
            ccc_vital_articles_availability_count[langcode]+=1

        query = 'INSERT OR IGNORE INTO wcdo_lists_article_page_titles (langcode_original, qitem, langcode_target, page_title_original, page_title_target, generation_method, measurement_date) values (?,?,?,?,?,?,?);'
        cursor2.executemany(query, parameters)
        conn2.commit()

        for lang in remaining_lags: langs_missing_qitems[lang].append(qitem)

        if qitems.index(qitem)==99:
            for langcode, lang_ccc_count in ccc_vital_articles_availability_count.items():
                values = ('articles',list_origin,list_name,langcode,'wp',ccc_vital_articles_availability_count[langcode],ccc_vital_articles_availability_count[langcode], measurement_date)
                cursor.execute(query_insert,values); conn.commit()
                cursor.execute('SELECT last_insert_rowid()')
                intersection_id = cursor.fetchone()[0]
                generate_increments(intersection_id,'articles',list_origin,list_name,langcode,'wp', measurement_date)


    print (origin+', list_name, languagecode_2, wp, '+measurement_date)

    for langcode, lang_ccc_count in ccc_vital_articles_availability_count.items():
        values = ('articles',list_origin,list_name+'_all_computed',langcode,'wp',ccc_vital_articles_availability_count[langcode],100*ccc_vital_articles_availability_count[langcode]/length, measurement_date)
        cursor.execute(query_insert,values); conn.commit()
        cursor.execute('SELECT last_insert_rowid()')
        intersection_id = cursor.fetchone()[0]
        generate_increments(intersection_id,'articles','languagecode_1','list_name_all_computed','languagecode_2','wp', measurement_date)

    print (origin+', list_name_all_computed, languagecode_2, wp, '+measurement_date)

    languageTime = time.time()
    for langcode_target, missing_qitems in langs_missing_qitems.items():
        print (langcode_target)

        first_time_done = 0
        for missing_qitem in missing_qitems:
            page_title_original = ccc_df.loc[qitem]['page_title']

            # check if it was in the last month translation generated titles and reinsert it if it was there.
            query = 'SELECT page_title_target FROM wcdo_lists_article_page_titles WHERE generation_method = "translation" AND qitem = ?'
            cursor2.execute(query, missing_qitem)
            page_title_target = cursor2.fetchone()
            if page_title_target != None: 
                page_title_target = page_title_target[0]

                parameters = (languagecode, missing_qitem, langcode_target, page_title_original, page_title_target, "translation", measurement_date)
                query = 'INSERT OR IGNORE INTO wcdo_lists_article_page_titles (langcode_original, qitem, langcode_target, page_title_original, page_title_target, generation_method, measurement_date) values (?,?,?,?,?,?,?);'
                cursor2.execute(query, parameters)
                conn2.commit()

            # check wikidata labels and reinsert it if there is one.
            else:
                query = 'SELECT label FROM labels WHERE langcode = ? AND qitem = ?'
                parameters = (langcode_target,missing_qitem)
                cursor3.execute(query, missing_qitem)
                page_title_target = cursor3.fetchone()

                if page_title_target != None: 
                    page_title_target = page_title_target[0]
                    parameters = (languagecode, missing_qitem, langcode_target, page_title_original, page_title_target, "label", measurement_date)
                    query = 'INSERT OR IGNORE INTO wcdo_lists_article_page_titles (langcode_original, qitem, langcode_target, page_title_original, page_title_target, generation_method, measurement_date) values (?,?,?,?,?,?,?);'
                    cursor2.execute(query, parameters)
                    conn2.commit()

                # check if there is a translation pair between 
                else:
                    parameters = (languagecode, langcode_target, "translation")
                    query = 'SELECT COUNT(*) FROM wcdo_lists_article_page_titles WHERE langcode_original = ? AND langcode_target = ? and generation_method = ?;'
                    cursor2.execute(query, parameters)                    
                    providedpair = cursor2.fetchone()[0]

                    if providedpair != 0 or first_time_done == 0:
                        try: 
                            title=ccc_df.loc[qitem]['page_title'].replace('_',' ') # local title
                            r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+languagecode+"/"+languagecode_target+"/Apertium", data={'html': '<div>'+title+'</div>'}, timeout=0.5) # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                            word = str(r.text)
                        except: print ('timeout.')

                        if word != 'Provider not supported':
                            page_title_target = word.split('<div>')[1].split('</div>')[0].replace(' ','_')
                            parameters = (languagecode, missing_qitem, langcode_target, page_title_original, page_title_target, "translation", measurement_date)
                            query = 'INSERT OR IGNORE INTO wcdo_lists_article_page_titles (langcode_original, qitem, langcode_target, page_title_original, page_title_target, generation_method, measurement_date) values (?,?,?,?,?,?,?);'
                            cursor2.execute(query, parameters)
                            conn2.commit()
                        else:
                            page_title_target = ''
                            parameters = (languagecode, missing_qitem, langcode_target, page_title_original, page_title_target, "None", measurement_date)
                            query = 'INSERT OR IGNORE INTO wcdo_lists_article_page_titles (langcode_original, qitem, langcode_target, page_title_original, page_title_target, generation_method, measurement_date) values (?,?,?,?,?,?,?);'
                            cursor2.execute(query, parameters)
                            conn2.commit()
                    first_time_done = 1



        print ('* language target_titles completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime)))

    print ('* generate_ccc_vital_articles_lists Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def delete_last_iteration_ccc_vital_articles_lists():
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    query = 'DELETE FROM ccc_vital_lists_features WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_vital_lists_features)'
    cursor.execute(query); conn.commit()

    query = 'DELETE FROM ccc_vital_lists_rankings WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_vital_lists_features)'
    cursor.execute(query); conn.commit()

    query = 'DELETE FROM ccc_vital_lists_coverage WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_vital_lists_features)'
    cursor.execute(query); conn.commit()


def delete_last_iteration_increments():
    conn = sqlite3.connect('wcdo_data.db'); cursor = conn.cursor()

    query = 'DELETE FROM wcdo_increments WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_vital_lists_features)'
    cursor.execute(query); conn.commit()


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

    measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'

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
    # Only those with a geographical context
    wikilanguagecodes_real = wikilanguagecodes
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = load_wikipedia_language_editions_numberofarticles()

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    # TESTS
#    wikilanguagecodes = obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
#    wikilanguagecodes = ['ca']
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('ii'):]

    print ('\n* Starting the STATS GENERATION CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* STATS GENERATION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    finish_email()