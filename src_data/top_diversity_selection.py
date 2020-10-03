# -*- coding: utf-8 -*-

# common resources
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
import requests
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# data and compute
import pandas as pd
import numpy as np
import shutil
from sklearn.linear_model import LinearRegression


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("top_diversity_selection"+""+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass



######################################################################

# MAIN
def main():

    create_top_diversity_lists_db()
    update_top_ccc_diversity_articles_features()
    update_top_ccc_diversity_articles_titles('sitelinks')
    update_top_ccc_diversity_articles_titles('labels')
    update_top_ccc_diversity_articles_titles('old translations')
    # update_top_ccc_diversity_articles_titles('translations')

    # update_top_diversity_articles_interwiki()
    update_top_diversity_articles_intersections() # the data for Top CCC Coverage / Spread
    # wikilanguages_utils.copy_db_for_production(top_diversity_db, 'top_diversity_selection.py', databases_path)



##### TOP DIVERSITY ARTICLES SELECTION FUNCTIONS ####

def create_top_diversity_lists_db():
    function_name = 'create_top_diversity_lists_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    # Removes database (just for code debugging)
    try:
        os.remove(databases_path + top_diversity_db); 
        print (top_diversity_db+' deleted.');
    except:
        print (top_diversity_db+' could not be deleted.')
        pass

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + top_diversity_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        query = ('CREATE table if not exists '+languagecode+'wiki_top_articles_lists ('+
        'qitem text,'+
        'position integer,'+
        'country text,'+
        'list_name text,'+
        'measurement_date text,'+

        'PRIMARY KEY (qitem, list_name, country))')
        cursor.execute(query)

        query = ('CREATE table if not exists '+languagecode+'wiki_top_articles_features ('+
        'qitem text,'+
        'page_title_original text,'+
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
        'wikirank real, '+
        'featured_article integer, '+
        'num_inlinks_from_CCC integer, '+
        'date_created integer, '+
        'sexual_orientation text, '+
        'ethnic_group text, '+
        'religious_group text, '+
        'measurement_date text, '+

        'PRIMARY KEY (qitem))')
        cursor.execute(query)

        query = ('CREATE table if not exists '+languagecode+'wiki_top_articles_page_titles ('+
        'qitem text,'+
        'page_title_target text,'+ 
        'generation_method text,'+ # page_title_target can either be the REAL (from sitelinks wikitada), the label proposal (from labels wikitada) or translated (content translator tool).
        'measurement_date text,'+

        'PRIMARY KEY (qitem))')
        cursor.execute(query)

        query = ('CREATE table if not exists wdo_intersections ('+
        'set1 text not null, '+
        'set1descriptor text, '+

        'set2 text, '+
        'set2descriptor text, '+

        'abs_value integer,'+
        'rel_value float,'+

        'measurement_date text,'
        'PRIMARY KEY (set1,set1descriptor,set2,set2descriptor))')

        cursor.execute(query)
        conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_all_top_ccc_diversity_articles_lists():

    print ('Generating all the Top articles lists.')

    function_realname = 'generate_all_top_ccc_diversity_articles_lists'
    functionrealstartTime = time.time()

    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_realname, 'check','')==1: return

    # wikilanguagecodes_real = ['ca']
#    wikilanguagecodes_real=['it']
#    wikilanguagecodes_real = ['ca','it','en','es','ro']
#    wikilanguagecodes_real=['it', 'fr', 'ca', 'en', 'de', 'es', 'nl', 'uk', 'pt', 'pl']
#    wikilanguagecodes_real=wikilanguagecodes[wikilanguagecodes.index('ki'):]

    # LANGUAGES
    for languagecode in wikilanguagecodes_real:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        try:
            print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | ')
        except:
            pass

        # COUNTRIES FOR THE CCC COUNTRY LISTS
        countries = wikilanguages_utils.load_countries_from_language(languagecode,territories)
        countries.append('')
        print ('these are the countries from this language:')
        print (countries)
        length = 500

        only_languages_ccc = 'no' # LIMIT TO ONLY LANGUAGES CCC
        if only_languages_ccc == 'yes': countries = ['']

        for country in countries:
            country_Time = time.time()
            print ('\n\nThis country starts now: '+str(country_Time))

            country_name = ''
            # for the wiki_path
            if country != '': 
                country_name = territories.loc[territories['ISO3166'] == country].loc[languagecode]['country']
                if isinstance(country_name, str) != True: country_name=list(country_name)[0]
            else: country = ''

            # category
            if country != '': 
                qitems_list = wikilanguages_utils.load_territories_from_language_country(country, languagecode, territories)
                category = qitems_list
            else: category = ''

            # print country and territories
            if country_name != '': print ('Lists for country: '+country_name+' ('+languages.loc[languagecode]['languagename']+' speaking territories)')
            else: print ('Lists for entire language: '+languages.loc[languagecode]['languagename'])


            function_name = 'generate_all_top_ccc_diversity_articles_lists '+languagecode+' '+country
            if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: continue
            functionstartTime = time.time()



            # (languagecode, content_type, category, percentage_filtered, time_frame, relevance_rank, relevance_sense, window, representativity, columns, page_titles_qitems, country, list_name)

            ## GENERAL CCC ###

            # EDITORS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_editors': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'editors')

            # EDITS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'edits')

            # MOST EDITED DURING THE LAST MONTH
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_edits_last_month': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'edited_last_month')

            # CREATED DURING FIRST THREE YEARS AND MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, 'first_three_years', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_first_three_years')

            # CREATED DURING LAST YEAR AND MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, 'last_year', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_last_year')

            # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_pageviews':1}, 'positive', length, 'none', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'pageviews')

            # MOST DISCUSSED (EDITS DISCUSSIONS)
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_discussions': 1}, 'positive', length, 'none', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'discussions')

            # FEATURED, LONG AND CITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'featured_article': 0.8, 'num_references':0.1, 'num_bytes':0.1}, 'positive', length, 'none', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'featured')

            # IMAGES, LONG AND CITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_images': 0.8, 'num_bytes':0.1, 'num_references':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'images')

            # MOST WD STATEMENTS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_wdproperty': 0.9, 'num_editors':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'wdproperty_many')

            # MOST INTERWIKI
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'num_interwiki': 0.9, 'num_editors':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_many')

            # LEAST INTERWIKI (EDITOR PEARLS)
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'interwiki_relationship': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_editors')

            # LEAST INTERWIKI (WD STATEMENTS PEARLS)
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'interwiki_relationship': 0.9, 'num_pageviews': 0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_wdproperty')

            # HIGHEST WIKIRANK AND POPULAR
            make_top_ccc_diversity_articles_list(languagecode, ['ccc'], category, 80, '', {'wikirank': 0.9, 'num_pageviews':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'wikirank')



            ### SPECIFIC CCC ###

            # GL MOST INLINKED FROM CCC
            make_top_ccc_diversity_articles_list(languagecode, ['gl'], category, 80, '', {'num_inlinks_from_CCC': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'geolocated')

            # KEYWORDS ON TITLE WITH MOST BYTES
            make_top_ccc_diversity_articles_list(languagecode, ['kw'], category, 80, '', {'num_bytes': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'keywords')


            ### OTHER DIVERSITY SEGMENTS ###
            # WOMEN BIOGRAPHY MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','female'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'women')

            # MEN BIOGRAPHY MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','male'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'men')




            # LGTB BIOGRAPHY MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','lgtb'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'sexual_orientation')

            # ETHNIC GROUPS BIOGRAPHY MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','ethnic_group'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_wdproperty','num_interwiki','supra_ethnic_groups'], page_titles_qitems, country, 'ethnic_group')

            # RELIGIOUS GROUPS BIOGRAPHY MOST EDITED
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','religious_group'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'religious_group')



            ### TOPICS (people, places and things) ###

            # FOLK MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','folk'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'folk')

            # EARTH MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','earth'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'earth')

            # MONUMENTS AND BUILDINGS MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','monuments_and_buildings'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'monuments_and_buildings')

            # MUSIC CREATIONS AND ORGANIZATIONS MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','music_creations_and_organizations'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'music_creations_and_organizations')

            # SPORTS MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','sport_and_teams'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'sport_and_teams')

            # FOOD MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','food'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'food')

            # PAINTINGS MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','paintings'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'paintings')

            # GLAM MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','glam'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'glam')

            # BOOKS MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','books'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'books')

            # CLOTHING AND FASHION MOST PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','clothing_and_fashion'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'clothing_and_fashion')

            # INDUSTRY PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','industry'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'industry')

            # RELIGION PAGEVIEWS
            make_top_ccc_diversity_articles_list(languagecode, ['ccc','religion'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'religion')


            with open('top_diversity_selection.txt', 'a') as f: f.write(languagecode+'\t'+languages.loc[languagecode]['languagename']+'\t'+country+'\t'+str(datetime.timedelta(seconds=time.time() - country_Time))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')
            print (languagecode+'\t'+languages.loc[languagecode]['languagename']+'\t'+country+'\t'+str(datetime.timedelta(seconds=time.time() - country_Time))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

            duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
            wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    duration = str(datetime.timedelta(seconds=time.time() - functionrealstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_realname, 'mark', duration)


def make_top_ccc_diversity_articles_list(languagecode, content_type, category, percentage_filtered, time_frame, relevance_rank, relevance_sense, window, representativity, columns, page_titles_qitems, country, list_name):

    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('\n\n* make_table_top_diversity_articles_list')
    print (list_name)
    print ('Obtaining a prioritized article list based on these parameters:')
    print (languagecode, content_type, category, time_frame, relevance_rank, relevance_sense, window, representativity, columns)

    # Databases connections
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
#    conn = sqlite3.connect(databases_path + 'ccc_old.db'); cursor = conn.cursor()

    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()

    # DEFINE CONTENT TYPE
    # rding to the content type, we make a query or another.
    print ('define the content type')
    if content_type[0] == 'ccc': query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary=1'
    if content_type[0] == 'gl': query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NOT NULL'
    if content_type[0] == 'kw': query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
    if content_type[0] == 'ccc_not_gl': query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NULL'
    if content_type[0] == 'ccc_main_territory': query = 'SELECT * FROM '+languagecode+'wiki WHERE ccc_binary=1'


    # DEFINE CATEGORY TO FILTER THE DATA (specific territory, specific topic)
    print ('define the specific category.')
    if category != '':
        print ('We are usign these categories to filter the content (either topics or territories).')
        print (category)

        if isinstance(category,str): query = query + ' AND (main_territory = "'+str(category)+'")'
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
        cursor.execute("SELECT MIN(date_created) FROM "+languagecode+"wiki;")
        timestamp = cursor.fetchone()
        timestamp = timestamp[0]
        if timestamp == None or timestamp == 'None': return
        print (timestamp)
        first_year=(datetime.datetime.strptime(str(timestamp),'%Y%m%d%H%M%S') + datetime.timedelta(days=365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_year)
    if time_frame == 'first_three_years':
        cursor.execute("SELECT MIN(date_created) FROM "+languagecode+"wiki;")
        timestamp = cursor.fetchone()
        timestamp = timestamp[0]
        if timestamp == None or timestamp == 'None': return
        print (timestamp)
        first_three_years=(datetime.datetime.strptime(str(timestamp),'%Y%m%d%H%M%S') + datetime.timedelta(days=3*365)).strftime('%Y%m%d%H%M%S')
        print (first_three_years)
        query = query + ' AND date_created < '+str(first_three_years)


    if time_frame == 'first_five_years':
        cursor.execute("SELECT MIN(date_created) FROM "+languagecode+"wiki;")
        timestamp = cursor.fetchone()
        timestamp = timestamp[0]
        if timestamp == None or timestamp == 'None': return
        print (timestamp)
        first_five_years=(datetime.datetime.strptime(str(timestamp),'%Y%m%d%H%M%S') + datetime.timedelta(days=5*365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_five_years)

    # OBTAIN THE DATA.
    print ('obtain the data.')
    print (query)
    ccc_df = pd.read_sql_query(query, conn)

    print (ccc_df.columns.values)
    ccc_df = ccc_df.set_index(['qitem'])
    ccc_df = ccc_df.fillna(0)
    print ('this is the number of lines of the dataframe: '+str(len(ccc_df)))
    if len(ccc_df)==0: 
        return

#    print (ccc_df.index.values)
    print (len(set(ccc_df.index.values)))
#    ccc_df = ccc_df.reindex(index = list(set(ccc_df.index.values)))
    ccc_df = ccc_df[~ccc_df.index.duplicated(keep='first')]
    print ('number of lines after removing duplicates: ')
    print (len(ccc_df))

#    print (ccc_df.page_title.values)
#    print (ccc_df.index.values)

    # FILTER ARTICLES IN CASE OF CONTENT TYPE
    if len(content_type)>1:
        ctype = content_type[1]
        if ctype == 'lgtb': ctype = 'sexual_orientation'

        if content_type[1] == 'people': 
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender IS NOT NULL'

        elif content_type[1] == 'male':
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender = "Q6581097"'

        elif content_type[1] == 'female':
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender = "Q6581072"'

        elif content_type[1] == 'lgtb':
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE sexual_orientation != "Q1035954"'

        elif content_type[1] == 'religious_group':
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE religious_group IS NOT NULL AND gender IS NOT NULL'

        elif content_type[1] == 'ethnic_group':
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ethnic_group IS NOT NULL AND gender IS NOT NULL'

        else:
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE '+content_type[1]+' IS NOT NULL;'

        topic_selected=set()
        print (query)
#        print (languagecode)
        for row in cursor.execute(query):
            if row[0] in ccc_df.index:
                topic_selected.add(row[0])
        print (len(topic_selected))

        ccc_df = ccc_df.reindex(index = list(topic_selected))
        print ('this is the number of lines of the dataframe after the content type selection: '+str(len(ccc_df)))
        ccc_df = ccc_df.fillna(0)


    # FILTER THE LOWEST PART OF CCC (POSITIVE FEATURES)
    if len(ccc_df)>2*window:
        print ('filter and save the first '+str(percentage_filtered)+'% of the CCC articles in terms of number of strategies and inlinks from CCC.')

        ranked_saved_1=ccc_df['num_inlinks_from_CCC'].sort_values(ascending=False).index.tolist()
        ranked_saved_1=ranked_saved_1[:int(percentage_filtered*len(ranked_saved_1)/100)]

        ranked_saved_2=ccc_df['num_retrieval_strategies'].sort_values(ascending=False).index.tolist()
        ranked_saved_2=ranked_saved_2[:int(percentage_filtered*len(ranked_saved_2)/100)]

        intersection = list(set(ranked_saved_1)&set(ranked_saved_2))
        print (len(intersection))

        ccc_df = ccc_df.reindex(index = intersection)
        print ('There are now: '+str(len(ccc_df))+' articles.')
    else:
        print ('Not enough articles to filter.')
#    if (len(ccc_df)<len(territories.loc[languagecode]['QitemTerritory'])): return


    # RANK ARTICLES BY RELEVANCE
    # PEARLS MODE
    if list_name == 'interwiki_editors': # the number of interwiki correlates 0.7 with the number of editors in the article.
        y = ccc_df[['num_editors']].values
        x = ccc_df[['num_interwiki']].values

        linearRegressor = LinearRegression()
        linearRegressor.fit(x, y)
        coef = linearRegressor.coef_[0][0]
        intercept = linearRegressor.intercept_[0]
        print(coef)
        print(intercept)

        ccc_df['expected_interwiki'] = (ccc_df['num_editors'] - intercept)/coef
        ccc_df['interwiki_relationship'] = ccc_df['expected_interwiki']/(ccc_df['num_interwiki']+1)


    if list_name == 'interwiki_wdproperty': # the number of interwiki correlates 0.5 with the number of wdproperties.
        y = ccc_df[['num_wdproperty']].values
        x = ccc_df[['num_interwiki']].values

        linearRegressor = LinearRegression()
        linearRegressor.fit(x, y)
        coef = linearRegressor.coef_[0][0]
        intercept = linearRegressor.intercept_[0]
        print(coef)
        print(intercept)

        ccc_df['expected_interwiki'] = (ccc_df['num_wdproperty'] - intercept)/coef
        ccc_df['interwiki_relationship'] = ccc_df['expected_interwiki']/(ccc_df['num_interwiki']+1)

    """
    # WIKI RANK MODE -> only for the API version.
    if list_name == 'wikirank': #         ranked_saved=ccc_df['num_pageviews'].sort_values(ascending=False).index.tolist()
        ranked_saved=ranked_saved[:5000]
        ccc_df = ccc_df.reindex(index = ranked_saved)

        langcode = languagecode
        if langcode == 'zh_min_nan': langcode = 'zhminnan'

        # https://stackoverflow.com/questions/48249963/wikirank-parser
        wikirank_values = []
        for page_t in ccc_df.page_title:
            page_t = urllib.parse.quote_plus(page_t.encode('utf-8'))
            print (page_t)
            with urllib.request.urlopen("https://api.wikirank.net/api.php?lang="+languagecode+"&name="+page_t) as url:
                data = json.loads(url.read().decode())
                wikirank_value = data['result'][languagecode]['quality']
                wikirank_values.append(wikirank_value)

        ccc_df['wikirank'] = wikirank_values
    """


    # FEATURE RANK
    print ('rank articles by relevance')
    articles_ranked = {}
    if relevance_sense=='positive': # cles top priority of relevance
        ascending=False
    if relevance_sense=='negative': # cles for deletion (no one cares)
        ascending=True


    ccc_df = ccc_df.fillna(0)
    
    # print(ccc_df.head(10))
    # print (ccc_df.num_edits)
    # print(ccc_df.dtypes)

    ccc_df["num_edits"] = ccc_df["num_edits"].astype(int)


    relevance_measures = ['num_inlinks', 'num_outlinks', 'num_bytes', 'num_references', 'num_edits','num_edits_last_month', 'num_editors', 'num_pageviews', 'num_wdproperty', 'num_interwiki', 'num_discussions', 'num_images', 'featured_article', 'num_inlinks_from_CCC', 'num_retrieval_strategies', 'interwiki_relationship', 'wikirank', 'num_outlinks_to_women', 'percent_outlinks_to_women', 'num_inlinks_from_women', 'num_outlinks_to_lgbt', 'num_inlinks_from_lgbt', 'percent_outlinks_to_lgbt']
    rank_dict = {}
    for parameter in relevance_rank.keys():
        if parameter in relevance_measures:
            print (parameter)
            coefficient=relevance_rank[parameter]
            ccc_ranked=ccc_df[parameter].sort_values(ascending=ascending).index.tolist()
            print ('parameter of relevance: '+parameter+ ' with coefficient: '+str(coefficient))
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

    if len(ccc_df)==0: 
        return

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

    if representativity == 'all_equal': # equal. get all the qitems for the language code. divide the 
        try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems = [territories.loc[languagecode]['QitemTerritory']]
        if isinstance(category,list) and category[0][0] == 'Q': qitems = list(set.intersection(set(qitems),set(category)))

        coefficient=1/(len(qitems)+1)
        for x in qitems: representativity_coefficients[x]=coefficient
        representativity_coefficients[0]=coefficient

    if representativity == 'proportional_articles' or representativity == 'proportional_articles_compensation': # proportional to the number of articles for each territory. check data from: ccc_extent_by_qitem.
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
        query = 'SELECT qitem, ccc_articles_qitem FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'" AND measurement_date IN (SELECT MAX(measurement_date) FROM ccc_extent_qitem WHERE languagecode = "'+languagecode+'")'
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

    if representativity == 'proportional_ccc_relevance': # proportional to the relevance of each qitem.
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
    # print (representativity_coefficients)
    # print (representativity_coefficients_sorted)
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

                    # print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,'\t\t\t\t\t'+str(list(relevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[0]]),

                    # '\t\t'+str('interwiki')+':'+str(ccc_df.loc[qitem]['num_interwiki']),
                    # '\t\t'+str('editors')+':'+str(ccc_df.loc[qitem]['num_editors']),

                    # '\t\t'+str(ctype)+':'+str(ccc_df.loc[qitem][ctype]),


                    # print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,'\t\t\t\t\t'+str(list(relevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[0]]),
                    # '\t'+str(list(relevance_rank.keys())[1])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[1]]))



                    # print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,'\t\t\t\t\t'+str(list(relevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[0]]),
                    # '\t'+str(list(relevance_rank.keys())[1])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[1]]),
                    # '\t'+str(list(relevance_rank.keys())[2])+':'+str(ccc_df.loc[qitem][list(relevance_rank.keys())[2]]),
                    # );

                    # qitem,territory,main_territory,x); #input('')

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

    # print ('we stop here by now.')
    # return




    # INSERT ARTICLES
    measurement_date_dict={}
    for x in ccc_df.index.values: measurement_date_dict[x]=measurement_date
    ccc_df['measurement_date'] = pd.Series(measurement_date_dict)

#    langcode_original_dict={}
#    for x in ccc_df.index.values: langcode_original_dict[x]=languagecode
#    ccc_df['langcode_original'] = pd.Series(langcode_original_dict)

    ccc_df=ccc_df.reset_index()
    formatted_columns = ['qitem', 'page_title','measurement_date']
    subset = ccc_df[formatted_columns]
    tuples = (tuple(x) for x in subset.values)

    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_top_articles_features (qitem, page_title_original, measurement_date) VALUES (?,?,?)'
    cursor4.executemany(query,tuples)

    query = 'UPDATE '+languagecode+'wiki_top_articles_features SET qitem = ?, page_title_original = ?, measurement_date = ?;'
    cursor4.executemany(query,tuples)

    conn4.commit()

    # INSERT RANKING
    if country == '': 
        list_origin = 'all'
        origin = 'all'
    else: 
        list_origin = country
        origin = 'iso3166'

    list_name_dict={}
    for x in ccc_df.index.values: list_name_dict[x]=list_name
    ccc_df['list_name'] = pd.Series(list_name_dict)

    list_entity_dict={}
    for x in ccc_df.index.values: list_entity_dict[x]=list_origin
    ccc_df['country'] = pd.Series(list_entity_dict)

    ccc_df.index = np.arange(1, len(ccc_df)+1)

    ccc_df=ccc_df.reset_index()
    ccc_df.rename(columns={'index': 'position'}, inplace=True)

    formatted_columns = ['position','qitem','country','list_name','measurement_date']

    subset = ccc_df[formatted_columns]
    tuples = [tuple(x) for x in subset.values]

    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_top_articles_lists (position, qitem, country, list_name, measurement_date) VALUES (?,?,?,?,?)'
    cursor4.executemany(query,tuples)
    conn4.commit()

    print ('* make_top_ccc_diversity_articles_list '+list_name+', for '+list_origin+'. Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def update_top_ccc_diversity_articles_features():
    function_name = 'update_top_ccc_diversity_articles_features'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + top_diversity_db); cursor2 = conn2.cursor()

    print ('* update_top_ccc_diversity_articles_features')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        lists_qitems = set()
        query = 'SELECT qitem, measurement_date FROM '+languagecode+'wiki_top_articles_lists'
        for row in cursor2.execute(query):
            lists_qitems.add(row[0])
            measurement_date = row[1]

        if len(lists_qitems) == 0: continue
        print ('There is this number of qitems in '+languagecode+'wiki_top_articles_lists: '+str(len(lists_qitems)))

        page_asstring = ','.join( ['?'] * len( lists_qitems ) )
        query = 'SELECT num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, featured_article, sexual_orientation, ethnic_group, religious_group, num_inlinks_from_CCC, wikirank, num_images, date_created, qitem FROM '+languagecode+'wiki WHERE qitem IN (%s)' % page_asstring

        parameters = []
        for row in cursor.execute(query, list(lists_qitems)):
            featured_article = row[10]
            if featured_article != 1: featured_article = 0
            parameters.append((row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],featured_article,row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],measurement_date))

        print ('Number of articles updated with features: '+str(len(parameters)))

        query = 'UPDATE '+languagecode+'wiki_top_articles_features SET num_inlinks = ?, num_outlinks = ?, num_bytes = ?, num_references = ?, num_edits = ?, num_editors = ?, num_discussions = ?, num_pageviews = ?, num_wdproperty = ?, num_interwiki = ?, featured_article = ?, sexual_orientation = ?, ethnic_group = ?, religious_group = ?, num_inlinks_from_CCC = ?, wikirank = ?, num_images = ?, date_created = ? WHERE qitem = ? AND measurement_date = ?;'
        cursor2.executemany(query,parameters)
        conn2.commit()

    print ('Measurement date is: '+str(measurement_date))
    print ('* update_top_ccc_diversity_articles_features Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def update_top_ccc_diversity_articles_titles(type):
    function_name = 'update_top_ccc_diversity_articles_titles '+type
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()

    if (type=='sitelinks'):
        intersections = list()
        for languagecode_1 in wikilanguagecodes:
            print ('\n* '+languagecode_1)
            langTime = time.time()

            (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode_1)
            qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

            titles = list()
            for languagecode_2 in wikilanguagecodes:
                languagecode_2_qitems = {}
                query = 'SELECT qitem, country, list_name, position FROM '+languagecode_2+'wiki_top_articles_lists WHERE measurement_date = "'+measurement_date+'" ORDER BY country, list_name, position ASC'
                count = 0
                list_name = 'initial'
                country = ''
                position = 0
                for row in cursor4.execute(query): 
                    qitem = row[0]
                    cur_country = row[1]
                    cur_list_name = row[2]
                    position = row[3]

                    # intersections
                    if cur_list_name != list_name and list_name!='initial':

                        if country != 'all': list_origin = country+'_('+languagecode_2+')'
                        else: list_origin = languagecode_2

                        if old_position < 100: base = old_position
                        else: base = 100

                        intersections.append((list_origin,list_name,languagecode_1,'wp',count,100*count/base, measurement_date)) # second field: ca_(ca)
                        count = 0

                    old_position = position

                    # titles
                    try:
                        page_title=qitems_page_titles[qitem]

                        if qitem not in languagecode_2_qitems:
                            titles.append((measurement_date,page_title,'sitelinks',qitem))
                            languagecode_2_qitems[qitem]=None

                        if position <= 100: count+=1 # for intersections
                    except:
                        pass

                    country = cur_country
                    list_name = cur_list_name

                # LAST ITERATION
                if list_name!='initial':
                    if country != 'all' and country != '': 
                        list_origin = country+'_('+languagecode_2+')'
                    else: list_origin = languagecode_2

                    if position < 100: base = position
                    else: base = 100

                    if base != 0:
                        rel_value = 100*count/base
                    else:
                        rel_value = 0

                    intersections.append((list_origin,list_name,languagecode_1,'wp',count,rel_value, measurement_date)) # second field: ca_(ca)
    #                print (list_origin,list_name,languagecode_1,'wp',count,rel_value, measurement_date)


            # INSERT PAGE TITLES
            query = 'INSERT OR IGNORE INTO '+languagecode_1+'wiki_top_articles_page_titles (measurement_date, page_title_target, generation_method, qitem) values (?,?,?,?)'
            cursor4.executemany(query, titles); # to top_diversity_articles.db

            query = 'UPDATE '+languagecode_1+'wiki_top_articles_page_titles SET measurement_date = ?, page_title_target = ?, generation_method = ? WHERE qitem = ?;'
            cursor4.executemany(query, titles); # to top_diversity_articles.db

            conn4.commit()
            print (str(len(titles))+' titles that exist already (sitelinks).') # including repeated qitems from different lists in the same language

            print ('* '+languagecode_1 + ' done with page_titles sitelinks.')
            with open('top_diversity_selection.txt', 'a') as f: f.write('* '+languagecode_1 + ' done with page_titles sitelinks. '+str(len(titles))+' titles. '+ str(datetime.timedelta(seconds=time.time() - langTime))+'\n')


            # INSERT INTERSECTIONS
            if len(intersections) > 500000 or wikilanguagecodes.index(languagecode_1) == len(wikilanguagecodes)-1:
                query = 'INSERT OR IGNORE INTO wdo_intersections (set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date) VALUES (?,?,?,?,?,?,?)'
                cursor4.executemany(query,intersections); 
                conn4.commit() # to stats.db
                print (str(len(intersections))+' intersections inserted.')
                with open('top_diversity_selection.txt', 'a') as f: f.write(str(len(intersections))+' intersections calculated.\n')
                intersections = list()
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





    if (type=='labels'):
        print ('UPDATING DB WITH LABELS SUGGESTIONS.')
        print ('Calculating the labels for '+str(len(wikilanguagecodes))+' languages.\n')

        conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

#        wikilanguagecodes_2 = wikilanguagecodes[wikilanguagecodes.index('am')+1:]
#        wikilanguagecodes_2 = ['is']
        for langcode_target in wikilanguagecodes:
            print ('\n* ### * language '+langcode_target+' with name '+languages.loc[langcode_target]['languagename']+'.')
            languageTime = time.time()

            query = 'delete from '+langcode_target+'wiki_top_articles_page_titles where generation_method != "sitelinks";'
            cursor4.execute(query)
            conn4.commit()

            
            # UPDATING FROM PAST MONTH LABELS AND TRANSLATIONS
            qitems_title_to_update = dict()
            print('get current missing qitems without any title:')
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM '+languagecode+'wiki_top_articles_features WHERE qitem NOT IN (SELECT qitem FROM '+langcode_target+'wiki_top_articles_page_titles WHERE generation_method = "sitelinks");'
                for row in cursor4.execute(query):
                    qitems_title_to_update[row[0]]=None
            print (str(len(qitems_title_to_update)))

            print('get the labels for this language for the qitems.')
            query = 'SELECT qitem, label FROM labels WHERE langcode = "'+langcode_target+'wiki";';
            parameters=[]
            i=0
            labelscounter = 0
            for row in cursor3.execute(query): 
                missing_qitem=row[0]
                label=row[1].replace(' ','_')
                try:
                    qitems_title_to_update[missing_qitem]
                    parameters.append((measurement_date, label, "label", missing_qitem))
                    labelscounter+=1
                except:
                    pass
                i+=1
            print (str(i)+' number of labels for this language')

            query = 'INSERT OR IGNORE INTO '+langcode_target+'wiki_top_articles_page_titles (measurement_date, page_title_target, generation_method, qitem) values (?,?,?,?)'
            cursor4.executemany(query, parameters)

            query = 'UPDATE '+langcode_target+'wiki_top_articles_page_titles SET measurement_date = ?, page_title_target = ?, generation_method = ? WHERE qitem = ?;'
            cursor4.executemany(query, parameters)

            print (str(labelscounter)+' labels that became useful for possible page_titles.')
            conn4.commit()

            with open('top_diversity_selection.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

            print ('* language target_titles labels for language '+langcode_target+' completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime))+'\n')

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



    if (type=='old translations'): # JUST USED ONCE. IT DOES NOT NEED TO BE USED CONSTANTLY. IN FACT, NOT NEEDED ANYMORE.

        conn3 = sqlite3.connect(databases_path + top_diversity_db); cursor3 = conn3.cursor()
        for languagecode in wikilanguagecodes:
            print (languagecode)
            parameters = []
            try:
                query = 'SELECT page_title_target, qitem FROM '+languagecode+'wiki_top_articles_page_titles WHERE generation_method = "translation";'
                for row in cursor3.execute(query):
                    title = row[0]
                    if '@' in title or '#' in title: continue
                    parameters.append((measurement_date,title,row[1]))
                print (len(parameters))

                # input('')

                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_top_articles_page_titles (measurement_date, page_title_target, qitem, generation_method) values (?,?,?, "translation")'
    #            query = 'UPDATE '+languagecode+'wiki_top_articles_page_titles SET measurement_date = ?, page_title_target = ?, generation_method = "translation" WHERE qitem = ? AND generation_method IS NULL;'

                cursor4.executemany(query, parameters)
                conn4.commit()
    
            except:
                pass


        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    """
    posar a processar les dades de Top Diversity articles. fer un codi que revisi quants dels Top Diversity no tenen títol per cada llengua en els títols del mes anterior.
    ------
    Thanks for the message akosiaris. I'm sorry the posts were heavy. This process runs only once a month.
    I will concentrate text in longer queries. What is the text length limit for a single query?
    Hi,
    It is more about number of queries per second (ie lots of requests) Also, please inform #language-team before you run queries.
    Let me know if you've any questions.
    E-mail: [Maniphest] [Changed Subscribers] T210485: Investigate high usage of Apertium and V2 endpoint
    Hauria d’ajuntar el processament de títols tot en un de llarg per agilitzar-ho.
    """


    if (type=='translations'): # TO BE CHECKED. THIS CODE MAY NOT WORK PROPERLY.
        languagecode_translated_from = wikilanguages_utils.load_language_pairs_apertium(wikilanguagecodes)     
        with open('top_diversity_selection.txt', 'a') as f: f.write(','.join(map(str, list(languagecode_translated_from.keys())))+'\n')

        print ('UPDATING DB WITH LABELS SUGGESTIONS.')
        for langcode_target in list(languagecode_translated_from.keys()):

            print ('\n* ### * language '+langcode_target+' with name '+languages.loc[langcode_target]['languagename']+'.')
            languageTime = time.time()

            # GETTING TRANSLATIONS FROM ORIGINAL
            print ('- update from translation from original.')
            print('get current missing qitems and the original page_title:')
            third_qitems_none = {}
            for languagecode in languagecode_translated_from[langcode_target]:
                query = 'SELECT qitem, page_title_original FROM '+languagecode+'wiki_top_articles_features WHERE qitem NOT IN (SELECT qitem FROM '+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?) AND measurement_date = ?' # it should get the titles from the languages in which there is a translation, even though it is not the original language of the Q.
                for row in cursor4.execute(query, (measurement_date,measurement_date)):
                    if row[0] not in third_qitems_none:
                        third_qitems_none[row[0]]=[languagecode,row[1]]
                    else:
                        third_qitems_none[row[0]]+=[languagecode,row[1]]
            print (str(len(third_qitems_none)))

            parameters=[]
            for qitem, original in third_qitems_none.items():
                if len(original) == 2:
                    langcode_original = original[0]
                    page_title_original = original[1]
                else:
#                        print (original)
                    for x in range(0,int(len(original)/2)):
                        langcode_original=original[2*x]
                        page_title_original = original[2*x+1]
                        if langcode_original in languagecode_translated_from[langcode_target]: break

                if langcode_original in languagecode_translated_from[langcode_target]:
                    title=page_title_original.replace('_',' ') # local title
                    tryit=1
                    while(tryit==1):
                        try:
                            r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+langcode_original+"/"+langcode_target+"/Apertium", data={'html': '<div>'+title+'</div>'}, timeout=0.3)
                            tryit=0 # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                        except:
                            pass
                            # print ('timeout.')

                    if r!=None and r.text!='Provider not supported':
                        page_title_target = str(r.text).split('<div>')[1].split('</div>')[0].replace(' ','_')
                        parameters.append((measurement_date, qitem, page_title_target, "translation"))
                    if len(parameters) % 1000 == 0:
                        print (len(parameters))
                        with open('top_diversity_selection.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+str(len(parameters))+'\t'+str(datetime.datetime.now())+'\n')

            print (str(len(parameters))+' translated titles to '+langcode_target+'.')

            query = 'INSERT OR IGNORE INTO '+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
            cursor4.executemany(query, parameters)
            conn4.commit()


            # GETTING TRANSLATIONS FROM VERSION
            print ('\n- update from translation from the copy.')
            fourth_qitems_none = list()
            print('get current missing qitems:')
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM '+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM '+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?)'
                for row in cursor4.execute(query, (measurement_date,measurement_date)): fourth_qitems_none.append(row[0])
            print (str(len(fourth_qitems_none)))

            print ('languages from which we can translate: ')
            print (languagecode_translated_from[langcode_target])

            parameters=[]
            for language_origin in languagecode_translated_from[langcode_target]:

                print ('/ '+language_origin)
                print ('remaining qitems: '+str(len(fourth_qitems_none)))
                print ('translated in previous round: '+str(len(parameters)))
                print ('start.')

                initialy = 100000
                x = 0; y = initialy

                while x < len(fourth_qitems_none):
                    sample = fourth_qitems_none[x:y]
                    page_asstring = ','.join( ['?'] * len(sample) )

                    page_titles_language_origin = {}
                    query = 'SELECT qitem, page_title_target FROM '+language_origin+'wiki_top_articles_page_titles WHERE measurement_date = "'+measurement_date+'" AND qitem IN (%s)' % page_asstring
                    for row in cursor4.execute(query, sample):
                        page_titles_language_origin[row[0]]=row[1]

#                        print (x,y)
#                        print (len(page_titles_language_origin))

                    for qitem, page_title in page_titles_language_origin.items():
                        title=page_title.replace('_',' ') # local title
                        tryit=1
                        while(tryit==1):
                            try:
                                r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+language_origin+"/"+langcode_target+"/Apertium", data={'html': '<div>'+title+'</div>'}, timeout=0.5)
                                tryit=0 # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                            except:
                                print ('timeout.')

                        if r!=None and r.text!='Provider not supported':
                            page_title_target = str(r.text).split('<div>')[1].split('</div>')[0].replace(' ','_')
                            parameters.append((measurement_date, qitem, page_title_target, "translation"))
                            fourth_qitems_none.remove(qitem)
                        if len(parameters) % 1000 == 0:
                            print (len(parameters))
                            with open('top_diversity_selection.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+str(len(parameters))+'\t'+str(datetime.datetime.now())+'\n')

                    x = y
                    y = y + initialy

            print ('remaining qitems: '+str(len(fourth_qitems_none)))
            print (str(len(parameters))+' translated titles to '+langcode_target)

            query = 'INSERT OR IGNORE INTO '+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
            cursor4.executemany(query, parameters)
            conn4.commit()

            print ('total number of missing titles in the end: '+str(len(fourth_qitems_none))+'.')

            # DONE!
            print ('* language target_titles translations for language '+langcode_target+' completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime))+'\n')
            with open('top_diversity_selection.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def update_top_diversity_articles_interwiki():

    functionstartTime = time.time()
    conn = sqlite3.connect(databases_path + top_diversity_db); cursor = conn.cursor()
    print ('* update_top_diversity_articles_interwiki')

    for languagecode in wikilanguagecodes:       
        print (languagecode)
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)
        qitems_page_titles = {v: k for k, v in page_titles_qitems.items()}

        list_page_ids = set()
        page_ids_qitems = {}        
        query = 'SELECT DISTINCT page_title_original, qitem FROM '+languagecode+'wiki_top_articles_features'

        try:
            for row in cursor.execute(query):
    #            print (row[0],row[1])

                qitem=row[1]
                try:
                    page_id = page_titles_page_ids[qitems_page_titles[qitem]]
                except:
                    continue
                list_page_ids.add(page_id)
                page_ids_qitems[page_id]=qitem
        except:
            print ('This language has no table.')
            continue

        if len(page_ids_qitems) == 0: continue
        print (len(page_ids_qitems))

        page_asstring = ','.join( ['%s'] * len( list_page_ids ) )
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        query = 'SELECT ll_from, ll_lang, ll_title FROM langlinks WHERE ll_from IN (%s);' % page_asstring

        mysql_cur_read.execute(query,list_page_ids)
        rows = mysql_cur_read.fetchall()

        old_page_id = 0
        i=0
        for row in rows:
            i+=1
            page_id=row[0]
            target_lang = row[1].decode('utf-8').replace('-','_')
#            if target_lang == 'be_x_old': target_lang = 'be_tarask'
#            if target_lang == 'zh_min_nan': target_lang = 'nan'

            target_title = row[2]
            qitem = page_ids_qitems[page_id]

            if old_page_id != page_id and old_page_id != 0:
                # TAULA de features (num_interwiki)
                query = 'UPDATE '+languagecode+'wiki_top_articles_features SET num_interwiki = ?, measurement_date = ? WHERE qitem=?'
                cursor.execute(query,(i,measurement_date,old_qitem));
                i=0

            try:
                query = 'INSERT OR IGNORE INTO '+target_lang+'wiki_top_articles_page_titles (measurement_date, page_title_target, generation_method, qitem) VALUES (?,?,?,?)'
                cursor.execute(query, (measurement_date,target_title,'sitelinks',qitem));

                query = 'UPDATE '+target_lang+'wiki_top_articles_page_titles SET measurement_date = ?, page_title_target = ?, generation_method = ? WHERE qitem=?'
                cursor.execute(query, (measurement_date,target_title,'sitelinks',qitem));
            except:
                pass

            old_page_id = page_id
            old_qitem = qitem

        conn.commit()


    for languagecode in wikilanguagecodes:
        print (languagecode)
        try:
            query = 'UPDATE '+languagecode+'wiki_top_articles_page_titles SET measurement_date = "'+measurement_date+'"'
            cursor.execute(query)
            query = 'UPDATE '+languagecode+'wiki_top_articles_features SET measurement_date = "'+measurement_date+'"'
            cursor.execute(query)
            conn.commit()
        except:
            pass

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print ('total function duration: '+duration)



def update_top_diversity_articles_intersections():

    functionstartTime = time.time()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()

    # for x in ['mnw', 'nqo', 'ban', 'gcr', 'szy']: wikilanguagecodes.remove(x)

    for languagecode_1 in wikilanguagecodes:
        print ('\n* '+languagecode_1)
        langTime = time.time()
        intersections = list()

        qitems_page_titles = {}
        query = 'SELECT qitem, page_title_target FROM '+languagecode_1+'wiki_top_articles_page_titles WHERE generation_method = "sitelinks";'
        for row in cursor4.execute(query):
            qitem = row[0]
            page_title_target = row[1]
            qitems_page_titles[qitem]=page_title_target
        print ('This language has '+str(len(qitems_page_titles))+' articles of other Top CCC.')

        lang_art = set(qitems_page_titles.keys())


        titles = list()
        count_intersections = 0
        for languagecode_2 in wikilanguagecodes:
            languagecode_2_qitems = {}

            cursor4.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='"+languagecode_2+"wiki_top_articles_lists';")
            if cursor4.fetchone()[0]==0: 
                print ('This language has no table: '+languagecode_2)
                continue

            query = 'SELECT qitem, country, list_name, position FROM '+languagecode_2+'wiki_top_articles_lists WHERE position <= 100 ORDER BY country, list_name, position ASC'

            count = 0
            list_name = 'initial'
            country = ''
            position = 0

            country_language_qitems = set()

            qitems_unique = set()

            for row in cursor4.execute(query): 
                qitem = row[0]
                cur_country = row[1]
                cur_list_name = row[2]
                cur_position = row[3]

                qitems_unique.add(row[0])


                # INTERSECTIONS
                # list level
                if cur_list_name != list_name and list_name!='initial':

                    if country != 'all': list_origin = country+'_('+languagecode_2+')'
                    else: list_origin = languagecode_2

                    if position < 100: base = position
                    else: base = 100

                    intersections.append((count,100*count/base, measurement_date,list_origin,list_name,languagecode_1,'wp')) # second field: ca_(ca)
#                    print ((list_origin,list_name,languagecode_1,'wp',count,100*count/base, measurement_date))
                    count = 0

                # country level
                if cur_country != country and country != '':
                    coincident_qitems_all_qitems = len(country_language_qitems.intersection(lang_art))
                    list_origin = ''
                    if country != 'all': 
                        list_origin = country+'_('+languagecode_2+')'
                    else: 
                        list_origin = languagecode_2

                    if len(country_language_qitems) == 0: rel_value = 0
                    else: rel_value = 100*coincident_qitems_all_qitems/len(country_language_qitems)
                    intersections.append((coincident_qitems_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode_1,'wp'))

                    country_language_qitems = set()

                country_language_qitems.add(qitem)

                # titles
                try:
                    page_title=qitems_page_titles[qitem]
                    count+=1 # for intersections
                except:
                    pass

                position = cur_position
                country = cur_country
                list_name = cur_list_name


            # LAST ITERATION
            # list level
            if list_name!='initial':
                if country != 'all' and country != '': 
                    list_origin = country+'_('+languagecode_2+')'
                else: list_origin = languagecode_2

                if position < 100: base = position
                else: base = 100

                if base != 0:
                    rel_value = 100*count/base
                else:
                    rel_value = 0

                intersections.append((count,rel_value, measurement_date,list_origin,list_name,languagecode_1,'wp')) # second field: ca_(ca)
                # print (list_origin,list_name,languagecode,'wp',count,rel_value, measurement_date)

            # country level
            if country != 'all' and country != '': 
                list_origin = country+'_('+languagecode_2+')'
            else: 
                list_origin = languagecode_2

            coincident_qitems_all_qitems = len(country_language_qitems.intersection(lang_art))
            if len(country_language_qitems) == 0: rel_value = 0
            else: rel_value = 100*coincident_qitems_all_qitems/len(country_language_qitems)
            intersections.append((coincident_qitems_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode_1,'wp'))


            # covered articles
            coincident_language_all_qitems = len(qitems_unique.intersection(lang_art))
            if len(qitems_unique) == 0: rel_value = 0
            else: rel_value = 100*coincident_language_all_qitems/len(qitems_unique)
            list_origin = languagecode_2
            intersections.append((coincident_language_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode_1,'wp'))
            # print ((coincident_language_all_qitems,rel_value, measurement_date,list_origin,'all_top_ccc_articles',languagecode_1,'wp'))


        # INSERT INTERSECTIONS FOR THE LANGUAGE
        # if len(intersections) > 500000 or wikilanguagecodes.index(languagecode_1) == len(wikilanguagecodes)-1:

        # print (intersections)

        query = 'INSERT OR IGNORE INTO wdo_intersections (abs_value, rel_value, measurement_date, set1, set1descriptor, set2, set2descriptor) VALUES (?,?,?,?,?,?,?)'
        cursor4.executemany(query,intersections);

        query = 'UPDATE wdo_intersections SET abs_value = ?, rel_value = ?, measurement_date = ? WHERE set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ?;'
        cursor4.executemany(query,intersections);
        conn4.commit() 
        count_intersections += len(intersections)

        # print (str(len(intersections))+' intersections inserted')
        # with open('top_diversity_selection.txt', 'a') as f: f.write(str(len(intersections))+' intersections calculated.\n')

        print (str(count_intersections)+' intersections inserted for this language: '+languagecode_1)
        print (str(datetime.timedelta(seconds=time.time() - langTime)))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print ('total function duration: '+duration)



def top_diversity_db_transfer():
    conn3 = sqlite3.connect(databases_path + 'top_articles_ccc.db'); cursor3 = conn3.cursor()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()
    for languagecode in wikilanguagecodes:
        print (languagecode)
        parameters = []
        query = 'SELECT measurement_date, qitem, page_title_target, generation_method FROM '+languagecode+'wiki_top_articles_page_titles'
        for row in cursor3.execute(query):
            parameters.append((row[0],row[1],row[2],row[3]))

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
        cursor4.executemany(query, parameters)
        conn4.commit()



##################################################################################

### SAFETY FUNCTIONS ###
def main_with_email():
    try:
        main()
    except:
        wikilanguages_utils.send_email_toolaccount('Top Diversity Selection and Update Error: '+ cycle_year_month, 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()        #          main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/top_diversity_selection.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('Top Diversity Selection and Update Error: '+ year_month, 'ERROR.' + lines); print("Now let's try it again...")
            continue



#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("top_diversity_selection"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("top_diversity_selection"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':

    script_name = 'top_diversity_selection.py'

    sys.stdout = Logger_out()
    sys.stderr = Logger_err()


    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
    # cycle_year_month = '2020-07'

#    check_time_for_script_run(script_name, cycle_year_month)
    startTime = time.time()

    # ATTENTION: the measurement date and the current period (year month) should be the same as the CCC creation. 
#            measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");
    measurement_date = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime('/srv/wcdo/databases/wikipedia_diversity.db')))
#    measurement_date = '20180926'


    # Import the language-territories mappings
    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()

    # Import the Wikipedia languages characteristics
    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = languages.index.tolist()

    # the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    # Verify/Remove all languages without a table in ccc.db
    wikipedialanguage_currentnumberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'last')
    for languagecode in wikilanguagecodes:
        if languagecode not in wikipedialanguage_currentnumberarticles: wikilanguagecodes.remove(languagecode)

    # languageswithoutterritory=list(set(languages.index.tolist()) - set(list(territories.index.tolist())))
    # # Only those with a geographical context
    # wikilanguagecodes_real = wikilanguagecodes.copy()
    # for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)
    # Final Wikipedia languages to process
    print (wikilanguagecodes)


    # if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)

    wikilanguages_utils.finish_email(startTime,'top_diversity_selection.out','Top Diversity lists created')

