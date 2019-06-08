# -*- coding: utf-8 -*-

# common resources
import wikilanguages_utils
# time
import time
import datetime
from dateutil import relativedelta
import calendar
# system
import os
import sys
import requests
import urllib.request, json
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
        self.log = open("stats_generation"+""+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass


# SEE document sets_intersections.xls for more information about the stats.
# There are four possible time range: a) accumulated monthly, b) last accumulated, c) monthly and d) last month. The first two comprise all the articles, and c and d only an increment of articles created within a month.

######################################################################

# MAIN
######################## STATS GENERATION SCRIPT ##################### 
def main():
    generate_all_top_ccc_articles_lists()






"""

    wikilanguages_utils.send_email_toolaccount('WCDO', '# GENERATE TOP CCC ARTICLES LISTS')
    print ('Create Top CCC articles lists.')
    change_top_ccc_to_top_ccc_temp()
    create_top_ccc_articles_lists_db()

    generate_all_top_ccc_articles_lists()
    update_top_ccc_articles_features()
    update_top_ccc_articles_titles('sitelinks')
    update_top_ccc_articles_titles('labels')

    update_top_ccc_articles_titles('translations')
    delete_last_iteration_top_ccc_articles_lists()


    wikilanguages_utils.send_email_toolaccount('WCDO', '# GENERATE THE MAIN STATS')
    create_intersections_db()

    # ONCE
    # accumulated monthly
    generate_ccc_segments_intersections('accumulated monthly') 
    generate_people_segments_intersections('accumulated monthly')
    generate_geolocated_segments_intersections('accumulated monthly')
    # monthly
    generate_monthly_articles_intersections('monthly')


    # RECURRING
    # last accumulated
    generate_langs_intersections()
    generate_ccc_segments_intersections('last accumulated')
    generate_ccc_qitems_intersections()
    generate_langs_ccc_intersections()
    generate_ccc_ccc_intersections()
    generate_people_segments_intersections()
    generate_people_ccc_intersections()
    generate_geolocated_segments_intersections()
    generate_top_ccc_articles_lists_intersections()
    # last month
    generate_monthly_articles_intersections('last month')
    generate_pageviews_intersections()


"""

### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

"""
def transfer():
    conn3 = sqlite3.connect(databases_path + 'top_articles_current.db'); cursor3 = conn3.cursor()
    conn4 = sqlite3.connect(databases_path + top_ccc_db); cursor4 = conn4.cursor()
    for languagecode in wikilanguagecodes:
        print (languagecode)
        parameters = []
        query = 'SELECT measurement_date, qitem, page_title_target, generation_method FROM ccc_'+languagecode+'wiki_top_articles_page_titles'
        for row in cursor3.execute(query):
            parameters.append((row[0],row[1],row[2],row[3]))

        query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
        cursor4.executemany(query, parameters)
        conn4.commit()
"""

# Drop the CCC database.
def change_top_ccc_to_top_ccc_temp():
    try:
        shutil.copyfile(databases_path + top_ccc_db, databases_path + "top_ccc_articles_temp.db")
        print ('temp copied.')
    except:
        print ('No older file to backup.')

def change_top_ccc_temp_to_top_ccc():
    try:
        shutil.copyfile(databases_path + "top_ccc_articles_temp.db", databases_path + top_ccc_db)
    except:
        print ('The new one could not be copied.')    


def remove_create_wcdo_stats_db():
    try:
        os.remove(databases_path + stats_db); print ('stats.db deleted.');
    except:
        pass


# LISTS
def create_top_ccc_articles_lists_db():
    functionstartTime = time.time()
    print ('* create_top_ccc_articles_lists_db')

    conn = sqlite3.connect(databases_path + top_ccc_db); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        query = ('CREATE table if not exists ccc_'+languagecode+'wiki_top_articles_lists ('+
        'qitem text,'+
        'position integer,'+
        'country text,'+
        'list_name text,'+
        'measurement_date text,'+

        'PRIMARY KEY (qitem, list_name, country, measurement_date))')
        cursor.execute(query)

        query = ('CREATE table if not exists ccc_'+languagecode+'wiki_top_articles_features ('+
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
        'featured_article integer, '+
        'num_inlinks_from_CCC integer, '+
        'date_created integer, '+
        'measurement_date text,'+

        'PRIMARY KEY (qitem, measurement_date))')
        cursor.execute(query)

        query = ('CREATE table if not exists ccc_'+languagecode+'wiki_top_articles_page_titles ('+
        'qitem text,'+
        'page_title_target text,'+ 
        'generation_method text,'+ # page_title_target can either be the REAL (from sitelinks wikitada), the label proposal (from labels wikitada) or translated (content translator tool).
        'measurement_date text,'+

        'PRIMARY KEY (qitem, measurement_date))')
        cursor.execute(query)

        query = ('CREATE table if not exists wcdo_intersections ('+
        'set1 text not null, '+
        'set1descriptor text, '+

        'set2 text, '+
        'set2descriptor text, '+

        'abs_value integer,'+
        'rel_value float,'+

        'measurement_date text,'
        'PRIMARY KEY (set1,set1descriptor,set2,set2descriptor,measurement_date))')

        cursor.execute(query)
        conn.commit()

    print ('* create_top_ccc_articles_lists_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# COMMAND LINE: sqlite3 -header -csv stats.db "SELECT * FROM create_intersections_db;" > create_intersections_db.csv
def create_intersections_db():

    functionstartTime = time.time()
    print ('* create_intersections_db')
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

    query = ('CREATE table if not exists wcdo_intersections_accumulated ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    cursor.execute(query)

    query = ('CREATE table if not exists wcdo_intersections_monthly ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    cursor.execute(query)


    conn.commit()

    print ('* create_intersections_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def insert_intersections_values(time_range, cursor2, content, set1, set1descriptor, set2, set2descriptor, abs_value, base, period):

    if time_range == '' or time_range == 'last accumulated':
        table_value = 'accumulated'

    if time_range == 'monthly' or 'last month':
        table_value = 'monthly'


    if abs_value == None: abs_value = 0

    if base == None or base == 0: rel_value = 0
    else: rel_value = 100*abs_value/base

    if 'avg' in set1 or 'avg' in set2: rel_value = base # exception for calculations in generate_langs_ccc_intersections()

    query_insert = 'INSERT OR IGNORE INTO wcdo_intersections_'+table_value+' (abs_value, rel_value, content, set1, set1descriptor, set2, set2descriptor, period) VALUES (?,?,?,?,?,?,?,?)'
    values = (abs_value, rel_value, content, set1, set1descriptor, set2, set2descriptor, period)
    cursor2.execute(query_insert,values);

    query_update = 'UPDATE wcdo_intersections_'+table_value+' SET abs_value = ?, rel_value = ? WHERE content = ? AND set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ? AND period = ?'
    cursor2.execute(query_update,values);



def generate_langs_intersections():
    functionstartTime = time.time()
    print ('* generate_langs_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    time_range = 'last accumulated'

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1)
        qitems = set()
        query = 'SELECT qitem FROM ccc_'+languagecode_1+'wiki'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')

    # LANGUAGE EDITIONS
    for languagecode_1 in wikilanguagecodes:
        partialtime = time.time()
        print ('* '+languagecode_1)
        current_wpnumberofarticles_1=wikipedialanguage_currentnumberarticles[languagecode_1]

        # entire wp
        query = 'SELECT count(*) FROM ccc_'+languagecode_1+'wiki WHERE num_interwiki = 0'
        cursor.execute(query)
        zero_ill_wp_count = cursor.fetchone()[0]
        insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_1,'zero_ill',zero_ill_wp_count,current_wpnumberofarticles_1, period)


        query = 'SELECT count(*) FROM ccc_'+languagecode_1+'wiki WHERE qitem IS NULL'
        cursor.execute(query)
        null_qitem_count = cursor.fetchone()[0]
        insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_1,'null_qitems',null_qitem_count,current_wpnumberofarticles_1, period)

        if current_wpnumberofarticles_1 == 0: continue
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue
#            query = 'SELECT COUNT(*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem'
#            cursor.execute(query)
#            article_count = cursor.fetchone()[0]
            article_count=len(all_articles[languagecode_1].intersection(all_articles[languagecode_2]))
            insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_2,'wp',article_count,current_wpnumberofarticles_1,period)

        print ('. '+languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - partialtime)))

    conn2.commit()
    print ('languagecode, wp, languagecode, zero_ill,'+period)
    print ('languagecode, wp, languagecode, null_qitems,'+period)
    print ('languagecode_1, wp, languagecode_2, wp,'+period)

    print ('* generate_langs_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_ccc_segments_intersections(time_range):
    functionstartTime = time.time()
    print ('* generate_ccc_segments_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    if time_range == 'accumulated monthly':
        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)

    if time_range == 'last accumulated':
#        period = list(sorted(periods_accum.keys()))[len(periods_accum-1)]
        print (time_range, period)
        query_part = ''
        for_time_range(time_range,query_part,period)

    def for_time_range(time_range,query_part,period):
        # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

            query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki'
            if time_range == 'accumulated monthly': query+= ' WHERE '+query_part

            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            query = 'SELECT COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
            if time_range == 'accumulated monthly': query+= ' AND '+query_part
            cursor.execute(query)
            row = cursor.fetchone()

            ccc_count = row[0]
            ccc_geolocated_count = row[1]
            ccc_keywords_count = row[2]


            # In regards of WP
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp',languagecode,'ccc',ccc_count,wpnumberofarticles,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp',languagecode,'ccc_geolocated',ccc_geolocated_count,wpnumberofarticles,period)
     
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp',languagecode,'ccc_keywords',ccc_keywords_count,wpnumberofarticles,period)
     
            # In regards of CCC
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc',languagecode,'ccc_keywords',ccc_keywords_count,ccc_count,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc',languagecode,'ccc_geolocated',ccc_geolocated_count,ccc_count,period)


        print ('languagecode, wp, languagecode, ccc,'+period)
        print ('languagecode, wp, languagecode, ccc_geolocated,'+period)
        print ('languagecode, wp, languagecode, ccc_keywords,'+period)

        print ('languagecode, ccc, languagecode, ccc_geolocated,'+period)
        print ('languagecode, ccc, languagecode, ccc_keywords,'+period)

    print ('* generate_ccc_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_ccc_qitems_intersections():
    functionstartTime = time.time()
    print ('* generate_ccc_qitems_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    time_range = 'last accumulated'

    # WIKIDATA AND CCC
    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki'
        cursor.execute(query)
        row = cursor.fetchone()
        wpnumberofarticles=row[0]

        query = 'SELECT COUNT(ccc_binary) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
        cursor.execute(query)
        row = cursor.fetchone()

        ccc_count = row[0]

        # In regards of wikidata qitems
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems',None,languagecode,'ccc',ccc_count,wikidata_article_qitems_count,period)

        # zero ill
        query = 'SELECT count(page_title) FROM ccc_'+languagecode+'wiki WHERE num_interwiki = 0 AND ccc_binary=1'
        cursor.execute(query)
        zero_ill_ccc_count = cursor.fetchone()[0]
        insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc',languagecode,'zero_ill',zero_ill_ccc_count,ccc_count, period)

        # MAIN TERRITORIES
        query = 'SELECT main_territory, COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 GROUP BY main_territory'
        for row in cursor.execute(query):
            main_territory=row[0]
            if main_territory == '' or main_territory == None:
                main_territory = 'Not Assigned'
            ccc_articles_count=row[1]
            ccc_geolocated_count=row[2]
            ccc_keywords_count=row[3]

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc','ccc',main_territory,ccc_articles_count,ccc_count, period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc','ccc_geolocated',main_territory,ccc_geolocated_count,ccc_count, period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc','ccc_keywords',main_territory,ccc_keywords_count,ccc_count, period)

    conn2.commit()

    print ('languagecode, ccc, languagecode, zero_ill,'+period)
    print ('wikidata_article_qitems, , languagecode, ccc, '+ period)
    print ('languagecode, ccc, ccc, qitem,'+period)
    print ('languagecode, ccc, ccc_geolocated, qitem,'+period)
    print ('languagecode, ccc, ccc_keywords, qitem,'+period)

    print ('* generate_ccc_qitems_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




def generate_langs_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_langs_ccc_intersections')

    time_range = 'last accumulated'

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    # LANGUAGES AND LANGUAGES CCC
    language_ccc_in_all_wp_total_art = {}
    language_ccc_in_all_wp_total_percent = {}

    language_all_wp_articles = {}
    all_wp_articles = 0
    for languagecode in wikilanguagecodes:
        language_ccc_in_all_wp_total_art[languagecode] = 0
        language_ccc_in_all_wp_total_percent[languagecode] = 0
        all_wp_articles += wikipedialanguage_currentnumberarticles[languagecode]

    for languagecode_1 in wikilanguagecodes:
        langTime = time.time()

        allwp_allnumberofarticles=0
        all_ccc_articles_count_total=0 # all ccc articles from all languages count
        all_ccc_articles_count=0 # language 1 ccc articles covered by other languages count
        all_ccc_rel_value_ccc_total =0
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode_1]
        language_all_wp_articles[languagecode_1]=all_wp_articles-wpnumberofarticles
    
        query = 'SELECT COUNT(*) FROM ccc_'+languagecode_1+'wiki WHERE ccc_binary=1'
        cursor.execute(query)
        row = cursor.fetchone()
        ccc_count = row[0]

        language_ccc_count = {}
        for languagecode_2 in wikilanguagecodes:
            query = 'SELECT COUNT(ccc_binary), COUNT(keyword_title), COUNT(ccc_geolocated) FROM ccc_'+languagecode_2+'wiki WHERE ccc_binary=1'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_articles_count_total = row[0]
            ccc_keywords_count_total = row[1]
            ccc_geolocated_count_total = row[2]


            language_ccc_count[languagecode_2]=ccc_articles_count_total
            all_ccc_articles_count_total+=ccc_articles_count_total
            allwp_allnumberofarticles+=wikipedialanguage_currentnumberarticles[languagecode_2]

            if languagecode_1 == languagecode_2: continue



            query = 'SELECT COUNT(ccc_'+languagecode_2+'wiki.ccc_binary), COUNT(ccc_'+languagecode_2+'wiki.keyword_title), COUNT(ccc_'+languagecode_2+'wiki.ccc_geolocated) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_articles_count = row[0]
            ccc_keywords_count = row[1]
            ccc_geolocated_count = row[2]

            # for CCC% covered by all language editions. relative coverage.
            all_ccc_articles_count+=ccc_articles_count
            if ccc_articles_count_total != 0: all_ccc_rel_value_ccc_total+=100*ccc_articles_count/ccc_articles_count_total 

            # for CCC% impact in all language editions. relative spread.
            if ccc_articles_count!=0:
                language_ccc_in_all_wp_total_art[languagecode_2]+=ccc_articles_count
                if wikipedialanguage_currentnumberarticles[languagecode_1]!=0:
                    language_ccc_in_all_wp_total_percent[languagecode_2]+=100*ccc_articles_count/wikipedialanguage_currentnumberarticles[languagecode_1]

            ## coverage
            insert_intersections_values(time_range,cursor2,'articles',languagecode_2,'ccc',languagecode_1,'wp',ccc_articles_count,ccc_articles_count_total,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode_2,'ccc_keywords',languagecode_1,'wp',ccc_keywords_count,ccc_keywords_count_total,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode_2,'ccc_geolocated',languagecode_1,'wp',ccc_geolocated_count,ccc_geolocated_count_total,period)

            ## spread
            insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc',ccc_articles_count,wpnumberofarticles,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc_keywords',ccc_keywords_count,wpnumberofarticles,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc_geolocated',ccc_geolocated_count,wpnumberofarticles,period)
        
        ### all ccc articles ###
        # what is the extent of all ccc articles in this language edition?
        insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'wp','all_ccc_articles','',all_ccc_articles_count+ccc_count,wpnumberofarticles, period)

        # COVERAGE
        ### total langs ###
        # how well this language edition covered all CCC articles? t.coverage and coverage art.
        insert_intersections_values(time_range,cursor2,'articles','all_ccc_articles','',languagecode_1,'wp',all_ccc_articles_count,all_ccc_articles_count_total-ccc_count, period)

        ### relative langs ###
        # how well this language edition covered all CCC articles in average? relative coverage.
        all_ccc_rel_value_ccc_total_avg=all_ccc_rel_value_ccc_total/(len(wikilanguagecodes)-1)
        all_ccc_abs_value_avg=all_ccc_articles_count/(len(wikilanguagecodes)-1)
        insert_intersections_values(time_range,cursor2,'articles','all_ccc_avg','',languagecode_1,'wp',all_ccc_abs_value_avg,all_ccc_rel_value_ccc_total_avg, period)

        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - langTime)))


    for languagecode_1 in wikilanguagecodes:
        # SPREAD
        ### total langs ###
        # what is the extent of language 1 ccc articles in all the articles of the other languages? t.spread and spread art.
        insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','',languagecode_1,'ccc',language_ccc_in_all_wp_total_art[languagecode_1],language_all_wp_articles[languagecode_1], period)

        ### relative langs ###
        # what is the average extent of this language ccc in all languages? relative spread.
        insert_intersections_values(time_range,cursor2,'articles','all_wp_avg','',languagecode_1,'ccc', 0,language_ccc_in_all_wp_total_percent[languagecode_1]/(len(wikilanguagecodes)-1), period)

        # what is the extent of this language ccc in all the languages ccc?
        insert_intersections_values(time_range,cursor2,'articles','all_ccc_articles','',languagecode_1,'ccc',language_ccc_count[languagecode_1],all_ccc_articles_count_total, period)


    # what is the extent of all ccc articles in all wp all articles
    insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','','all_ccc_articles','',all_ccc_articles_count_total,allwp_allnumberofarticles, period)



    conn2.commit()

    print ('languagecode_2, ccc, languagecode_1, wp,'+ period)
    print ('languagecode_2, ccc_keywords, languagecode_1, wp,'+ period)
    print ('languagecode_2, ccc_geolocated, languagecode_1, wp,'+ period)

    print ('languagecode_1, wp, languagecode_2, ccc,'+ period)
    print ('languagecode_1, wp, languagecode_2, ccc_keywords,'+ period)
    print ('languagecode_1, wp, languagecode_2, ccc_geolocated,'+ period)

    print ('languagecode_1, wp, all_ccc_articles, ,'+ period) # all ccc articles

    # coverage
    print ('all_ccc_articles, ,languagecode_1, wp, '+period)
    print ('all_ccc_avg, ,languagecode_1, wp, '+period)

    # spread
    print ('all_wp_all_articles, ,languagecode_1, ccc, '+period)
    print ('all_wp_avg, ,languagecode_1, ccc, '+period)
    print ('all_ccc_articles, ,languagecode_1, ccc, '+period+'\n')

    # all languages ccc in all languages wp all articles
    print ('all_wp_all_articles, ,all_ccc_articles, ccc, '+period+'\n')


    print ('* generate_langs_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_ccc_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_ccc_ccc_intersections')

    time_range = 'last accumulated'

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    # between languages ccc
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        query = 'SELECT COUNT(ccc_binary) FROM ccc_'+languagecode_1+'wiki WHERE ccc_binary=1'
        cursor.execute(query)
        language_ccc_count = cursor.fetchone()[0]
        if language_ccc_count == 0: continue

        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            query = 'SELECT COUNT (*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND ccc_'+languagecode_1+'wiki.ccc_binary = 1'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_coincident_articles_count = row[0]

            insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'ccc',languagecode_2,'ccc',ccc_coincident_articles_count,language_ccc_count,period)

    conn2.commit()
    print ('languagecode_1, ccc, languagecode_2, ccc,'+ period)

    print ('* generate_ccc_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_people_segments_intersections():
    functionstartTime = time.time()
    print ('* generate_people_segments_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    time_range = 'last accumulated'

    # PEOPLE SEGMENTS (PEOPLE, MALE, FEMALE)
    gender = {'Q6581097':'male','Q6581072':'female', 'Q1052281':'transgender female','Q1097630':'intersex','Q1399232':"fa'afafine",'Q17148251':'travesti','Q19798648':'unknown value','Q207959':'androgyny','Q215627':'person','Q2449503':'transgender male','Q27679684':'transfeminine','Q27679766':'transmasculine','Q301702':'two-Spirit','Q303479':'hermaphrodite','Q3177577':'muxe','Q3277905':'māhū','Q430117':'Transgene','Q43445':'female non-human organism'}
    gender_name_count_total = {}
    people_count_total = 0
    query = 'SELECT qitem2, COUNT(*) FROM people_properties WHERE qitem2!="Q5" GROUP BY qitem2'
    cursor3.execute(query)
    for row in cursor3.execute(query):
        if row[0] in gender: gender_name_count_total[gender[row[0]]]=row[1]
        people_count_total += row[1]
    gender_name_count_total['people']=people_count_total

    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems',None,'wikidata_article_qitems','people',gender_name_count_total['people'],wikidata_article_qitems_count, period)

    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','male',gender_name_count_total['male'],gender_name_count_total['people'], period)

    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','female',gender_name_count_total['female'],gender_name_count_total['people'], period)

    conn2.commit()
    print ('wikidata_article_qitems, , wikidata_article_qitems, people, '+period)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, female, '+period)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, male, '+period)
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    gender_name_count_total_zero_ill = {}
    people_count_total_zero_ill = 0
    query = 'SELECT qitem2, count(qitem2) FROM people_properties WHERE qitem in (SELECT qitem FROM sitelinks GROUP BY qitem HAVING COUNT(qitem)=1) AND qitem2!="Q5" GROUP BY qitem2 order by 2'
    cursor3.execute(query)
    for row in cursor3.execute(query):
        if row[0] in gender: gender_name_count_total_zero_ill[gender[row[0]]]=row[1]
        people_count_total_zero_ill += row[1]
    gender_name_count_total_zero_ill['people']=people_count_total_zero_ill

    # zero ill: people
    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['people'],gender_name_count_total['people'], period)

    print ('wikidata_article_qitems, people, wikidata_article_qitems, zero_ill, '+period)

    # zero ill: male
    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['male'],gender_name_count_total['people'], period)

    print ('wikidata_article_qitems, male, wikidata_article_qitems, zero_ill, '+period)

    # zero ill: female
    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','female','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['female'],gender_name_count_total['female'], period)

    print ('wikidata_article_qitems, female, wikidata_article_qitems, zero_ill, '+period)
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))


    # languages
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

        query = 'SELECT gender, COUNT(*) FROM ccc_'+languagecode+'wiki GROUP BY gender'
#        query = 'SELECT qitem2, COUNT(*) FROM people_properties INNER JOIN sitelinks ON people_properties.qitem = sitelinks.qitem WHERE langcode="'+languagecode+'wiki" AND qitem2!="Q5" GROUP BY qitem2'
        gender_name_count = {}
        people_count = 0
        for row in cursor.execute(query):
            if row[0] in gender: gender_name_count[gender[row[0]]]=row[1]
            people_count += row[1]
        gender_name_count['people']=people_count

        for gender_name, gender_count in gender_name_count.items():
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems',gender_name, gender_count,wpnumberofarticles,period)

            insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems', gender_name, languagecode, 'wp', gender_count,gender_name_count_total[gender_name],period)

    conn2.commit()
    print ('languagecode, wp, wikidata_article_qitems, male,'+period)
    print ('languagecode, wp, wikidata_article_qitems, female,'+period)
    print ('languagecode, wp, wikidata_article_qitems, people,'+period)

    print ('wikidata_article_qitems, male, languagecode, wp, '+period)
    print ('wikidata_article_qitems, female, languagecode, wp, '+period)
    print ('wikidata_article_qitems, people, languagecode, wp, '+period)

    print ('* generate_people_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




def generate_people_segments_intersections(time_range):
    functionstartTime = time.time()
    print ('* generate_people_segments_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    if time_range == 'accumulated monthly':
        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)

    if time_range == 'last accumulated':
#        period = list(sorted(periods_accum.keys()))[len(periods_accum-1)]
        query_part = ''
        print (time_range,period)
        for_time_range(time_range,query_part,period)

    def for_time_range(time_range,query_part,period):
        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

            query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki'
            if time_range == 'accumulated monthly': query+= ' WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            if time_range == 'accumulated monthly': query_part+= 'WHERE '+query_part+' '
            query = 'SELECT gender, COUNT(*) FROM ccc_'+languagecode+'wiki '+query_part+'GROUP BY gender'

            gender_name_count = {}
            people_count = 0
            for row in cursor.execute(query):
                if row[0] in gender: gender_name_count[gender[row[0]]]=row[1]
                people_count += row[1]
            gender_name_count['people']=people_count

            for gender_name, gender_count in gender_name_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems',gender_name, gender_count,wpnumberofarticles,period)

        conn2.commit()
        print ('languagecode, wp, wikidata_article_qitems, male,'+period)
        print ('languagecode, wp, wikidata_article_qitems, female,'+period)
        print ('languagecode, wp, wikidata_article_qitems, people,'+period)

    print ('* generate_people_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_people_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_people_ccc_intersections')

    time_range = 'last accumulated'

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    # PEOPLE SEGMENTS AND CCC
    language_ccc_count = {}
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
        for row in cursor.execute(query):
            qitems.append(row[0])
        language_ccc_count[languagecode]=len(qitems)

        # male
        male=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097"'
#        query = 'SELECT DISTINCT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki"'
        for row in cursor.execute(query):
            male.append(row[0])
        malecount=len(male)
        male_ccc = set(male).intersection(set(qitems))
        male_ccc_count=len(male_ccc)
#        print (malecount,male_ccc_count)

        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'male', languagecode, 'ccc', male_ccc_count, malecount,period)

        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'ccc', languagecode, 'male', male_ccc_count, language_ccc_count[languagecode],period)

        # female
        female=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072"'
#        query = 'SELECT DISTINCT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki"'
        for row in cursor.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female_ccc = set(female).intersection(set(qitems))
        female_ccc_count=len(female_ccc)

        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'female', languagecode, 'ccc', female_ccc_count, femalecount,period)

        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'ccc', languagecode, 'female', female_ccc_count,language_ccc_count[languagecode],period)

        # people
        people_count=femalecount+malecount
        ccc_peoplecount=male_ccc_count+female_ccc_count
        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'people', languagecode, 'ccc', ccc_peoplecount, people_count,period)

        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'ccc', languagecode, 'people', ccc_peoplecount, language_ccc_count[languagecode],period)

        # in relation to the entire wp
        insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', languagecode, 'ccc_people', ccc_peoplecount,wpnumberofarticles,period)

    conn2.commit()
    print ('languagecode, male, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, male,'+period)

    print ('languagecode, female, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, female,'+period)

    print ('languagecode, people, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, people,'+period)

    print ('languagecode, wp, languagecode, ccc_people,'+period)

    print ('* generate_people_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_geolocated_segments_intersections():
    functionstartTime = time.time()
    print ('* generate_geolocated_segments_intersections')

    time_range = 'last accumulated'

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    # GEOLOCATED SEGMENTS (COUNTRIES, SUBREGIONS, REGIONS)
    country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions() # iso 3166 to X


    query = 'SELECT iso3166, COUNT(DISTINCT qitem) FROM geolocated_property GROUP BY iso3166'
    iso3166_qitems = {}
    geolocated_items_count_total = 0
    for row in cursor3.execute(query):
        iso3166_qitems[row[0]]=row[1]
        geolocated_items_count_total+=row[1]

    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems',None,'wikidata_article_qitems','geolocated',geolocated_items_count_total,wikidata_article_qitems_count, period)

    print ('wikidata_article_qitems, , wikidata_article_qitems, geolocated, '+period)

    query = 'SELECT iso3166, COUNT(DISTINCT qitem) FROM geolocated_property WHERE qitem IN (SELECT qitem FROM sitelinks GROUP BY qitem HAVING (COUNT(qitem) = 1)) GROUP BY iso3166'
    iso3166_qitems_zero_ill = {}
    geolocated_items_zero_ill_count_total = 0
    for row in cursor3.execute(query):
        iso3166_qitems_zero_ill[row[0]]=row[1]
        geolocated_items_zero_ill_count_total+=row[1]

    insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems',None,'geolocated','ill_zero',geolocated_items_zero_ill_count_total,wikidata_article_qitems_count, period)

    print ('wikidata_article_qitems, geolocated, geolocated, ill_zero, '+period)


    regions_count_total={}
    subregions_count_total={}
    regions_count_total_zero_ill={}
    subregions_count_total_zero_ill={}
    for iso3166_code, iso3166_count in iso3166_qitems.items():
        if iso3166_code == None: continue

#        print (subregions[iso3166_code])
        # all
        if subregions[iso3166_code] not in subregions_count_total:
            subregions_count_total[subregions[iso3166_code]]=iso3166_count
        else: 
            subregions_count_total[subregions[iso3166_code]]+=iso3166_count

        if regions[iso3166_code] not in regions_count_total: 
            regions_count_total[regions[iso3166_code]]=iso3166_count
        else: 
            regions_count_total[regions[iso3166_code]]+=iso3166_count

        # zero ill
        if subregions[iso3166_code] not in subregions_count_total_zero_ill: 
            subregions_count_total_zero_ill[subregions[iso3166_code]]=iso3166_qitems_zero_ill[iso3166_code]
        else: 
            subregions_count_total_zero_ill[subregions[iso3166_code]]+=iso3166_qitems_zero_ill[iso3166_code]

        if regions[iso3166_code] not in regions_count_total_zero_ill: 
            regions_count_total_zero_ill[regions[iso3166_code]]=iso3166_qitems_zero_ill[iso3166_code]
        else: 
            regions_count_total_zero_ill[regions[iso3166_code]]+=iso3166_qitems_zero_ill[iso3166_code]


        # countries
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','countries',iso3166_code,iso3166_count,geolocated_items_count_total, period)

        # countries ILL zero
        insert_intersections_values(time_range,cursor2,'articles','countries',iso3166_code,'geolocated','ill_zero',iso3166_qitems_zero_ill[iso3166_code],iso3166_count, period)


    # subregions
    for subregion_name, subregion_count in subregions_count_total.items():
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','subregions',subregion_name,subregion_count,geolocated_items_count_total, period)

        # subregions ILL zero
        insert_intersections_values(time_range,cursor2,'articles','subregions',subregion_name,'geolocated','ill_zero',subregions_count_total_zero_ill[subregion_name],subregion_count, period)

    # regions
    for region_name, region_count in regions_count_total.items():
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','regions',region_name,region_count,geolocated_items_count_total, period)

        # regions ILL zero
        insert_intersections_values(time_range,cursor2,'articles','regions',region_name,'geolocated','ill_zero',regions_count_total_zero_ill[region_name],region_count, period)

    conn2.commit()
    print ('wikidata_article_qitems, geolocated, countries, iso3166,'+period)
    print ('wikidata_article_qitems, geolocated, subregions, subregion_name,'+period)
    print ('wikidata_article_qitems, geolocated, regions, region_name,'+period)

    print ('countries, iso3166, geolocated, ill_zero,'+period)
    print ('subregions, subregion_name, geolocated, ill_zero,'+period)
    print ('regions, region_name, geolocated, ill_zero,'+period)




    regions_all_langs_count={}
    subregions_all_langs_count={}
    iso3166_all_langs_count={}
    all_wp_all_geolocated_articles_count = 0

    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

        geolocated_articles_count = 0
        iso3166_articles = {}
        query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki WHERE iso3166 IS NOT NULL GROUP BY iso3166'
        cursor.execute(query)
        for row in cursor.execute(query):
            iso3166_articles[row[0]]=row[1]
            geolocated_articles_count+=row[1]

            if row[0] not in iso3166_all_langs_count: iso3166_all_langs_count[row[0]]=row[1]
            else: iso3166_all_langs_count[row[0]]+=row[1]

        all_wp_all_geolocated_articles_count+=geolocated_articles_count

        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated',languagecode,'geolocated',geolocated_articles_count,geolocated_items_count_total, period)

        insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','languagecode','geolocated',geolocated_articles_count,wpnumberofarticles, period)



        regions_count={}
        subregions_count={}
        for iso3166_code, iso3166_count in iso3166_articles.items():
            if iso3166_code == None: continue

            if regions[iso3166_code] not in regions_count: regions_count[regions[iso3166_code]]=iso3166_count
            else: regions_count[regions[iso3166_code]]+=iso3166_count

            if subregions[iso3166_code] not in subregions_count: subregions_count[subregions[iso3166_code]]=iso3166_count
            else: subregions_count[subregions[iso3166_code]]+=iso3166_count

            # accumulating for al languages
            if regions[iso3166_code] not in regions_all_langs_count: regions_all_langs_count[regions[iso3166_code]]=iso3166_count
            else: regions_all_langs_count[regions[iso3166_code]]+=iso3166_count

            if subregions[iso3166_code] not in subregions_all_langs_count: subregions_all_langs_count[subregions[iso3166_code]]=iso3166_count
            else: subregions_all_langs_count[subregions[iso3166_code]]+=iso3166_count

            # countries
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','countries',iso3166_code,iso3166_count,geolocated_articles_count, period)

            # countries
            insert_intersections_values(time_range,cursor2,'articles','countries',iso3166_code,languagecode,'geolocated',iso3166_count,iso3166_qitems[iso3166_code], period)

        # subregions
        for subregion_name, subregion_count in subregions_count.items():

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,geolocated_articles_count, period)

            insert_intersections_values(time_range,cursor2,'articles','subregions', subregion_name, languagecode, 'geolocated', subregion_count,subregions_count_total[subregion_name], period)

        # regions
        for region_name, region_count in regions_count.items():
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','regions',region_name,region_count,geolocated_articles_count, period)

            insert_intersections_values(time_range,cursor2,'articles','regions',region_name,languagecode,'geolocated',region_count,regions_count_total[region_name], period)

    conn2.commit()
    print ('wikidata_article_qitems, geolocated, languagecode, geolocated, '+period)
    print ('languagecode, wp, languagecode, geolocated, '+period)

    print ('languagecode, geolocated, countries, iso3166, '+period)
    print ('languagecode, geolocated, subregions, iso3166, '+period)
    print ('languagecode, geolocated, regions, iso3166, '+period)

    print ('countries, iso3166, languagecode, geolocated, '+period)
    print ('subregions, subregion_name, languagecode, geolocated, '+period)
    print ('regions, region_name, languagecode, geolocated, '+period)


        # countries
    for iso3166_code, iso3166_count in iso3166_all_langs_count.items():
        insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','countries',iso3166_code,iso3166_count,all_wp_all_geolocated_articles_count, period)

        # subregions
    for subregion_name, subregion_count in subregions_all_langs_count.items():
        insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','subregions',subregion_name,subregion_count,all_wp_all_geolocated_articles_count, period)

        # regions
    for region_name, region_count in regions_all_langs_count.items():
        insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','regions',region_name,region_count,all_wp_all_geolocated_articles_count, period)

    conn2.commit()
    print ('all_wp_all_articles, geolocated, geolocated, countries, '+ period)
    print ('all_wp_all_articles, geolocated, geolocated, subregions, '+ period)
    print ('all_wp_all_articles, geolocated, geolocated, regions, '+ period)

    print ('* generate_geolocated_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_geolocated_segments_intersections(time_range):
    functionstartTime = time.time()
    print ('* generate_geolocated_segments_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    if time_range == 'accumulated monthly':
        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)

    if time_range == 'last accumulated':
#        period = list(sorted(periods_accum.keys()))[len(periods_accum-1)]
        query_part = ''
        print (time_range,period)
        for_time_range(time_range,query_part,period)


    def for_time_range(time_range,query_part,period):

        regions_all_langs_count={}
        subregions_all_langs_count={}
        iso3166_all_langs_count={}
        all_wp_all_geolocated_articles_count = 0

        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
            wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

            geolocated_articles_count = 0
            iso3166_articles = {}

            if time_range == 'accumulated monthly': query_part+= 'AND '+query_part+' '
            query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki WHERE iso3166 IS NOT NULL '+query_part+'GROUP BY iso3166'

            cursor.execute(query)
            for row in cursor.execute(query):
                iso3166_articles[row[0]]=row[1]
                geolocated_articles_count+=row[1]

                if row[0] not in iso3166_all_langs_count: iso3166_all_langs_count[row[0]]=row[1]
                else: iso3166_all_langs_count[row[0]]+=row[1]

            all_wp_all_geolocated_articles_count+=geolocated_articles_count

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','languagecode','geolocated',geolocated_articles_count,wpnumberofarticles, period)

            regions_count={}
            subregions_count={}
            for iso3166_code, iso3166_count in iso3166_articles.items():
                if iso3166_code == None: continue

                if regions[iso3166_code] not in regions_count: regions_count[regions[iso3166_code]]=iso3166_count
                else: regions_count[regions[iso3166_code]]+=iso3166_count

                if subregions[iso3166_code] not in subregions_count: subregions_count[subregions[iso3166_code]]=iso3166_count
                else: subregions_count[subregions[iso3166_code]]+=iso3166_count

                # accumulating for al languages
                if regions[iso3166_code] not in regions_all_langs_count: regions_all_langs_count[regions[iso3166_code]]=iso3166_count
                else: regions_all_langs_count[regions[iso3166_code]]+=iso3166_count

                if subregions[iso3166_code] not in subregions_all_langs_count: subregions_all_langs_count[subregions[iso3166_code]]=iso3166_count
                else: subregions_all_langs_count[subregions[iso3166_code]]+=iso3166_count

                # countries
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','countries',iso3166_code,iso3166_count,geolocated_articles_count, period)

            # subregions
            for subregion_name, subregion_count in subregions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,geolocated_articles_count, period)

            # regions
            for region_name, region_count in regions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','regions',region_name,region_count,geolocated_articles_count, period)

        conn2.commit()
        print ('languagecode, wp, languagecode, geolocated, '+period)
        print ('languagecode, geolocated, countries, iso3166, '+period)
        print ('languagecode, geolocated, subregions, iso3166, '+period)
        print ('languagecode, geolocated, regions, iso3166, '+period)

            # countries
        for iso3166_code, iso3166_count in iso3166_all_langs_count.items():
            insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','countries',iso3166_code,iso3166_count,all_wp_all_geolocated_articles_count, period)

            # subregions
        for subregion_name, subregion_count in subregions_all_langs_count.items():
            insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','subregions',subregion_name,subregion_count,all_wp_all_geolocated_articles_count, period)

            # regions
        for region_name, region_count in regions_all_langs_count.items():
            insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','geolocated','regions',region_name,region_count,all_wp_all_geolocated_articles_count, period)

        conn2.commit()
        print ('all_wp_all_articles, geolocated, geolocated, countries, '+ period)
        print ('all_wp_all_articles, geolocated, geolocated, subregions, '+ period)
        print ('all_wp_all_articles, geolocated, geolocated, regions, '+ period)

    print ('* generate_geolocated_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_pageviews_intersections():
    functionstartTime = time.time()
    print ('* generate_pageviews_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles_temp.db'); cursor4 = conn4.cursor()

    time_range = 'last month'    

#    wikilanguagecodes2 = ['ca']
    # CCC TOP ARTICLES PAGEVIEWS
    all_ccc_lists_items=set()
    wikipedialanguage_ccclistsitems={}
    for languagecode in wikilanguagecodes:
        lists_qitems = []
        query = 'SELECT DISTINCT qitem FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists) AND position <= 100'
        for row in cursor4.execute(query):
            all_ccc_lists_items.add(row[0])
            lists_qitems.append(row[0])
            wikipedialanguage_ccclistsitems[languagecode]=lists_qitems
        if languagecode not in wikipedialanguage_ccclistsitems: wikipedialanguage_ccclistsitems[languagecode]=lists_qitems

    wikipedialanguage_numberpageviews={}
    wikipedialanguageccc_numberpageviews={}
    # LANGUAGE PAGEVIEWS
    for languagecode in wikilanguagecodes:
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki'
        cursor.execute(query)
        pageviews = cursor.fetchone()[0]
        if pageviews == None or pageviews == '': pageviews = 0
        wikipedialanguage_numberpageviews[languagecode]=pageviews

        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
        cursor.execute(query)
        pageviews = cursor.fetchone()[0]
        if pageviews == None or pageviews == '': pageviews = 0
        wikipedialanguageccc_numberpageviews[languagecode]=pageviews

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp',languagecode,'ccc',wikipedialanguageccc_numberpageviews[languagecode],wikipedialanguage_numberpageviews[languagecode],period)

        page_asstring = ','.join( ['?'] * len(wikipedialanguage_ccclistsitems[languagecode]))
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s)' % page_asstring
        cursor.execute(query,(wikipedialanguage_ccclistsitems[languagecode]))
        ccc_lists_pageviews = cursor.fetchone()[0]

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],period)

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'ccc',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguageccc_numberpageviews[languagecode],period)

        page_asstring = ','.join( ['?'] * len(all_ccc_lists_items))
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s)' % page_asstring
        cursor.execute(query,(list(all_ccc_lists_items)))
        all_ccc_lists_pageviews = cursor.fetchone()[0]

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp','ccc','all_top_ccc_articles',all_ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],period)

#    print (wikipedialanguage_numberpageviews)
#    print (wikipedialanguageccc_numberpageviews)
    conn2.commit()
    print ('languagecode, wp, languagecode, ccc,'+period)
    print ('languagecode, wp, languagecode, all_top_ccc_articles,'+period)
    print ('languagecode, ccc, languagecode, all_top_ccc_articles,'+period)
    print ('languagecode, wp, ccc, all_top_ccc_articles,'+period)

    # LANGUAGES AND LANGUAGES CCC PAGEVIEWS
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            query = 'SELECT SUM(ccc_'+languagecode_1+'wiki.num_pageviews) FROM ccc_'+languagecode_1+'wiki INNER JOIN ccc_'+languagecode_2+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1'
#            print (query)
            cursor.execute(query)
            row = cursor.fetchone()
            languagecode_2_ccc_pageviews = row[0]
            if languagecode_2_ccc_pageviews == None or languagecode_2_ccc_pageviews == '': languagecode_2_ccc_pageviews = 0

            insert_intersections_values(time_range,cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'ccc',languagecode_2_ccc_pageviews,wikipedialanguage_numberpageviews[languagecode_1],period)


            page_asstring = ','.join( ['?'] * len(wikipedialanguage_ccclistsitems[languagecode_2]))
            query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode_1+'wiki WHERE qitem IN (%s)' % page_asstring
            cursor.execute(query,(wikipedialanguage_ccclistsitems[languagecode_2]))
            row = cursor.fetchone()
            languagecode_2_top_ccc_articles_lists_pageviews = row[0]

            insert_intersections_values(time_range,cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'all_top_ccc_articles',languagecode_2_top_ccc_articles_lists_pageviews,wikipedialanguage_numberpageviews[languagecode_1],period)

    conn2.commit()
    print ('languagecode, wp, languagecode_2, ccc,'+period)
    print ('languagecode, wp, languagecode_2, all_top_ccc_articles,'+period)

    for languagecode in wikilanguagecodes:
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE list_name ="pageviews" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists)'
        list_qitems = list()
        for row in cursor4.execute(query):
            list_qitems.append(row[0])

        page_asstring = ','.join( ['?'] * len(list_qitems))
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s)' % page_asstring
        cursor.execute(query,list_qitems)
        pageviews = cursor.fetchone()[0]
        if pageviews == None or pageviews == '': pageviews = 0

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'ccc','top_articles_lists','pageviews',pageviews,wikipedialanguageccc_numberpageviews[languagecode],period)
    conn2.commit()

    print ('languagecode, wp, top_articles_lists, pageviews,'+period)
    print ('* generate_pageviews_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_top_ccc_articles_lists_intersections():
    functionstartTime = time.time()
    print ('* generate_top_ccc_articles_lists_intersections')

    time_range = 'last accumulated'

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles_temp.db'); cursor4 = conn4.cursor()

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1)
        qitems = set()
        query = 'SELECT qitem FROM ccc_'+languagecode_1+'wiki'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')


    # PERHAPS: THIS SHOULD BE LIMITED TO 100 ARTICLES PER LIST.
    # CCC TOP ARTICLES LISTS
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']

    for languagecode in wikilanguagecodes:
        print (languagecode)
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]
        all_top_ccc_articles_count = 0
        all_top_ccc_articles_coincident_count = 0

        all_ccc_lists_items=set()
        for list_name in lists:
            lists_qitems = set()

            for languagecode_2 in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode_2+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode_2+'wiki_top_articles_lists)'
                for row in cursor4.execute(query):
                    all_ccc_lists_items.add(row[0])
                    lists_qitems.add(row[0])
        #           lists_qitems_count=len(lists_qitems)

            all_top_ccc_articles_count+=len(lists_qitems)
            ccc_list_coincident_count=len(lists_qitems.intersection(all_articles[languagecode]))

            insert_intersections_values(time_range,cursor2,'articles','top_ccc_articles_lists',list_name,'wp',languagecode,ccc_list_coincident_count,len(lists_qitems), period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','top_ccc_articles_lists',list_name,ccc_list_coincident_count,wpnumberofarticles, period)


            #  CCC Top articles lists - sum spread and sum coverage
            for languagecode_2 in wikilanguagecodes:
                qitems_unique = set()
                country = ''
                query = 'SELECT qitem, country FROM ccc_'+languagecode_2+'wiki_top_articles_lists WHERE measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode_2+'wiki_top_articles_lists) AND position <= 100 ORDER BY country'
                for row in cursor4.execute(query):
                    cur_country = row[1]

                    if cur_country != country and country != '':
                        list_origin = ''
                        if country != 'all': list_origin = country+'_('+languagecode_2+')'
                        else: list_origin = languagecode_2

                        coincident_qitems_all_qitems = len(qitems_unique.intersection(all_articles[languagecode]))
                        insert_intersections_values(time_range,cursor2,'articles',list_origin,'all_top_ccc_articles',languagecode,'wp',coincident_qitems_all_qitems,len(qitems_unique), period)
                        qitems_unique = set()

                    qitems_unique.add(row[0])
                    country = cur_country

                # last iteration
                if country != 'all': list_origin = country+'_('+languagecode_2+')'
                else: list_origin = languagecode_2

                coincident_qitems_all_qitems = len(qitems_unique.intersection(all_articles[languagecode]))
                insert_intersections_values(time_range,cursor2,'articles',list_origin,'all_top_ccc_articles',languagecode,'wp',coincident_qitems_all_qitems,len(qitems_unique), period)



        # all CCC Top articles lists
        all_top_ccc_articles_coincident_count = len(all_ccc_lists_items.intersection(all_articles[languagecode]))
        insert_intersections_values(time_range,cursor2,'articles','ccc','all_top_ccc_articles',languagecode,'wp',all_top_ccc_articles_coincident_count,all_top_ccc_articles_count, period)

        insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','ccc','all_top_ccc_articles',all_top_ccc_articles_coincident_count,wpnumberofarticles, period)

    conn2.commit()
    print ('top_ccc_articles_lists, list_name, wp, languagecode,'+ period)
    print ('wp, languagecode, top_ccc_articles_lists, list_name,'+ period)

    print ('languagecode_2, all_top_ccc_articles, languagecode, list_name,'+ period)

    print ('ccc, all_top_ccc_articles, languagecode, wp,'+ period)
    print ('languagecode, wp, ccc, all_top_ccc_articles,'+ period)

    print ('* generate_top_ccc_articles_lists_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_monthly_articles_intersections(time_range):
    functionstartTime = time.time()
    print ('* generate_monthly_articles_intersections')

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles_temp.db'); cursor4 = conn4.cursor()

    if time_range == 'monthly':
        for period in sorted(periods_monthly.keys()):
            print (time_range,period,'\t',periods_monthly[period])
            for_time_range(time_range,periods_accum[period],period)

    if time_range == 'last month':
        period = list(sorted(periods_monthly.keys()))[len(periods_monthly-1)]
        query_part = periods_monthly[period]
        print (time_range,period,'\t',query_part)
        for_time_range(time_range,query_part,period)


    def for_time_range(time_range,query_part,period):

        # ccc top article lists
        lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
        all_qitems = set()
        lists_dict = {}
        for list_name in lists:
            lists_qitems = set()
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists)'
                for row in cursor4.execute(query):
                    lists_qitems.add(row[0])
                    all_qitems.add(row[0])
            lists_dict[list_name] = lists_qitems

    #    wikilanguagecodes2=['ca']
        for languagecode in wikilanguagecodes:
            print ('\n'+languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

            # wikipedia num of accumulated articles
            query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki'
            if time_range == 'monthly': query+= ' WHERE '+periods_accum[period]
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
            if time_range == 'monthly': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            ccc_articles_count = cursor.fetchone()[0]

            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND ccc_geolocated=1'
            if time_range == 'monthly': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            ccc_geolocated_articles_count = cursor.fetchone()[0]

            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
            if time_range == 'monthly': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            ccc_keywords_articles_count = cursor.fetchone()[0]


            #  month articles
            qitems = []
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE '+query_part
            print (query)
            for row in cursor.execute(query):
                qitems.append(row[0])
            created_articles_count = len(qitems)
            print (created_articles_count)

            # PEOPLE
            # male
            male=[]
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097"'
            for row in cursor.execute(query):
                male.append(row[0])
            malecount=len(male)
            male = set(male).intersection(set(qitems))
            month_articles_male_count=len(male)

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles', languagecode, 'male', month_articles_male_count,created_articles_count,period)

            # female
            female=[]
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072"'
            for row in cursor.execute(query): 
                female.append(row[0])
            femalecount=len(female)
            female = set(female).intersection(set(qitems))
            last_month_female_count=len(female)

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles', languagecode, 'female', last_month_female_count,created_articles_count,period)

            # people
            last_month_peoplecount=month_articles_male_count+last_month_female_count
            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles', languagecode, 'people', last_month_peoplecount,created_articles_count,period)



            # CCC created during the month
            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+ query_part + ' AND ccc_binary=1'
            cursor.execute(query)
            ccc_articles_created_count = cursor.fetchone()[0]

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles',languagecode, 'ccc', ccc_articles_created_count, created_articles_count, period)

            # CCC geolocated created during the month
            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+query_part+' AND ccc_binary=1 AND ccc_geolocated=1'
            cursor.execute(query)
            ccc_geolocated_articles_created_count = cursor.fetchone()[0]

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles',languagecode, 'ccc_geolocated', ccc_geolocated_articles_created_count, created_articles_count, period)

            # CCC keywords
            query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+query_part+' AND ccc_binary=1 AND keyword_title IS NOT NULL'
            cursor.execute(query)
            ccc_keywords_articles_created_count = cursor.fetchone()[0]

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles',languagecode, 'ccc_keywords', ccc_keywords_articles_created_count, created_articles_count, period)

            # Not own CCC
            not_own_ccc = wpnumberofarticles - ccc_articles_count
            not_own_ccc_created_count = created_articles_count - ccc_articles_created_count

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles',languagecode, 'not_own_ccc', not_own_ccc_created_count, created_articles_count, period)
            

            # Other Langs CCC
            for languagecode_2 in wikilanguagecodes:
                if languagecode == languagecode_2: continue
                query = 'SELECT COUNT (*) FROM ccc_'+languagecode+'wiki INNER JOIN ccc_'+languagecode_2+'wiki ON ccc_'+languagecode+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND '
                query_extra = query_part.replace('date_created','ccc_'+languagecode+'wiki.date_created')
                query += query_extra
                #'ccc_'+languagecode+'wiki.date_created >= "'+first_day+'" AND ccc_'+languagecode+'wiki.date_created < "'+last_day+'"'

                cursor.execute(query)
                ccc_articles_created_count = cursor.fetchone()[0]
#                print (languagecode_2,ccc_articles_created_count,created_articles_count,languagecode)

                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'month_articles',languagecode_2, 'ccc', ccc_articles_created_count, created_articles_count, period)


            # CCC TOP ARTICLES LISTS
            for list_name in lists:
    #           lists_qitems_count=len(lists_qitems)
                coincident_qitems = set(lists_dict[list_name]).intersection(set(qitems))
                last_month_list_count=len(coincident_qitems)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','top_ccc_articles_lists',list_name,last_month_list_count,created_articles_count, period)

            coincident_qitems_all_qitems = len(all_qitems.intersection(set(qitems)))
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','ccc','all_top_ccc_articles',coincident_qitems_all_qitems,created_articles_count, period)




            # GEOLOCATED SEGMENTS
            country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions() # iso 3166 to X

            geolocated_articles_count = 0
            iso3166_articles = {}
            query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki WHERE iso3166 IS NOT NULL AND '+query_part + ' GROUP BY iso3166'
    #        print (query)
            cursor.execute(query)
            for row in cursor.execute(query):
                iso3166_articles[row[0]]=row[1]
                geolocated_articles_count+=row[1]

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','wikidata_article_qitems','geolocated',geolocated_articles_count,created_articles_count, period)

    #        print (iso3166_articles)
            regions_count={}
            subregions_count={}
            for iso3166_code, iso3166_count in iso3166_articles.items():
                if iso3166_code == None: continue
                if regions[iso3166_code] not in regions_count: regions_count[regions[iso3166_code]]=iso3166_count
                else: regions_count[regions[iso3166_code]]+=iso3166_count

                if subregions[iso3166_code] not in subregions_count: subregions_count[subregions[iso3166_code]]=iso3166_count
                else: subregions_count[subregions[iso3166_code]]+=iso3166_count

                # countries
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','countries',iso3166_code,iso3166_count,created_articles_count, period)

            # subregions
            for subregion_name, subregion_count in subregions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','subregions',subregion_name,subregion_count,created_articles_count, period)

            # regions
            for region_name, region_count in regions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'month_articles','regions',region_name,region_count,created_articles_count, period)

        conn2.commit()


        print ('languagecode, month_articles, languagecode, male,'+period)
        print ('languagecode, month_articles, languagecode, female,'+period)
        print ('languagecode, month_articles, languagecode, people,'+period)

        print ('languagecode, month_articles, languagecode, ccc,'+period)   
        print ('languagecode, month_articles, languagecode, ccc_geolocated,'+period)
        
        print ('languagecode, month_articles,languagecode, ccc_keywords,'+period)  
        print ('languagecode, month_articles, languagecode, not_own_ccc,'+period)
        print ('languagecode, month_articles, languagecode_2, ccc,'+period)
        
        print ('languagecode, month_articles, top_ccc_articles_lists, list_name,'+period)
        print ('languagecode, month_articles, ccc, all_top_ccc_articles,'+period)

        print ('languagecode, month_articles, wikidata_article_qitems, geolocated,'+period)  
        print ('languagecode, month_articles, countries, iso3166,'+period)
        print ('languagecode, month_articles, subregions, subregion_name,'+period)
        print ('languagecode, month_articles, regions, region_name,'+period)
    
    print ('* generate_monthly_articles_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))




##########################################################################################


def generate_all_top_ccc_articles_lists():

    print ('Generating all the Top articles lists.')

    wikilanguagecodes_real = ['ca']
#    wikilanguagecodes_real=['it']
#    wikilanguagecodes_real = ['ca','it','en','es','ro']
#    wikilanguagecodes_real=['it', 'fr', 'ca', 'en', 'de', 'es', 'nl', 'uk', 'pt', 'pl']
#    wikilanguagecodes_real=wikilanguagecodes[wikilanguagecodes.index('uz'):]

    # LANGUAGES
    for languagecode in wikilanguagecodes_real:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

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



            # (languagecode, content_type, category, percentage_filtered, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns, page_titles_qitems, country, list_name)

            ### GENERAL CCC ###

            # EDITORS
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_editors': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'editors')

            # EDITS
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'edits')

            # MOST EDITED DURING THE LAST MONTH
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_edits_last_month': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'edited_last_month')

            # CREATED DURING FIRST THREE YEARS AND MOST EDITED
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, 'first_three_years', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_first_three_years')

            # CREATED DURING LAST YEAR AND MOST EDITED
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, 'last_year', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_last_year')

            # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_pageviews':1}, 'positive', length, 'none', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'pageviews')

            # MOST DISCUSSED (EDITS DISCUSSIONS)
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_discussions': 1}, 'positive', length, 'none', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'discussions')

            # FEATURED, LONG AND CITED
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'featured_article': 0.8, 'num_references':0.1, 'num_bytes':0.1}, 'positive', length, 'none', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'featured')

            # IMAGES, LONG AND CITED
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_images': 0.8, 'num_bytes':0.1, 'num_references':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'images')

            # MOST WD STATEMENTS
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_wdproperty': 0.9, 'num_editors':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'wdproperty_many')

            # MOST INTERWIKI
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_interwiki': 0.9, 'num_editors':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_many')

            # LEAST INTERWIKI (EDITOR PEARLS)
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'interwiki_relationship': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_editors')

            # LEAST INTERWIKI (WD STATEMENTS PEARLS)
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'interwiki_relationship': 0.9, 'num_pageviews': 0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'interwiki_wdproperty')

            # HIGHEST WIKIRANK AND POPULAR
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'wikirank': 0.9, 'num_pageviews':0.1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'wikirank')


            ### TOPICS (people, places and things) ###

            # WOMEN BIOGRAPHY MOST EDITED
            make_top_ccc_articles_list(languagecode, ['ccc','female'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'women')

            # MEN BIOGRAPHY MOST EDITED
            make_top_ccc_articles_list(languagecode, ['ccc','male'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'men')

            # FOLK MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','folk'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'folk')

            # EARTH MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','earth'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'earth')

            # MONUMENTS AND BUILDINGS MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','monuments_and_buildings'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'monuments_and_buildings')

            # MUSIC CREATIONS AND ORGaNIZATIONS MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','music_creations_and_organizations'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'music_creations_and_organizations')

            # SPORTS MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','sport_and_teams'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'sport_and_teams')

            # FOOD MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','food'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'food')

            # PAINTINGS MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','paintings'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'paintings')

            # GLAM MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','glam'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'glam')

            # BOOKS MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','books'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'books')

            # CLOTHING AND FASHION MOST PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','clothing_and_fashion'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'clothing_and_fashion')

            # INDUSTRY PAGEVIEWS
            make_top_ccc_articles_list(languagecode, ['ccc','industry'], category, 80, '', {'num_pageviews': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'industry')


            ### SPECIFIC CCC ###

            # GL MOST INLINKED FROM CCC
            make_top_ccc_articles_list(languagecode, ['gl'], category, 80, '', {'num_inlinks_from_CCC': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'geolocated')

            # KEYWORDS ON TITLE WITH MOST BYTES
            make_top_ccc_articles_list(languagecode, ['kw'], category, 80, '', {'num_bytes': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'keywords')



            with open('top_ccc_articles.txt', 'a') as f: f.write(languagecode+'\t'+languages.loc[languagecode]['languagename']+'\t'+country+'\t'+str(datetime.timedelta(seconds=time.time() - country_Time))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')
            print (languagecode+'\t'+languages.loc[languagecode]['languagename']+'\t'+country+'\t'+str(datetime.timedelta(seconds=time.time() - country_Time))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')



def make_top_ccc_articles_list(languagecode, content_type, category, percentage_filtered, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns, page_titles_qitems, country, list_name):

    functionstartTime = time.time()
    last_period_time = functionstartTime
    print ('\n\n* make_table_top_ccc_articles_list')
    print (list_name)
    print ('Obtaining a prioritized article list based on these parameters:')
    print (languagecode, content_type, category, time_frame, rellevance_rank, rellevance_sense, window, representativity, columns)

    # Databases connections
    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
#    conn = sqlite3.connect(databases_path + 'ccc_old.db'); cursor = conn.cursor()

    conn4 = sqlite3.connect(databases_path + top_ccc_db); cursor4 = conn4.cursor()

    # DEFINE CONTENT TYPE
    # according to the content type, we make a query or another.
    print ('define the content type')
    if content_type[0] == 'ccc': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'
    if content_type[0] == 'gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NOT NULL'
    if content_type[0] == 'kw': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
    if content_type[0] == 'ccc_not_gl': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND geocoordinates IS NULL'
    if content_type[0] == 'ccc_main_territory': query = 'SELECT * FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1'


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
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = cursor.fetchone()
        timestamp = timestamp[0]
        if timestamp == None or timestamp == 'None': return
        print (timestamp)
        first_year=(datetime.datetime.strptime(str(timestamp),'%Y%m%d%H%M%S') + datetime.timedelta(days=365)).strftime('%Y%m%d%H%M%S')
        query = query + ' AND date_created < '+str(first_year)
    if time_frame == 'first_three_years':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
        timestamp = cursor.fetchone()
        timestamp = timestamp[0]
        if timestamp == None or timestamp == 'None': return
        print (timestamp)
        first_three_years=(datetime.datetime.strptime(str(timestamp),'%Y%m%d%H%M%S') + datetime.timedelta(days=3*365)).strftime('%Y%m%d%H%M%S')
        print (first_three_years)
        query = query + ' AND date_created < '+str(first_three_years)


    if time_frame == 'first_five_years':
        cursor.execute("SELECT MIN(date_created) FROM ccc_"+languagecode+"wiki;")
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

        if content_type[1] == 'people': 
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender IS NOT NULL'
        elif content_type[1] == 'male':
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097"'
        elif content_type[1] == 'female':
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072"'
        else:
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE '+content_type[1]+' IS NOT NULL'

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


    # RANK ARTICLES BY RELLEVANCE
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
    if list_name == 'wikirank': # API
        ranked_saved=ccc_df['num_pageviews'].sort_values(ascending=False).index.tolist()
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
    print ('rank articles by rellevance')
    articles_ranked = {}
    if rellevance_sense=='positive': # articles top priority of rellevance
        ascending=False
    if rellevance_sense=='negative': # articles for deletion (no one cares)
        ascending=True

    rellevance_measures = ['num_inlinks', 'num_outlinks', 'num_bytes', 'num_references', 'num_edits','num_edits_last_month', 'num_editors', 'num_pageviews', 'num_wdproperty', 'num_interwiki', 'num_discussions', 'num_images', 'featured_article', 'num_inlinks_from_CCC', 'num_retrieval_strategies', 'interwiki_relationship', 'wikirank']
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

    if representativity == 'all_equal': # all equal. get all the qitems for the language code. divide the 
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

                    print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,'\t\t\t\t\t'+str(list(rellevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[0]]),

                    '\t\t'+str('images')+':'+str(ccc_df.loc[qitem]['num_images']),
                    '\t\t'+str('interwiki')+':'+str(ccc_df.loc[qitem]['num_interwiki']),
                    '\t\t'+str('editors')+':'+str(ccc_df.loc[qitem]['num_editors']),

#                    print (i,"("+str(y)+")",ccc_df.loc[qitem]['page_title'],qitem,'\t\t\t\t\t'+str(list(rellevance_rank.keys())[0])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[0]]),
#                    '\t'+str(list(rellevance_rank.keys())[1])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[1]]),
#                    '\t'+str(list(rellevance_rank.keys())[2])+':'+str(ccc_df.loc[qitem][list(rellevance_rank.keys())[2]]),
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

    print ('we stop here by now.')
    return

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

    query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki_top_articles_features (qitem, page_title_original, measurement_date) VALUES (?,?,?)'
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

    query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki_top_articles_lists (position, qitem, country, list_name, measurement_date) VALUES (?,?,?,?,?)'
    cursor4.executemany(query,tuples)
    conn4.commit()

    print ('* make_top_ccc_articles_list '+list_name+', for '+list_origin+'. Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def update_top_ccc_articles_features():
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + top_ccc_db); cursor2 = conn2.cursor()

    print ('* update_top_ccc_articles_features')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        lists_qitems = set()
        query = 'SELECT qitem, measurement_date FROM ccc_'+languagecode+'wiki_top_articles_lists'
        for row in cursor2.execute(query):
            lists_qitems.add(row[0])
            measurement_date = row[1]

        if len(lists_qitems) == 0: continue
        print ('There is this number of qitems in ccc_'+languagecode+'wiki_top_articles_lists: '+str(len(lists_qitems)))

        page_asstring = ','.join( ['?'] * len( lists_qitems ) )
        query = 'SELECT num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_wdproperty, num_interwiki, featured_article, num_inlinks_from_CCC, date_created, qitem FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s)' % page_asstring

        parameters = []
        for row in cursor.execute(query, list(lists_qitems)):
            featured_article = row[10]
            if featured_article != 1: featured_article = 0
            parameters.append((row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],featured_article,row[11],row[12],row[13],measurement_date))

        print ('Number of articles updated with features: '+str(len(parameters)))

        query = 'UPDATE ccc_'+languagecode+'wiki_top_articles_features SET num_inlinks = ?, num_outlinks = ?, num_bytes = ?, num_references = ?, num_edits = ?, num_editors = ?, num_discussions = ?, num_pageviews = ?, num_wdproperty = ?, num_interwiki = ?, featured_article = ?, num_inlinks_from_CCC = ?, date_created = ? WHERE qitem = ? AND measurement_date = ?'
        cursor2.executemany(query,parameters)
        conn2.commit()

    print ('Measurement date is: '+str(measurement_date))
    print ('* update_top_ccc_articles_features Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def update_top_ccc_articles_titles(type):

    functionstartTime = time.time()
    conn4 = sqlite3.connect(databases_path + top_ccc_db); cursor4 = conn4.cursor()
    print ('* update_top_ccc_articles_titles '+ type)

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
                query = 'SELECT qitem, country, list_name, position FROM ccc_'+languagecode_2+'wiki_top_articles_lists WHERE measurement_date = "'+measurement_date+'" ORDER BY country, list_name, position ASC'
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
                            titles.append((measurement_date,qitem,page_title,'sitelinks'))
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
            query = 'INSERT OR IGNORE INTO ccc_'+languagecode_1+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
            cursor4.executemany(query, titles); # to top_ccc_articles.db
            conn4.commit()
            print (str(len(titles))+' titles that exist.') # including repeated qitems from different lists in the same language

            print ('* '+languagecode_1 + ' done with page_titles sitelinks.')
            with open('top_ccc_articles.txt', 'a') as f: f.write('* '+languagecode_1 + ' done with page_titles sitelinks. '+str(len(titles))+' titles. '+ str(datetime.timedelta(seconds=time.time() - langTime))+'\n')


            # INSERT INTERSECTIONS
            if len(intersections) > 500000 or wikilanguagecodes.index(languagecode_1) == len(wikilanguagecodes)-1:
                query = 'INSERT OR IGNORE INTO wcdo_intersections (set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date) VALUES (?,?,?,?,?,?,?)'
                cursor4.executemany(query,intersections); 
                conn4.commit() # to stats.db
                print (str(len(intersections))+' intersections inserted.')
                with open('top_ccc_articles.txt', 'a') as f: f.write(str(len(intersections))+' intersections calculated.\n')
                intersections = list()


    if (type=='labels'):
        print ('UPDATING DB WITH LABELS SUGGESTIONS.')
        print ('Calculating the labels for '+str(len(wikilanguagecodes))+' languages.\n')

        conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

        languagecode_translated_from = wikilanguages_utils.load_language_pairs_apertium(wikilanguagecodes)

#        wikilanguagecodes_2 = wikilanguagecodes[wikilanguagecodes.index('am')+1:]
#        wikilanguagecodes_2 = ['is']
        for langcode_target in wikilanguagecodes:
            print ('\n* ### * language '+langcode_target+' with name '+languages.loc[langcode_target]['languagename']+'.')
            languageTime = time.time()
            
            # UPDATING FROM PAST MONTH LABELS AND TRANSLATIONS
            first_qitems_none = set()
            print ('\n- update from past iteration')
            print('get current missing qitems:')
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?)'
                for row in cursor4.execute(query, (measurement_date,measurement_date)):
                    first_qitems_none.add(row[0])
            print (str(len(first_qitems_none)))
            print('change date from those missing qitems that were already in the database with labels and translations.')

            initialy = 10000
            x = 0; y = initialy
            first_qitems_none = list(first_qitems_none)
            while x < len(first_qitems_none):
#                print (x,y)
                sample = first_qitems_none[x:y]
#                print (len(sample))
                page_asstring = ','.join( ['?'] * len(sample) )
                query = 'UPDATE ccc_'+langcode_target+'wiki_top_articles_page_titles SET measurement_date = "'+measurement_date+'" WHERE qitem IN (%s)' % page_asstring
                cursor4.execute(query, (sample))
                x = y
                y = y + initialy
            conn4.commit()

            # DELETING THOSE USELESS FROM PAST MONTH
            query = 'DELETE FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+langcode_target+'wiki_top_articles_page_titles)'
            cursor4.execute(query)
            conn4.commit()

            # GETTING LABELS
            print ('\n- update from current wikidata labels')
            print('get current missing qitems:')
            second_qitems_none = set()
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?)'
                for row in cursor4.execute(query, (measurement_date,measurement_date)):
                    second_qitems_none.add(row[0])
            print (str(len(second_qitems_none)))

            print('get the labels for this language for these qitems.')
#            initialy = 1
            initialy = 200000
            x = 0; y = initialy
            labelscounter = 0
            second_qitems_none = list(second_qitems_none)
            while x < len(second_qitems_none):
                sample = second_qitems_none[x:y]
                page_asstring = ','.join( ['?'] * len(sample) )
                query = 'SELECT qitem, label FROM labels WHERE langcode = "'+langcode_target+'wiki" AND qitem IN (%s)' % page_asstring
                parameters=[]
                for row in cursor3.execute(query,sample): 
                    missing_qitem=row[0]
                    label=row[1].replace(' ','_')
                    parameters.append((measurement_date, missing_qitem, label, "label"))
                    labelscounter+=1
                print (x,y)
                query = 'INSERT INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
                cursor4.executemany(query, parameters)
                x = y
                y = y + initialy
            print (str(labelscounter)+' labels that became useful for possible page_titles.')
            conn4.commit()

            with open('top_ccc_articles.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

            print ('* language target_titles labels for language '+langcode_target+' completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime))+'\n')


    if (type=='translations'):

        languagecode_translated_from = wikilanguages_utils.load_language_pairs_apertium(wikilanguagecodes)     
        with open('top_ccc_articles.txt', 'a') as f: f.write(','.join(map(str, list(languagecode_translated_from.keys())))+'\n')

        print ('UPDATING DB WITH LABELS SUGGESTIONS.')
        for langcode_target in list(languagecode_translated_from.keys()):

            print ('\n* ### * language '+langcode_target+' with name '+languages.loc[langcode_target]['languagename']+'.')
            languageTime = time.time()

            # GETTING TRANSLATIONS FROM ORIGINAL
            print ('- update from translation from original.')
            print('get current missing qitems and the original page_title:')
            third_qitems_none = {}
            for languagecode in languagecode_translated_from[langcode_target]:
                query = 'SELECT qitem, page_title_original FROM ccc_'+languagecode+'wiki_top_articles_features WHERE qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?) AND measurement_date = ?' # it should get the titles from the languages in which there is a translation, even though it is not the original language of the Q.
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
                            print ('timeout.')

                    if r!=None and r.text!='Provider not supported':
                        page_title_target = str(r.text).split('<div>')[1].split('</div>')[0].replace(' ','_')
                        parameters.append((measurement_date, qitem, page_title_target, "translation"))
                    if len(parameters) % 1000 == 0:
                        print (len(parameters))
                        with open('top_ccc_articles.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+str(len(parameters))+'\t'+str(datetime.datetime.now())+'\n')

            print (str(len(parameters))+' translated titles to '+langcode_target+'.')

            query = 'INSERT OR IGNORE INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
            cursor4.executemany(query, parameters)
            conn4.commit()


            # GETTING TRANSLATIONS FROM VERSION
            print ('\n- update from translation from the copy.')
            fourth_qitems_none = list()
            print('get current missing qitems:')
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?)'
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
                    query = 'SELECT qitem, page_title_target FROM ccc_'+language_origin+'wiki_top_articles_page_titles WHERE measurement_date = "'+measurement_date+'" AND qitem IN (%s)' % page_asstring
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
                            with open('top_ccc_articles.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+str(len(parameters))+'\t'+str(datetime.datetime.now())+'\n')

                    x = y
                    y = y + initialy

            print ('remaining qitems: '+str(len(fourth_qitems_none)))
            print (str(len(parameters))+' translated titles to '+langcode_target)

            query = 'INSERT OR IGNORE INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?)'
            cursor4.executemany(query, parameters)
            conn4.commit()

            print ('total number of missing titles in the end: '+str(len(fourth_qitems_none))+'.')

            # DONE!
            print ('* language target_titles translations for language '+langcode_target+' completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime))+'\n')
            with open('top_ccc_articles.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

    print ('* update_top_ccc_articles_titles **'+type+'** Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def delete_last_iteration_top_ccc_articles_lists():
    conn = sqlite3.connect(databases_path + top_ccc_db); cursor = conn.cursor()

    print ('Deleting all the rest from the last iteration.')
    for languagecode in wikilanguagecodes:
        print (languagecode)

        query = 'SELECT count(DISTINCT measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_features'
        cursor.execute(query)
        if cursor.fetchone()[0] > 1:
            query = 'DELETE FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_features)'
            cursor.execute(query); conn.commit()
        else: print ('only one measurement_date in wiki_top_articles_features')

        query = 'SELECT count(DISTINCT measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists'
        cursor.execute(query)
        if cursor.fetchone()[0] > 1:
            query = 'DELETE FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists)'
            cursor.execute(query); conn.commit()
        else: print ('only one measurement_date in wiki_top_articles_lists')


    query = 'SELECT count(DISTINCT measurement_date) FROM wcdo_intersections'
    cursor.execute(query)
    if cursor.fetchone()[0] > 1:
        query = 'DELETE FROM wcdo_intersections WHERE measurement_date IN (SELECT MIN(measurement_date) FROM wcdo_intersections)'
        cursor.execute(query); conn.commit()
    else: print ('only one measurement_date in wcdo_intersections')


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



#######################################################################################

### MAIN:
if __name__ == '__main__':
    startTime = time.time()
    sys.stdout = Logger()

    # Database path
    databases_path = '/srv/wcdo/databases/'
    ccc_db = 'ccc.db'
    stats_db = 'stats.db'
    top_ccc_db = 'top_ccc_articles.db'

    measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");
#    measurement_date = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime(databases_path+ccc_db)))
#    measurement_date = '20180926'

    current_year_month_period = datetime.date.today().strftime('%Y-%m')
#    current_year_month_period = time.strftime('%Y-%m', time.gmtime(os.path.getmtime(databases_path+ccc_db)))

    periods_monthly,periods_accum = get_months_queries()


    # Import the language-territories mappings
    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()

    # Import the Wikipedia languages characteristics
    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = languages.index.tolist()

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
    # Only those with a geographical context
    wikilanguagecodes_real = wikilanguagecodes.copy()
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # Verify/Remove all languages without a table in ccc.db
    wikipedialanguage_currentnumberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'last')
    for languagecode in wikilanguagecodes:
        if languagecode not in wikipedialanguage_currentnumberarticles: wikilanguagecodes.remove(languagecode)

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    print ('\n* Starting the STATS GENERATION CYCLE '+current_year_month_period+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* STATS GENERATION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    wikilanguages_utils.finish_email(startTime,'stats_generation.out','Stats generation')