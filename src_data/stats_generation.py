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
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# data and compute
import pandas as pd
import numpy as np


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("stats_generation"+""+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass


######################################################################

# MAIN
######################## STATS GENERATION SCRIPT ##################### 
def main():

    wikilanguages_utils.send_email_toolaccount('WCDO', '# GENERATE TOP CCC ARTICLES LISTS')
    print ('Create Top CCC articles lists.')
    create_top_ccc_articles_lists_db()
    generate_all_top_ccc_articles_lists()
    update_top_ccc_articles_features()
    update_top_ccc_articles_titles('sitelinks')
    update_top_ccc_articles_titles('labels')
    update_top_ccc_articles_titles('translations')
    delete_last_iteration_top_ccc_articles_lists()

    wikilanguages_utils.send_email_toolaccount('WCDO', '# GENERATE THE MAIN STATS')
    create_intersections_db()
    generate_langs_intersections()
    generate_ccc_segments_intersections()
    generate_langs_ccc_intersections()
    generate_ccc_ccc_intersections()
    generate_people_segments_intersections()
    generate_people_ccc_intersections()
    generate_geolocated_segments_intersections()
    generate_top_ccc_articles_lists_intersections()
    generate_last_month_articles_intersections()
    generate_pageviews_intersections()

#    create_increments_db()
#    generate_all_increments()
#    delete_last_iteration_increments()


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 


def remove_create_wcdo_stats_db():
    try:
        os.remove(databases_path + "wcdo_stats.db"); print ('wcdo_stats.db deleted.');
    except:
        pass

# INTERSECTIONS AND INCREMENTS
# COMMAND LINE: sqlite3 -header -csv wcdo_stats.db "SELECT * FROM create_intersections_db;" > create_intersections_db.csv
def create_intersections_db():

    functionstartTime = time.time()
    print ('* create_intersections_db')
    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

    query = ('CREATE table if not exists wcdo_intersections ('+
    'intersection_id integer, '+

    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'measurement_date text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,measurement_date));')

    cursor.execute(query)
    conn.commit()

    print ('* create_intersections_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

def create_increments_db():
    functionstartTime = time.time()
    print ('* create_increments_db')

    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

    query = ('CREATE table if not exists wcdo_increments ('+
    'cur_intersection_id integer, '+
    'abs_increment integer,'+
    'rel_increment float,'+
    'period text,'+
    'measurement_date text,'+
    'PRIMARY KEY (cur_intersection_id, period));')

    cursor.execute(query)
    conn.commit()

    print ('* create_increments_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


# LISTS
def create_top_ccc_articles_lists_db():
    functionstartTime = time.time()
    print ('* create_top_ccc_articles_lists_db')

    conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()

    for languagecode in wikilanguagecodes:

        query = ('CREATE table if not exists ccc_'+languagecode+'wiki_top_articles_lists ('+
        'qitem text,'+
        'position integer,'+
        'country text,'+
        'list_name text,'+
        'measurement_date text,'+

        'PRIMARY KEY (qitem, list_name, country, measurement_date));')
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

        'PRIMARY KEY (qitem, measurement_date));')
        cursor.execute(query)

        query = ('CREATE table if not exists ccc_'+languagecode+'wiki_top_articles_page_titles ('+
        'qitem text,'+
        'page_title_target text,'+ 
        'generation_method text,'+ # page_title_target can either be the REAL (from sitelinks wikitada), the label proposal (from labels wikitada) or translated (content translator tool).
        'measurement_date text,'+

        'PRIMARY KEY (qitem, measurement_date));')
        cursor.execute(query)

        query = ('CREATE table if not exists wcdo_intersections ('+
        'set1 text not null, '+
        'set1descriptor text, '+

        'set2 text, '+
        'set2descriptor text, '+

        'abs_value integer,'+
        'rel_value float,'+

        'measurement_date text,'
        'PRIMARY KEY (set1,set1descriptor,set2,set2descriptor,measurement_date));')

        cursor.execute(query)
        conn.commit()

    print ('* create_top_ccc_articles_lists_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def check_cur_intersection_id():
    if os.path.isfile(databases_path + 'wcdo_stats.db'):
        conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
        query = 'SELECT MAX(intersection_id) FROM wcdo_intersections;'
        cursor2.execute(query)
        row = cursor2.fetchone()
        cur_intersection_id = row[0];
        if cur_intersection_id == None: cur_intersection_id = 0
    else:
        cur_intersection_id = 0
    print ('\ncurrent intersection_id is: '+str(cur_intersection_id))
    return cur_intersection_id


def insert_intersections_values(cursor2, content, set1, set1descriptor, set2, set2descriptor, abs_value, base, measurement_date):
    global cur_intersection_id
    cur_intersection_id+=1

    if abs_value == None: abs_value = 0

    if base == None or base == 0: rel_value = 0
    else: rel_value = 100*abs_value/base

    if 'avg' in set1 or 'avg' in set2: rel_value = base # exception for calculations in generate_langs_ccc_intersections()

    values = (cur_intersection_id, content, set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date)
    cursor2.execute(query_insert,values);


def generate_langs_intersections():
    functionstartTime = time.time()
    print ('* generate_langs_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1)
        qitems = set()
        query = 'SELECT qitem FROM ccc_'+languagecode_1+'wiki;'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')

    # LANGUAGE EDITIONS
    for languagecode_1 in wikilanguagecodes:
        partialtime = time.time()
        print ('* '+languagecode_1)
        wpnumberofarticles_1=wikipedialanguage_numberarticles[languagecode_1]

        # entire wp
        query = 'SELECT count(*) FROM ccc_'+languagecode_1+'wiki WHERE num_interwiki = 0;'
        cursor.execute(query)
        zero_ill_wp_count = cursor.fetchone()[0]
        insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_1,'zero_ill',zero_ill_wp_count,wpnumberofarticles_1, measurement_date)


        query = 'SELECT count(*) FROM ccc_'+languagecode_1+'wiki WHERE qitem IS NULL;'
        cursor.execute(query)
        null_qitem_count = cursor.fetchone()[0]
        insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_1,'null_qitems',null_qitem_count,wpnumberofarticles_1, measurement_date)

        if wpnumberofarticles_1 == 0: continue
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue
#            query = 'SELECT COUNT(*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem;'
#            cursor.execute(query)
#            article_count = cursor.fetchone()[0]
            article_count=len(all_articles[languagecode_1].intersection(all_articles[languagecode_2]))
            insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_2,'wp',article_count,wpnumberofarticles_1,measurement_date)

        print ('. '+languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - partialtime)))

    conn2.commit()
    print ('languagecode, wp, languagecode, zero_ill,'+measurement_date)
    print ('languagecode, wp, languagecode, null_qitems,'+measurement_date)
    print ('languagecode_1, wp, languagecode_2, wp,'+measurement_date)

    print ('* generate_langs_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_ccc_segments_intersections():
    functionstartTime = time.time()
    print ('* generate_ccc_segments_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    # WIKIDATA AND CCC
    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks;'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        query = 'SELECT COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;';
        cursor.execute(query)
        row = cursor.fetchone()

        ccc_count = row[0]
        not_own_ccc_created_count = wpnumberofarticles - ccc_count
        ccc_geolocated_count = row[1]
        ccc_keywords_count = row[2]

        # In regards of wikidata qitems
        insert_intersections_values(cursor2,'articles','wikidata_article_qitems',None,languagecode,'ccc',ccc_count,wikidata_article_qitems_count,measurement_date)

        # In regards of WP
        insert_intersections_values(cursor2,'articles',languagecode,'wp',languagecode,'ccc',ccc_count,wpnumberofarticles,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode,'wp',languagecode,'not_own_ccc',not_own_ccc_created_count,wpnumberofarticles,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode,'wp',languagecode,'ccc_geolocated',ccc_geolocated_count,wpnumberofarticles,measurement_date)
 
        insert_intersections_values(cursor2,'articles',languagecode,'wp',languagecode,'ccc_keywords',ccc_keywords_count,wpnumberofarticles,measurement_date)
 
        # In regards of CCC
        insert_intersections_values(cursor2,'articles',languagecode,'ccc',languagecode,'ccc_keywords',ccc_keywords_count,ccc_count,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode,'ccc',languagecode,'ccc_geolocated',ccc_geolocated_count,ccc_count,measurement_date)

        # zero ill
        query = 'SELECT count(page_title) FROM ccc_'+languagecode+'wiki WHERE num_interwiki = 0 AND ccc_binary=1'
        cursor.execute(query)
        zero_ill_ccc_count = cursor.fetchone()[0]
        insert_intersections_values(cursor2,'articles',languagecode,'ccc',languagecode,'zero_ill',zero_ill_ccc_count,ccc_count, measurement_date)

        # MAIN TERRITORIES
        query = 'SELECT main_territory, COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 GROUP BY main_territory;';
        for row in cursor.execute(query):
            main_territory=row[0]
            if main_territory == '' or main_territory == None:
                main_territory = 'Not Assigned'
            ccc_articles_count=row[1]
            ccc_geolocated_count=row[2]
            ccc_keywords_count=row[3]

            insert_intersections_values(cursor2,'articles',languagecode,'ccc','ccc',main_territory,ccc_articles_count,ccc_count, measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode,'ccc','ccc_geolocated',main_territory,ccc_geolocated_count,ccc_count, measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode,'ccc','ccc_keywords',main_territory,ccc_keywords_count,ccc_count, measurement_date)

    conn2.commit()
    print ('wikidata_article_qitems, , languagecode, ccc, '+ measurement_date)

    print ('languagecode, wp, languagecode, ccc,'+measurement_date)
    print ('languagecode, wp, languagecode, not_own_ccc,'+measurement_date)
    print ('languagecode, wp, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, wp, languagecode, ccc_keywords,'+measurement_date)

    print ('languagecode, ccc, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, ccc, languagecode, ccc_keywords,'+measurement_date)
    print ('languagecode, ccc, languagecode, zero_ill,'+measurement_date)

    print ('languagecode, ccc, ccc, qitem,'+measurement_date)
    print ('languagecode, ccc, ccc_geolocated, qitem,'+measurement_date)
    print ('languagecode, ccc, ccc_keywords, qitem,'+measurement_date)

    print ('* generate_ccc_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_langs_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_langs_ccc_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    # LANGUAGES AND LANGUAGES CCC
    language_ccc_in_all_wp_total_art = {}
    language_ccc_in_all_wp_total_percent = {}

    language_all_wp_articles = {}
    all_wp_articles = 0
    for languagecode in wikilanguagecodes:
        language_ccc_in_all_wp_total_art[languagecode] = 0
        language_ccc_in_all_wp_total_percent[languagecode] = 0
        all_wp_articles += wikipedialanguage_numberarticles[languagecode]

    for languagecode_1 in wikilanguagecodes:
        langTime = time.time()

        allwp_allnumberofarticles=0
        all_ccc_articles_count_total=0 # all ccc articles from all languages count
        all_ccc_articles_count=0 # language 1 ccc articles covered by other languages count
        all_ccc_rel_value_ccc_total =0
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode_1]
        language_all_wp_articles[languagecode_1]=all_wp_articles-wpnumberofarticles
    
        query = 'SELECT COUNT(*) FROM ccc_'+languagecode_1+'wiki WHERE ccc_binary=1;';
        cursor.execute(query)
        row = cursor.fetchone()
        ccc_count = row[0]

        language_ccc_count = {}
        for languagecode_2 in wikilanguagecodes:
            query = 'SELECT COUNT(ccc_binary), COUNT(keyword_title), COUNT(ccc_geolocated) FROM ccc_'+languagecode_2+'wiki WHERE ccc_binary=1;'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_articles_count_total = row[0]
            ccc_keywords_count_total = row[1]
            ccc_geolocated_count_total = row[2]


            language_ccc_count[languagecode_2]=ccc_articles_count_total
            all_ccc_articles_count_total+=ccc_articles_count_total
            allwp_allnumberofarticles+=wikipedialanguage_numberarticles[languagecode_2]

            if languagecode_1 == languagecode_2: continue



            query = 'SELECT COUNT(ccc_'+languagecode_2+'wiki.ccc_binary), COUNT(ccc_'+languagecode_2+'wiki.keyword_title), COUNT(ccc_'+languagecode_2+'wiki.ccc_geolocated) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1;'
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
                if wikipedialanguage_numberarticles[languagecode_1]!=0:
                    language_ccc_in_all_wp_total_percent[languagecode_2]+=100*ccc_articles_count/wikipedialanguage_numberarticles[languagecode_1]

            ## coverage
            insert_intersections_values(cursor2,'articles',languagecode_2,'ccc',languagecode_1,'wp',ccc_articles_count,ccc_articles_count_total,measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode_2,'ccc_keywords',languagecode_1,'wp',ccc_keywords_count,ccc_keywords_count_total,measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode_2,'ccc_geolocated',languagecode_1,'wp',ccc_geolocated_count,ccc_geolocated_count_total,measurement_date)

            ## spread
            insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc',ccc_articles_count,wpnumberofarticles,measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc_keywords',ccc_keywords_count,wpnumberofarticles,measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode_1,'wp',languagecode_2,'ccc_geolocated',ccc_geolocated_count,wpnumberofarticles,measurement_date)
        
        ### all ccc articles ###
        # what is the extent of all ccc articles in this language edition?
        insert_intersections_values(cursor2,'articles',languagecode_1,'wp','all_ccc_articles','',all_ccc_articles_count+ccc_count,wpnumberofarticles, measurement_date)

        # COVERAGE
        ### total langs ###
        # how well this language edition covered all CCC articles? t.coverage and coverage art.
        insert_intersections_values(cursor2,'articles','all_ccc_articles','',languagecode_1,'wp',all_ccc_articles_count,all_ccc_articles_count_total-ccc_count, measurement_date)

        ### relative langs ###
        # how well this language edition covered all CCC articles in average? relative coverage.
        all_ccc_rel_value_ccc_total_avg=all_ccc_rel_value_ccc_total/(len(wikilanguagecodes)-1)
        all_ccc_abs_value_avg=all_ccc_articles_count/(len(wikilanguagecodes)-1)
        insert_intersections_values(cursor2,'articles','all_ccc_avg','',languagecode_1,'wp',all_ccc_abs_value_avg,all_ccc_rel_value_ccc_total_avg, measurement_date)

        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - langTime)))


    for languagecode_1 in wikilanguagecodes:
        # SPREAD
        ### total langs ###
        # what is the extent of language 1 ccc articles in all the articles of the other languages? t.spread and spread art.
        insert_intersections_values(cursor2,'articles','all_wp_all_articles','',languagecode_1,'ccc',language_ccc_in_all_wp_total_art[languagecode_1],language_all_wp_articles[languagecode_1], measurement_date)

        ### relative langs ###
        # what is the average extent of this language ccc in all languages? relative spread.
        insert_intersections_values(cursor2,'articles','all_wp_avg','',languagecode_1,'ccc', 0,language_ccc_in_all_wp_total_percent[languagecode_1]/(len(wikilanguagecodes)-1), measurement_date)

        # what is the extent of this language ccc in all the languages ccc?
        insert_intersections_values(cursor2,'articles','all_ccc_articles','',languagecode_1,'ccc',language_ccc_count[languagecode_1],all_ccc_articles_count_total, measurement_date)


    # what is the extent of all ccc articles in all wp all articles
    insert_intersections_values(cursor2,'articles','all_wp_all_articles','','all_ccc_articles','',all_ccc_articles_count_total,allwp_allnumberofarticles, measurement_date)



    conn2.commit()

    print ('languagecode_2, ccc, languagecode_1, wp,'+ measurement_date)
    print ('languagecode_2, ccc_keywords, languagecode_1, wp,'+ measurement_date)
    print ('languagecode_2, ccc_geolocated, languagecode_1, wp,'+ measurement_date)

    print ('languagecode_1, wp, languagecode_2, ccc,'+ measurement_date)
    print ('languagecode_1, wp, languagecode_2, ccc_keywords,'+ measurement_date)
    print ('languagecode_1, wp, languagecode_2, ccc_geolocated,'+ measurement_date)

    print ('languagecode_1, wp, all_ccc_articles, ,'+ measurement_date) # all ccc articles

    # coverage
    print ('all_ccc_articles, ,languagecode_1, wp, '+measurement_date)
    print ('all_ccc_avg, ,languagecode_1, wp, '+measurement_date)

    # spread
    print ('all_wp_all_articles, ,languagecode_1, ccc, '+measurement_date)
    print ('all_wp_avg, ,languagecode_1, ccc, '+measurement_date)
    print ('all_ccc_articles, ,languagecode_1, ccc, '+measurement_date+'\n')

    # all languages ccc in all languages wp all articles
    print ('all_wp_all_articles, ,all_ccc_articles, ccc, '+measurement_date+'\n')


    print ('* generate_langs_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_ccc_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_ccc_ccc_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()

    # between languages ccc
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        query = 'SELECT COUNT(ccc_binary) FROM ccc_'+languagecode_1+'wiki WHERE ccc_binary=1;'
        cursor.execute(query)
        language_ccc_count = cursor.fetchone()[0]
        if language_ccc_count == 0: continue

        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            query = 'SELECT COUNT (*) FROM ccc_'+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND ccc_'+languagecode_1+'wiki.ccc_binary = 1;'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_coincident_articles_count = row[0]

            insert_intersections_values(cursor2,'articles',languagecode_1,'ccc',languagecode_2,'ccc',ccc_coincident_articles_count,language_ccc_count,measurement_date)

    conn2.commit()
    print ('languagecode_1, ccc, languagecode_2, ccc,'+ measurement_date)

    print ('* generate_ccc_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def generate_people_segments_intersections():
    functionstartTime = time.time()
    print ('* generate_people_segments_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    # PEOPLE SEGMENTS (PEOPLE, MALE, FEMALE)
    gender = {'Q6581097':'male','Q6581072':'female', 'Q1052281':'transgender female','Q1097630':'intersex','Q1399232':"fa'afafine",'Q17148251':'travesti','Q19798648':'unknown value','Q207959':'androgyny','Q215627':'person','Q2449503':'transgender male','Q27679684':'transfeminine','Q27679766':'transmasculine','Q301702':'two-Spirit','Q303479':'hermaphrodite','Q3177577':'muxe','Q3277905':'māhū','Q430117':'Transgene','Q43445':'female non-human organism'}
    gender_name_count_total = {}
    people_count_total = 0
    query = 'SELECT qitem2, COUNT(*) FROM people_properties WHERE qitem2!="Q5" GROUP BY qitem2;'
    cursor3.execute(query)
    for row in cursor3.execute(query):
        if row[0] in gender: gender_name_count_total[gender[row[0]]]=row[1]
        people_count_total += row[1]
    gender_name_count_total['people']=people_count_total

    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks;'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    insert_intersections_values(cursor2,'articles','wikidata_article_qitems',None,'wikidata_article_qitems','people',gender_name_count_total['people'],wikidata_article_qitems_count, measurement_date)

    insert_intersections_values(cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','male',gender_name_count_total['male'],gender_name_count_total['people'], measurement_date)

    insert_intersections_values(cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','female',gender_name_count_total['female'],gender_name_count_total['people'], measurement_date)

    conn2.commit()
    print ('wikidata_article_qitems, , wikidata_article_qitems, people, '+measurement_date)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, female, '+measurement_date)
    print ('wikidata_article_qitems, people, wikidata_article_qitems, male, '+measurement_date)
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    gender_name_count_total_zero_ill = {}
    people_count_total_zero_ill = 0
    query = 'SELECT qitem2, count(qitem2) FROM people_properties WHERE qitem in (SELECT qitem FROM sitelinks GROUP BY qitem HAVING COUNT(qitem)=1) AND qitem2!="Q5" GROUP BY qitem2 order by 2;'
    cursor3.execute(query)
    for row in cursor3.execute(query):
        if row[0] in gender: gender_name_count_total_zero_ill[gender[row[0]]]=row[1]
        people_count_total_zero_ill += row[1]
    gender_name_count_total_zero_ill['people']=people_count_total_zero_ill

    # zero ill: people
    insert_intersections_values(cursor2,'articles','wikidata_article_qitems','people','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['people'],gender_name_count_total['people'], measurement_date)

    print ('wikidata_article_qitems, people, wikidata_article_qitems, zero_ill, '+measurement_date)

    # zero ill: male
    insert_intersections_values(cursor2,'articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['male'],gender_name_count_total['people'], measurement_date)

    print ('wikidata_article_qitems, male, wikidata_article_qitems, zero_ill, '+measurement_date)

    # zero ill: female
    insert_intersections_values(cursor2,'articles','wikidata_article_qitems','female','wikidata_article_qitems','zero_ill',gender_name_count_total_zero_ill['female'],gender_name_count_total['female'], measurement_date)

    print ('wikidata_article_qitems, female, wikidata_article_qitems, zero_ill, '+measurement_date)
    print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))


    # languages
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        query = 'SELECT gender, COUNT(*) FROM ccc_'+languagecode+'wiki GROUP BY gender;'
#        query = 'SELECT qitem2, COUNT(*) FROM people_properties INNER JOIN sitelinks ON people_properties.qitem = sitelinks.qitem WHERE langcode="'+languagecode+'wiki" AND qitem2!="Q5" GROUP BY qitem2'
        gender_name_count = {}
        people_count = 0
        for row in cursor.execute(query):
            if row[0] in gender: gender_name_count[gender[row[0]]]=row[1]
            people_count += row[1]
        gender_name_count['people']=people_count

        for gender_name, gender_count in gender_name_count.items():
            insert_intersections_values(cursor2,'articles',languagecode,'wp','wikidata_article_qitems',gender_name, gender_count,wpnumberofarticles,measurement_date)

            insert_intersections_values(cursor2,'articles','wikidata_article_qitems', gender_name, languagecode, 'wp', gender_count,gender_name_count_total[gender_name],measurement_date)

    conn2.commit()
    print ('languagecode, wp, wikidata_article_qitems, male,'+measurement_date)
    print ('languagecode, wp, wikidata_article_qitems, female,'+measurement_date)
    print ('languagecode, wp, wikidata_article_qitems, people,'+measurement_date)

    print ('wikidata_article_qitems, male, languagecode, wp, '+measurement_date)
    print ('wikidata_article_qitems, female, languagecode, wp, '+measurement_date)
    print ('wikidata_article_qitems, people, languagecode, wp, '+measurement_date)

    print ('* generate_all_articles_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_people_ccc_intersections():
    functionstartTime = time.time()
    print ('* generate_people_ccc_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()

    # PEOPLE SEGMENTS AND CCC
    language_ccc_count = {}
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            qitems.append(row[0])
        language_ccc_count[languagecode]=len(qitems)

        # male
        male=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097";'
#        query = 'SELECT DISTINCT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor.execute(query):
            male.append(row[0])
        malecount=len(male)
        male_ccc = set(male).intersection(set(qitems))
        male_ccc_count=len(male_ccc)
#        print (malecount,male_ccc_count)

        insert_intersections_values(cursor2,'articles',languagecode, 'male', languagecode, 'ccc', male_ccc_count, malecount,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc', languagecode, 'male', male_ccc_count, language_ccc_count[languagecode],measurement_date)

        # female
        female=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072";'
#        query = 'SELECT DISTINCT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female_ccc = set(female).intersection(set(qitems))
        female_ccc_count=len(female_ccc)

        insert_intersections_values(cursor2,'articles',languagecode, 'female', languagecode, 'ccc', female_ccc_count, femalecount,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc', languagecode, 'female', female_ccc_count,language_ccc_count[languagecode],measurement_date)

        # people
        people_count=femalecount+malecount
        ccc_peoplecount=male_ccc_count+female_ccc_count
        insert_intersections_values(cursor2,'articles',languagecode, 'people', languagecode, 'ccc', ccc_peoplecount, people_count,measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc', languagecode, 'people', ccc_peoplecount, language_ccc_count[languagecode],measurement_date)

        # in relation to the entire wp
        insert_intersections_values(cursor2,'articles',languagecode, 'wp', languagecode, 'ccc_people', ccc_peoplecount,wpnumberofarticles,measurement_date)

    conn2.commit()
    print ('languagecode, male, languagecode, ccc,'+measurement_date)
    print ('languagecode, ccc, languagecode, male,'+measurement_date)

    print ('languagecode, female, languagecode, ccc,'+measurement_date)
    print ('languagecode, ccc, languagecode, female,'+measurement_date)

    print ('languagecode, people, languagecode, ccc,'+measurement_date)
    print ('languagecode, ccc, languagecode, people,'+measurement_date)

    print ('languagecode, wp, languagecode, ccc_people,'+measurement_date)

    print ('* generate_people_ccc_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_geolocated_segments_intersections():
    functionstartTime = time.time()
    print ('* generate_geolocated_segments_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks;'
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

    insert_intersections_values(cursor2,'articles','wikidata_article_qitems',None,'wikidata_article_qitems','geolocated',geolocated_items_count_total,wikidata_article_qitems_count, measurement_date)

    print ('wikidata_article_qitems, , wikidata_article_qitems, geolocated, '+measurement_date)

    query = 'SELECT iso3166, COUNT(DISTINCT qitem) FROM geolocated_property WHERE qitem IN (SELECT qitem FROM sitelinks GROUP BY qitem HAVING (COUNT(qitem) = 1)) GROUP BY iso3166'
    iso3166_qitems_zero_ill = {}
    geolocated_items_zero_ill_count_total = 0
    for row in cursor3.execute(query):
        iso3166_qitems_zero_ill[row[0]]=row[1]
        geolocated_items_zero_ill_count_total+=row[1]

    insert_intersections_values(cursor2,'articles','wikidata_article_qitems',None,'geolocated','ill_zero',geolocated_items_zero_ill_count_total,wikidata_article_qitems_count, measurement_date)

    print ('wikidata_article_qitems, , wikidata_article_qitems, geolocated, '+measurement_date)


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
        insert_intersections_values(cursor2,'articles','wikidata_article_qitems','geolocated','countries',iso3166_code,iso3166_count,geolocated_items_count_total, measurement_date)

        # countries ILL zero
        insert_intersections_values(cursor2,'articles','countries',iso3166_code,'geolocated','ill_zero',iso3166_qitems_zero_ill[iso3166_code],iso3166_count, measurement_date)


    # subregions
    for subregion_name, subregion_count in subregions_count_total.items():
        insert_intersections_values(cursor2,'articles','wikidata_article_qitems','geolocated','subregions',subregion_name,subregion_count,geolocated_items_count_total, measurement_date)

        # subregions ILL zero
        insert_intersections_values(cursor2,'articles','subregions',subregion_name,'geolocated','ill_zero',subregions_count_total_zero_ill[subregion_name],subregion_count, measurement_date)

    # regions
    for region_name, region_count in regions_count_total.items():
        insert_intersections_values(cursor2,'articles','wikidata_article_qitems','geolocated','regions',region_name,region_count,geolocated_items_count_total, measurement_date)

        # regions ILL zero
        insert_intersections_values(cursor2,'articles','regions',region_name,'geolocated','ill_zero',regions_count_total_zero_ill[region_name],region_count, measurement_date)

    conn2.commit()
    print ('wikidata_article_qitems, geolocated, countries, iso3166,'+measurement_date)
    print ('wikidata_article_qitems, geolocated, subregions, subregion_name,'+measurement_date)
    print ('wikidata_article_qitems, geolocated, regions, region_name,'+measurement_date)

    print ('countries, iso3166, geolocated, ill_zero,'+measurement_date)
    print ('subregions, subregion_name, geolocated, ill_zero,'+measurement_date)
    print ('regions, region_name, geolocated, ill_zero,'+measurement_date)

    regions_all_langs_count={}
    subregions_all_langs_count={}
    iso3166_all_langs_count={}
    all_wp_all_geolocated_articles_count = 0

    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

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

        insert_intersections_values(cursor2,'articles','wikidata_article_qitems','geolocated',languagecode,'geolocated',geolocated_articles_count,geolocated_items_count_total, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode,'wp','wikidata_article_qitems','geolocated',geolocated_articles_count,wpnumberofarticles, measurement_date)

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
            insert_intersections_values(cursor2,'articles',languagecode,'geolocated','countries',iso3166_code,iso3166_count,100*iso3166_count/geolocated_articles_count, measurement_date)

            # countries
            insert_intersections_values(cursor2,'articles','countries',iso3166_code,languagecode,'geolocated',iso3166_count,100*iso3166_count/iso3166_qitems[iso3166_code], measurement_date)

        # subregions
        for subregion_name, subregion_count in subregions_count.items():

            insert_intersections_values(cursor2,'articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,geolocated_articles_count, measurement_date)

            insert_intersections_values(cursor2,'articles','subregions', subregion_name, languagecode, 'geolocated', subregion_count,subregions_count_total[subregion_name], measurement_date)

        # regions
        for region_name, region_count in regions_count.items():
            insert_intersections_values(cursor2,'articles',languagecode,'geolocated','regions',region_name,region_count,geolocated_articles_count, measurement_date)

            insert_intersections_values(cursor2,'articles','regions',region_name,languagecode,'geolocated',region_count,regions_count_total[region_name], measurement_date)

    conn2.commit()
    print ('wikidata_article_qitems, geolocated, languagecode, geolocated, '+measurement_date)
    print ('languagecode, wp, wikidata_article_qitems, geolocated, '+measurement_date)

    print ('languagecode, geolocated, countries, iso3166, '+measurement_date)
    print ('languagecode, geolocated, subregions, iso3166, '+measurement_date)
    print ('languagecode, geolocated, regions, iso3166, '+measurement_date)

    print ('countries, iso3166, languagecode, geolocated, '+measurement_date)
    print ('subregions, subregion_name, languagecode, geolocated, '+measurement_date)
    print ('regions, region_name, languagecode, geolocated, '+measurement_date)


        # countries
    for iso3166_code, iso3166_count in iso3166_all_langs_count.items():
        insert_intersections_values(cursor2,'articles','all_wp_all_articles','geolocated','countries',iso3166_code,iso3166_count,all_wp_all_geolocated_articles_count, measurement_date)

        # subregions
    for subregion_name, subregion_count in subregions_all_langs_count.items():
        insert_intersections_values(cursor2,'articles','all_wp_all_articles','geolocated','subregions',subregion_name,subregion_count,all_wp_all_geolocated_articles_count, measurement_date)

        # regions
    for region_name, region_count in regions_all_langs_count.items():
        insert_intersections_values(cursor2,'articles','all_wp_all_articles','geolocated','regions',region_name,region_count,all_wp_all_geolocated_articles_count, measurement_date)

    conn2.commit()
    print ('all_wp_all_articles, geolocated, geolocated, countries, '+ measurement_date)
    print ('all_wp_all_articles, geolocated, geolocated, subregions, '+ measurement_date)
    print ('all_wp_all_articles, geolocated, geolocated, regions, '+ measurement_date)

    print ('* generate_geolocated_segments_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_last_month_articles_intersections():
    functionstartTime = time.time()
    print ('* generate_last_month_articles_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor4 = conn4.cursor()

    # calculate last month
#    last_month_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
    last_month_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=61)

    first_day = last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S')
    last_day = last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S')
    month_condition = 'date_created >= "'+ first_day +'" AND date_created < "'+last_day+'"'
    print (month_condition)

    # ccc top article lists
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
    all_qitems = set()
    lists_dict = {}
    for list_name in lists:
        lists_qitems = set()
        for languagecode in wikilanguagecodes:
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists);'
            for row in cursor4.execute(query):
                lists_qitems.add(row[0])
                all_qitems.add(row[0])
        lists_dict[list_name] = lists_qitems

#    wikilanguagecodes2=['ca']
    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]

        # last month articles
        qitems = []
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE '+month_condition
        print (query)
        for row in cursor.execute(query):
            qitems.append(row[0])
        created_articles_count = len(qitems)
        print (created_articles_count)

        # ALL ARTICLES
        insert_intersections_values(cursor2,'articles',languagecode, 'wp', languagecode, 'last_month_articles', created_articles_count, wpnumberofarticles, measurement_date)


        # CCC
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+ month_condition + ' AND ccc_binary=1;'
        cursor.execute(query)
        ccc_articles_created_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;')
        ccc_articles_count = cursor.fetchone()[0]

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc', languagecode, 'last_month_articles', ccc_articles_created_count, ccc_articles_count, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles',languagecode, 'ccc', ccc_articles_created_count, created_articles_count, measurement_date)

        # CCC geolocated
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+month_condition+' AND ccc_binary=1 AND ccc_geolocated=1'
        cursor.execute(query)
        ccc_geolocated_articles_created_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND ccc_geolocated=1;')
        ccc_geolocated_articles_count = cursor.fetchone()[0]

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc_geolocated', languagecode, 'last_month_articles', ccc_geolocated_articles_created_count, ccc_geolocated_articles_count, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles',languagecode, 'ccc_geolocated', ccc_geolocated_articles_created_count, created_articles_count, measurement_date)

        # CCC keywords
        query = 'SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE '+month_condition+' AND ccc_binary=1 AND keyword_title IS NOT NULL;'
        cursor.execute(query)
        ccc_keywords_articles_created_count = cursor.fetchone()[0]

        cursor.execute('SELECT count(*) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL;')
        ccc_keywords_articles_count = cursor.fetchone()[0]

        insert_intersections_values(cursor2,'articles',languagecode, 'ccc_keywords', languagecode, 'last_month_articles', ccc_keywords_articles_created_count, ccc_keywords_articles_count, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles',languagecode, 'ccc_keywords', ccc_keywords_articles_created_count, created_articles_count, measurement_date)


        # Not own CCC
        not_own_ccc = wpnumberofarticles - ccc_articles_count
        not_own_ccc_created_count = created_articles_count - ccc_articles_created_count

        insert_intersections_values(cursor2,'articles',languagecode, 'not_own_ccc', languagecode, 'last_month_articles', not_own_ccc_created_count, not_own_ccc, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles',languagecode, 'not_own_ccc', not_own_ccc_created_count, created_articles_count, measurement_date)
        

        # Other Langs CCC
        for languagecode_2 in wikilanguagecodes:
            if languagecode == languagecode_2: continue
            query = 'SELECT COUNT (*) FROM ccc_'+languagecode+'wiki INNER JOIN ccc_'+languagecode_2+'wiki ON ccc_'+languagecode+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1 AND ccc_'+languagecode+'wiki.date_created >= "'+first_day+'" AND ccc_'+languagecode+'wiki.date_created < "'+last_day+'";'

            cursor.execute(query)
            ccc_articles_created_count = cursor.fetchone()[0]
            print (languagecode_2,ccc_articles_created_count,created_articles_count,languagecode)

            insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles',languagecode_2, 'ccc', ccc_articles_created_count, created_articles_count, measurement_date)

        # CCC TOP ARTICLES LISTS
        for list_name in lists:
#           lists_qitems_count=len(lists_qitems)
            coincident_qitems = set(lists_dict[list_name]).intersection(set(qitems))
            last_month_list_count=len(coincident_qitems)

            insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','top_ccc_articles_lists',list_name,last_month_list_count,created_articles_count, measurement_date)

        coincident_qitems_all_qitems = len(all_qitems.intersection(set(qitems)))
        insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','ccc','all_top_ccc_articles',coincident_qitems_all_qitems,created_articles_count, measurement_date)


        # PEOPLE
        # male
        male=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097";'
#        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581097" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor.execute(query):
            male.append(row[0])
        malecount=len(male)
        male = set(male).intersection(set(qitems))
        last_month_articles_male_count=len(male)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles', languagecode, 'male', last_month_articles_male_count,created_articles_count,measurement_date)


        # female
        female=[]
        query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072";'
#        query = 'SELECT people_properties.qitem FROM people_properties INNER JOIN sitelinks ON people_properties.qitem=sitelinks.qitem WHERE people_properties.qitem2 = "Q6581072" AND sitelinks.langcode="'+languagecode+'wiki";'
        for row in cursor.execute(query): 
            female.append(row[0])
        femalecount=len(female)
        female = set(female).intersection(set(qitems))
        last_month_female_count=len(female)

        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles', languagecode, 'female', last_month_female_count,created_articles_count,measurement_date)


        # people
        last_month_peoplecount=last_month_articles_male_count+last_month_female_count
        insert_intersections_values(cursor2,'articles',languagecode, 'last_month_articles', languagecode, 'people', last_month_peoplecount,created_articles_count,measurement_date)


        # GEOLOCATED SEGMENTS
        country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions() # iso 3166 to X

        geolocated_articles_count = 0
        iso3166_articles = {}
        query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM ccc_'+languagecode+'wiki WHERE iso3166 IS NOT NULL AND '+month_condition + ' GROUP BY iso3166'
#        print (query)
        cursor.execute(query)
        for row in cursor.execute(query):
            iso3166_articles[row[0]]=row[1]
            geolocated_articles_count+=row[1]

        insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','wikidata_article_qitems','geolocated',geolocated_articles_count,created_articles_count, measurement_date)


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
            insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','countries',iso3166_code,iso3166_count,created_articles_count, measurement_date)

#        print (subregions_count)
#        print (regions_count)

        # subregions
        for subregion_name, subregion_count in subregions_count.items():
            insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','subregions',subregion_name,subregion_count,created_articles_count, measurement_date)

        # regions
        for region_name, region_count in regions_count.items():
            insert_intersections_values(cursor2,'articles',languagecode,'last_month_articles','regions',region_name,region_count,created_articles_count, measurement_date)

    conn2.commit()
    print ('languagecode, wp, languagecode, last_month_articles,'+measurement_date)
    print ('languagecode, ccc, languagecode, last_month_articles,'+measurement_date)

    print ('languagecode, last_month_articles, languagecode, ccc,'+measurement_date)
    print ('languagecode, ccc_geolocated, languagecode, last_month_articles,'+measurement_date)
    
    print ('languagecode, last_month_articles, languagecode, ccc_geolocated,'+measurement_date)
    print ('languagecode, ccc_keywords, languagecode, last_month_articles,'+measurement_date)
    
    print ('languagecode, last_month_articles,languagecode, ccc_keywords,'+measurement_date)
    print ('languagecode, not_own_ccc, languagecode, last_month_articles,'+measurement_date)
    
    print ('languagecode, last_month_articles, languagecode, not_own_ccc,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode_2, ccc,'+measurement_date)
    
    print ('languagecode, last_month_articles, top_ccc_articles_lists, list_name,'+measurement_date)
    print ('languagecode, last_month_articles, ccc, all_top_ccc_articles,'+measurement_date)


    print ('languagecode, last_month_articles, languagecode, male,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, female,'+measurement_date)
    print ('languagecode, last_month_articles, languagecode, people,'+measurement_date)
    
    print ('languagecode, last_month_articles, wikidata_article_qitems, geolocated,'+measurement_date)  

    print ('languagecode, last_month_articles, countries, iso3166,'+measurement_date)
    print ('languagecode, last_month_articles, subregions, subregion_name,'+measurement_date)
    print ('languagecode, last_month_articles, regions, region_name,'+measurement_date)
    
    print ('* generate_last_month_articles_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_pageviews_intersections():
    functionstartTime = time.time()
    print ('* generate_pageviews_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor4 = conn4.cursor()

#    wikilanguagecodes2 = ['ca']
    # CCC TOP ARTICLES PAGEVIEWS
    all_ccc_lists_items=set()
    wikipedialanguage_ccclistsitems={}
    for languagecode in wikilanguagecodes:
        lists_qitems = []
        query = 'SELECT DISTINCT qitem FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists) AND position <= 100;'
        for row in cursor4.execute(query):
            all_ccc_lists_items.add(row[0])
            lists_qitems.append(row[0])
            wikipedialanguage_ccclistsitems[languagecode]=lists_qitems
        if languagecode not in wikipedialanguage_ccclistsitems: wikipedialanguage_ccclistsitems[languagecode]=lists_qitems

    wikipedialanguage_numberpageviews={}
    wikipedialanguageccc_numberpageviews={}
    # LANGUAGE PAGEVIEWS
    for languagecode in wikilanguagecodes:
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki;'
        cursor.execute(query)
        pageviews = cursor.fetchone()[0]
        if pageviews == None or pageviews == '': pageviews = 0
        wikipedialanguage_numberpageviews[languagecode]=pageviews

        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;';
        cursor.execute(query)
        pageviews = cursor.fetchone()[0]
        if pageviews == None or pageviews == '': pageviews = 0
        wikipedialanguageccc_numberpageviews[languagecode]=pageviews

        insert_intersections_values(cursor2,'pageviews',languagecode,'wp',languagecode,'ccc',wikipedialanguageccc_numberpageviews[languagecode],wikipedialanguage_numberpageviews[languagecode],measurement_date)

        page_asstring = ','.join( ['?'] * len(wikipedialanguage_ccclistsitems[languagecode]))
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s);' % page_asstring
        cursor.execute(query,(wikipedialanguage_ccclistsitems[languagecode]))
        ccc_lists_pageviews = cursor.fetchone()[0]

        insert_intersections_values(cursor2,'pageviews',languagecode,'wp',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],measurement_date)

        insert_intersections_values(cursor2,'pageviews',languagecode,'ccc',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguageccc_numberpageviews[languagecode],measurement_date)

        page_asstring = ','.join( ['?'] * len(all_ccc_lists_items))
        query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode+'wiki WHERE qitem IN (%s);' % page_asstring
        cursor.execute(query,(list(all_ccc_lists_items)))
        all_ccc_lists_pageviews = cursor.fetchone()[0]

        insert_intersections_values(cursor2,'pageviews',languagecode,'wp','ccc','all_top_ccc_articles',all_ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],measurement_date)

#    print (wikipedialanguage_numberpageviews)
#    print (wikipedialanguageccc_numberpageviews)
    conn2.commit()
    print ('languagecode, wp, languagecode, ccc,'+measurement_date)
    print ('languagecode, wp, languagecode, all_top_ccc_articles,'+measurement_date)
    print ('languagecode, ccc, languagecode, all_top_ccc_articles,'+measurement_date)
    print ('languagecode, wp, ccc, all_top_ccc_articles,'+measurement_date)

    # LANGUAGES AND LANGUAGES CCC PAGEVIEWS
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            query = 'SELECT SUM(ccc_'+languagecode_1+'wiki.num_pageviews) FROM ccc_'+languagecode_1+'wiki INNER JOIN ccc_'+languagecode_2+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem WHERE ccc_'+languagecode_2+'wiki.ccc_binary = 1;'
#            print (query)
            cursor.execute(query)
            row = cursor.fetchone()
            languagecode_2_ccc_pageviews = row[0]
            if languagecode_2_ccc_pageviews == None or languagecode_2_ccc_pageviews == '': languagecode_2_ccc_pageviews = 0

            insert_intersections_values(cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'ccc',languagecode_2_ccc_pageviews,wikipedialanguage_numberpageviews[languagecode_1],measurement_date)


            page_asstring = ','.join( ['?'] * len(wikipedialanguage_ccclistsitems[languagecode_2]))
            query = 'SELECT SUM(num_pageviews) FROM ccc_'+languagecode_1+'wiki WHERE qitem IN (%s)' % page_asstring
            cursor.execute(query,(wikipedialanguage_ccclistsitems[languagecode_2]))
            row = cursor.fetchone()
            languagecode_2_top_ccc_articles_lists_pageviews = row[0]

            insert_intersections_values(cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'all_top_ccc_articles',languagecode_2_top_ccc_articles_lists_pageviews,wikipedialanguage_numberpageviews[languagecode_1],measurement_date)

    conn2.commit()
    print ('languagecode, wp, languagecode_2, ccc,'+measurement_date)
    print ('languagecode, wp, languagecode_2, all_top_ccc_articles,'+measurement_date)

    print ('* generate_pageviews_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_top_ccc_articles_lists_intersections():
    functionstartTime = time.time()
    print ('* generate_top_ccc_articles_lists_intersections')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor4 = conn4.cursor()

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1)
        qitems = set()
        query = 'SELECT qitem FROM ccc_'+languagecode_1+'wiki;'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')


    # PERHAPS: THIS SHOULD BE LIMITED TO 100 ARTICLES PER LIST.
    # CCC TOP ARTICLES LISTS
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']

    for languagecode in wikilanguagecodes:
        print (languagecode)
        wpnumberofarticles=wikipedialanguage_numberarticles[languagecode]
        all_top_ccc_articles_count = 0
        all_top_ccc_articles_coincident_count = 0

        all_ccc_lists_items=set()
        for list_name in lists:
            lists_qitems = set()

            for languagecode_2 in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode_2+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode_2+'wiki_top_articles_lists);'
                for row in cursor4.execute(query):
                    all_ccc_lists_items.add(row[0])
                    lists_qitems.add(row[0])
        #           lists_qitems_count=len(lists_qitems)

            all_top_ccc_articles_count+=len(lists_qitems)
            ccc_list_coincident_count=len(lists_qitems.intersection(all_articles[languagecode]))

            insert_intersections_values(cursor2,'articles','top_ccc_articles_lists',list_name,'wp',languagecode,ccc_list_coincident_count,len(lists_qitems), measurement_date)

            insert_intersections_values(cursor2,'articles',languagecode,'wp','top_ccc_articles_lists',list_name,ccc_list_coincident_count,wpnumberofarticles, measurement_date)

        # all CCC Top articles lists
        all_top_ccc_articles_coincident_count = len(all_ccc_lists_items.intersection(all_articles[languagecode]))
        insert_intersections_values(cursor2,'articles','ccc','all_top_ccc_articles',languagecode,'wp',all_top_ccc_articles_coincident_count,all_top_ccc_articles_count, measurement_date)

        insert_intersections_values(cursor2,'articles',languagecode,'wp','ccc','all_top_ccc_articles',all_top_ccc_articles_coincident_count,wpnumberofarticles, measurement_date)

    conn2.commit()
    print ('top_ccc_articles_lists, list_name, wp, languagecode,'+ measurement_date)
    print ('wp, languagecode, top_ccc_articles_lists, list_name,'+ measurement_date)

    print ('ccc, all_top_ccc_articles, languagecode, wp,'+ measurement_date)
    print ('languagecode, wp, ccc, all_top_ccc_articles,'+ measurement_date)

    print ('* generate_top_ccc_articles_lists_intersections Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def generate_all_increments():
    functionstartTime = time.time()
    print ('* generate_all_increments_function')

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + 'wikidata.db'); cursor3 = conn3.cursor()

    print ('start!')

    print ('* lang_intersections_increments')
    for languagecode_1 in wikilanguagecodes:
        calculate_insert_increments_values('articles',languagecode_1,'wp',languagecode_1,'zero_ill')
        calculate_insert_increments_values('articles',languagecode_1,'wp',languagecode_1,'null_qitems')
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'wp')

    print ('* ccc_segments_intersections_increments')
    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('articles','wikidata_article_qitems',None,languagecode,'ccc')
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'ccc')
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'not_own_ccc')
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'ccc_geolocated')
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'ccc_keywords')

        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'ccc_keywords')
        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'ccc_geolocated')
        calculate_insert_increments_values('articles',languagecode, 'ccc',languagecode,'zero_ill',measurement_date)

        query = 'SELECT DISTINCT main_territory FROM ccc_'+languagecode+'wiki WHERE ccc_binary=1;';
        for row in cursor.execute(query):
            main_territory=row[0]
            calculate_insert_increments_values('articles',languagecode,'ccc','ccc',main_territory)
            calculate_insert_increments_values('articles',languagecode,'ccc','ccc_geolocated',main_territory)
            calculate_insert_increments_values('articles',languagecode,'ccc','ccc_keywords',main_territory)

    print ('* langs_ccc_intersections_increments')
    for languagecode_1 in wikilanguagecodes:
        for languagecode_2 in wikilanguagecodes:
            calculate_insert_increments_values('articles',languagecode_2, 'ccc', languagecode_1, 'wp')
            calculate_insert_increments_values('articles',languagecode_2, 'ccc_keywords', languagecode_1, 'wp')
            calculate_insert_increments_values('articles',languagecode_2, 'ccc_geolocated', languagecode_1, 'wp')
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'ccc')
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'ccc_keywords')
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'ccc_geolocated')
        calculate_insert_increments_values('articles','all_wp_all_articles','',languagecode_1,'all_ccc_articles')
        calculate_insert_increments_values('articles','all_ccc_articles','',languagecode_1,'ccc')
        calculate_insert_increments_values('articles','all_ccc_articles','',languagecode_1,'wp')
        calculate_insert_increments_values('articles','all_ccc_articles','',languagecode_1,'wp')
        calculate_insert_increments_values('articles','all_ccc_avg', '',languagecode_1, 'wp')
        calculate_insert_increments_values('articles',languagecode_1, 'ccc', 'all_wp_avg',None)


    print ('* ccc_ccc_intersections_increments')
    for languagecode_1 in wikilanguagecodes:
        for languagecode_2 in wikilanguagecodes:
            calculate_insert_increments_values('articles',languagecode_1,'ccc',languagecode_2,'ccc')

    print ('* people_segments_intersections_increments')
    calculate_insert_increments_values('articles','wikidata_article_qitems',None,'wikidata_article_qitems','people')
    calculate_insert_increments_values('articles','wikidata_article_qitems','people','wikidata_article_qitems','male')
    calculate_insert_increments_values('articles','wikidata_article_qitems','people','wikidata_article_qitems','female')
    calculate_insert_increments_values('articles','wikidata_article_qitems','people','wikidata_article_qitems','zero_ill')
    calculate_insert_increments_values('articles','wikidata_article_qitems','male','wikidata_article_qitems','zero_ill')
    calculate_insert_increments_values('articles','wikidata_article_qitems','female','wikidata_article_qitems','zero_ill')
    for languagecode in wikilanguagecodes:
        query = 'SELECT DISTINCT qitem2 FROM people_properties INNER JOIN sitelinks ON people_properties.qitem = sitelinks.qitem WHERE langcode="'+languagecode+'wiki" AND qitem2!="Q5" GROUP BY qitem2'
        gender_name_count = []
        for row in cursor3.execute(query):
            calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, row[0])
            calculate_insert_increments_values('articles','wikidata_article_qitems', row[0], languagecode, 'wp')

    print ('* people_ccc_intersections_increments')
    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('articles',languagecode, 'male', languagecode, 'ccc')
        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'male')
        calculate_insert_increments_values('articles',languagecode, 'female', languagecode, 'ccc')
        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'female')
        calculate_insert_increments_values('articles',languagecode, 'people', languagecode, 'ccc')
        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'people')
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'ccc_people')

    print ('* geolocated_segments_intersections_increments')
    calculate_insert_increments_values('articles','wikidata_article_qitems',None,'wikidata_article_qitems','geolocated')
    calculate_insert_increments_values('articles','wikidata_article_qitems',None,'wikidata_article_qitems','geolocated')

    country_names, regions, subregions = load_iso_3166_to_geographical_regions() # iso 3166 to X

    query = 'SELECT distinct iso3166 FROM geolocated_property GROUP BY iso3166'
    iso3166_qitems = []
    for row in cursor3.execute(query):
        iso3166_qitems.append(row[0])

    regions_names=set()
    subregions_names=set()
    for iso3166_code in iso3166_qitems:
        if iso3166_code == None: continue

        regions_names.add(regions[iso3166_code])
        subregions_names.add(subregions[iso3166_code])
        calculate_insert_increments_values('articles','wikidata_article_qitems','geolocated','countries',iso3166_code)

    for subregion_name in subregions_names:
        calculate_insert_increments_values('articles','wikidata_article_qitems','geolocated','subregions',subregion_name)
        calculate_insert_increments_values('articles','subregions',subregion_name,'geolocated','ill_zero')

    for region_name in region_names:
        calculate_insert_increments_values('articles','wikidata_article_qitems','geolocated','regions',region_name)
        calculate_insert_increments_values('articles','regions',region_name,'geolocated','ill_zero')

    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('articles','wikidata_article_qitems','geolocated',languagecode,'geolocated')
        calculate_insert_increments_values('articles',languagecode,'wp','wikidata_article_qitems','geolocated')

        for iso3166_code in iso3166_qitems:
            calculate_insert_increments_values('articles',languagecode,'geolocated','countries',iso3166_code)
            calculate_insert_increments_values('articles','countries',iso3166_code,languagecode,'geolocated')

        for subregion_name in subregions_names:
            calculate_insert_increments_values('articles', languagecode,'geolocated','subregions',subregion_name)
            calculate_insert_increments_values('articles','subregions', subregion_name, languagecode, 'geolocated')

        for region_name in region_names:
            calculate_insert_increments_values('articles',languagecode,'geolocated','regions',region_name)
            calculate_insert_increments_values('articles','regions',region_name,languagecode,'geolocated')

    for iso3166_code in iso3166_qitems:
        if iso3166_code == None: continue
        calculate_insert_increments_values('articles','all_wp_all_articles','geolocated','countries',iso3166_code)

    for subregion_name in subregions_names:
        calculate_insert_increments_values('articles', 'all_wp_all_articles','geolocated','subregions',subregion_name)

    for region_name in region_names:
        calculate_insert_increments_values('articles','all_wp_all_articles','geolocated','regions',region_name)


    print ('* last_month_articles_intersections_increments')
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years','created_last_year', 'pageviews', 'discussions']

    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('articles',languagecode, 'wp', languagecode, 'last_month_articles')
        calculate_insert_increments_values('articles',languagecode, 'ccc', languagecode, 'last_month_articles')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles',languagecode, 'ccc')
        calculate_insert_increments_values('articles',languagecode, 'ccc_geolocated', languagecode, 'last_month_articles')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles',languagecode, 'ccc_geolocated')
        calculate_insert_increments_values('articles',languagecode, 'ccc_keywords', languagecode, 'last_month_articles')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles',languagecode, 'ccc_keywords')
        calculate_insert_increments_values('articles',languagecode, 'not_own_ccc', languagecode, 'last_month_articles')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles',languagecode, 'not_own_ccc')


        for languagecode_2 in wikilanguagecodes:
            if languagecode == languagecode_2: continue
            calculate_insert_increments_values('articles',languagecode, 'last_month_articles',languagecode_2, 'ccc')

        for list_name in lists:
            calculate_insert_increments_values('articles',languagecode,'last_month_articles','top_ccc_articles_lists',list_name)

        calculate_insert_increments_values('articles',languagecode,'last_month_articles','ccc','all_top_ccc_articles')

        calculate_insert_increments_values('articles',languagecode, 'last_month_articles', languagecode, 'male')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles', languagecode, 'female')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles', languagecode, 'people')
        calculate_insert_increments_values('articles',languagecode, 'last_month_articles', 'wikidata_article_qitems','geolocated')

        for iso3166_code in iso3166_qitems:
            if iso3166_code == None: continue
            calculate_insert_increments_values('articles',languagecode,'last_month_articles','countries',iso3166_code)

        for subregion_name in subregions_names:
            calculate_insert_increments_values('articles',languagecode,'last_month_articles','subregions',subregion_name)

        for region_name in region_names:
            calculate_insert_increments_values('articles',languagecode,'last_month_articles','regions',region_name)


    print ('* pageviews_intersections_increments')
    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('pageviews', languagecode, 'wp', languagecode, 'ccc')
        calculate_insert_increments_values('pageviews',languagecode, 'wp', languagecode, 'all_top_ccc_articles')
        calculate_insert_increments_values('pageviews',languagecode, 'ccc', languagecode, 'all_top_ccc_articles')
        calculate_insert_increments_values('pageviews',languagecode, 'wp', languagecode, 'all_top_ccc_articles')

    for languagecode_1 in wikilanguagecodes:
        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'ccc')
            calculate_insert_increments_values('articles',languagecode_1, 'wp', languagecode_2, 'all_top_ccc_articles')

    print ('* top_ccc_articles_lists_intersections_increments')
    for languagecode in wikilanguagecodes:
        calculate_insert_increments_values('articles','top_ccc_articles_lists',list_name,'wp',languagecode)
        calculate_insert_increments_values('articles',languagecode,'wp','top_ccc_articles_lists',list_name)

        calculate_insert_increments_values('articles','ccc','all_top_ccc_articles',languagecode,'wp')
        calculate_insert_increments_values('articles',languagecode,'wp','ccc','all_top_ccc_articles')

    # this one is from the same generate_all_vital_article_lists
    for languagecode in wikilanguagecodes:
        countries = wikilanguages_utils.load_countries_from_language(languagecode)
        countries.append('')
        for country in countries:
            for list_name in lists:
                calculate_insert_increments_values('articles',country,list_name,langcode,'wp')



def calculate_insert_increments_values(content, set1, set1descriptor, set2, set2descriptor, measurement_date):

    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()
    query = 'SELECT abs_value, rel_value, measurement_date, cur_intersection_id FROM wcdo_intersections WHERE set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ? AND content = ? ORDER BY measurement_date DESC'

    measurement_dates = []
    abs_values = []
    rel_values = []

    parameters = (set1, set1descriptor, set2, set2descriptor, content)

    if set1 == None: 
        query=query.replace('set1 = ?','set1 IS NULL')
        parameters.remove(set1)
    if set1descriptor == None: 
        query=query.replace('set1descriptor = ?','set1descriptor IS NULL')
        parameters.remove(set1descriptor)
    if set2 == None: 
        query=query.replace('set2 = ?','set2 IS NULL')
        parameters.remove(set2)
    if set2descriptor == None: 
        query=query.replace('set2descriptor = ?','set2descriptor IS NULL')
        parameters.remove(set2descriptor)


    # GETTING THE VALUES
    i = 0
    for row in cursor.execute(query,(parameters)):
        if i == 0: 
            measurement_date = row[3]
            cur_intersection_id = row[4]
        i+=1
        measurement_dates.append(str(row[2]))
        abs_values.append(row[0])
        rel_values.append(row[1])

    current_date = datetime.datetime.strptime(measurement_dates[0], '%Y%m%d')
    current_abs_value = abs_values[0]
    current_rel_value = rel_values[0]


    # CALCULATING THE INCREMENTS WITH THE VALUES
    query = 'INSERT INTO wcdo_increments (cur_intersection_id, abs_increment, rel_increment, period,measurement_date) VALUES (?,?,?,?,?)'
    for x in range(1,len(measurement_dates)):
        period = ''
        old_date = datetime.datetime.strptime(str(measurement_dates[x]), '%Y%m%d')
#        old_date = datetime.datetime.strptime('20170901', '%Y%m%d')
        r = relativedelta.relativedelta(current_date, old_date)
#        print (old_date, current_date)
        year_count = r.years
        month_count = r.years*12 + r.months
#        print (month_count)
        
        if month_count == 1: period = 'month'
        if month_count == 3: period = 'quarter' 
        if month_count == 6: period = 'semester'
        if month_count == 12: period = 'year'
        if month_count == 60: period = 'five years'

        if period != '':
            abs_increment = current_abs_value-abs_values[x]
            rel_increment = current_rel_value-rel_values[x]
            insert_intersections_values(cursor2,cur_intersection_id,abs_increment,rel_increment,period, measurement_date)
#            print ((cur_intersection_id,abs_increment,rel_increment,period))
            cursor.execute(query,values); conn.commit()


def delete_last_iteration_increments():
    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

    query = 'SELECT count(DISTINCT measurement_date) FROM wcdo_increments;'
    cursor.execute(query)
    if cursor.fetchone()[0] > 1:
        query = 'DELETE FROM wcdo_increments WHERE measurement_date IN (SELECT MIN(measurement_date) FROM wcdo_increments) AND '
        cursor.execute(query); conn.commit()


##########################################################################################


def generate_all_top_ccc_articles_lists():

    print ('Generating all the Top articles lists.')

#    wikilanguagecodes_real=['fr']
#    wikilanguagecodes_real=['it', 'fr', 'ca', 'en', 'de', 'es', 'nl', 'uk', 'pt', 'pl']

    # LANGUAGES
    for languagecode in wikilanguagecodes_real:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(1,languagecode)

        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        # COUNTRIES FOR THE CCC COUNTRY LISTS
        countries = wikilanguages_utils.load_countries_from_language(languagecode,territories)
        countries.append('')
        print ('these are the countries from this language:')
        print (countries)
        print ('\n')
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
            print (category)

            # MAKING LISTS!!!
            # EDITORS
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_editors': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'editors')

            # FEATURED, LONG AND CITED
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'featured_article': 0.8, 'num_references':0.1, 'num_bytes':0.1}, 'positive', length, 'none', ['featured_article','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'featured')

            # GL MOST INLINKED FROM CCC
            make_top_ccc_articles_list(languagecode, ['gl'], category, 80, '', {'num_inlinks_from_CCC': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'geolocated')

            # KEYWORDS ON TITLE WITH MOST BYTES
            make_top_ccc_articles_list(languagecode, ['kw'], category, 80, '', {'num_bytes': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_bytes','num_references','featured_article','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'keywords')

            # MOST EDITED WOMEN BIOGRAPHY
            make_top_ccc_articles_list(languagecode, ['ccc','female'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'women')

            # MOST EDITED MEN BIOGRAPHY
            make_top_ccc_articles_list(languagecode, ['ccc','male'], category, 80, '', {'num_edits': 1}, 'positive', length, 'none', ['num_edits','num_editors','num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'men')

            # MOST EDITED AND CREATED DURING FIRST THREE YEARS
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, 'first_three_years', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_first_three_years')

            # MOST EDITED AND CREATED DURING LAST YEAR
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, 'last_year', {'num_edits': 1}, 'positive', length, 'none', ['num_editors','num_pageviews','num_edits','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'created_last_year')

            # MOST SEEN (PAGEVIEWS) DURING LAST MONTH
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_pageviews':1}, 'positive', length, 'none', ['num_pageviews','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'pageviews')

            # MOST DISCUSSED (EDITS DISCUSSIONS)
            make_top_ccc_articles_list(languagecode, ['ccc'], category, 80, '', {'num_discussions': 1}, 'positive', length, 'none', ['num_discussions','num_bytes','num_references','num_wdproperty','num_interwiki'], page_titles_qitems, country, 'discussions')

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
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor4 = conn4.cursor()

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

#    print (ccc_df.page_title.values)
#    print (ccc_df.index.values)

    # FILTER ARTICLES IN CASE OF CONTENT TYPE
    if len(content_type)>1:

        if content_type[1] == 'people': 
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender IS NOT NULL;'
        if content_type[1] == 'male':
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581097";'
        if content_type[1] == 'female':
            query = 'SELECT qitem FROM ccc_'+languagecode+'wiki WHERE gender = "Q6581072";'
        if content_type[1] == 'topical': query = ''

        topic_selected=[]
        print (query)
#        print (languagecode)
        for row in cursor.execute(query):
            if row[0] in ccc_df.index:
                topic_selected.append(row[0])
        print (len(topic_selected))

        ccc_df = ccc_df.reindex(topic_selected)
        print ('this is the number of lines of the dataframe after the content filtering: '+str(len(ccc_df)))
        ccc_df = ccc_df.fillna(0)


    # FILTER THE LOWEST PART OF CCC (POSITIVE FEATURES)
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
        conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
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

    query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki_top_articles_features (qitem, page_title_original, measurement_date) VALUES (?,?,?);';
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

    query = 'INSERT OR IGNORE INTO ccc_'+languagecode+'wiki_top_articles_lists (position, qitem, country, list_name, measurement_date) VALUES (?,?,?,?,?);';
    cursor4.executemany(query,tuples)
    conn4.commit()

    print ('* make_top_ccc_articles_list '+list_name+', for '+list_origin+'. Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def update_top_ccc_articles_features():
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + 'top_articles.db'); cursor2 = conn2.cursor()

    print ('* update_top_ccc_articles_features')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        lists_qitems = set()
        query = 'SELECT qitem, measurement_date FROM ccc_'+languagecode+'wiki_top_articles_lists;'
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

        query = 'UPDATE ccc_'+languagecode+'wiki_top_articles_features SET num_inlinks = ?, num_outlinks = ?, num_bytes = ?, num_references = ?, num_edits = ?, num_editors = ?, num_discussions = ?, num_pageviews = ?, num_wdproperty = ?, num_interwiki = ?, featured_article = ?, num_inlinks_from_CCC = ?, date_created = ? WHERE qitem = ? AND measurement_date = ?;';
        cursor2.executemany(query,parameters)
        conn2.commit()

    print ('Measurement date is: '+str(measurement_date))
    print ('* update_top_ccc_articles_features Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def update_top_ccc_articles_titles(type):

    functionstartTime = time.time()
    conn4 = sqlite3.connect(databases_path + 'top_articles.db'); cursor4 = conn4.cursor()
#    conn4 = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor4 = conn4.cursor()
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
                query = 'SELECT qitem, country, list_name, position FROM ccc_'+languagecode_2+'wiki_top_articles_lists WHERE measurement_date = "'+measurement_date+'" ORDER BY country, list_name, position ASC;'
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
            query = 'INSERT OR IGNORE INTO ccc_'+languagecode_1+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?);'
            cursor4.executemany(query, titles); # to top_ccc_articles.db
            conn4.commit()
            print (str(len(titles))+' titles that exist.') # including repeated qitems from different lists in the same language

            print ('* '+languagecode_1 + ' done with page_titles sitelinks.')
            with open('top_ccc_articles.txt', 'a') as f: f.write('* '+languagecode_1 + ' done with page_titles sitelinks. '+str(len(titles))+' titles. '+ str(datetime.timedelta(seconds=time.time() - langTime))+'\n')


            # INSERT INTERSECTIONS
            if len(intersections) > 500000 or wikilanguagecodes.index(languagecode_1) == len(wikilanguagecodes)-1:
                query = 'INSERT OR IGNORE INTO wcdo_intersections (set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date) VALUES (?,?,?,?,?,?,?);'
                cursor4.executemany(query,intersections); 
                conn4.commit() # to wcdo_stats.db
                print (str(len(intersections))+' intersections inserted.')
                with open('top_ccc_articles2.txt', 'a') as f: f.write(str(len(intersections))+' intersections calculated.\n')
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
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?);'
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
                query = 'UPDATE ccc_'+langcode_target+'wiki_top_articles_page_titles SET measurement_date = "'+measurement_date+'" WHERE qitem IN (%s);' % page_asstring
                cursor4.execute(query, (sample))
                x = y
                y = y + initialy
            conn4.commit()

            # DELETING THOSE USELESS FROM PAST MONTH
            query = 'DELETE FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+langcode_target+'wiki_top_articles_page_titles);'
            cursor4.execute(query)
            conn4.commit()

            # GETTING LABELS
            print ('\n- update from current wikidata labels')
            print('get current missing qitems:')
            second_qitems_none = set()
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?);'
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
                query = 'SELECT qitem, label FROM labels WHERE langcode = "'+langcode_target+'wiki" AND qitem IN (%s);' % page_asstring
                parameters=[]
                for row in cursor3.execute(query,sample): 
                    missing_qitem=row[0]
                    label=row[1].replace(' ','_')
                    parameters.append((measurement_date, missing_qitem, label, "label"))
                    labelscounter+=1
                print (x,y)
                query = 'INSERT INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?);'
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
                query = 'SELECT qitem, page_title_original FROM ccc_'+languagecode+'wiki_top_articles_features WHERE qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?) AND measurement_date = ?;' # it should get the titles from the languages in which there is a translation, even though it is not the original language of the Q.
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

            query = 'INSERT OR IGNORE INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?);'
            cursor4.executemany(query, parameters)
            conn4.commit()


            # GETTING TRANSLATIONS FROM VERSION
            print ('\n- update from translation from the copy.')
            fourth_qitems_none = list()
            print('get current missing qitems:')
            for languagecode in wikilanguagecodes:
                query = 'SELECT qitem FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date = ? AND qitem NOT IN (SELECT qitem FROM ccc_'+langcode_target+'wiki_top_articles_page_titles WHERE measurement_date = ?);'
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

            query = 'INSERT OR IGNORE INTO ccc_'+langcode_target+'wiki_top_articles_page_titles (measurement_date, qitem, page_title_target, generation_method) values (?,?,?,?);'
            cursor4.executemany(query, parameters)
            conn4.commit()

            print ('total number of missing titles in the end: '+str(len(fourth_qitems_none))+'.')

            # DONE!
            print ('* language target_titles translations for language '+langcode_target+' completed after: ' + str(datetime.timedelta(seconds=time.time() - languageTime))+'\n')
            with open('top_ccc_articles.txt', 'a') as f: f.write(langcode_target+'\t'+languages.loc[langcode_target]['languagename']+'\t'+str(datetime.timedelta(seconds=time.time() - languageTime))+'\t'+'done'+'\t'+str(datetime.datetime.now())+'\n')

    print ('* update_top_ccc_articles_titles **'+type+'** Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))


def delete_last_iteration_top_ccc_articles_lists():
    conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()

    print ('Deleting all the rest from the last iteration.')
    for languagecode in wikilanguagecodes:
        print (languagecode)

        query = 'SELECT count(DISTINCT measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_features;'
        cursor.execute(query)
        if cursor.fetchone()[0] > 1:
            query = 'DELETE FROM ccc_'+languagecode+'wiki_top_articles_features WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_features);'
            cursor.execute(query); conn.commit()
        else: print ('only one measurement_date in wiki_top_articles_features')

        query = 'SELECT count(DISTINCT measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists;'
        cursor.execute(query)
        if cursor.fetchone()[0] > 1:
            query = 'DELETE FROM ccc_'+languagecode+'wiki_top_articles_lists WHERE measurement_date IN (SELECT MIN(measurement_date) FROM ccc_'+languagecode+'wiki_top_articles_lists)'
            cursor.execute(query); conn.commit()
        else: print ('only one measurement_date in wiki_top_articles_lists')


    query = 'SELECT count(DISTINCT measurement_date) FROM wcdo_intersections;'
    cursor.execute(query)
    if cursor.fetchone()[0] > 1:
        query = 'DELETE FROM wcdo_intersections WHERE measurement_date IN (SELECT MIN(measurement_date) FROM wcdo_intersections)'
        cursor.execute(query); conn.commit()
    else: print ('only one measurement_date in wcdo_intersections')



#######################################################################################

### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");
    measurement_date = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime('/srv/wcdo/databases/wikidata.db')))
    measurement_date = '20180926'

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'

    # Import the language-territories mappings
    territories = wikilanguages_utils.load_languageterritories_mapping()

    # Import the Wikipedia languages characteristics / UPGRADE CASE: in case of extending the project to WMF SISTER PROJECTS, a) this should be extended with other lists for Wikimedia sister projects b) along with more get functions in the MAIN for each sister project.
    languages = wikilanguages_utils.load_wiki_projects_information(territories);
    wikilanguagecodes = languages.index.tolist()

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
    # Only those with a geographical context
    wikilanguagecodes_real = wikilanguagecodes.copy()
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes)

    # Verify/Remove all languages without a table in ccc_current.db
    for languagecode in wikilanguagecodes:
        if languagecode not in wikipedialanguage_numberarticles: wikilanguagecodes.remove(languagecode)

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    # Checking the current intersection_id / Query to compute intersections
    cur_intersection_id = check_cur_intersection_id()
#    query_insert = 'INSERT INTO wcdo_intersections (intersection_id, content, set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date) VALUES (?,?,?,?,?,?,?,?,?);'
    query_insert = 'INSERT OR IGNORE INTO wcdo_intersections (intersection_id, content, set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, measurement_date) VALUES (?,?,?,?,?,?,?,?,?);'


    # TESTS
    """
    wikilanguagecodes = obtain_region_wikipedia_language_list(languages, '', 'Eastern Europe', '').index.tolist() # e.g. get the languages from a particular region.
    wikilanguagecodes = obtain_region_wikipedia_language_list(languages, '', 'Western Europe', '').index.tolist() # e.g. get the languages from a particular region.
    wikilanguagecodes = obtain_region_wikipedia_language_list(languages, '', 'Northern Europe', '').index.tolist() # e.g. get the languages from a particular region.
    wikilanguagecodes = obtain_region_wikipedia_language_list(languages, '', 'Southern Europe', '').index.tolist() # e.g. get the languages from a particular region.
    """

#    wikilanguagecodes = obtain_region_wikipedia_language_list('', 'Eastern Europe', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
#    wikilanguagecodes = ['ca']
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('ii'):]

    print ('\n* Starting the STATS GENERATION CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* STATS GENERATION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    wikilanguages_utils.finish_email(startTime,'stats_generation.out','Stats generation')