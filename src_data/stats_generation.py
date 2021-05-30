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
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
import pandas as pd
# data and compute
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


##### STATS CREATION FUNCTIONS ####

# LEGEND:
# A: "accumulated monthly" means the accumulated stats for the entire dataset up to each month since the beginning of WP.
# B: "last accumulated" means the accumulated stats for the entire dataset but up till last month.
# C: "monthly created" means the stats for the articles created during every month since the beginning of WP.
# D: "last month created" means the stats for the articles created during the last month.

# FOR MORE INFO SEE document sets_intersections.xls for more information about the stats.
# There are four possible time range: a) accumulated monthly, b) last accumulated, c) monthly and d) last month. The first two comprise all the articles, and c and d only an increment of articles created within a month.


# TODO LIST:
# generate_ethnic_supraethnic_groups_intersections(time_range)
# generate_religious_groups_interesections(time_range)


######################################################################

# MAIN
######################## STATS GENERATION SCRIPT ##################### 
def main():



    print ('fet.')




    input('')
    input('')
    input('')

    create_intersections_db()

    # RUN ONCE AND THEN IT IS DONE FOREVER
    # accumulated monthly (A)
    generate_langs_ccc_intersections('accumulated monthly')
    generate_ccc_segments_intersections('accumulated monthly')
    generate_people_segments_intersections('accumulated monthly')
    generate_geolocated_segments_intersections('accumulated monthly')

    generate_ethnic_groups_intersections('accumulated monthly')
    generate_supra_ethnic_groups_intersections('accumulated monthly')
    generate_religious_groups_intersections ('accumulated monthly')
    generate_sexual_orientation_intersections('accumulated monthly')

    # monthly created (C)
    generate_monthly_articles_intersections('monthly created')


    # RECURRING
    # last accumulated (B)
    generate_langs_intersections()
    generate_ccc_segments_intersections('last accumulated')
    generate_ccc_diversity_topics_intersections()
    generate_ccc_qitems_intersections()
    generate_langs_ccc_intersections('last accumulated')
    generate_ccc_ccc_intersections()
    generate_people_segments_intersections('last accumulated')
    generate_geolocated_segments_intersections('last accumulated')
    generate_people_ccc_intersections()

    generate_ethnic_groups_intersections('last accumulated')
    generate_supra_ethnic_groups_intersections('last accumulated')
    generate_religious_groups_intersections ('last accumulated')
    generate_sexual_orientation_intersections('last accumulated')


    # created last month (D)
    generate_monthly_articles_intersections('last month')
    generate_pageviews_intersections() # after the 2020 first Top CCC data.
    generate_top_ccc_articles_lists_intersections() # after the 2020 first Top CCC data.
    generate_features_stats() # after the 2020 data on the new diversity groups.

    input('')
    input('')
    input('')

    # mv db to production
    wikilanguages_utils.copy_db_for_production(stats_db, 'stats_generation.py', databases_path)
    wikilanguages_utils.copy_db_for_production(wikipedia_diversity_db, 'stats_generation.py', databases_path)
    wikilanguages_utils.copy_db_for_production(diversity_categories_db, 'stats_generation.py', databases_path)
    wikilanguages_utils.extract_wikipedia_diversity_tables_to_files()



    # This are left for the future.
    # generate_time_intersections('accumulated monthly')
    # generate_time_intersections('last accumulated')



### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

##################################################################################

# COMMAND LINE: sqlite3 -header -csv stats.db "SELECT * FROM create_intersections_db;" > create_intersections_db.csv
def create_intersections_db():

    function_name = 'create_intersections_db'
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    try:
        os.remove(databases_path + stats_db); print (stats_db+' deleted.');
    except:
        pass

    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

    functionstartTime = time.time()

    query = ('CREATE table if not exists wdo_intersections_accumulated ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor not null, '+

    'set2 not null, '+
    'set2descriptor not null, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('There is already Wikipedia Diversity Stats table.')

    query = ('CREATE table if not exists wdo_intersections_monthly ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+

    'set2 text, '+
    'set2descriptor text, '+

    'abs_value integer,'+
    'rel_value float,'+

    'period text,'
    'PRIMARY KEY (content,set1,set1descriptor,set2,set2descriptor,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('There is already Wikipedia Diversity Stats table.')


    query = ('CREATE table if not exists wdo_stats ('+
    'content text not null, '+
    'set1 text not null, '+
    'set1descriptor text, '+
    'statistic text,'+
    'value float,'+
    'period text,'+
    'PRIMARY KEY (content,set1,set1descriptor,statistic,period))')

    try:
        cursor.execute(query)
        conn.commit()
    except:
        print ('already created.')


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def insert_intersections_values(time_range, cursor2, content, set1, set1descriptor, set2, set2descriptor, abs_value, base, period):

    if time_range == 'monthly created' or  time_range == 'last month':
        table_value = 'monthly'
    else:
        table_value = 'accumulated'       

    if abs_value == None: abs_value = 0

    if base == None or base == 0: rel_value = 0
    else: rel_value = 100*abs_value/base

    if 'avg' in set1 or 'avg' in set2: rel_value = base # exception for calculations in generate_langs_ccc_intersections()

    if rel_value != 0.0 or abs_value != 0:
        values = (abs_value, rel_value, content, set1, set1descriptor, set2, set2descriptor, period)

        query_insert = 'INSERT OR IGNORE INTO wdo_intersections_'+table_value+' (abs_value, rel_value, content, set1, set1descriptor, set2, set2descriptor, period) VALUES (?,?,?,?,?,?,?,?)'
        cursor2.execute(query_insert,values);

        query_update = 'UPDATE wdo_intersections_'+table_value+' SET abs_value = ?, rel_value = ? WHERE content = ? AND set1 = ? AND set1descriptor = ? AND set2 = ? AND set2descriptor = ? AND period = ?'
        cursor2.execute(query_update,values);
#    input('')


def generate_langs_intersections():
    time_range = 'last accumulated'

    function_name = 'generate_langs_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1)
        qitems = set()
        query = 'SELECT qitem FROM '+languagecode_1+'wiki'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')

    # LANGUAGE EDITIONS
    for languagecode_1 in wikilanguagecodes:
        partialtime = time.time()
        print ('* '+languagecode_1)
        current_wpnumberofarticles_1=wikipedialanguage_currentnumberarticles[languagecode_1]

        # entire wp
        query = 'SELECT count(*) FROM '+languagecode_1+'wiki WHERE num_interwiki = 0'
        cursor.execute(query)
        zero_ill_wp_count = cursor.fetchone()[0]
        insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_1,'zero_ill',zero_ill_wp_count,current_wpnumberofarticles_1, period)


        # % articles edited last month
        query = 'SELECT count(*) FROM '+languagecode_1+'wiki WHERE num_edits_last_month != 0'
        cursor.execute(query)
        edited_last_month_count = cursor.fetchone()[0]
        insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_1,'edited_last_month',edited_last_month_count,current_wpnumberofarticles_1, period)


        query = 'SELECT count(*) FROM '+languagecode_1+'wiki WHERE qitem IS NULL'
        cursor.execute(query)
        null_qitem_count = cursor.fetchone()[0]
        insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_1,'null_qitems',null_qitem_count,current_wpnumberofarticles_1, period)

        if current_wpnumberofarticles_1 == 0: continue
        for languagecode_2 in wikilanguagecodes:
#            if languagecode_1 == languagecode_2: continue
#            query = 'SELECT COUNT(*) FROM '+languagecode_2+'wiki INNER JOIN ccc_'+languagecode_1+'wiki ON ccc_'+languagecode_1+'wiki.qitem = ccc_'+languagecode_2+'wiki.qitem'
#            cursor.execute(query)
#            article_count = cursor.fetchone()[0]
            article_count=len(all_articles[languagecode_1].intersection(all_articles[languagecode_2]))
            insert_intersections_values(time_range, cursor2,'articles',languagecode_1,'wp',languagecode_2,'wp',article_count,current_wpnumberofarticles_1,period)

        print ('. '+languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - partialtime)))

    conn2.commit()
    print ('languagecode, wp, languagecode, zero_ill,'+period)
    print ('languagecode, wp, languagecode, edited_last_month,'+period)
    print ('languagecode, wp, languagecode, null_qitems,'+period)
    print ('languagecode_1, wp, languagecode_2, wp,'+period)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_ccc_segments_intersections(time_range):

    def for_time_range(time_range,query_part,period):

        # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)

            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki'
            if time_range == 'accumulated monthly': query+= ' WHERE '+query_part

            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            query = 'SELECT COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM '+languagecode+'wiki WHERE ccc_binary=1'
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



    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()


    if time_range == 'accumulated monthly':
        function_name = 'generate_ccc_segments_intersections accumulated monthly'
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)

        conn2.commit()
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':
        function_name = 'generate_ccc_segments_intersections last accumulated'
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        period = cycle_year_month

#        period = list(sorted(periods_accum.keys()))[len(periods_accum-1)]
        print (time_range, period)
        query_part = ''
        for_time_range(time_range,query_part,period)

        conn2.commit()
        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_ccc_diversity_topics_intersections():
    time_range = 'last accumulated'

    function_name = 'generate_ccc_diversity_topics_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        list_topics = ['folk','earth','monuments_and_buildings','music_creations_and_organizations','sport_and_teams','food','paintings','glam','books','clothing_and_fashion','industry','people','religion','time_interval', 'ethnic_group', 'supra_ethnic_group', 'sexual_orientation',' religious_group']
        ccc_topics = {}
        wp_topics = {}

        query = 'SELECT ccc_binary, folk, earth, monuments_and_buildings, music_creations_and_organizations, sport_and_teams, food, paintings, glam, books, clothing_and_fashion, industry, gender, religion, time_interval, ethnic_group, supra_ethnic_group, sexual_orientation, religious_group FROM '+languagecode+'wiki;'

        wp_count = 0
        wp_topic_count = 0
        ccc_count = 0
        ccc_topic_count = 0

        for topic in list_topics:
            wp_topics[topic]=0
            ccc_topics[topic]=0

        for row in cursor.execute(query):
            wp_count+=1
            ccc_binary = row[0]
            if ccc_binary == 1: ccc_count+=1

            tv = None
            for x in range(len(list_topics)):
                topic_value = row[x+1]
                topic = list_topics[x]

                if topic_value != None:
                    tv = 1
                    if ccc_binary == 1:
                        ccc_topics[topic]=ccc_topics[topic]+1

                    wp_topics[topic]=wp_topics[topic]+1

            if tv == None:
                if ccc_binary == 1:
                    ccc_topic_count+=1
                wp_topic_count+=1
                        
        for topic, topic_value in ccc_topics.items():
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc','topic',topic,topic_value,ccc_count, period)
        insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc','topic',"no_topic",ccc_topic_count,ccc_count, period)
#        print (ccc_count)

        for topic, topic_value in wp_topics.items():
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','topic',topic,topic_value,wp_count, period)
        insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','topic',"no_topic",wp_topic_count,wp_count, period)
#        print (wp_count)

        # for topic in ccc_topics.keys():
        #     print ('languagecode, ccc, topic, '+topic+', '+ period)
        # for topic in wp_topics.keys():
        #     print ('languagecode, wp, topic, '+topic+', '+ period)

    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



# The other types of diversity would require other methods.
# gender, sexual_orientation, religion, race_and_ethnia, time

def generate_ccc_qitems_intersections():
    time_range = 'last accumulated'

    function_name = 'generate_ccc_qitems_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
#    print ('generate_ccc_qitems_intersections')

    functionstartTime = time.time()

    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn3 = sqlite3.connect(databases_path + wikidata_db); cursor3 = conn3.cursor()

    # WIKIDATA AND CCC
    query = 'SELECT COUNT(DISTINCT qitem) FROM sitelinks;'
    cursor3.execute(query)
    wikidata_article_qitems_count = cursor3.fetchone()[0]

    # LANGUAGE EDITIONS AND CCC, NO CCC, CCC SEGMENTS (CCC GEOLOCATED, CCC KEYWORDS)
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        query = 'SELECT COUNT(*) FROM '+languagecode+'wiki'
        cursor.execute(query)
        row = cursor.fetchone()
        wpnumberofarticles=row[0]

        query = 'SELECT COUNT(ccc_binary) FROM '+languagecode+'wiki WHERE ccc_binary=1'
        cursor.execute(query)
        row = cursor.fetchone()

        ccc_count = row[0]

        # In regards of wikidata qitems
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','',languagecode,'ccc',ccc_count,wikidata_article_qitems_count,period)

        # zero ill
        query = 'SELECT count(page_title) FROM '+languagecode+'wiki WHERE num_interwiki = 0 AND ccc_binary=1'
        cursor.execute(query)
        zero_ill_ccc_count = cursor.fetchone()[0]
        insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc',languagecode,'zero_ill',zero_ill_ccc_count,ccc_count, period)

        # MAIN TERRITORIES
        query = 'SELECT main_territory, COUNT(ccc_binary), COUNT(ccc_geolocated), COUNT (keyword_title) FROM '+languagecode+'wiki WHERE ccc_binary=1 GROUP BY main_territory'
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

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_langs_ccc_intersections(time_range):

    if time_range == 'accumulated monthly':
        function_name = 'generate_langs_ccc_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        lang_ccc_qitems={} 
        for languagecode in wikilanguagecodes:
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'
            ccc_qitems = set()
            for row in cursor.execute(query):
                ccc_qitems.add(row[0])
            lang_ccc_qitems[languagecode]=ccc_qitems
#            print (len(ccc_qitems))

        print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        print ('loaded all the CCC')

        for languagecode in wikilanguagecodes:
            ccc_accum_count = {}
            wp_accum_count = {}

            for lang in wikilanguagecodes:
                ccc_accum_count[lang] = 0
                wp_accum_count[lang] = 0

            for period in sorted(periods_monthly.keys()):
                query_part = periods_monthly[period]

                qitems = set()
                query = 'SELECT qitem FROM '+languagecode+'wiki WHERE '+query_part
#                print (query)
#                cursor.execute(query)
#                qitems = set(cursor.fetchall()) 

                for row in cursor.execute(query): qitems.add(row[0])
                created_articles_count = len(qitems)

#                print (created_articles_count)
                if created_articles_count == 0: continue

                wp_accum_c = wp_accum_count[languagecode] + created_articles_count
                wp_accum_count[languagecode] = wp_accum_c

                # Other Langs CCC
                for languagecode_2 in wikilanguagecodes:
                    ccc_articles_created_count = len(qitems.intersection(lang_ccc_qitems[languagecode_2]))
#                    print (ccc_articles_created_count)
                    if ccc_articles_created_count == 0: continue

                    ccc_accum_co = ccc_accum_count[languagecode_2] + ccc_articles_created_count
                    ccc_accum_count[languagecode_2] = ccc_accum_co

                    insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode_2, 'ccc', ccc_accum_co, wp_accum_c, period)

            print (languagecode, str(datetime.timedelta(seconds=time.time() - functionstartTime)))
            conn2.commit()
#            input('')

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_langs_ccc_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()


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
        
            query = 'SELECT COUNT(*) FROM '+languagecode_1+'wiki WHERE ccc_binary=1'
            cursor.execute(query)
            row = cursor.fetchone()
            ccc_count = row[0]

            language_ccc_count = {}
            for languagecode_2 in wikilanguagecodes:
                query = 'SELECT COUNT(ccc_binary), COUNT(keyword_title), COUNT(ccc_geolocated) FROM '+languagecode_2+'wiki WHERE ccc_binary=1'
                cursor.execute(query)
                row = cursor.fetchone()
                ccc_articles_count_total = row[0]
                ccc_keywords_count_total = row[1]
                ccc_geolocated_count_total = row[2]


                language_ccc_count[languagecode_2]=ccc_articles_count_total
                all_ccc_articles_count_total+=ccc_articles_count_total
                allwp_allnumberofarticles+=wikipedialanguage_currentnumberarticles[languagecode_2]

                if languagecode_1 == languagecode_2: continue

                query = 'SELECT COUNT('+languagecode_2+'wiki.ccc_binary), COUNT('+languagecode_2+'wiki.keyword_title), COUNT('+languagecode_2+'wiki.ccc_geolocated) FROM '+languagecode_2+'wiki INNER JOIN '+languagecode_1+'wiki ON '+languagecode_1+'wiki.qitem = '+languagecode_2+'wiki.qitem WHERE '+languagecode_2+'wiki.ccc_binary = 1'
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
            
            # ### all ccc articles ###
            # # what is the extent of all ccc articles in this language edition?
            # insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'wp','all_ccc_articles','',all_ccc_articles_count+ccc_count,wpnumberofarticles, period) -> this is not correct because it introduces redundancy. when some articles belong to different CCC it is added twice and the percentage does not work.

            # COVERAGE
            ### total langs ###
            # how well this language edition covered all CCC articles? t.coverage and coverage art.
            insert_intersections_values(time_range,cursor2,'articles','all_ccc_articles','',languagecode_1,'wp',all_ccc_articles_count,all_ccc_articles_count_total-ccc_count, period)

            ### relative langs ###
            # how well this language edition covered all CCC articles in average? relative coverage.
            all_ccc_rel_value_ccc_total_avg=all_ccc_rel_value_ccc_total/(len(wikilanguagecodes)-1)
    #        all_ccc_abs_value_avg=all_ccc_articles_count/(len(wikilanguagecodes)-1)
            insert_intersections_values(time_range,cursor2,'articles','all_ccc_avg','',languagecode_1,'wp',0,all_ccc_rel_value_ccc_total_avg, period)

            print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - langTime)))


        print ('Done with COVERAGE.')
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


            print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - langTime)))

        # what is the extent of all ccc articles in all wp all articles
        insert_intersections_values(time_range,cursor2,'articles','all_wp_all_articles','','all_ccc_articles','',all_ccc_articles_count_total,allwp_allnumberofarticles, period)
        print ('Done with SPREAD.')

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

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def generate_ccc_ccc_intersections():
    time_range = 'last accumulated'
    function_name = 'generate_ccc_ccc_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    lang_ccc_qitems={} 
    for languagecode in wikilanguagecodes:
        ccc_qitems = set()
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'
        for row in cursor.execute(query):
            ccc_qitems.add(row[0])
        lang_ccc_qitems[languagecode]=ccc_qitems

    # between languages ccc
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        ccc_lang1 = lang_ccc_qitems[languagecode_1]
        language_ccc_count = len(ccc_lang1)

        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            ccc_lang2 = lang_ccc_qitems[languagecode_2]

            ccc_coincident_articles_count = len(ccc_lang1.intersection(ccc_lang2))
            insert_intersections_values(time_range,cursor2,'articles',languagecode_1,'ccc',languagecode_2,'ccc',ccc_coincident_articles_count,language_ccc_count,period)

    conn2.commit()
    print ('languagecode_1, ccc, languagecode_2, ccc,'+ period)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def generate_people_segments_intersections(time_range):

    def for_time_range(time_range,query_part,period):
        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

#            print(query_part)
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            query = 'SELECT gender, COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part+' GROUP BY gender'
            gender_name_count = {}
            people_count = 0
            for row in cursor.execute(query):
                if row[0] in gender_dict: 
                    gender_name_count[gender_dict[row[0]]]=row[1]
                    people_count += row[1]
            gender_name_count['people']=people_count

            for gender_name, gender_count in gender_name_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems',gender_name, gender_count,wpnumberofarticles,period)

        print ('languagecode, wp, wikidata_article_qitems, male,'+period)
        print ('languagecode, wp, wikidata_article_qitems, female,'+period)
        print ('languagecode, wp, wikidata_article_qitems, people,'+period)


    if time_range == 'accumulated monthly':

        function_name = 'generate_people_segments_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_people_segments_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
        conn3 = sqlite3.connect(databases_path + wikidata_db); cursor3 = conn3.cursor()

        gender_name_count_total = {}
        people_count_total = 0
        query = 'SELECT qitem2, COUNT(*) FROM people_properties WHERE qitem2!="Q5" GROUP BY qitem2'
        cursor3.execute(query)
        for row in cursor3.execute(query):
            if row[0] in gender_dict: gender_name_count_total[gender_dict[row[0]]]=row[1]
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
            if row[0] in gender_dict: gender_name_count_total_zero_ill[gender_dict[row[0]]]=row[1]
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

            query = 'SELECT gender, COUNT(*) FROM '+languagecode+'wiki GROUP BY gender;'
    #        query = 'SELECT qitem2, COUNT(*) FROM people_properties INNER JOIN sitelinks ON people_properties.qitem = sitelinks.qitem WHERE langcode="'+languagecode+'wiki" AND qitem2!="Q5" GROUP BY qitem2'
            gender_name_count = {}
            people_count = 0
            for row in cursor.execute(query):
                if row[0] in gender_dict: 
                    gender_name_count[gender_dict[row[0]]]=row[1]
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

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def generate_people_ccc_intersections():
    time_range = "last accumulated"
    function_name = 'generate_people_ccc_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    # PEOPLE SEGMENTS AND CCC
    language_ccc_count = {}
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

        qitems = []
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ccc_binary=1'
        for row in cursor.execute(query):
            qitems.append(row[0])
        language_ccc_count[languagecode]=len(qitems)

        # male
        male=[]
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender = "Q6581097"'
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
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender = "Q6581072"'
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

        gdr = []
        for q, gender in gender_dict.items():
            if q == 'Q6581097' or q == 'Q1052281': continue
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE gender = "'+q+'"'
            for row in cursor.execute(query): 
                gdr.append(row[0])
            gdrcount=len(gdr)
            gdr_ccc = set(female).intersection(set(qitems))
            gdr_ccc_count=len(gdr_ccc)

            insert_intersections_values(time_range,cursor2,'articles',languagecode, gender, languagecode, 'ccc', gdr_ccc_count, gdrcount,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'ccc', languagecode, gender, gdr_ccc_count,language_ccc_count[languagecode],period)



    conn2.commit()
    print ('languagecode, male, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, male,'+period)

    print ('languagecode, female, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, female,'+period)

    print ('languagecode, people, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, people,'+period)

    print ('languagecode, wp, languagecode, ccc_people,'+period)

    print ('languagecode, other_genders, languagecode, ccc,'+period)
    print ('languagecode, ccc, languagecode, other_genders,'+period)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def generate_geolocated_segments_intersections(time_range):

    def for_time_range(time_range,query_part,period):

        regions_all_langs_count={}
        subregions_all_langs_count={}
        iso3166_all_langs_count={}
        all_wp_all_geolocated_articles_count = 0

        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)
#            wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            geolocated_articles_count = 0
            iso3166_articles = {}

            query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM '+languagecode+'wiki WHERE iso3166 IS NOT NULL AND '+query_part+' GROUP BY iso3166'

            for row in cursor.execute(query):
                iso3166_articles[row[0]]=row[1]
                geolocated_articles_count+=row[1]

                try:
                    iso3166_all_langs_count[row[0]]+=row[1]
                except:
                    iso3166_all_langs_count[row[0]]=row[1]

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

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','countries',iso3166_code,iso3166_count,wpnumberofarticles, period)

            # subregions
            for subregion_name, subregion_count in subregions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,geolocated_articles_count, period)

            for subregion_name, subregion_count in subregions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','subregions',subregion_name,subregion_count,wpnumberofarticles, period)

            # regions
            for region_name, region_count in regions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','regions',region_name,region_count,geolocated_articles_count, period)

            for region_name, region_count in regions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','regions',region_name,region_count,wpnumberofarticles, period)

        conn2.commit()
        print ('languagecode, wp, languagecode, geolocated, '+period)

        print ('languagecode, geolocated, countries, iso3166, '+period)
        print ('languagecode, geolocated, subregions, iso3166, '+period)
        print ('languagecode, geolocated, regions, iso3166, '+period)

        print ('languagecode, wp, countries, iso3166, '+period)
        print ('languagecode, wp, subregions, iso3166, '+period)
        print ('languagecode, wp, regions, iso3166, '+period)


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


    if time_range == 'accumulated monthly':

        function_name = 'generate_geolocated_segments_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        period = cycle_year_month

        country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions() # iso 3166 to X

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_geolocated_segments_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
        conn3 = sqlite3.connect(databases_path + wikidata_db); cursor3 = conn3.cursor()

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

        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems',None,'geolocated','zero_ill',geolocated_items_zero_ill_count_total,wikidata_article_qitems_count, period)

        print ('wikidata_article_qitems, geolocated, geolocated, zero_ill, '+period)

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
            insert_intersections_values(time_range,cursor2,'articles','countries',iso3166_code,'geolocated','zero_ill',iso3166_qitems_zero_ill[iso3166_code],iso3166_count, period)

        # subregions
        for subregion_name, subregion_count in subregions_count_total.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','subregions',subregion_name,subregion_count,geolocated_items_count_total, period)

            # subregions ILL zero
            insert_intersections_values(time_range,cursor2,'articles','subregions',subregion_name,'geolocated','zero_ill',subregions_count_total_zero_ill[subregion_name],subregion_count, period)

        # regions
        for region_name, region_count in regions_count_total.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','regions',region_name,region_count,geolocated_items_count_total, period)

            # regions ILL zero
            insert_intersections_values(time_range,cursor2,'articles','regions',region_name,'geolocated','zero_ill',regions_count_total_zero_ill[region_name],region_count, period)

        conn2.commit()
        print ('wikidata_article_qitems, geolocated, countries, iso3166,'+period)
        print ('wikidata_article_qitems, geolocated, subregions, subregion_name,'+period)
        print ('wikidata_article_qitems, geolocated, regions, region_name,'+period)

        print ('countries, iso3166, geolocated, zero_ill,'+period)
        print ('subregions, subregion_name, geolocated, zero_ill,'+period)
        print ('regions, region_name, geolocated, zero_ill,'+period)


        regions_all_langs_count={}
        subregions_all_langs_count={}
        iso3166_all_langs_count={}
        all_wp_all_geolocated_articles_count = 0

        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
            wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]

            geolocated_articles_count = 0
            iso3166_articles = {}
            query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM '+languagecode+'wiki WHERE iso3166 IS NOT NULL GROUP BY iso3166;'
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
                if iso3166_count > iso3166_qitems[iso3166_code]:
                    iso3166_count = iso3166_qitems[iso3166_code] # we see that ceb and sv have more geolocated items to some countries that are even tagged in wikidata. we wrongly assumed that wikidata was the main source.
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

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_sexual_orientation_intersections(time_range):

    def for_time_range(time_range,query_part,period):


        language_lgbttopics = {}
        lgbttopics_qitems = set()

        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

#            print(query_part)
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]

            df = pd.read_sql_query('SELECT qitem FROM '+languagecode+'wiki WHERE lgbt_topic>0 AND '+query_part, conn)
            lgbtofarticles = len(df)
            language_lgbttopics[languagecode] = lgbtofarticles
            lgbttopics_qitems.update(set(df.qitem.tolist()))

            query = 'SELECT qitem, sexual_orientation, gender, ccc_binary FROM '+languagecode+'wiki WHERE '+query_part+' AND (gender IS NOT NULL OR sexual_orientation IS NOT NULL);'

            df = pd.read_sql_query(query, conn)#, parameters)
            people_count = len(df)

            sexual_orientation_count = len(df.loc[df['sexual_orientation'].notnull()])

            # languagecode  wp  wikidata_articles_qitems    sexual_orientation
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','sexual_orientation', sexual_orientation_count,wpnumberofarticles,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','lgbt_topics', lgbtofarticles, wpnumberofarticles, period)

            sexual_orientation_types_count = df.sexual_orientation.value_counts().to_dict()

            # languagecode  people  sexual_orientation  sexual_orientation_name
            for sex_type, sex_type_count in sexual_orientation_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','sexual_orientation',sex_type, sex_type_count,people_count,period)

            ccc_people = df.loc[df['ccc_binary'] == 1]
            ccc_people_count = len(ccc_people)
            ccc_people_sexual_orientation_types_count = ccc_people.sexual_orientation.value_counts().to_dict()

            # languagecode  people  sexual_orientation  sexual_orientation_name
            for ccc_sex_type, ccc_sex_type_count in ccc_people_sexual_orientation_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc_people','sexual_orientation',ccc_sex_type, ccc_sex_type_count,ccc_people_count,period)

            print ('languagecode, wp, wikidata_article_qitems, lgbt_topics,'+period)
            print ('languagecode, wp, wikidata_article_qitems, sexual_orientation,'+period)
            print ('languagecode, people, sexual_orientation, sexual_orientation_name,'+period)
            print ('languagecode, ccc_people, wikidata_article_qitems, sexual_orientation_name,'+period)


        for languagecode, count in language_lgbttopics.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','lgbt_topics',languagecode,'wp', count, len(lgbttopics_qitems), period)

        print ('wikidata_article_qitems, lgbt_topics, languagecode, wp'+period)



    if time_range == 'accumulated monthly':

        function_name = 'generate_sexual_orientation_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_sexual_orientation_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for_time_range(time_range,periods_accum[period],period)


        people_set = set()
        lgbttopics_qitems = set()
        wikidata_article_qitems_sex_or_dict = {}
        language_sexual_orientation_dict = {}
        language_lgbttopics_dict = {}
        for languagecode in wikilanguagecodes:

            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki;'
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]


            query = 'SELECT qitem, sexual_orientation, gender FROM '+languagecode+'wiki WHERE (gender IS NOT NULL OR sexual_orientation IS NOT NULL);'
            df = pd.read_sql_query(query, conn)#, parameters)

            ppl_lang = set(df.qitem.unique())
            people_set = people_set.union(ppl_lang)
            sexual_orientation_count = len(df.loc[df['sexual_orientation'].notnull()])
            people_count = len(ppl_lang)
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','wikidata_articles_qitems','sexual_orientation', sexual_orientation_count,people_count,period)

            language_sexual_orientation_dict[languagecode] = df.sexual_orientation.value_counts().to_dict()

            dict_sexor = df.loc[df['sexual_orientation'].notnull()][['qitem','sexual_orientation']].set_index('qitem').to_dict('dict')['sexual_orientation']

            wikidata_article_qitems_sex_or_dict.update(dict_sexor)


            df = pd.read_sql_query('SELECT qitem FROM '+languagecode+'wiki WHERE lgbt_topic > 0', conn)
            lgbtofarticles = len(df)
            language_lgbttopics_dict[languagecode] = lgbtofarticles
            lgbttopics_qitems.update(set(df.qitem.tolist()))

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','lgbt_topics', lgbtofarticles, wpnumberofarticles, period)

        wikidata_df = pd.DataFrame(wikidata_article_qitems_sex_or_dict.items(),columns=['qitem','sexual_orientation'])
        wikidata_article_qitems_count_dict = wikidata_df.sexual_orientation.value_counts().to_dict()

        for languagecode, sex_orientation_types_dict in language_sexual_orientation_dict.items():
            for sex_orientation_type, sex_orientation_type_count in sex_orientation_types_dict.items():

                insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems',sex_orientation_type,languagecode,'wp', sex_orientation_type_count,wikidata_article_qitems_count_dict[sex_orientation_type],period)

        sex_orientation_sum = 0
        for sex_orientation_type, sex_orientation_type_count in wikidata_article_qitems_count_dict.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','sexual_orientation',sex_orientation_type, sex_orientation_type_count,len(people_set),period)

            sex_orientation_sum += sex_orientation_type_count

        insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','wikidata_articles_qitems','sexual_orientation',sex_orientation_sum,len(people_set),period)


        print ('languagecode, people, wikidata_articles_qitems, sexual_orientation, ' +period)

        print ('wikidata_articles_qitems, sex_orientation_type, languagecode, wp, ' +period)

        print ('wikidata_articles_qitems, people, sexual_orientation, sex_orientation_type, ' +period)

        print ('wikidata_articles_qitems, people, wikidata_articles_qitems, sexual_orientation, ' +period)

        print ('languagecode, wp, wikidata_article_qitems, lgbt_topics, '+period)

        print ('wikidata_article_qitems, lgbt_topics, languagecode, wp, '+period)

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_religious_groups_intersections(time_range):

    def for_time_range(time_range,query_part,period):
        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

#            print(query_part)
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]


            query = 'SELECT qitem, religious_group, gender, ccc_binary FROM '+languagecode+'wiki WHERE '+query_part+' AND (gender IS NOT NULL OR religious_group IS NOT NULL);'

            df = pd.read_sql_query(query, conn)#, parameters)
            people_count = len(df)

            religious_group_count = len(df.loc[df['religious_group'].notnull()])

            # languagecode  wp  wikidata_articles_qitems    religious_group
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','religious_group', religious_group_count,wpnumberofarticles,period)

            religious_group_types_count = df.religious_group.value_counts().to_dict()

            # languagecode  people  religious_group  religious_group_name
            for religious_group_type, religious_group_type_count in religious_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','religious_group',religious_group_type, religious_group_type_count,people_count,period)

            ccc_people = df.loc[df['ccc_binary'] == 1]
            ccc_people_count = len(ccc_people)
            ccc_people_religious_group_types_count = ccc_people.religious_group.value_counts().to_dict()

            # languagecode  people  religious_group  religious_group_name
            for ccc_religious_group_type, ccc_religious_group_type_count in ccc_people_religious_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc_people','religious_group',ccc_religious_group_type, ccc_religious_group_type_count, ccc_people_count, period)

            print ('languagecode, wp, wikidata_article_qitems, religious_group,'+period)
            print ('languagecode, people, religious_group, religious_group_name,'+period)
            print ('languagecode, ccc_people, wikidata_article_qitems, religious_group_name,'+period)


    if time_range == 'accumulated monthly':

        function_name = 'generate_religious_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_religious_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for_time_range(time_range,periods_accum[period],period)

        people_set = set()
        wikidata_article_qitems_rel_group_dict = {}
        language_religious_group_dict = {}
        for languagecode in wikilanguagecodes:

            query = 'SELECT qitem, religious_group, gender FROM '+languagecode+'wiki WHERE (gender IS NOT NULL OR religious_group IS NOT NULL);'
            df = pd.read_sql_query(query, conn)#, parameters)

            ppl_lang = set(df.qitem.unique())
            people_set = people_set.union(ppl_lang)
            religious_group_count = len(df.loc[df['religious_group'].notnull()])
            people_count = len(ppl_lang)
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','wikidata_articles_qitems','religious_group', religious_group_count,people_count,period)

            language_religious_group_dict[languagecode] = df.religious_group.value_counts().to_dict()

            dict_regor = df.loc[df['religious_group'].notnull()][['qitem','religious_group']].set_index('qitem').to_dict('dict')['religious_group']

            wikidata_article_qitems_rel_group_dict.update(dict_regor)

        wikidata_df = pd.DataFrame(wikidata_article_qitems_rel_group_dict.items(),columns=['qitem','religious_group'])
        wikidata_article_qitems_count_dict = wikidata_df.religious_group.value_counts().to_dict()


        for languagecode, religious_group_types_dict in language_religious_group_dict.items():
            for religious_group_type, religious_group_type_count in religious_group_types_dict.items():

                insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems_religious_group',religious_group_type,languagecode,'wp', religious_group_type_count,wikidata_article_qitems_count_dict[religious_group_type],period)

        religious_group_sum = 0
        for religious_group_type, religious_group_type_count in wikidata_article_qitems_count_dict.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','religious_group',religious_group_type, religious_group_type_count,len(people_set),period)

            religious_group_sum += religious_group_type_count

        insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','wikidata_articles_qitems','religious_group',religious_group_sum,len(people_set),period)


        print ('languagecode, people, wikidata_articles_qitems, religious_group, ' +period)
        print ('wikidata_articles_qitems_religious_group, religious_group_type, languagecode, wp, ' +period)
        print ('wikidata_articles_qitems, people, religious_group, religious_group_type, ' +period)
        print ('wikidata_articles_qitems, people, wikidata_articles_qitems, religious_group, ' +period)

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)





def group_dict_fix(ethnic_group_types_count):

    ethnic_group_types_new_dict = {}
    for ethnic_group_type, ethnic_group_count in ethnic_group_types_count.items():
        if ethnic_group_type == None: continue
        if ';' in ethnic_group_type:
            for group in ethnic_group_type.split(';'):
                try:
                    ethnic_group_types_new_dict[group]=ethnic_group_types_new_dict[group]+1
                except:
                    ethnic_group_types_new_dict[group]=1
        else:
            try:
                ethnic_group_types_new_dict[ethnic_group_type]=ethnic_group_types_new_dict[ethnic_group_type]+ethnic_group_count
            except:
                ethnic_group_types_new_dict[ethnic_group_type]=ethnic_group_count

    return ethnic_group_types_new_dict



def generate_ethnic_groups_intersections(time_range):

    def for_time_range(time_range,query_part,period):
        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

#            print(query_part)
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]


            query = 'SELECT qitem, ethnic_group, gender, ccc_binary FROM '+languagecode+'wiki WHERE '+query_part+' AND (gender IS NOT NULL OR ethnic_group IS NOT NULL);'

            df = pd.read_sql_query(query, conn)#, parameters)
            people_count = len(df)

            ethnic_group_count = len(df.loc[df['ethnic_group'].notnull()])

            # languagecode  wp  wikidata_articles_qitems    ethnic_group
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','ethnic_group', ethnic_group_count,wpnumberofarticles,period)

            ethnic_group_types_count = group_dict_fix(df.ethnic_group.value_counts().to_dict())

            # languagecode  people  ethnic_group  ethnic_group_name
            for ethnic_group_type, ethnic_group_type_count in ethnic_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','ethnic_group',ethnic_group_type, ethnic_group_type_count,people_count,period)


            ccc_people = df.loc[df['ccc_binary'] == 1]
            ccc_people_count = len(ccc_people)
            ccc_people_ethnic_group_types_count = group_dict_fix(ccc_people.ethnic_group.value_counts().to_dict())

            # languagecode  people  ethnic_group  ethnic_group_name
            for ccc_ethnic_group_type, ccc_ethnic_group_type_count in ccc_people_ethnic_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc_people','ethnic_group',ccc_ethnic_group_type, ccc_ethnic_group_type_count, ccc_people_count, period)


            query = 'SELECT ethnic_group_topic FROM '+languagecode+'wiki WHERE '+query_part
            df = pd.read_sql_query(query, conn)#, parameters)

            ethnic_group_types_count = group_dict_fix(df.ethnic_group_topic.value_counts().to_dict())
            # languagecode  wp  ethnic_group  ethnic_group_name
            for ethnic_group_type, ethnic_group_type_count in ethnic_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','ethnic_group_topic',ethnic_group_type, ethnic_group_type_count,wpnumberofarticles,period)


        print ('languagecode, wp, wikidata_article_qitems, ethnic_group,'+period)
        print ('languagecode, people, ethnic_group, ethnic_group_name,'+period)
        print ('languagecode, ccc_people, wikidata_article_qitems, ethnic_group_name,'+period)
        print ('languagecode, wp, ethnic_group_topic, ethnic_group_name,'+period)


    if time_range == 'accumulated monthly':

        function_name = 'generate_ethnic_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_ethnic_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for_time_range(time_range,periods_accum[period],period)

        people_set = set()
        wikidata_article_qitems_ethnic_group_dict = {}
        wikidata_article_qitems_ethnic_group_topic_dict = {}

        language_ethnic_group_dict = {}
        language_ethnic_group_topic_dict = {}
        for languagecode in wikilanguagecodes:

            query = 'SELECT qitem, ethnic_group, gender FROM '+languagecode+'wiki WHERE (gender IS NOT NULL OR ethnic_group IS NOT NULL);'
            df = pd.read_sql_query(query, conn)#, parameters)

            ppl_lang = set(df.qitem.unique())
            people_set = people_set.union(ppl_lang)
            ethnic_group_count = len(df.loc[df['ethnic_group'].notnull()])
            people_count = len(ppl_lang)
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','wikidata_articles_qitems','ethnic_group', ethnic_group_count,people_count,period)

            language_ethnic_group_dict[languagecode] = group_dict_fix(df.ethnic_group.value_counts().to_dict())

            dict_eth_group_1 = df.loc[df['ethnic_group'].notnull()][['qitem','ethnic_group']].set_index('qitem').to_dict('dict')['ethnic_group']
            wikidata_article_qitems_ethnic_group_dict.update(dict_eth_group_1)


            query = 'SELECT qitem, ethnic_group_topic FROM '+languagecode+'wiki WHERE ethnic_group_topic IS NOT NULL;'
            df = pd.read_sql_query(query, conn)#, parameters)

            language_ethnic_group_topic_dict[languagecode] = group_dict_fix(df.ethnic_group_topic.value_counts().to_dict())

            dict_eth_group_2 = df.loc[df['ethnic_group_topic'].notnull()][['qitem','ethnic_group_topic']].set_index('qitem').to_dict('dict')['ethnic_group_topic']
            wikidata_article_qitems_ethnic_group_topic_dict.update(dict_eth_group_2)


        # ethnic groups people
        wikidata_df = pd.DataFrame(wikidata_article_qitems_ethnic_group_dict.items(),columns=['qitem','ethnic_group'])
        wikidata_article_ethnic_group_qitems_count_dict = group_dict_fix(wikidata_df.ethnic_group.value_counts().to_dict())

        for languagecode, ethnic_group_types_dict in language_ethnic_group_dict.items():
            for ethnic_group_type, ethnic_group_type_count in ethnic_group_types_dict.items():
                insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_ethnic_group',ethnic_group_type,languagecode,'wp', ethnic_group_type_count,wikidata_article_ethnic_group_qitems_count_dict[ethnic_group_type],period)
  

        # ethnic groups topic
        wikidata_df = pd.DataFrame(wikidata_article_qitems_ethnic_group_topic_dict.items(),columns=['qitem','ethnic_group_topic'])
        wikidata_article_ethnic_group_topic_qitems_count_dict = group_dict_fix(wikidata_df.ethnic_group_topic.value_counts().to_dict())

        for languagecode, ethnic_group_types_dict in language_ethnic_group_topic_dict.items():
            for ethnic_group_type, ethnic_group_type_count in ethnic_group_types_dict.items():

                insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_ethnic_group_topic',ethnic_group_type,languagecode,'wp', ethnic_group_type_count,wikidata_article_ethnic_group_topic_qitems_count_dict[ethnic_group_type],period)


        ethnic_group_sum = 0
        for ethnic_group_type, ethnic_group_type_count in wikidata_article_ethnic_group_qitems_count_dict.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','ethnic_group',ethnic_group_type, ethnic_group_type_count,len(people_set),period)

            ethnic_group_sum += ethnic_group_type_count

        insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','wikidata_articles_qitems','ethnic_group',ethnic_group_sum,len(people_set),period)

        print ('languagecode, people, wikidata_articles_qitems, ethnic_group, ' +period)
        print ('wikidata_articles_qitems_ethnic_group, ethnic_group_name, languagecode, wp, ' +period)
        print ('wikidata_articles_ethnic_group_topic, ethnic_group_name, languagecode, wp, ' +period)

        print ('wikidata_articles_qitems, people, ethnic_group, ethnic_group_name, ' +period)
        print ('wikidata_articles_qitems, people, wikidata_articles_qitems, ethnic_group, ' +period)

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)



def generate_supra_ethnic_groups_intersections(time_range):

    def for_time_range(time_range,query_part,period):
        # languages
        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

#            print(query_part)
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki WHERE '+query_part
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]


            query = 'SELECT qitem, supra_ethnic_group, gender, ccc_binary FROM '+languagecode+'wiki WHERE '+query_part+' AND (gender IS NOT NULL OR supra_ethnic_group IS NOT NULL);'

            df = pd.read_sql_query(query, conn)#, parameters)
            people_count = len(df)

            supra_ethnic_group_count = len(df.loc[df['supra_ethnic_group'].notnull()])

            # languagecode  wp  wikidata_articles_qitems    supra_ethnic_group
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','supra_ethnic_group', supra_ethnic_group_count,wpnumberofarticles,period)

            supra_ethnic_group_types_count = group_dict_fix(df.supra_ethnic_group.value_counts().to_dict())

            # languagecode  people  supra_ethnic_group  supra_ethnic_group_name
            for sex_type, sex_type_count in supra_ethnic_group_types_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','supra_ethnic_group',sex_type, sex_type_count,people_count,period)

            print ('languagecode, wp, wikidata_article_qitems, supra_ethnic_group,'+period)
            print ('languagecode, people, supra_ethnic_group, supra_ethnic_group_name,'+period)


    if time_range == 'accumulated monthly':

        function_name = 'generate_supra_ethnic_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated':

        function_name = 'generate_supra_ethnic_groups_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for_time_range(time_range,periods_accum[period],period)

        people_set = set()
        wikidata_article_qitems_supra_ethnic_group_dict = {}
        language_supra_ethnic_group_dict = {}
        for languagecode in wikilanguagecodes:

            query = 'SELECT qitem, supra_ethnic_group, gender FROM '+languagecode+'wiki WHERE (gender IS NOT NULL OR supra_ethnic_group IS NOT NULL);'
            df = pd.read_sql_query(query, conn)#, parameters)

            ppl_lang = set(df.qitem.unique())
            people_set = people_set.union(ppl_lang)

            language_supra_ethnic_group_dict[languagecode] = group_dict_fix(df.supra_ethnic_group.value_counts().to_dict())

            dict_eth_group = df.loc[df['supra_ethnic_group'].notnull()][['qitem','supra_ethnic_group']].set_index('qitem').to_dict('dict')['supra_ethnic_group']
            wikidata_article_qitems_supra_ethnic_group_dict.update(dict_eth_group)


        wikidata_df = pd.DataFrame(wikidata_article_qitems_supra_ethnic_group_dict.items(),columns=['qitem','supra_ethnic_group'])
        wikidata_article_qitems_count_dict = group_dict_fix(wikidata_df.supra_ethnic_group.value_counts().to_dict())


        for languagecode, supra_ethnic_group_types_dict in language_supra_ethnic_group_dict.items():
            for supra_ethnic_group_type, supra_ethnic_group_type_count in supra_ethnic_group_types_dict.items():

                insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems',supra_ethnic_group_type,languagecode,'wp', supra_ethnic_group_type_count,wikidata_article_qitems_count_dict[supra_ethnic_group_type],period)

        supra_ethnic_group_sum = 0
        for supra_ethnic_group_type, supra_ethnic_group_type_count in wikidata_article_qitems_count_dict.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','supra_ethnic_group',supra_ethnic_group_type, supra_ethnic_group_type_count,len(people_set),period)

            supra_ethnic_group_sum += supra_ethnic_group_type_count

        insert_intersections_values(time_range,cursor2,'articles','wikidata_articles_qitems','people','wikidata_articles_qitems','supra_ethnic_group',supra_ethnic_group_sum,len(people_set),period)


        print ('wikidata_articles_qitems, supra_ethnic_group_type, languagecode, wp, ' +period)
        print ('wikidata_articles_qitems, people, supra_ethnic_group, supra_ethnic_group_type, ' +period)
        print ('wikidata_articles_qitems, people, wikidata_articles_qitems, supra_ethnic_group, ' +period)

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def generate_top_ccc_articles_lists_intersections():
    time_range = 'last accumulated'
    function_name = 'generate_top_ccc_articles_lists_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()
    period = cycle_year_month

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()

    all_articles = {}
    for languagecode_1 in wikilanguagecodes:
        qitems = set()
        query = 'SELECT qitem FROM '+languagecode_1+'wiki'
        for row in cursor.execute(query): qitems.add(row[0])
        all_articles[languagecode_1]=qitems
    print ('all loaded.')


    # PERHAPS: THIS SHOULD BE LIMITED TO 100 ARTICLES PER LIST.
    # CCC TOP ARTICLES LISTS
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions','edits','edited_last_month','images','wdproperty_many','interwiki_many','interwiki_editors','interwiki_wdproperty','wikirank','earth','monuments_and_buildings','sport_and_teams','glam','folk','music_creations_and_organizations','food','paintings','books','clothing_and_fashion','industry','religion','religious_group','sexual_orientation', 'ethnic_group']

    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        wpnumberofarticles=wikipedialanguage_currentnumberarticles[languagecode]
        all_top_ccc_articles_count = 0
        all_top_ccc_articles_coincident_count = 0

        all_ccc_lists_items=set()
        for list_name in lists:
            lists_qitems = set()

            for languagecode_2 in wikilanguagecodes:
#                query = 'SELECT qitem FROM '+languagecode_2+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM '+languagecode_2+'wiki_top_articles_lists);'

                query = 'SELECT qitem FROM '+languagecode_2+'wiki_top_articles_lists WHERE list_name ="'+list_name+'"'#+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode_2+'wiki_top_articles_lists);'
                for row in cursor4.execute(query):
                    all_ccc_lists_items.add(row[0])
                    lists_qitems.add(row[0])

            all_top_ccc_articles_count+=len(lists_qitems)
            ccc_list_coincident_count=len(lists_qitems.intersection(all_articles[languagecode]))

            insert_intersections_values(time_range,cursor2,'articles','top_ccc_articles_lists',list_name,languagecode,'wp',ccc_list_coincident_count,len(lists_qitems), period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','top_ccc_articles_lists',list_name,ccc_list_coincident_count,wpnumberofarticles, period)


        #  CCC Top articles lists - sum spread and sum coverage
        for languagecode_2 in wikilanguagecodes:
            qitems_unique = set()
            country = ''
#                query = 'SELECT qitem, country FROM '+languagecode_2+'wiki_top_articles_lists WHERE measurement_date IS (SELECT MAX(measurement_date) FROM '+languagecode_2+'wiki_top_articles_lists) AND position <= 100 ORDER BY country'

            query = 'SELECT qitem, country FROM '+languagecode_2+'wiki_top_articles_lists WHERE position <= 100 ORDER BY country'# AND measurement_date IS (SELECT MAX(measurement_date) FROM ccc_'+languagecode_2+'wiki_top_articles_lists)'
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
    print ('top_ccc_articles_lists, list_name, languagecode, wp,'+ period)
    print ('wp, languagecode, top_ccc_articles_lists, list_name,'+ period)

    print ('list_origin, all_top_ccc_articles, languagecode, wp,'+ period)

    print ('ccc, all_top_ccc_articles, languagecode, wp,'+ period)
    print ('languagecode, wp, ccc, all_top_ccc_articles,'+ period)



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def generate_monthly_articles_intersections(time_range):

    def for_time_range(time_range,query_part,period):

    #    wikilanguagecodes2=['ca']
        for languagecode in wikilanguagecodes:
            print ('\n'+languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)


            #  month articles
            qitems = set()
            query = 'SELECT qitem FROM '+languagecode+'wiki WHERE '+query_part
            for row in cursor.execute(query):
                qitems.add(row[0])
            created_articles_count = len(qitems)
            if created_articles_count == 0: continue

            # ALL created during the month
            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'wp', created_articles_count, created_articles_count, period)



            # wikipedia num of accumulated articles
            query = 'SELECT COUNT(*) FROM '+languagecode+'wiki'
            if time_range == 'monthly created': query+= ' WHERE '+periods_accum[period]
            cursor.execute(query)
            row = cursor.fetchone()
            wpnumberofarticles=row[0]
            if wpnumberofarticles == 0: continue

            # CCC accumulated articles
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary=1'
            if time_range == 'monthly created': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            try: ccc_articles_count = cursor.fetchone()[0]
            except: ccc_articles_count = 0

            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary=1 AND ccc_geolocated=1'
            if time_range == 'monthly created': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            try: ccc_geolocated_articles_count = cursor.fetchone()[0]
            except: ccc_geolocated_articles_count = 0

            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE ccc_binary=1 AND keyword_title IS NOT NULL'
            if time_range == 'monthly created': query+= ' AND '+periods_accum[period]
            cursor.execute(query)
            try: ccc_keywords_articles_count = cursor.fetchone()[0]
            except: ccc_keywords_articles_count = 0



            # PEOPLE accumulated articles
            last_month_peoplecount = 0
            for q, gender in gender_dict.items():
                query = 'SELECT qitem FROM '+languagecode+'wiki WHERE '+ query_part + ' AND gender = "'+q+'"'
                try: month_articles_q_count = cursor.fetchone()[0]
                except: month_articles_q_count = 0
                last_month_peoplecount+= month_articles_q_count
                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', languagecode, 'male', month_articles_q_count,created_articles_count,period)

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', languagecode, 'people', last_month_peoplecount,created_articles_count,period)


            # sexual orientation
            query = 'SELECT count(*), sexual_orientation FROM '+languagecode+'wiki WHERE '+ query_part+' GROUP BY 2'
            for row in cursor.execute(query):
                sexual_orientation = row[1]
                sexual_orientation_count = row[0]

                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', 'sexual_orientation', sexual_orientation, sexual_orientation_count, created_articles_count, period)


            # religious group
            query = 'SELECT count(*), religious_group FROM '+languagecode+'wiki WHERE '+ query_part+' GROUP BY 2'
            for row in cursor.execute(query):
                religious_group = row[1]
                religious_group_count = row[0]

                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', 'religious_group', religious_group, religious_group_count, created_articles_count, period)


            # ethnic group
            ethnic_group_dict = {}
            query = 'SELECT count(*), ethnic_group FROM '+languagecode+'wiki WHERE '+ query_part+' GROUP BY 2'
            for row in cursor.execute(query):
                ethnic_group = row[1]
                ethnic_group_count = row[0]
                ethnic_group_dict[ethnic_group] = ethnic_group_count

            ethnic_group_dict = group_dict_fix(ethnic_group_dict)

            for ethnic_group, ethnic_group_count in ethnic_group_dict.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', 'ethnic_group', ethnic_group, ethnic_group_count, created_articles_count, period)                


            # ethnic group topic
            ethnic_group_dict = {}
            query = 'SELECT count(*), ethnic_group_topic FROM '+languagecode+'wiki WHERE '+ query_part+' GROUP BY 2'
            for row in cursor.execute(query):
                ethnic_group = row[1]
                ethnic_group_count = row[0]
                ethnic_group_dict[ethnic_group] = ethnic_group_count

            ethnic_group_dict = group_dict_fix(ethnic_group_dict)

            for ethnic_group, ethnic_group_count in ethnic_group_dict.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp', 'ethnic_group_topic', ethnic_group, ethnic_group_count, created_articles_count, period)                

            # LGBT binary created during the month
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+ query_part + ' AND lgbt_topic=1'
            cursor.execute(query)
            try: lgbt_articles_created_count = cursor.fetchone()[0]
            except: lgbt_articles_created_count = 0

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'lgbt_topics', lgbt_articles_created_count, created_articles_count, period)



            # Time-related article created during the month
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+ query_part + ' AND (start_time IS NOT NULL OR end_time IS NOT NULL OR time_interval IS NOT NULL);'
            cursor.execute(query)
            try: time_articles_created_count = cursor.fetchone()[0]
            except: time_articles_created_count = 0

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'time', time_articles_created_count, created_articles_count, period)


            # CCC created during the month
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+ query_part + ' AND ccc_binary=1'
            cursor.execute(query)
            try: ccc_articles_created_count = cursor.fetchone()[0]
            except: ccc_articles_created_count = 0

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'ccc', ccc_articles_created_count, created_articles_count, period)

            # CCC geolocated created during the month
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+query_part+' AND ccc_binary=1 AND ccc_geolocated=1'
            cursor.execute(query)
            try: ccc_geolocated_articles_created_count = cursor.fetchone()[0]
            except: ccc_geolocated_articles_created_count = 0

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'ccc_geolocated', ccc_geolocated_articles_created_count, created_articles_count, period)

            # CCC keywords
            query = 'SELECT count(*) FROM '+languagecode+'wiki WHERE '+query_part+' AND ccc_binary=1 AND keyword_title IS NOT NULL'
            cursor.execute(query)
            try: ccc_keywords_articles_created_count = cursor.fetchone()[0]
            except: ccc_keywords_articles_created_count = 0

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'ccc_keywords', ccc_keywords_articles_created_count, created_articles_count, period)

            # Not own CCC
            not_own_ccc = wpnumberofarticles - ccc_articles_count
            not_own_ccc_created_count = created_articles_count - ccc_articles_created_count

            insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode, 'not_own_ccc', not_own_ccc_created_count, created_articles_count, period)
            

            # Other Langs CCC
            for languagecode_2 in wikilanguagecodes:
                if languagecode == languagecode_2: continue
                ccc_articles_created_count = len(qitems.intersection(lang_ccc_qitems[languagecode_2]))

                insert_intersections_values(time_range,cursor2,'articles',languagecode, 'wp',languagecode_2, 'ccc', ccc_articles_created_count, created_articles_count, period)


            # CCC TOP ARTICLES LISTS
            for list_name in lists:
    #           lists_qitems_count=len(lists_qitems)
                coincident_qitems = qitems.intersection(lists_dict[list_name])
                last_month_list_count=len(coincident_qitems)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','top_ccc_articles_lists',list_name,last_month_list_count,created_articles_count, period)

            coincident_qitems_all_qitems = len(all_qitems.intersection(set(qitems)))
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','ccc','all_top_ccc_articles',coincident_qitems_all_qitems,created_articles_count, period)


            # GEOLOCATED SEGMENTS
            country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions() # iso 3166 to X

            geolocated_articles_count = 0
            iso3166_articles = {}
            query = 'SELECT iso3166, COUNT(DISTINCT page_id) FROM '+languagecode+'wiki WHERE iso3166 IS NOT NULL AND '+query_part + ' GROUP BY iso3166'
    #        print (query)
            cursor.execute(query)
            for row in cursor.execute(query):
                iso3166_articles[row[0]]=row[1]
                geolocated_articles_count+=row[1]

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','geolocated',geolocated_articles_count,created_articles_count, period)

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
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','countries',iso3166_code,iso3166_count,created_articles_count, period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','countries',iso3166_code,iso3166_count,geolocated_articles_count, period)


            # subregions
            for subregion_name, subregion_count in subregions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','subregions',subregion_name,subregion_count,created_articles_count, period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','subregions',subregion_name,subregion_count,geolocated_articles_count, period)

            # regions
            for region_name, region_count in regions_count.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','regions',region_name,region_count,created_articles_count, period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','regions',region_name,region_count,geolocated_articles_count, period)


        conn2.commit()

        print ('languagecode, wp, languagecode, wp,'+period)

        print ('languagecode, wp, languagecode, male,'+period)
        print ('languagecode, wp, languagecode, female,'+period)
        print ('languagecode, wp, languagecode, people,'+period)


        print ('languagecode, wp, ethnic_group_topic, ethnic_group_name,'+period)  
        print ('languagecode, wp, ethnic_group, ethnic_group_name,'+period)  
        print ('languagecode, wp, religious_group, religious_group_name,'+period)  
        print ('languagecode, wp, sexual_orientation, sexual_orientation_name,'+period)  

        print ('languagecode, wp, languagecode, lgbt_topics,'+period)   
        print ('languagecode, wp, languagecode, time,'+period)   

        print ('languagecode, wp, languagecode, ccc,'+period)   
        print ('languagecode, wp, languagecode, ccc_geolocated,'+period)
        
        print ('languagecode, wp, languagecode, ccc_keywords,'+period)  
        print ('languagecode, wp, languagecode, not_own_ccc,'+period)
        print ('languagecode, wp, languagecode_2, ccc,'+period)
        
        print ('languagecode, wp, top_ccc_articles_lists, list_name,'+period)
        print ('languagecode, wp, ccc, all_top_ccc_articles,'+period)

        print ('languagecode, wp, wikidata_article_qitems, geolocated,'+period)  

        print ('languagecode, wp, countries, iso3166,'+period)
        print ('languagecode, wp, subregions, subregion_name,'+period)
        print ('languagecode, wp, regions, region_name,'+period)

        print ('languagecode, geolocated, countries, iso3166,'+period)
        print ('languagecode, geolocated, subregions, subregion_name,'+period)
        print ('languagecode, geolocated, regions, region_name,'+period)

        print ('languagecode, wp, countries, iso3166,'+period)
        print ('languagecode, wp, subregions, subregion_name,'+period)
        print ('languagecode, wp, regions, region_name,'+period)


    #### HERE IT STARTS

    function_name = 'generate_monthly_articles_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()


    # ccc
    lang_ccc_qitems={} 
    for languagecode in wikilanguagecodes:
        query = 'SELECT qitem FROM '+languagecode+'wiki WHERE ccc_binary=1;'
        ccc_qitems = set()
        for row in cursor.execute(query):
            ccc_qitems.add(row[0])
        lang_ccc_qitems[languagecode]=ccc_qitems

    # top diversity article lists
    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions','edits','edited_last_month','images','wdproperty_many','interwiki_many','interwiki_editors','interwiki_wdproperty','wikirank','earth','monuments_and_buildings','sport_and_teams','glam','folk','music_creations_and_organizations','food','paintings','books','clothing_and_fashion','industry','religion','religious_group','sexual_orientation', 'ethnic_group']


    all_qitems = set()
    lists_dict = {}
    for list_name in lists:
        lists_qitems = set()
        for languagecode in wikilanguagecodes:
            query = 'SELECT qitem FROM '+languagecode+'wiki_top_articles_lists WHERE list_name ="'+list_name+'" AND measurement_date IS (SELECT MAX(measurement_date) FROM '+languagecode+'wiki_top_articles_lists)'
            try:
                for row in cursor4.execute(query):
                    lists_qitems.add(row[0])
                    all_qitems.add(row[0])
            except:
                pass
        lists_dict[list_name] = lists_qitems
#            print (len(lists_qitems))


    if time_range == 'monthly created':
        for period in sorted(periods_monthly.keys()):
            print (time_range,period,periods_monthly[period])
            for_time_range(time_range,periods_monthly[period],period)

    if time_range == 'last month':
        period = cycle_year_month
        query_part = periods_monthly[period]
        print (time_range,period,query_part)
        for_time_range(time_range,query_part,period)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)
    


def generate_time_intersections(time_range):

    def for_time_range(time_range,query_part,period):

        all_wikidata_items = set()
        all_wikidata_time_items = set()
        all_wikidata_start_time_items = set()

        language_time_qitems = {}

        for languagecode in wikilanguagecodes:
            print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime)))

            query = 'SELECT qitem, page_title, geocoordinates, gender, ccc_binary, start_time, end_time, time_interval FROM '+languagecode+'wiki WHERE '+query_part

            df = pd.read_sql_query(query, conn)#, parameters)

            all_wikidata_items = all_wikidata_items.union(set(df.qitem.unique()))

            time_related_items = df.loc[(df['time_interval'].notnull()) | (df['start_time'].notnull()) | (df['end_time'].notnull())]
            time_related_qs = set(time_related_items.qitem.unique())
            language_time_qitems[languagecode] = len(time_related_qs)
            all_wikidata_time_items = all_wikidata_time_items.union(time_related_qs)
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','time', len(time_related_qs), len(df), period)

            all_start_times = df.loc[(df['start_time'].notnull())][['qitem','start_time']]
            all_start_time_lang = set(all_start_times.qitem.unique())
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','start_time', len(all_start_time_lang), len(df), period)


            geolocated_items = df.loc[(df['geocoordinates'].notnull())].qitem.unique()
            geolocated_start_time = df.loc[(df['geocoordinates'].notnull()) & (df['start_time'].notnull())].qitem.unique()
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','time','start_time', len(geolocated_start_time), len(geolocated_items), period)


            geolocated_items = df.loc[(df['gender'].notnull())].qitem.unique()
            geolocated_start_time = df.loc[(df['gender'].notnull()) & (df['start_time'].notnull())].qitem.unique()
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','time','start_time', len(geolocated_start_time), len(geolocated_items), period)

            geolocated_items = df.loc[(df['gender'] == 'Q6581097')].qitem.unique() # male
            geolocated_start_time = df.loc[(df['gender'] == 'Q6581097') & (df['start_time'].notnull())].qitem.unique()
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'male','time','start_time', len(geolocated_start_time), len(geolocated_items), period)


            geolocated_items = df.loc[(df['gender'] == 'Q6581072')].qitem.unique() # female
            geolocated_start_time = df.loc[(df['gender'] == 'Q6581072') & (df['start_time'].notnull())].qitem.unique()
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'female','time','start_time', len(geolocated_start_time), len(geolocated_items), period)


            geolocated_items = df.loc[(df['ccc_binary'] == 1)].qitem.unique()
            geolocated_start_time = df.loc[(df['ccc_binary'] == 1) & (df['start_time'].notnull())].qitem.unique()
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','time','start_time', len(geolocated_start_time), len(geolocated_items), period)


        all_wikidata_time_items_count = len(all_wikidata_time_items)
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems', None,'time','time', all_wikidata_time_items_count, len(all_wikidata_items), period)

        for languagecode in wikilanguagecodes:
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','time', language_time_qitems[languagecode], all_wikidata_time_items_count, period)


        print ('wikidata_article_qitems, time, languagecode, wp'+period)
        print ('wikidata_article_qitems, , time, time,'+period)

        print ('languagecode, wp, wikidata_article_qitems, time,'+period)
        print ('languagecode, wp, wikidata_article_qitems, start_time,'+period)

        print ('languagecode, geolocated, time, start_time,'+period)
        print ('languagecode, people, time, start_time,'+period)
        print ('languagecode, male, time, start_time,'+period)
        print ('languagecode, female, time, start_time,'+period)
        print ('languagecode, ccc, time, start_time,'+period)



    if time_range == 'accumulated monthly': # this is A in the spreadsheet

        function_name = 'generate_time_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

        for period in sorted(periods_accum.keys()):
            print (time_range,period,'\t',periods_accum[period])
            for_time_range(time_range,periods_accum[period],period)
        conn2.commit()

        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


    if time_range == 'last accumulated': # this is B in the spreadsheet

        function_name = 'generate_time_intersections '+time_range
        if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

        functionstartTime = time.time()
        period = cycle_year_month

        conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
        conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()


        all_wikidata_items = set()
        all_wikidata_time_items = set()
        all_wikidata_start_time_items = set()

        all_wikidata_people_items = set()
        all_wikidata_people_time_items = set()

        all_wikidata_geolocated_items = set()
        all_wikidata_geolocated_time_items = set()

        all_wikidata_starts_century_dict = {}
        language_time_qitems = {}

        for languagecode in wikilanguagecodes:

            query = 'SELECT qitem, start_time, end_time, time_interval, gender, geocoordinates, ccc_binary FROM '+languagecode+'wiki;'
            df = pd.read_sql_query(query, conn)#, parameters)


            ###
            all_wikidata_items = all_wikidata_items.union(set(df.qitem.unique()))
            time_related_items = df.loc[(df['time_interval'].notnull()) | (df['start_time'].notnull()) | (df['end_time'].notnull())]
            time_related_qs = set(time_related_items.qitem.unique())
            language_time_qitems[languagecode] = len(time_related_qs)
            all_wikidata_time_items = all_wikidata_time_items.union(time_related_qs)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','time', len(time_related_qs), len(df), period)
            ###


            ###
            people_items = df.loc[(df['gender'].notnull())].qitem.unique()
            all_wikidata_people_items = all_wikidata_people_items.union(people_items)

            people_start_time = df.loc[(df['gender'].notnull()) & (df['start_time'].notnull())].qitem.unique()
            all_wikidata_people_time_items = all_wikidata_people_time_items.union(people_start_time)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'people','time','start_time', len(people_start_time), len(people_items), period)
            ###



            ###
            geolocated_items = df.loc[(df['geocoordinates'].notnull())].qitem.unique()
            all_wikidata_geolocated_items = all_wikidata_geolocated_items.union(geolocated_items)

            geolocated_start_time = df.loc[(df['geocoordinates'].notnull()) & (df['start_time'].notnull())].qitem.unique()
            all_wikidata_geolocated_time_items = all_wikidata_geolocated_time_items.union(geolocated_start_time)

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'geolocated','time','start_time', len(geolocated_start_time), len(geolocated_items), period)
            ###


            ###
            all_start_times = df.loc[(df['start_time'].notnull())][['qitem','start_time']]
            all_start_time_lang = set(all_start_times.qitem.unique())

            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','start_time', len(all_start_time_lang), len(df), period)

            all_wikidata_start_time_items = all_wikidata_start_time_items.union(all_start_time_lang)
            all_start_time_count = len(all_start_times)
            all_starts = all_start_times.start_time.value_counts().to_dict()
            starts_century_dict = {}
            for year, count in all_starts.items():

                if year > 0:
                    century = (year - 1) // 100 + 1 
                if year < 0:
                    century = (year - 1) // 100
                elif year == 0:
                    century = 1

                try:
                    all_wikidata_starts_century_dict[century]+=count
                except:
                    all_wikidata_starts_century_dict[century]=count

                try:
                    starts_century_dict[century]+=count
                except:
                    starts_century_dict[century]=count

            for century, count in starts_century_dict.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'time','century',century, count, all_start_time_count, period)
            ###

            ###
            ccc_start_time = df.loc[(df['ccc_binary'] == 1) & (df['start_time'].notnull())]
            ccc_start_time_count = len(ccc_start_time)
            all_starts_ccc = ccc_start_time.start_time.value_counts().to_dict()
            ccc_starts_century_dict = {}
            for year, count in all_starts_ccc.items():

                if year > 0:
                    century = (year - 1) // 100 + 1 
                if year < 0:
                    century = (year - 1) // 100
                elif year == 0:
                    century = 1

                try:
                    ccc_starts_century_dict[century]+=count
                except:
                    ccc_starts_century_dict[century]=count

            for century, count in ccc_starts_century_dict.items():
                insert_intersections_values(time_range,cursor2,'articles',languagecode,'ccc_time','century',century, count, ccc_start_time_count, period)
            ###
        

        for century, count in all_wikidata_starts_century_dict.items():
            insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','time','century',century, count, len(all_wikidata_start_time_items), period)

        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','people','time','start_time', len(all_wikidata_people_time_items), len(all_wikidata_people_items), period)


        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems','geolocated','time','start_time', len(all_wikidata_geolocated_time_items), len(all_wikidata_geolocated_items), period)


        all_wikidata_time_items_count = len(all_wikidata_time_items)
        insert_intersections_values(time_range,cursor2,'articles','wikidata_article_qitems', None,'time','time', all_wikidata_time_items_count, len(all_wikidata_items), period)

        for languagecode in wikilanguagecodes:
            insert_intersections_values(time_range,cursor2,'articles',languagecode,'wp','wikidata_article_qitems','time', language_time_qitems[languagecode], all_wikidata_time_items_count, period)


        print ('wikidata_article_qitems, , time, time, '+period)
        print ('wikidata_article_qitems, time, languagecode, wp, '+period)

        print ('languagecode, wp, wikidata_article_qitems, time,'+period)
        print ('languagecode, wp, wikidata_article_qitems, start_time,'+period)

        print ('languagecode, people, time, start_time, '+period)
        print ('wikidata_article_qitems, people, time, start_time, '+period)

        print ('languagecode, geolocated, time, start_time, '+period)
        print ('wikidata_article_qitems, geolocated, time, start_time, '+period)

        print ('wikidata_article_qitems, time, century, century_name, '+period)
        print ('languagecode, ccc_time, century, century_name, '+period)
        print ('languagecode, time, century, century_name, '+period)


        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)




def generate_pageviews_intersections():
    time_range = 'last month'    

    function_name = 'generate_pageviews_intersections '+time_range
    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return

    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()
    conn4 = sqlite3.connect(databases_path + top_diversity_db); cursor4 = conn4.cursor()

    period = cycle_year_month


    # CCC
    languages_ccc = {}
    languages_pageviews={}
    all_ccc_lists_items=set()
    wikipedialanguage_ccclistsitems={}

    wikipedialanguage_numberpageviews={}
    wikipedialanguageccc_numberpageviews={}

    for languagecode in wikilanguagecodes:
        qitems_pageviews={}
        pageviews = 0
        ccc_pageviews = 0
        ccc = set()

        ccc_qitem_count = 0
        geolocated_regions_count = {}
        people_count = {}

        i = 0
        query = 'SELECT qitem, num_pageviews, ccc_binary, gender, region FROM '+languagecode+'wiki ORDER BY num_pageviews DESC;'
        for row in cursor.execute(query):
            i+=1

            qitem = row[0]
            pv = row[1]
            if pv == None: pv = 0
            qitems_pageviews[qitem]=pv

            ccc_binary = row[2]
            if ccc_binary != None: ccc_binary = int(ccc_binary)
            if ccc_binary==1: 
                ccc.add(qitem)
                ccc_qitem_count += 1

            gender = row[3]
            region = row[4]

            if i <= 1000:
                if region != None:
                    try:
                        geolocated_regions_count[region]=geolocated_regions_count[region]+1
                    except:
                        geolocated_regions_count[region]=1

                if gender != None:
                    try:
                        people_count[gender]=people_count[gender]+1
                    except:
                        people_count[gender]=1

            if i == 10:
                for region, count in geolocated_regions_count.items():
                    insert_intersections_values(time_range,cursor2,'articles',languagecode,'top10pageviews','regions',region,count,10,period)

                for gender, count in people_count.items():
                    if gender in gender_dict:
                        insert_intersections_values(time_range,cursor2,'articles',languagecode,'top10pageviews','people',gender_dict[gender],count,10,period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'top10pageviews',languagecode,'ccc',ccc_qitem_count,10,period)

            if i == 100:
                for region, count in geolocated_regions_count.items():
                    insert_intersections_values(time_range,cursor2,'articles',languagecode,'top100pageviews','regions',region,count,100,period)

                for gender, count in people_count.items():
                    if gender in gender_dict:
                        insert_intersections_values(time_range,cursor2,'articles',languagecode,'top100pageviews','people',gender_dict[gender],count,100,period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'top100pageviews',languagecode,'ccc',ccc_qitem_count,100,period)

            if i == 500:
                for region, count in geolocated_regions_count.items():
                    insert_intersections_values(time_range,cursor2,'articles',languagecode,'top500pageviews','regions',region,count,500,period)

                for gender, count in people_count.items():
                    if gender in gender_dict:
                        insert_intersections_values(time_range,cursor2,'articles',languagecode,'top500pageviews','people',gender_dict[gender],count,500,period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'top500pageviews',languagecode,'ccc',ccc_qitem_count,500,period)

            if i == 1000:
                for region, count in geolocated_regions_count.items():
                    insert_intersections_values(time_range,cursor2,'articles',languagecode,'top1000pageviews','regions',region,count,1000,period)

                for gender, count in people_count.items():
                    if gender in gender_dict:
                        insert_intersections_values(time_range,cursor2,'articles',languagecode,'top1000pageviews','people',gender_dict[gender],count,1000,period)

                insert_intersections_values(time_range,cursor2,'articles',languagecode,'top1000pageviews',languagecode,'ccc',ccc_qitem_count,1000,period)


        for qi,pv in qitems_pageviews.items():
            if qi in ccc:
                ccc_pageviews=ccc_pageviews+pv
            pageviews+=pv

        languages_pageviews[languagecode]=qitems_pageviews
        languages_ccc[languagecode]=ccc
        wikipedialanguage_numberpageviews[languagecode]=pageviews
        wikipedialanguageccc_numberpageviews[languagecode]=ccc_pageviews

        lists_qitems = []
        try:
            query = 'SELECT DISTINCT qitem FROM '+languagecode+'wiki_top_articles_lists WHERE measurement_date IS (SELECT MAX(measurement_date) FROM '+languagecode+'wiki_top_articles_lists) AND position <= 100'
            for row in cursor4.execute(query):
                qi = row[0]
                if qi in ccc:
                    all_ccc_lists_items.add(qi)
                    lists_qitems.append(qi)
        except:
            pass
        wikipedialanguage_ccclistsitems[languagecode]=lists_qitems

        pviews = 0
        try:
            query = 'SELECT DISTINCT qitem FROM '+languagecode+'wiki_top_articles_lists WHERE list_name ="pageviews" AND measurement_date IS (SELECT MAX(measurement_date) FROM '+languagecode+'wiki_top_articles_lists)'
            for row in cursor4.execute(query):
                qi = row[0]
                if qi in ccc:
                    pviews = pviews + qitems_pageviews[qi]
        except:
            pass

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'ccc','top_articles_lists','pageviews',pviews,ccc_pageviews,period)

    conn2.commit()
    print ('languagecode, wp, top_articles_lists, pageviews,'+period)


    # LANGUAGE CCC, TOP CCC LISTS
    for languagecode in wikilanguagecodes:
        print ('\n'+languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp',languagecode,'ccc',wikipedialanguageccc_numberpageviews[languagecode],wikipedialanguage_numberpageviews[languagecode],period)


        qitems_pageviews=languages_pageviews[languagecode]

        ccc_lists_pageviews=0
        qitems = wikipedialanguage_ccclistsitems[languagecode]
        for qitem in qitems:
            try:
                ccc_lists_pageviews+=qitems_pageviews[qitem]
            except:
                pass

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],period)

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'ccc',languagecode,'all_top_ccc_articles',ccc_lists_pageviews,wikipedialanguageccc_numberpageviews[languagecode],period)


        all_ccc_lists_pageviews=0
        for qitem in all_ccc_lists_items:
            try:
                all_ccc_lists_pageviews+=qitems_pageviews[qitem]
            except:
                pass

        insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp','ccc','all_top_ccc_articles',all_ccc_lists_pageviews,wikipedialanguage_numberpageviews[languagecode],period)

#    print (wikipedialanguage_numberpageviews)
#    print (wikipedialanguageccc_numberpageviews)
    conn2.commit()
    print ('languagecode, wp, languagecode, ccc, '+period)
    print ('languagecode, wp, languagecode, all_top_ccc_articles, '+period)
    print ('languagecode, ccc, languagecode, all_top_ccc_articles, '+period)
    print ('languagecode, wp, ccc, all_top_ccc_articles, '+period)


    # LANGUAGES AND LANGUAGES CCC PAGEVIEWS
    for languagecode_1 in wikilanguagecodes:
        print (languagecode_1 +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)
        qitems_pageviews=languages_pageviews[languagecode_1]

        for languagecode_2 in wikilanguagecodes:
            if languagecode_1 == languagecode_2: continue

            languagecode_2_ccc_pageviews=0
            ccc=languages_ccc[languagecode_2]
            for qitem in ccc:
                try:
                    languagecode_2_ccc_pageviews+=qitems_pageviews[qitem]
                except:
                    pass

            insert_intersections_values(time_range,cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'ccc',languagecode_2_ccc_pageviews,wikipedialanguage_numberpageviews[languagecode_1],period)

            languagecode_2_top_ccc_articles_lists_pageviews=0
            lists_ccc_qitems=wikipedialanguage_ccclistsitems[languagecode_2]
            for qitem in lists_ccc_qitems:
                try:
                    languagecode_2_top_ccc_articles_lists_pageviews+=qitems_pageviews[qitem]
                except:
                    pass

            insert_intersections_values(time_range,cursor2,'pageviews',languagecode_1,'wp',languagecode_2,'all_top_ccc_articles',languagecode_2_top_ccc_articles_lists_pageviews,wikipedialanguage_numberpageviews[languagecode_1],period)

    conn2.commit()
    print ('languagecode, wp, languagecode_2, ccc,'+period)
    print ('languagecode, wp, languagecode_2, all_top_ccc_articles,'+period)



    # GEOGRAPHY
    for languagecode in wikilanguagecodes:
        print (languagecode +'\t'+ str(datetime.timedelta(seconds=time.time() - functionstartTime))+'\t'+period)

        query = 'SELECT ccc_binary, num_pageviews, gender, region, iso3166 FROM '+languagecode+'wiki;'

        wp_pageviews_count = 0
        ccc_pageviews_count = 0

        gender_pageviews = {}
        region_pageviews = {}
        country_pageviews = {}
        for row in cursor.execute(query):
            ccc_binary = row[0]
            num_pageviews = row[1]
            if num_pageviews == None: num_pageviews = 0

            try:
                gender = gender_dict[row[2]]
            except:
                gender = None
            region = row[3]
            country = row[4]

            if gender != None:
                try:
                    gender_pageviews[gender]=gender_pageviews[gender]+num_pageviews
                except:
                    gender_pageviews[gender]=0

            if region != None:
                try:
                    region_pageviews[region]=region_pageviews[region]+num_pageviews
                except:
                    region_pageviews[region]=0

            if country != None:
                try:
                    country_pageviews[country]=country_pageviews[country]+num_pageviews
                except:
                    country_pageviews[country]=0

            wp_pageviews_count+= num_pageviews

        for country, country_pv in country_pageviews.items():
            insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp','country',country,country_pv,wp_pageviews_count, period)

        for region, region_pv in region_pageviews.items():
            insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp','region',region,region_pv,wp_pageviews_count, period)

        for gender, gender_pv in gender_pageviews.items():
            insert_intersections_values(time_range,cursor2,'pageviews',languagecode,'wp','gender',gender,gender_pv,wp_pageviews_count, period)
        
        conn2.commit()

    print ('languagecode, wp, country, iso3166,'+period)
    print ('languagecode, wp, region, region_name,'+period)
    print ('languagecode, wp, gender, gender_name,'+period)
    conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def generate_features_stats():

    function_name = 'generate_features_stats'

    if wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'check','')==1: return
    functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + stats_db); cursor2 = conn2.cursor()

    query_insert = 'INSERT OR IGNORE INTO wdo_stats (content, statistic, set1, set1descriptor, value, period) VALUES (?,?,?,?,?,?);'

    period = cycle_year_month
    for languagecode in wikilanguagecodes: #wikilanguagecodes[wikilanguagecodes.index('vec'):]
        query = 'SELECT qitem, ccc_binary, geocoordinates, gender, folk, earth, monuments_and_buildings, music_creations_and_organizations, sport_and_teams, food, paintings, glam,books,clothing_and_fashion, industry, religion, gender, sexual_orientation, ethnic_group, supra_ethnic_group, religious_group, num_editors, num_edits, num_images, num_pageviews, num_inlinks, num_references, num_bytes, num_outlinks, num_interwiki, num_wdproperty, num_discussions, date_created, featured_article, wikirank FROM '+languagecode+'wiki;'

        langdata = pd.read_sql_query(query,conn)
        langdata = langdata.set_index('qitem')
        langdata = langdata.fillna(0)

        features = ['num_editors','num_edits','num_images','num_pageviews','num_inlinks','num_references','num_bytes','num_outlinks','num_interwiki','num_wdproperty','num_discussions','date_created','featured_article','wikirank']

        params = []


        conditions_names = ['wp' ,'ccc', 'geolocated', 'people', 'men', 'women','sexual_orientation']
        for i in range(len(conditions_names)):

            condition = conditions_names[i]

            try:
                if condition == 'wp':
                    langdata_specific = langdata

                if condition == 'ccc':
                    langdata_specific = langdata.loc[langdata['ccc_binary']==1]

                if condition == 'geolocated':
                    langdata_specific = langdata.loc[langdata['geocoordinates']!=0]

                if condition == 'people':
                    langdata_specific = langdata.loc[langdata['gender']!=0]

                if condition == 'men':
                    langdata_specific = langdata.loc[langdata['gender']=="Q6581097"]

                if condition == 'women':
                    langdata_specific = langdata.loc[langdata['gender']=="Q6581072"]

                if condition == 'sexual_orientation':
                    langdata_specific = langdata.loc[langdata['sexual_orientation']!="Q1035954"]


            except:
                continue

            if langdata == None: continue

            for feature in features:
                stats = langdata_specific[feature].describe() # The 50 percentile is the same as the median.

                print ('\n')
                print (languagecode)
                print (condition)
                print (feature)
#                print (stats)
#                input('')

                content = feature
                set1 = languagecode
                set1descriptor = condition 

                params.append((content, 'mean', set1, set1descriptor, stats['mean'], period))
                params.append((content, 'std', set1, set1descriptor, stats['std'], period))
                params.append((content, 'median', set1, set1descriptor, stats['50%'], period))

            params.append(('count', 'count', set1, set1descriptor, stats['count'], period))

            cursor2.executemany(query_insert, params);
            conn2.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    wikilanguages_utils.verify_function_run(cycle_year_month, script_name, function_name, 'mark', duration)


def remove_create_wdo_stats_db():
    try:
        os.remove(databases_path + stats_db); print ('stats.db deleted.');
    except:
        pass



##################################################################################

def main_with_email():
    try:
        main()
    except:
        wikilanguages_utils.send_email_toolaccount('Stats Generation for CCC Error: '+ cycle_year_month, 'ERROR.')


def main_loop_retry():
    page = ''
    while page == '':
        try:
            main()        #          main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/stats_generation.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('Stats Generation for CCC Error: '+ year_month, 'ERROR.' + lines); print("Now let's try it again...")
            continue

#######################################################################################



class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("stats_generation"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("stats_generation"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    startTime = time.time()
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    script_name = 'stats_generation.py'
    cycle_year_month = wikilanguages_utils.get_current_cycle_year_month() # the cycle is always the last completed month
#    check_time_for_script_run(script_name, cycle_year_month)

    periods_monthly,periods_accum = wikilanguages_utils.get_months_queries()

    # Import the language-territories mappings
    # Import the Wikipedia languages characteristics
    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = languages.index.tolist()


    gender_dict = {'Q6581097':'male','Q6581072':'female', 'Q1052281':'transgender female','Q1097630':'intersex','Q1399232':"fa'afafine",'Q17148251':'travesti','Q19798648':'unknown value','Q207959':'androgyny','Q215627':'person','Q2449503':'transgender male','Q27679684':'transfeminine','Q27679766':'transmasculine','Q301702':'two-Spirit','Q303479':'hermaphrodite','Q3177577':'muxe','Q3277905':'māhū','Q430117':'Transgene','Q43445':'female non-human organism'}


    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=list(set(languages.index.tolist()) - set(list(territories.index.tolist())))
    # Only those with a geographical context
    wikilanguagecodes_real = wikilanguagecodes.copy()
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # Verify/Remove all languages without a table in wikipedia_diversity.db
    wikipedialanguage_currentnumberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'last')
    for languagecode in wikilanguagecodes:
        if languagecode not in wikipedialanguage_currentnumberarticles: wikilanguagecodes.remove(languagecode)
    wikilanguagecodes = list(wikipedialanguage_currentnumberarticles.keys())

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

#    if wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'check', '') == 1: exit()
    main()
#    main_with_exception_email()
#    main_loop_retry()
    duration = str(datetime.timedelta(seconds=time.time() - startTime))
    wikilanguages_utils.verify_script_run(cycle_year_month, script_name, 'mark', duration)

    wikilanguages_utils.finish_email(startTime,'stats_generation.out', 'Stats generation')


