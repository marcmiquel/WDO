# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
# time
# time
import time
import datetime
from dateutil import relativedelta, rrule
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
import bz2
import json
import csv


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("history_features"+''+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass

# MAIN
def main():


    languagecode = 'en'
    functionstartTime = time.time()
    dataset_date_month = '2019-08'
    d_paths = []

    dumps_path = '/public/dumps/public/other/mediawiki_history/'+dataset_date_month+'/'+languagecode+'wiki/all-time.tsv.bz2'
    print (dumps_path)
    if os.path.isfile(dumps_path):
        d_paths.append(dumps_path)

    else:
        for year in range (2001, 2025):
            dumps_path = '/public/dumps/public/other/mediawiki_history/'+dataset_date_month+'/'+languagecode+'wiki/'+str(year)+'.tsv.bz2'
            loop = os.path.isfile(dumps_path)
            if loop == True: 
                d_paths.append(dumps_path)
            else:
                break

        if len(d_paths) == 0:
            for year in range(2001, 2025): # months
                for month in range(1, 13):
                    if month > 9:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+dataset_date_month+'/'+languagecode+'wiki/'+str(year)+'-'+str(month)+'.tsv.bz2'
                    else:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+dataset_date_month+'/'+languagecode+'wiki/'+str(year)+'-0'+str(month)+'.tsv.bz2'

                    last = os.path.isfile(dumps_path)
                    if last == True:
                        d_paths.append(dumps_path)

    print(len(d_paths))
    print (d_paths)

    page_title_page_id = {}
    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values=line.split(' ')

        iter = 0
        while line != '':
            iter += 1
            if iter % 10000000 == 0: print (str(iter/10000000)+' million lines.')
            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values=line.split('\t')
#            print (values)

        print (str(datetime.timedelta(seconds=time.time() - iterTime)))

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print (duration)
    return





    for languagecode in wikilanguagecodes:
        (page_titles_qitems, page_titles_page_ids)=wikilanguages_utils.load_dicts_page_ids_qitems(0,languagecode)
        extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids)

    for languagecode in wikilanguagecodes:
        store_all_history_table(languagecode)
        store_all_history_table_monthly(languagecode,'last month')
        revisions_extend_editor_iterator(languagecode)


#### --- #### --- #### --- #### --- #### --- #### --- #### --- #### --- #### --- ####



def extend_articles_revision_features(languagecode, page_titles_qitems, page_titles_page_ids):

    functionstartTime = time.time()
    function_name = 'extend_articles_revision_features '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    num_editors = {}
    num_edits = {}
    num_edits_last_month = {}
    first_edit_timestamp = {}
    num_discussions = {}


    page_ids_qitems = {}
    for page_title,page_id in page_titles_page_ids.items():
        page_ids_qitems[page_id]=page_titles_qitems[page_title]

    for page_id in page_titles_page_ids.values():
        num_discussions[page_id]=0
        num_editors[page_id]=set()
        num_edits[page_id]=0
        num_edits_last_month[page_id]=0
        first_edit_timestamp[page_id]=''

    last_month_date = datetime.date.today() - relativedelta.relativedelta(months=1)
    first_day = int(last_month_date.replace(day = 1).strftime('%Y%m%d%H%M%S'))
    last_day = int(last_month_date.replace(day = calendar.monthrange(last_month_date.year, last_month_date.month)[1]).strftime('%Y%m%d%H%M%S'))

    d_paths = []
    i = 0
    loop = True
    while (loop):
        i+=1
        dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history'+str(i)+'.xml.gz'
        loop = os.path.isfile(dumps_path)
        dumps_path = '/srv/wcdo/dumps/enwiki-latest-stub-meta-history'+str(i)+'.xml'#.gz'
        if loop == True: 
            d_paths.append(dumps_path)
        if i==1 and loop == False:#True:
            d_paths.append('/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-stub-meta-history.xml.gz')

    print(len(d_paths))

    page_title_page_id = {}
    for dump_path in d_paths:

        print(dump_path)

        page_title = None
        cur_page_title = None
        ns = None
        page_id = None

        cur_time = time.time()
        i=0

        n_discussions = 0
        n_edits = 0
        n_editors = set()


#        with gzip.open(dump_path, 'rb') as xml_file:
        pages = etree.iterparse(dump_path, events=("start", "end"))
        for event, elem in pages:

            if event == 'start':

                if elem.tag == '{http://www.mediawiki.org/xml/export-0.10/}mediawiki':
                    root = elem

            elif event == 'end':
                # SECTION PAGETITLE, PAGEID, NS

                taggy = elem.tag.replace('{http://www.mediawiki.org/xml/export-0.10/}','')
                text =  elem.text
#                print (taggy)
#                print (text)

                if taggy == 'title': 
                    page_title = text.replace(' ','_')

                # if elem.attrib != None:
                #     try:
                #         page_title = elem.attrib['title'].replace(' ','_')
                #     except:
                #         pass

                if page_title != cur_page_title and cur_page_title != None:
                    num_edits[page_id] = n_edits
                    n_edits = 0
                    num_editors[page_id]=len(n_editors)
                    n_editors=set()
                    num_discussions[page_id]=n_discussions
                    page_id = None

                    i+=1
                    if i%100==0: 
                        last_time=cur_time
                        cur_time=time.time()
                        print('\t'+str(i)+' '+str(datetime.timedelta(seconds=cur_time - last_time)))
                        print(str(round(100/(cur_time - last_time),3))+' '+'pages per second.')

                cur_page_title = page_title

                if taggy == 'id' and page_id == None:
                    page_id = int(text)

                    if ns == 0:
                        try:
                          qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
                          page_title_page_id[cur_page_title]=page_id
                        except:
                            page_id = None

                    if ns == 1:
                        title = cur_page_title.split(':')[1]
                        try:
                            page_id = page_title_page_id[title]
                            qitem = page_ids_qitems[page_id] # only to create a page_id None in case it is not there
                        except:
                            page_id = None

                if taggy == 'ns':
                    ns = int(text)

                if (taggy == 'username' or taggy == 'ip') and page_id != None:
                    username = text

                    if ns == 0:
                        n_editors.add(username)
                        n_edits+=1

                    if ns == 1 and page_id!= None:
                        n_discussions+=1
#                        print(page_id,ns,cur_page_title)

                if taggy == 'timestamp' and ns == 0 and page_id != None: 

                    timestamp = datetime.datetime.strptime(text,'%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d%H%M%S')
                    if first_edit_timestamp[page_id]=='': first_edit_timestamp[page_id] = timestamp
                    timestamp = int(timestamp)
                    if timestamp > first_day and timestamp < last_day:
                        num_edits_last_month[page_id]+=1

                elem.clear()

    print('parsed')

    page_ids_page_titles = {v: k for k, v in page_title_page_id.items()}
    parameters = []
    for page_id in num_editors:
        # print((len(num_editors[page_id]), 
        #     num_edits[page_id], 
        #     num_edits_last_month[page_id], 
        #     first_edit_timestamp[page_id], 
        #     num_discussions[page_id], 
        #     page_id, 
        #     page_ids_qitems[page_id], 
        #     page_ids_page_titles[page_id]))

        try:
            parameters.append((num_editors[page_id], num_edits[page_id], num_edits_last_month[page_id], first_edit_timestamp[page_id], num_discussions[page_id], page_id, page_ids_qitems[page_id], page_ids_page_titles[page_id]))
        except:
            pass
#    print (len(parameters),len(page_titles_qitems))

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    query = 'UPDATE '+languagecode+'wiki SET (num_editors, num_edits, num_edits_last_month, date_created,num_discussions)=(?,?,?,?,?) WHERE page_id = ? AND qitem = ? AND page_title=?;'
    cursor.executemany(query,parameters)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)





def extend_articles_first_timestamp_lang():
    function_name = 'extend_articles_first_timestamp_lang'
    # if create_function_account_db(function_name, 'check','')==1: return
    # functionstartTime = time.time()

    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    
    lang_qitems_is_first_timestamp = {}
    qitems_timestamp_lang = {}
    for languagecode in wikilanguagecodes:
        print (languagecode)
        lang_qitems_is_first_timestamp[languagecode]=[]

        query = 'SELECT qitem, date_created FROM '+languagecode+'wiki;'
        for row in cursor.execute(query):
            qitem = row[0]
            first_timestamp = row[1]

            try:
                lang_timestamp = qitems_timestamp_lang[qitem]

                stored_timestamp = lang_timestamp[0]
                stored_lang = lang_timestamp[1]

                if stored_timestamp > first_timestamp:
                    qitems_timestamp_lang[qitem] = [first_timestamp,languagecode]

            except:
                qitems_timestamp_lang[qitem] = [first_timestamp,languagecode]

    parameters = []
    for qitem,lang_timestamp in qitems_timestamp_lang.items():
        parameters.append((str(lang_timestamp[1]),str(qitem)))
        stored_lang = lang_timestamp[1]

    print (len(parameters))
    print ('now introducing...')

    for languagecode in wikilanguagecodes:
        print (languagecode)
        query = 'UPDATE '+languagecode+'wiki SET first_timestamp_lang = ? WHERE qitem = ?;'
        cursor.executemany(query,parameters)
        conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



#######################################################################################


"""
USER DATA LEGEND:

source data:
* revision.db
wiki_revisions (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp)
wiki_revisions_monthly (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp)

target data:
* editor_diversity.db
wiki_editor_engagement_aggregated
wiki_editor_characteristics_aggregated
wiki_editors (user_id integer, user_name text, primarybinary integer, primarylang text, primarybinary_ecount, total_ecount, numberlangs integer, PRIMARY KEY (user_name, user_id

wiki_multilingualism_aggregated (user_id integer, user_name text, primarybinary integer, primarylang text, numberlangs integer, PRIMARY KEY (user_name, user_id)

wiki_ccc_distributed (user_id integer, user_name text, content_type text, target_lang text PRIMARY KEY (user_name, user_id))
-> els editors de cawiki i el què han editat de jawikiccc a cawiki.
"""


def store_all_history_table(languagecode):

    function_name = 'store_all_history_table '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime


    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS "+languagecode+"wiki_revisions (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_timestamp));")
    conn.commit()


    cursor.execute('SELECT max(rev_id) FROM '+languagecode+'wiki_revisions')
    max_rev_id=cursor.fetchone()[0]


    print ('Trying to run the query with batches.')
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute("SELECT max(rev_id) FROM revision;")
    maxval = mysql_cur_read.fetchone()[0]
    print (maxval)

#    filename = languagecode+'wiki_'+complete+'_'+last_month+'.csv'

    k=0
    i=0

    increment = 500000
#    increment = 250000
#    increment = 50000
#    increment = 25000

    iterations = int(maxval/increment)
    print ('There are '+str(iterations)+' intervals.')

    if max_rev_id != None:
        range_values = max_rev_id
    else:
        range_values = 0
    
    while (range_values < maxval):

        val1 = range_values
        range_values = range_values + increment
        if range_values > maxval: range_values = maxval
        val2 = range_values

        i+=1

        interval = 'AND rev_id BETWEEN '+str(val1)+' AND '+str(val2)
        query = 'SELECT rev_page, rev_user, rev_user_text, rev_timestamp, rev_id FROM revision WHERE rev_user != 0 '+interval

        print (query)
        parameters = []
        try:
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
    #        file = open(revisions_path+filename,'a') 

            j=0
            for row in rows:
                j+=1
    #            file.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+'\n')
    #            file.write(','.join(row))
                parameters.append((row[0],row[1],row[2].decode('utf-8'),row[3],row[4]))
            k+=j
        except:
            print ('the query timed out.')

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_revisions (rev_page, rev_user, rev_user_text, rev_timestamp, rev_id) VALUES (?,?,?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()

        print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')

        try:
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(j/tempo)+' rows per second.')
            print (str(int((iterations-i)*tempo))+' seconds as estimated remaining time.')
        except:
            print ('too fast.')

        print ('total of '+str(k)+' rows.')
        print ('* '+str(i)+' queries have been run out of '+str(iterations))

#        file.close()

        last_period_time = time.time()

    conn.close()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


def store_all_history_table_monthly(languagecode, time_range):

    function_name = 'store_all_history_table_monthly '+languagecode + ' '+time_range
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime


    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS "+languagecode+"wiki_revisions_monthly (rev_page integer, rev_user integer, rev_user_text text, rev_timestamp integer, rev_id integer, PRIMARY KEY (rev_user,rev_user_text,rev_timestamp));")

    conn.commit()


    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute("SELECT max(rev_id) FROM revision;")
    maxval = mysql_cur_read.fetchone()[0]
    print (maxval)
    print (time_range)


    if time_range == 'last month':
        period = list(sorted(periods_monthly.keys()))[len(periods_monthly-1)]
        interval = periods_monthly[period]
        periods_monthly = {}
        periods_monthly[period] = interval


    iterations = len(periods_monthly)

#    print (periods_monthly.keys())
    k=0
    i=0
    for period,interval in periods_monthly.items():
        i+=1

        interval = 'AND ' + interval
        query = 'SELECT rev_page, rev_user, rev_user_text, rev_timestamp, rev_id FROM revision WHERE rev_user != 0 '+interval

        print (query)

        try:
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
    #        file = open(revisions_path+filename,'a') 

            parameters = []

            j=0
            for row in rows:
                j+=1
    #            file.write(str(row[0])+','+str(row[1])+','+str(row[2])+','+str(row[3])+'\n')
    #            file.write(','.join(row))
                parameters.append((row[0],row[1],row[2],row[3],row[4]))
            k+=j
        except:
            print ('the query timed out.')

        query = 'INSERT OR IGNORE INTO '+languagecode+'_revisions_monthly (rev_page, rev_user, rev_user_text, rev_timestamp, rev_id) VALUES (?,?,?,?,?);';
        cursor.executemany(query,parameters)
        conn.commit()

        print (str(datetime.timedelta(seconds=time.time() - last_period_time))+' seconds.')

        try:
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(j/tempo)+' rows per second.')
            print (str(int(iterations*tempo))+' seconds as estimated remaining time.')
        except:
            print ('too fast.')

        print ('total of '+str(k)+' rows.')
        print ('* '+str(i)+' queries have been run out of '+str(iterations))

#        file.close()
        last_period_time = time.time()


    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)



# EXTENDED (REVISION TABLE) # https://www.mediawiki.org/wiki/Manual:Revision_table
def revisions_extend_editor_iterator(languagecode):

    function_name = 'revisions_extend_editor_iterator '+languagecode
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime

    conn = sqlite3.connect(databases_path + editor_diversity_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + revision_db); cursor2 = conn2.cursor()

    query = 'SELECT count(user_id) FROM '+languagecode+'wiki_editors;'
    cursor.execute(query)
    max_users = cursor.fetchone()[0]

    edits_page_ids = []; edits_timestamps = []

    # page_ids to intersect
    """
    page_ids_language_ccc = get_language_ccc_page_ids(languagecode)
    page_ids_namespaces = get_language_namespaces_page_ids(languagecode)
    user_id_userpage_page_id, user_id_userpage_talk_page_id = get_language_editors_user_page_ids(languagecode)
    """

    ccc_count = {}; 
    for languagecode2 in wikilanguagecodes: ccc_count[languagecode2]=0

    month_periods = []
    for day in datespan(datetime.date(2001, 1, 16), datetime.date.today(),delta=relativedelta.relativedelta(months=1)):
        month_period.append(day.strftime('%Y-%m'))


    ccc_params = []; 
    characteristics_params = []
    engagement_params = []

    rev_user = ''
    i = 0
    j = 0
    k = 0
    query = 'SELECT * FROM '+languagecode+'wiki_revisions ORDER BY rev_user, rev_timestamp ASC;'
    for row in cursor2.execute(query):

        cur_rev_user = row[1]
        cur_rev_page = row[0]
        cur_rev_user_text = row[2]
        cur_rev_timestamp = datetime.datetime.strptime(str(row[3].decode('utf-8')),'%Y%m%d%H%M%S')

        if cur_rev_user != rev_user and i!=0:
            j+=1

            # check the revisions
            """
            ccc_params += extend_editors_ccc_distributed(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_language_ccc, ccc_count)
            
            characteristics_params += extend_editors_namespaces_edits(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_namespaces, user_id_userpage_page_id, user_id_userpage_talk_page_id)
            """

            engagement_params += extend_editors_engagement_metrics(languagecode, rev_user, rev_user_text, edits_timestamps, month_period)

            edits_page_ids = []; edits_timestamps = []


        edits_page_ids.append(cur_rev_page)
        edits_timestamps.append(cur_rev_timestamp)

        rev_user=cur_rev_user
        rev_user_text=cur_rev_user_text
        i+=1

        # COUNT
        if i%1000000 == 0:

            print ('\nAccum. revision lines: '+str(i))
            print ('Accum. editors: '+str(j))


            # store the editors
            """
            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_ccc_distributed (user_id, user_name, edit_count, lang_ccc) VALUES (?,?,?,?);';
            cursor.executemany(query,ccc_params)
            conn.commit()

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_characteristics_aggregated (user_id, user_name, namespace_0_edits, namespace_1_edits, namespace_2_edits, namespace_3_edits, namespace_4_edits, namespace_5_edits, namespace_6_edits, namespace_7_edits, namespace_8_edits, namespace_8_edits, namespace_10_edits, namespace_11_edits, namespace_12_edits, namespace_13_edits, namespace_14_edits, namespace_15_edits, userpage_edits, userpage_talk_edits) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
            cursor.executemany(query,characteristics_params)
            conn.commit()
            """

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_engagement_aggregated (user_id, user_name, first_edit_timestamp, last_edit_timestamp, registered_within_month, registered_within_year, active_editor_last_month, edit_count, onemonthago_edit_count, twomonthago_edit_count, threemonthago_edit_count, edit_count_24h, edit_count_7d, edit_count_30d, edit_count_60d, edit_count_after_60d, active_months, total_months, max_months_row, lifetime_days, drop_off_days) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);';
            cursor.executemany(query,engagement_params)
            conn.commit()

            total = int(datetime.timedelta(seconds=time.time() - functionstartTime).total_seconds())
            tempo = int(datetime.timedelta(seconds=time.time() - last_period_time).total_seconds())
            print (str(round(1000000/tempo,3))+' rows per second in this iteration.')
            print (str(round(j/total,3))+' editors per second in total.')
            print (str(int(max_users-j/round(j/total,3)))+' seconds as estimated remaining time.')
            print ('Time for group of iterations: '+str(tempo)+' sec.')
            print ('Total time: ' + str(total))+' sec.'
            last_period_time = time.time()


    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_ccc_distributed (user_id, user_name, edit_count, lang_ccc) VALUES (?,?,?,?);';
    cursor.executemany(query,ccc_params)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)


# * EDITOR CHARACTERISTICS METRICS
# e.g. cawiki_editors_characteristic

def extend_editors_namespaces_edits(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_namespaces, user_id_userpage_page_id, user_id_userpage_talk_page_id):

    try: userpage_page_id = user_id_userpage_page_id[rev_user]
    except: userpage_page_id = None
    try: userpage_talk_page_id = user_id_userpage_talk_page_id[rev_user]
    except: userpage_talk_page_id = None

    
    namespace_0_edits = 0; namespace_1_edits = 0; namespace_2_edits = 0; namespace_3_edits = 0; namespace_4_edits = 0; namespace_5_edits = 0; namespace_6_edits = 0; namespace_7_edits = 0; namespace_8_edits = 0; namespace_8_edits = 0; namespace_10_edits = 0; namespace_11_edits = 0; namespace_12_edits = 0; namespace_13_edits = 0; namespace_14_edits = 0; namespace_15_edits = 0
    userpage_page_id_edits = 0; userpage_talk_page_id_edits = 0

    for rev in edits_page_ids:
        if rev == userpage_page_id: userpage_page_id_edits += 1
        elif rev == userpage_talk_page_id: userpage_talk_page_id_edits += 1
        try:
            if page_ids_namespaces[rev] == 0: namespace_0_edits += 1
            elif page_ids_namespaces[rev] == 1: namespace_1_edits += 1
            elif page_ids_namespaces[rev] == 2: namespace_2_edits += 1
            elif page_ids_namespaces[rev] == 3: namespace_3_edits += 1
            elif page_ids_namespaces[rev] == 4: namespace_4_edits += 1
            elif page_ids_namespaces[rev] == 5: namespace_5_edits += 1
            elif page_ids_namespaces[rev] == 6: namespace_6_edits += 1
            elif page_ids_namespaces[rev] == 7: namespace_7_edits += 1
            elif page_ids_namespaces[rev] == 8: namespace_8_edits += 1
            elif page_ids_namespaces[rev] == 9: namespace_9_edits += 1
            elif page_ids_namespaces[rev] == 10: namespace_10_edits += 1
            elif page_ids_namespaces[rev] == 11: namespace_11_edits += 1
            elif page_ids_namespaces[rev] == 12: namespace_12_edits += 1
            elif page_ids_namespaces[rev] == 13: namespace_13_edits += 1
            elif page_ids_namespaces[rev] == 14: namespace_14_edits += 1
            elif page_ids_namespaces[rev] == 15: namespace_15_edits += 1
        except:
            continue

    characteristics_params = [(rev_user, rev_user_text, 
        namespace_0_edits, namespace_1_edits, namespace_2_edits, namespace_3_edits, namespace_4_edits, namespace_5_edits, namespace_6_edits, namespace_7_edits, namespace_8_edits, namespace_8_edits, namespace_10_edits, namespace_11_edits, namespace_12_edits, namespace_13_edits, namespace_14_edits, namespace_15_edits, userpage_page_id_edits, userpage_talk_page_id_edits)]

    return characteristics_params


# general
def extend_editors_engagement_metrics(languagecode, rev_user, rev_user_text, edits_timestamps, month_period):
    # aquí faríem tots els càlculs amb els timestamps passats a datetime.

    # INITIAL DATA
    user_id = rev_user
    user_name = rev_user_text

    first = edits_timestamps[0]
    first_edit_timestamp = first.strftime('%Y%m%d%H%M%S')
    last = edits_timestamps[len(edits_timestamps)-1]; 
    last_edit_timestamp = last.strftime('%Y%m%d%H%M%S')

    lifetime_days = last - first
    drop_off_days = today - last

    edit_count = len(edits_timestamps)

    edit_count_24h = ''
    edit_count_7d = ''
    edit_count_30d = ''
    edit_count_60d = ''
    onemonthago_edit_count = ''
    twomonthago_edit_count = ''
    threemonthago_edit_count = ''

    # THRESHOLDS
    one_month_ago = datetime.date.today() - relativedelta.relativedelta(months=1)
    two_month_ago = datetime.date.today() - relativedelta.relativedelta(months=2)
    three_month_ago = datetime.date.today() - relativedelta.relativedelta(months=3)
    one_year_ago = datetime.date.today() - relativedelta.relativedelta(year=1)

#    24h_after_firstedit = first + relativedelta.relativedelta(hours=24)
#    7d_after_firstedit = first + relativedelta.relativedelta(days=7)
#    60d_after_firstedit = first + relativedelta.relativedelta(days=60)


    # total_months = r.relativedelta.relativedelta(last,first).months


    # # ITERATION
    # months_active = []
    # i = 0
    # for cur_edit_date in edits_timestamps:
    #     i+=1
    #     if cur_edit_date > 24h_after_firstedit and edit_count_24h == '': edit_count_24h = i
    #     if cur_edit_date > 7d_after_firstedit and edit_count_7d == '': edit_count_7d = i
    #     if cur_edit_date > 30d_after_firstedit and edit_count_30d == '': edit_count_30d = i
    #     if cur_edit_date > 60d_after_firstedit and edit_count_60d == '': edit_count_60d = i
    #     if cur_edit_date > three_month_ago and threemonthago_edit_count == '': threemonthago_edit_count = i
    #     if cur_edit_date > two_month_ago and twomonthago_edit_count == '': twomonthago_edit_count = i
    #     if cur_edit_date > one_month_ago and onemonthago_edit_count == '': onemonthago_edit_count = i

    #     cur_month = cur_edit_date.strftime('%Y-%m')
    #     if cur_month not in months_active: months_active.append(cur_month)

    # active_months = len(months_active)


    # j = 0
    # for month in months_active:

    #     cur_month = datetime.datetime.strptime(month,'%Y-%m')
    #     expected_next_month = prev_month + relativedelta.relativedelta(months=1)

    #     if cur_month == expected_next_month:
    #         j+= 1
    #     else:
    #         j=0

    #     prev_month = cur_month

    # max_months_row = str(j)

    # edit_count_after_60d = edit_count - edit_count_60d

    # if first < one_month_ago: registered_within_month = 1
    # else: registered_within_month = 0

    # if last > one_month_ago: active_editor_last_month = 1
    # else: active_editor_last_month = 0

    # engagement_params = []

    # return engagement_params




#### --- ########################################################################

def extend_editors_ccc_distributed(languagecode, rev_user, rev_user_text, edits_page_ids, page_ids_language_ccc, ccc_count):

    ccc_count = dict(ccc_count)
    edit_count = len(edits_page_ids)
#    if edit_count <= 100: return ccc_params

    for rev in edits_page_ids:
        try:
            for lang in page_ids_language_ccc[rev]:
                ccc_count[lang]+=1
        except:
            pass

    ccc_params = []
    for lang, count in ccc_count.items():
        if count != 0: ccc_params.append((rev_user, rev_user_text, count, lang))

    return ccc_params


def delete_all_history_tables(monthly):

    function_name = 'delete_all_history_tables'
    if create_function_account_db(function_name, 'check','')==1: return

    functionstartTime = time.time()
    last_period_time = functionstartTime

    conn = sqlite3.connect(databases_path + revision_db)
    cursor = conn.cursor()
    if _monthly != None:
        query = "DROP TABLE "+languagecode+"wiki_revisions;"
    else:
        query = "DROP TABLE "+languagecode+"wiki_revisions_monthly"
    cursor.execute(query)
    conn.commit()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    create_function_account_db(function_name, 'mark', duration)




#### --- ####
# OTHER UTILITIES


def get_language_ccc_page_ids(languagecode):
    
    page_ids_language_ccc = {}
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()

    for languagecode2 in wikilanguagecodes:
        if languagecode2 == languagecode: continue
        page_ids = {}
        query = 'SELECT page_id FROM '+languagecode+'wiki WHERE qitem IN (SELECT qitem FROM '+languagecode2+'wiki WHERE ccc_binary=1);'

        c = 0
        try:
            for row in cursor.execute(query):
                c+=1
                page_id = row[0]
                if page_id not in page_ids_language_ccc:
                    page_ids_language_ccc[page_id]=[languagecode2]
                else:
                    page_ids_language_ccc[page_id].append(languagecode2)
        except:
            pass
#        print (languagecode2,c)

    for row in cursor.execute('SELECT page_id FROM '+languagecode+'wiki WHERE ccc_binary=1;'):
        page_id = row[0]
        if page_id not in page_ids_language_ccc:
            page_ids_language_ccc[page_id]=[languagecode]
        else:
            page_ids_language_ccc[page_id].append(languagecode)

    print ("We've got the languages' CCC contained in this language: "+languagecode)

    return page_ids_language_ccc



def get_language_namespaces_page_ids(languagecode):

    page_ids_namespaces = {}
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    mysql_cur_read.execute('SELECT page_id, page_namespace FROM page ORDER BY page_namespace;')
    rows = mysql_cur_read.fetchall()
    for row in rows: page_ids_namespaces[row[0]]=row[1]


    print ("We've got the page_ids for all the page_namespaces used in this language: "+languagecode)

    return page_ids_namespaces


"""

    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id GROUP BY rev_user_text') # EDITS TOTAL
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY rev_user_text') # EDITS ARTICLES
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace!=0 AND page_is_redirect=0 GROUP BY rev_user_text') # EDITS NO ARTICLES
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (6, 7, 14, 100, 10) GROUP BY rev_user_text') # EDITS DATASPACES: Files, categories, portals and templates
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (1, 2, 3) GROUP BY rev_user_text') # EDITS SN: user, user talks and article talks
    queries.append('SELECT rev_user_text, COUNT(*) FROM revision_userindex INNER JOIN page ON rev_page=page_id WHERE page_namespace IN (4, 5, 8, 9, 11, 12, 13, 15) GROUP BY rev_user_text') # EDITS META: about Wikipedia, guidelines and help

"""

def get_language_editors_user_pages_page_ids(languagecode):

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); 
    mysql_cur_read = mysql_con_read.cursor()

    user_id_userpage_page_id = {}
    query = 'SELECT user_id, page_id FROM page INNER JOIN user ON user_name=page_title WHERE page.page_namespace = 2' # userpage
    mysql_cur_read.execute(query)
    rows = mysql_cur_read.fetchall()
    for row in rows: user_id_userpage_page_id[row[0]]=row[1]

    user_id_userpage_talk_page_id = {}
    query = 'SELECT user_id, page_id FROM page INNER JOIN user ON user_name=page_title WHERE page.page_namespace = 3' # userpage talk
    mysql_cur_read.execute(query)
    rows = mysql_cur_read.fetchall()
    for row in rows: user_id_userpage_talk_page_id[row[0]]=row[1]

    print ("We've got the user_pages for all the users in this language: "+languagecode)

    return user_id_userpage_page_id, user_id_userpage_talk_page_id


#######################################################################################

### SAFETY FUNCTIONS ###


def create_function_account_db(function_name, action, duration):
    function_name_string = function_name

    conn = sqlite3.connect(databases_path + editor_diversity_db)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS function_account (function_name text, year_month text, finish_time text, duration text, PRIMARY KEY (function_name, year_month));")

    if action == 'check':
        query = 'SELECT duration FROM function_account WHERE function_name = ? AND year_month = ?;'
        cursor.execute(query,(function_name,cycle_year_month))
        function_name = cursor.fetchone()
        if function_name != None:
            print ('= Process Accountant: The function "'+function_name_string+'" has already been run. It lasted: '+function_name[0])
            return 1
        else:
            print ('- Process Accountant: The function "'+function_name_string+'" has not run yet. Do it!')
            return 0

    if action == 'mark':
        finish_time = datetime.datetime.utcnow().strftime("%Y%m%d");
        query = "INSERT INTO function_account (function_name, year_month, finish_time, duration) VALUES (?,?,?,?);"
        cursor.execute(query,(function_name,cycle_year_month,finish_time,duration))
        conn.commit()
        print ('+ Process Accountant: '+function_name+' DONE! After '+duration+'.\n')


def main_with_exception_email():
    try:
        main()
    except:
        wikilanguages_utils.send_email_toolaccount('HISTORY FEATURES: '+ cycle_year_month, 'ERROR.')


def main_loop_retry():
    while page == '':
        try:
            main()        #          main()
            page = 'done.'
        except:
            print('There was an error in the main. \n')
            path = '/srv/wcdo/src_data/editor_retrieval.err'
            file = open(path,'r')
            lines = file.read()
            wikilanguages_utils.send_email_toolaccount('HISTORY FEATURES: '+ cycle_year_month, 'ERROR.' + lines); print("Now let's try it again...")
            continue


def verify_time_for_iteration():
    print ("Let's check it is the right time for HISTORY FEATURES iteration...")

    # CONDITION 1: CCC created this month.
    conn = sqlite3.connect(databases_path + wikipedia_diversity_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "set_production_ccc_db" AND year_month = ?;'
    cursor.execute(query,cycle_year_month)
    function_name1 = cursor.fetchone()

    # CONDITION 2: TOP CCC created this month.
    conn = sqlite3.connect(databases_path + top_ccc_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "update_top_ccc_articles_titles translations" AND year_month = ?;'
    cursor.execute(query,cycle_year_month)
    function_name2 = cursor.fetchone()

    # CONDITION 3: STATS created this month.
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "generate_pageviews_intersections" AND year_month = ?;'
    cursor.execute(query,cycle_year_month)
    function_name3 = cursor.fetchone()

    if function_name1 != None and function_name2 != None and function_name3 != None: 
        return True




######################################################################################


class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("history_features"+".out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("history_features"+".err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    databases_path = '/srv/wcdo/databases/'
    wikipedia_diversity_db = 'wikipedia_diversity.db'
    editor_diversity_db = 'editor_diversity.db'
    revision_db = 'revision.db'
    stats_db = 'stats.db'


    first_time = True
    if first_time == True:

        startTime = time.time()
        today = datetime.date.today().strftime('%Y%m%d%H%M%S')
        #year_month = datetime.date.today().strftime('%Y-%m')
        cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
        periods_monthly,periods_accum = wikilanguages_utils.get_months_queries()


        # Import the Wikipedia languages characteristics
        languages = wikilanguages_utils.load_wiki_projects_information();
        wikilanguagecodes = sorted(languages.index.tolist())

        print ('checking languages Replicas databases and deleting those without one...')
        # Verify/Remove all languages without a replica database
        for a in wikilanguagecodes:
            if wikilanguages_utils.establish_mysql_connection_read(a)==None:
                wikilanguagecodes.remove(a)
        print (wikilanguagecodes)

        print ('\n* Starting the HISTORY FEATURES retrieving and storing: ' + str(datetime.datetime.now()))

        main()
    #    main_with_exception_email()
    #    main_loop_retry()

        finishTime = time.time()
        print ('* Done with HISTORY FEATURES DATA AND STORING CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
        wikilanguages_utils.finish_email(startTime,'history_features.out', 'WIKIPEDIA DIVERSITY')

    else:

        while True:
            time.sleep(84600*90) # every ninety days
            print ("Good morning. It is: "+time.today()+". Let's see if today is the day to generate the HISTORY FEATURES...")

            # CHAINED TO CCC CREATION (ONCE A MONTH) AND TOP CCC
            if verify_time_for_iteration():
                wikilanguages_utils.send_email_toolaccount('WCDO - HISTORY FEATURES', '# HISTORY FEATURES')

                startTime = time.time()
                today = datetime.date.today().strftime('%Y%m%d%H%M%S')
                #year_month = datetime.date.today().strftime('%Y-%m')
                cycle_year_month = wikilanguages_utils.get_current_cycle_year_month()
                periods_monthly,periods_accum = wikilanguages_utils.get_months_queries()


                # Import the Wikipedia languages characteristics
                languages = wikilanguages_utils.load_wiki_projects_information();
                wikilanguagecodes = sorted(languages.index.tolist())

                print ('checking languages Replicas databases and deleting those without one...')
                # Verify/Remove all languages without a replica database
                for a in wikilanguagecodes:
                    if wikilanguages_utils.establish_mysql_connection_read(a)==None:
                        wikilanguagecodes.remove(a)
                print (wikilanguagecodes)

                print ('\n* Starting the HISTORY FEATURES retrieving and storing: ' + str(datetime.datetime.now()))

                main()
            #    main_with_exception_email()
            #    main_loop_retry()

                finishTime = time.time()
                print ('* Done with HISTORY FEATURES DATA AND STORING CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
                wikilanguages_utils.finish_email(startTime,'history_features.out', 'WIKIPEDIA DIVERSITY')

