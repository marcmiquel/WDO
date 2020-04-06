# -*- coding: utf-8 -*-

# time
import time
import datetime
from dateutil import relativedelta
import calendar
# system
import os
import sys
import re
import numpy as np
from IPython.display import HTML
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import codecs
import gzip
# requests and others
import requests
import urllib
# data
import pandas as pd
# pywikibot
import pywikibot
PYWIKIBOT2_DIR = '/srv/wcdo/src_viz/user-config.py'

# scripts
sys.path.insert(0, '/srv/wcdo/src_data')
import wikilanguages_utils


class Logger(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("meta_update.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass

# MAIN
######################## WCDO CREATION SCRIPT ######################## 

def main():

#    create_stubs_database()
#    update_stubs_database([1500,3000])
#    update_stubs_database('category')

    publish_stubs_pages()


def create_stubs_database():

    functionstartTime = time.time()
    print ('* stubs_db')
    os.remove(databases_path + "stubs.db"); print ('stubs.db deleted.');

    conn = sqlite3.connect(databases_path + 'stubs.db'); cursor = conn.cursor()

    query = ('CREATE table if not exists stubs ('+
    'languagecode text, '+
    'languagename text, '+

    'wp_number_articles integer, '+
    'stub_articles integer, '+

    'stub_percentage float, '+
    'stub_threshold integer, '+ # we agreed on 1500 Bytes

    'period text,'
    'measurement_date text,'
    'PRIMARY KEY (languagecode, stub_threshold, period))')

    cursor.execute(query)
    conn.commit()

    print ('* stubs_db Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))



def update_stubs_database(stub_threshold):

    mysql_con_read = mdb.connect(host="enwiki.analytics.db.svc.eqiad.wmflabs",db='en' + 'wiki_p',read_default_file=os.path.expanduser("/srv/wcdo/src_data/my.cnf"),charset='utf8mb4')
    mysql_cur_read = mysql_con_read.cursor()

#    print (stubs_categories)
#    input('')
    # wikilanguagecodes = ['af']

    for languagecode in wikilanguagecodes:

        print (languagecode)

        try:
            mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("/srv/wcdo/src_data/my.cnf"),charset='utf8mb4')
            mysql_cur_read = mysql_con_read.cursor()
        except:
            print ('This language does not exist: '+languagecode)
            continue


        if stub_threshold == 'category':

            stubs_categories = {}
            stubs_categories['en']='Stub_categories'
            query = 'SELECT ll_lang,ll_title FROM langlinks WHERE ll_from = 2009362;'
            mysql_cur_read.execute(query)
            rows = mysql_cur_read.fetchall()
            for row in rows:
                title = row[1].decode('utf-8')
        #        print (title,row[0].decode('utf-8'))
                if ":" in title: 
                    category_name = title.split(':')
                    if len(category_name)==2:
                        category_name = category_name[1].replace(' ','_')
                    else:
                        category_name = category_name[2].replace(' ','_')

                else:
                    category_name = title
                stubs_categories[row[0].decode('utf-8')] = category_name


            query = 'SELECT count(*) FROM page WHERE page_namespace = 0 and page_is_redirect = 0;'
            mysql_cur_read.execute(query)
            row = mysql_cur_read.fetchone()
            if row != None:
                wp_number_articles = row[0]

            if languagecode in stubs_categories:
                print ('category')
                
                category_names = {}
                category_names[stubs_categories[languagecode]]=0

                iteration_categories = []
                iteration_categories.append(stubs_categories[languagecode])

                article_names = {}
                new_articles = 1
                new_categories = 1
                level = 0

                while new_categories != 0:
                    level+= 1
                    new_articles = 0
                    new_categories = 0               


                    # GET ALL ARTICLES
                    page_asstring = ','.join( ['%s'] * len(iteration_categories) )
                    query = 'SELECT page_title FROM page INNER JOIN categorylinks ON page_id = cl_from WHERE page_namespace=0 AND page_is_redirect=0 AND cl_to IN (%s)' % page_asstring

                    mysql_cur_read.execute(query, iteration_categories)
                    result = mysql_cur_read.fetchall()
                    for row in result:
                        page_title = row[0].decode('utf-8')
                        try:
                            article_names[page_title]=article_names[page_title]+1
                        except:
                            article_names[page_title]=0
                            new_articles+=1

                    # GET ALL CATEGORIES
                    query = 'SELECT cat_title FROM page INNER JOIN categorylinks ON page_id=cl_from INNER JOIN category ON page_title=cat_title WHERE page_namespace=14 AND cl_to IN (%s)' % page_asstring

                    mysql_cur_read.execute(query, iteration_categories)
                    iteration_categories = []
                    result = mysql_cur_read.fetchall()
                    for row in result:
                        cat_title = row[0].decode('utf-8')
                        try:
                            category_names[cat_title]=category_names[cat_title]+1
                        except:
                            category_names[cat_title]=0
                            iteration_categories.append(cat_title)
                            new_categories+=1


                    print ('At level '+str(level)+' we have '+str(new_categories)+' new categories and '+str(new_articles)+' new titles.')


                stub_articles = len(set(article_names))
                print (str())

            else:
                stub_articles = None

            if stub_articles != None:
                stb_percent = round(100*(stub_articles/wp_number_articles),3)
            else:
                stb_percent = None

            conn = sqlite3.connect(databases_path + 'stubs.db'); cursor = conn.cursor()

            parameter = ((languagecode,languages.loc[languagecode]['languagename'],wp_number_articles,stub_articles,stb_percent,stub_threshold,current_year_month_period,measurement_date))

            query_insert = 'INSERT OR IGNORE INTO stubs (languagecode, languagename, wp_number_articles, stub_articles, stub_percentage, stub_threshold, period, measurement_date) VALUES (?,?,?,?,?,?,?,?)'

            cursor.execute(query_insert,parameter);
            conn.commit()

        else:
            wp_number_articles = 0
            stub_articles_count = {}
            for th in stub_threshold:
                stub_articles_count[th]=0

            # get all articles from dump
            # create parameters (page_id, page_title, qitem)
            dumps_path = '/public/dumps/public/'+languagecode+'wiki/latest/'+languagecode+'wiki-latest-page.sql.gz'
            dump_in = gzip.open(dumps_path, 'r')

            while True:
                line = dump_in.readline()
                try: line = line.decode("utf-8")
                except UnicodeDecodeError: line = str(line)

                if line == '':
                    i+=1
                    if i==3: break
                else: i=0

                if wikilanguages_utils.is_insert(line):
                    table_name = wikilanguages_utils.get_table_name(line)
                    columns = wikilanguages_utils.get_columns(line)
                    values = wikilanguages_utils.get_values(line)
                    if wikilanguages_utils.values_sanity_check(values): rows = wikilanguages_utils.parse_values(values)

                    for row in rows:
                        page_id = int(row[0])
                        page_namespace = int(row[1])
                        page_is_redirect = int(row[4])

                        if page_namespace != 0 or page_is_redirect!=0: continue
                        page_title = str(row[2])
                        page_len = int(row[10])
                        wp_number_articles+=1

                        # print (row)
                        # input('')
                        for th in stub_threshold:
                            if page_len < th:
                                stub_articles_count[th]=stub_articles_count[th]+1
                            # else:
                            #     print (page_title,page_len)

            print ('Done with the dump.')
            conn = sqlite3.connect(databases_path + 'stubs.db'); cursor = conn.cursor()

            for th in stub_threshold:
                stub_articles=stub_articles_count[th]

                if stub_articles != None:
                    stb_percent = round(100*(stub_articles/wp_number_articles),3)
                else:
                    stb_percent = None

                print((languagecode,languages.loc[languagecode]['languagename'],wp_number_articles,stub_articles,stb_percent,th,current_year_month_period,measurement_date))
                parameter = ((languagecode,languages.loc[languagecode]['languagename'],wp_number_articles,stub_articles,stb_percent,th,current_year_month_period,measurement_date))

                query_insert = 'INSERT OR IGNORE INTO stubs (languagecode, languagename, wp_number_articles, stub_articles, stub_percentage, stub_threshold, period, measurement_date) VALUES (?,?,?,?,?,?,?,?)'

                cursor.execute(query_insert,parameter);
                conn.commit()


# Aquest script el què fa és calcular els stubs i actualitzar una taula a meta amb el percentatge actual de stubs i una sèrie d'increments.
# * stubs_update.py stubs. Columna amb increment d'un mes, de sis mesos i d'un any. -> cron a wcdo.
def make_stubs_table():

    print ('* make_stubs_table')
    conn = sqlite3.connect(databases_path + 'stubs.db'); cursor = conn.cursor()

    LM_period = ''
    SixMA_period = ''
    OneYA_period = ''
    
    i = 0
    for period in list(sorted(periods_monthly.keys(),reverse=True)):
#        print (period)
        if i == 0: 
            LM_period = period
            LM_period_query = periods_monthly[period]
        if i == 2: 
            ThreeMA_period = period
            ThreeMA_period_query = periods_monthly[period]
        if i == 5: 
            SixMA_period = period
            SixMA_period_query = periods_monthly[period]
        if i == 11: 
            OneYA_period = period
            OneYA_period_query = periods_monthly[period]
        i+=1

#    print(current_year_month_period,LM_period,SixMA_period,OneYA_period)

    # NOW
    query = 'SELECT languagecode, languagename as Language, wp_number_articles as Articles, stub_articles as Stubs, stub_percentage FROM stubs WHERE measurement_date IN (SELECT MAX(measurement_date) FROM stubs) AND stub_threshold = 1500 ORDER BY stub_percentage ASC;'
    df_stubs = pd.read_sql_query(query, conn)

    df_stubs.index = np.arange(1,len(df_stubs)+1)
    df_stubs['No.'] = df_stubs.index
    df_stubs['(%)'] = df_stubs['stub_percentage']

    df_stubs.languagecode = df_stubs['languagecode'].str.replace('_','-')
    df_stubs = df_stubs.set_index('languagecode')
    df_stubs['Wiki'] = df_stubs.index


    query = 'SELECT languagecode, stub_articles, stub_percentage FROM stubs WHERE measurement_date IN (SELECT MAX(measurement_date) FROM stubs) AND stub_threshold = 3000 ORDER BY stub_percentage ASC;'
    df_stubs_3KB = pd.read_sql_query(query, conn)
#    df_stubs_3KB['Art. 3KB (%)'] = df_stubs_3KB['stub_articles'].map(str) + ' (' + df_stubs_3KB['stub_percentage'].map(str) + ')'

    df_stubs_3KB['3KB (%)'] = round(df_stubs_3KB['stub_percentage'],3).map(str)

    df_stubs_3KB = df_stubs_3KB.drop(columns=['stub_articles', 'stub_percentage'])

    df_stubs_3KB.languagecode = df_stubs_3KB['languagecode'].str.replace('_','-')
    df_stubs_3KB = df_stubs_3KB.set_index('languagecode')


    # LM
    query = 'SELECT languagecode, stub_articles as stub_articles_LM, stub_percentage as stub_percentage_LM FROM stubs WHERE '+LM_period_query+' AND stub_threshold = 1500 ORDER BY stub_percentage ASC;' # one month ago
    df_stubs_LM = pd.read_sql_query(query, conn)
    df_stubs_LM['Stubs LM (%)'] = df_stubs_LM['stub_articles_LM'].map(str) + ' (' + df_stubs_LM['stub_percentage_LM'].map(str) + ')'

    df_stubs_LM.languagecode = df_stubs_LM['languagecode'].str.replace('_','-')
    df_stubs_LM = df_stubs_LM.set_index('languagecode')

    # 3MA
    query = 'SELECT languagecode, stub_articles as stub_articles_3MA, stub_percentage as stub_percentage_3MA FROM stubs WHERE '+ThreeMA_period_query+' AND stub_threshold = 1500 ORDER BY stub_percentage ASC;' # one month ago
    df_stubs_3MA = pd.read_sql_query(query, conn)
    df_stubs_3MA['Stubs 3MA (%)'] = df_stubs_3MA['stub_articles_3MA'].map(str) + ' (' + df_stubs_3MA['stub_percentage_3MA'].map(str) + ')'

    df_stubs_3MA.languagecode = df_stubs_3MA['languagecode'].str.replace('_','-')
    df_stubs_3MA = df_stubs_3MA.set_index('languagecode')


    # 6MA
    query = 'SELECT languagecode, stub_articles as stub_articles_6MA, stub_percentage as stub_percentage_6MA FROM stubs WHERE '+SixMA_period_query+' AND stub_threshold = 1500 ORDER BY stub_percentage ASC;' # one month ago
    df_stubs_6MA = pd.read_sql_query(query, conn)
    df_stubs_6MA['Stubs 6MA (%)'] = df_stubs_6MA['stub_articles_6MA'].map(str) + ' (' + df_stubs_6MA['stub_percentage_6MA'].map(str) + ')'

    df_stubs_6MA.languagecode = df_stubs_6MA['languagecode'].str.replace('_','-')
    df_stubs_6MA = df_stubs_6MA.set_index('languagecode')


    # 1YA
    query = 'SELECT languagecode, stub_articles as stub_articles_1YA, stub_percentage as stub_percentage_1YA FROM stubs WHERE '+OneYA_period_query+' AND stub_threshold = 1500 ORDER BY stub_percentage ASC;' # one month ago
    df_stubs_1YA = pd.read_sql_query(query, conn)
    df_stubs_1YA['Stubs 1YA (%)'] = df_stubs_1YA['stub_articles_1YA'].map(str) + ' (' + df_stubs_1YA['stub_percentage_1YA'].map(str) + ')'

    df_stubs_1YA.languagecode = df_stubs_1YA['languagecode'].str.replace('_','-')
    df_stubs_1YA = df_stubs_1YA.set_index('languagecode')


   # Category
    query = 'SELECT languagecode, stub_articles as stub_articles_cat FROM stubs WHERE measurement_date IN (SELECT MAX(measurement_date) FROM stubs) AND stub_threshold = "category" ORDER BY stub_percentage ASC;'
    df_stubs_cat = pd.read_sql_query(query, conn)
    df_stubs_cat = df_stubs_cat.set_index('languagecode')

    """
   # Category - Last Month
    query = 'SELECT languagecode, stub_articles as stub_articles_catLM FROM stubs WHERE '+LM_period_query+' AND stub_threshold = "category" ORDER BY stub_percentage ASC;' # one month ago
    df_stubs_catLM = pd.read_sql_query(query, conn)
    df_stubs_catLM = df_stubs_catLM.set_index('languagecode')

    df_stubs_cat = pd.concat([df_stubs_cat, df_stubs_catLM], axis=1, join_axes=[df_stubs_cat.index])
    df_stubs_cat['diff'] = df_stubs_cat['stub_articles_cat']-df_stubs_cat['stub_articles_catLM']

    df_stubs_cat['Stubs Cat (Inc.)'] = df_stubs_cat['stub_articles_cat'].map(str) + ' (' + df_stubs_cat['diff'].map(str) + ')'

    df_stubs_cat.languagecode = df_stubs_cat['languagecode'].str.replace('_','-')
    df_stubs_cat = df_stubs_cat.set_index('languagecode')

    df_stubs=df_stubs.join(df_stubs_cat)
    """

    df_stubs=df_stubs.join(df_stubs_LM)
    df_stubs=df_stubs.join(df_stubs_3MA)
    df_stubs=df_stubs.join(df_stubs_6MA)
    df_stubs=df_stubs.join(df_stubs_1YA)
    df_stubs=df_stubs.join(df_stubs_3KB)
    df_stubs = df_stubs.fillna('-')


    columns = ['No.','Language','Wiki','Articles','3KB (%)','Stubs','(%)','Stubs LM (%)','Stubs 3MA (%)','Stubs 6MA (%)','Stubs 1YA (%)']

#    columns = ['No.','Language','Wiki','Articles','3KB (%)','Stubs Cat (Inc.)','Stubs','(%)','Stubs LM (%)','Stubs 3MA (%)','Stubs 6MA (%)','Stubs 1YA (%)']
    df_stubs = df_stubs[columns] # selecting the parameters to export
#    print (df_stubs.head())
 
    df_columns_list = df_stubs.columns.values.tolist()
    df_rows = df_stubs.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'Articles':'data-sort-type="number"|','Stubs (%)':'data-sort-type="number"|','Stubs 3MA (%)':'data-sort-type="number"|','Stubs 6MA (%)':'data-sort-type="number"|','Stubs 1YA (%)':'data-sort-type="number"|'}

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

            # here we might add colors. -> it would be nice to make a different colour for each language background, so it would be easy to see when one starts and another finishes.
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    wikitext = '* Generated at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext


def publish_stubs_pages():
    site = pywikibot.Site('meta','meta')
    main_page_name = 'List_of_Wikipedias_by_percentage_of_stubs'
    wiki_table = make_stubs_table()
    page = pywikibot.Page(site, main_page_name + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)



def get_months_queries():

    def datespan(startDate, endDate, delta=datetime.timedelta(days=1)):
        currentDate = startDate
        while currentDate < endDate:
            yield currentDate
            currentDate += delta

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
        month_condition = 'measurement_date >= "'+ first_day +'" AND measurement_date < "'+last_day+'"'
        periods_monthly[month_period]=month_condition
#        print (month_condition)    

    return periods_monthly


#######################################################################################


### MAIN:
if __name__ == '__main__':
    startTime = time.time()
    sys.stdout = Logger()

    measurement_date = datetime.datetime.utcnow().strftime("%Y%m%d");
#    measurement_date = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime('/srv/wcdo/databases/wikidata.db')))
#    measurement_date = '20180926'

    current_year_month_period = datetime.date.today().strftime('%Y-%m')
#    current_year_month_period = time.strftime('%Y-%m', time.gmtime(os.path.getmtime('/srv/wcdo/databases/wikidata.db')))

    periods_monthly = get_months_queries()

    # Database path
    databases_path = '/srv/wcdo/databases/'

    # Import the language-territories mappings
    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()

    # Import the Wikipedia languages characteristics
    languages = wikilanguages_utils.load_wiki_projects_information();
    wikilanguagecodes = languages.index.tolist()

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    print ('\n* Starting the STUBS GENERATION CYCLE '+current_year_month_period+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* STUBS GENERATION CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))