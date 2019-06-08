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
# data
import pandas as pd
# pywikibot
import pywikibot
PYWIKIBOT2_DIR = '/srv/wcdo/user-config.py'
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

    publish_wcdo_update_meta_pages()

######################################################################


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

# TABLES
# function name composition rule: x, y, (rows, columns)

# In this function we create the table language_territories_mapping.
def make_table_language_territories_mapping():

    df = pd.read_csv(databases_path + 'language_territories_mapping.csv',sep='\t',na_filter = False)
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

        if curqitem != None and curqitem!='': qitems[curindex]='[[wikidata:'+curqitem+'|'+curqitem+']]'
        else: qitems[curindex]=''

    df['Qitems'] = pd.Series(qitems)

    columns = ['Language Name','WikimediaLanguagecode','Qitems','territorynameNative','demonymNative','ISO3166','ISO31662']
#    columns = ['Language Name','WikimediaLanguagecode','Qitems','territoryname','territorynameNative','demonymNative','ISO3166','ISO31662','country']
    df = df[columns] # selecting the parameters to export
#    print (df.head())

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

    wikitext = '* Generated at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext


def make_table_ccc_extent_all_languages():
#   QUESTION: What is the extent of cultural context content in each language edition?

    # percentatge de contingut únic (sense cap ILL) -> pensar si posar-lo a la taula de extent.  
    # OBTAIN AND FORMAT THE DATA.
    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

    df = pd.DataFrame(wikilanguagecodes)
    df = df.set_index(0)
    reformatted_wp_numberarticles = {}
    for languagecode,value in wikipedialanguage_numberarticles.items():
        reformatted_wp_numberarticles[languagecode]='{:,}'.format(int(value))
    df['wp_number_articles']= pd.Series(reformatted_wp_numberarticles)

    # CCC %
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "wp" AND set2descriptor = "ccc" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    rank_dict = {}; i=1
    lang_dict = {}
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        lang_dict[row[0]]=languages.loc[row[0]]['languagename']
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
        rank_dict[row[0]]=i
        i=i+1  
    df['Language'] = pd.Series(lang_dict)
    df['Nº'] = pd.Series(rank_dict)
    df['ccc_number_articles'] = pd.Series(abs_rel_value_dict)

    # CCC GL % 
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "wp" AND set2descriptor = "ccc_geolocated" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
    df['geolocated_number_articles'] = pd.Series(abs_rel_value_dict)

    # CCC KW %
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "wp" AND set2descriptor = "ccc_keywords" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
    df['keyword_title'] = pd.Series(abs_rel_value_dict)

    # CCC People %
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "wp" AND set2descriptor = "ccc_people" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
    df['people_ccc_percent'] = pd.Series(abs_rel_value_dict)

    # CCC Female %
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "ccc" AND set2descriptor = "female" AND content = "articles" AND set1=set2  AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    female_abs_value_dict = {}
    for row in cursor.execute(query):
        female_abs_value_dict[row[0]]=row[1]
    df['female_ccc'] = pd.Series(female_abs_value_dict)

    # CCC Male %
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "ccc" AND set2descriptor = "male" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC'
    male_abs_value_dict = {}
    for row in cursor.execute(query):
        male_abs_value_dict[row[0]]=row[1]
    df['male_ccc'] = pd.Series(male_abs_value_dict)

    df = df.fillna(0)

    df['male_ccc'] = df.male_ccc.astype(str)
    df['female_ccc'] = df.female_ccc.astype(str)
    df['people_ccc_percent'] = df.people_ccc_percent.astype(str)

    female_male_CCC={}
    for x in df.index.values.tolist():
        sumpeople = int(float(df.loc[x]['male_ccc']))+int(float(df.loc[x]['female_ccc']))
        if sumpeople != 0:
            female_male_CCC[x] = str(round(100*int(float(df.loc[x]['female_ccc']))/sumpeople,1))+'%\t-\t'+str(round(100*int(float(df.loc[x]['male_ccc']))/sumpeople,1))+'%'
        else:
            female_male_CCC[x] = '0.0%'+'\t-\t'+'0.0%'
    df['female-male_ccc'] = pd.Series(female_male_CCC)

    df['Region']=languages.region
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

    WPlanguagearticle={}
    for x in df.index.values: WPlanguagearticle[x]='[[:'+x.replace('_','-')+':|'+x.replace('_','-')+']]'
    df['Wiki'] = pd.Series(WPlanguagearticle)

    languagelink={}
    for x in df.index.values:
        languagelink[x]='[[w:'+languages.loc[x]['WikipedialanguagearticleEnglish'].split('/')[4].replace('_',' ')+'|'+languages.loc[x]['languagename']+']]'
    df['Language'] = pd.Series(languagelink)

    # Renaming the columns
    columns_dict = {'wp_number_articles':'Articles','ccc_number_articles':'CCC (%)','geolocated_number_articles':'CCC GL (%)','keyword_title':'KW Title (%)','female-male_ccc':'CCC Female-Male %','people_ccc_percent':'CCC People (%)'}
    df=df.rename(columns=columns_dict)
    df = df.reset_index()

    # Choosing the final columns
    columns = ['Nº','Language','Wiki','Articles','CCC (%)','CCC GL (%)','KW Title (%)','CCC People (%)','CCC Female-Male %','Region']
    df = df[columns] # selecting the parameters to export

    # WIKITEXT
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

    return wikitext



def make_table_langs_langs_ccc(): # COVERAGE

    # QUESTION: How well each language edition covers the CCC of each other language edition?

    # OBTAIN THE DATA.
    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 

    coverage_art = {}
    t_coverage = {}
    query = 'SELECT set2, abs_value, rel_value FROM wcdo_intersections WHERE set1="all_ccc_articles" AND set1descriptor="" AND set2descriptor="wp" ORDER BY set2;'
    for row in cursor.execute(query):
        coverage_art[row[0]]=row[1]
        t_coverage[row[0]]=round(row[2],2)

    r_coverage = {}
    query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE set1="all_ccc_avg" AND set1descriptor="" AND set2descriptor="wp" ORDER BY set2;'
    for row in cursor.execute(query):
        r_coverage[row[0]]=round(row[1],2)


    language_dict={}
    query = 'SELECT set2, set1, rel_value FROM wcdo_intersections WHERE content="articles" AND set1descriptor="ccc" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set2, abs_value DESC;'

    ranking = 5
    row_dict = {}
    i=1
    languagecode_covering='aa'
    for row in cursor.execute(query):

        cur_languagecode_covering=row[0]
        
        if cur_languagecode_covering!=languagecode_covering: # time to save
            row_dict['language']=languages.loc[languagecode_covering]['languagename']
            row_dict['WP articles']='{:,}'.format(int(wikipedialanguage_numberarticles[languagecode_covering]))
            row_dict['relative_coverage_index']=r_coverage[languagecode_covering]
            row_dict['total_coverage_index']=t_coverage[languagecode_covering]
            row_dict['coverage_articles_sum']='{:,}'.format(int(coverage_art[languagecode_covering]))

            language_dict[languagecode_covering]=row_dict
            row_dict = {}
            i = 1

        if i <= ranking:
            languagecode_covered=row[1]
            rel_value=round(row[2],2)

            languagecode_covered = languagecode_covered.replace('be_tarask','be_x_old')
            languagecode_covered = languagecode_covered.replace('zh_min_nan','nan')
            languagecode_covered = languagecode_covered.replace('zh_classical','lzh')
            languagecode_covered = languagecode_covered.replace('_','-')
            value = languagecode_covered + ' ('+str(rel_value)+'%)'

            if rel_value == 0: value ='<small>0</small>'
            else: value = '<small>'+value+'</small>'
            row_dict[str(i)]=value
        i+=1

        languagecode_covering = cur_languagecode_covering


    column_list_dict = {'language':'Language', 'WP articles':'Articles','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_coverage_index':'R.Coverage','total_coverage_index':'T.Coverage','coverage_articles_sum':'Coverage Art.'}
    column_list = ['Language','Articles','nº1','nº2','nº3','nº4','nº5','R.Coverage','T.Coverage','Coverage Art.']

    df=pd.DataFrame.from_dict(language_dict,orient='index')
    df=df.rename(columns=column_list_dict)

    df = df[column_list] # selecting the parameters to export
    df = df.fillna('')


    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    # WIKITEXT
    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'CCC %':'data-sort-type="number"|'}

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

            row_string = row_string + str(value) + add # here is the value
              
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext


def make_table_langs_ccc_langs(): # SPREAD

    #   QUESTION: How well each language edition CCC is spread in other language editions?
    # TABLE COLUMN (spread):
    # language, CCC%, RANKING TOP 5, relative spread index, total spread index, spread articles sum.
    # relative spread index: the average of the percentages it occupies in other languages.
    # total spread index: the overall percentage of spread of the own CCC articles. (sum of x-lang CCC in every language / sum of all articles in every language)
    # spread articles sum: the number of articles from this language CCC in all languages.

    # OBTAIN THE DATA.
    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 

    ccc_percent_wp = {}
    query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE content="articles" AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc";'
    for row in cursor.execute(query):
        value = row[1]
        if value == None: value = 0
        ccc_number_articles = '{:,}'.format(int(value))
        value2 = row[2]
        if value2 == None: value2 = 0
        ccc_percent_wp[row[0]]=ccc_number_articles+' <small>'+'('+str(round(value2,2))+'%)</small>'

    spread_art = {}
    t_spread = {}
    query = 'SELECT set2, abs_value, rel_value FROM wcdo_intersections WHERE content="articles" AND set1="all_wp_all_articles" AND set1descriptor="" AND set2descriptor="ccc" ORDER BY set2;'
    for row in cursor.execute(query):
        spread_art[row[0]]=row[1]
        t_spread[row[0]]=round(row[2],2)


    r_spread = {}
    query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE content="articles" AND set1="all_wp_avg" AND set1descriptor="" AND set2descriptor="ccc" ORDER BY set2;'
    for row in cursor.execute(query):
        r_spread[row[0]]=round(row[1],2)


    language_dict={}
    query = 'SELECT set2, set1, rel_value FROM wcdo_intersections WHERE content="articles" AND set2descriptor="ccc" AND set1descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set2, abs_value DESC;'

    ranking = 5
    row_dict = {}
    i=1
    languagecode_spreading='aa'
    for row in cursor.execute(query):
        cur_languagecode_spreading=row[0]
        if row[0]==row[1]: continue
        
        if cur_languagecode_spreading!=languagecode_spreading: # time to save
            row_dict['language']=languages.loc[languagecode_spreading]['languagename']

            try:
                row_dict['CCC articles']=ccc_percent_wp[languagecode_spreading]
            except:
                row_dict['CCC articles']=0
            try:
                row_dict['relative_spread_index']=r_spread[languagecode_spreading]
            except:
                row_dict['relative_spread_index']=0
            try:
                row_dict['total_spread_index']=t_spread[languagecode_spreading]
            except:
                row_dict['total_spread_index']=0
            try:
                row_dict['spread_articles_sum']='{:,}'.format(int(spread_art[languagecode_spreading]))
            except:
                row_dict['spread_articles_sum']=0

            language_dict[languagecode_spreading]=row_dict
            row_dict = {}
            i = 1
#            input('')

        if i <= ranking:
            languagecode_spread=row[1]
            rel_value=round(row[2],2)

            languagecode_spread = languagecode_spread.replace('be_tarask','be_x_old')
            languagecode_spread = languagecode_spread.replace('zh_min_nan','nan')
            languagecode_spread = languagecode_spread.replace('zh_classical','lzh')
            languagecode_spread = languagecode_spread.replace('_','-')
            value = languagecode_spread + ' ('+str(rel_value)+'%)'
            if rel_value == 0: value ='<small>0</small>'
            else: value = '<small>'+value+'</small>'
            row_dict[str(i)]=value
#            print (cur_languagecode_spreading,languagecode_spread,i,value)

        languagecode_spreading = cur_languagecode_spreading
        i+=1

    column_list_dict = {'language':'Language', 'CCC articles':'CCC %','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_spread_index':'R.Spread','total_spread_index':'T.Spread','spread_articles_sum':'Spread Art.'}

    column_list = ['Language','CCC %','nº1','nº2','nº3','nº4','nº5','R.Spread','T.Spread','Spread Art.']

    df=pd.DataFrame.from_dict(language_dict,orient='index')
    df=df.rename(columns=column_list_dict)

    df = df[column_list] # selecting the parameters to export
    df = df.fillna('')


    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()




    # WIKITEXT
    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'CCC %':'data-sort-type="number"|','nº1':'data-sort-type="number"|','nº2':'data-sort-type="number"|','nº3':'data-sort-type="number"|','nº4':'data-sort-type="number"|','nº5':'data-sort-type="number"|'}

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

            color = ''

            row_string = row_string + str(value) + add # here is the value
              
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext


def make_table_geolocated_articles():

    conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

    country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()

    country_names_copy = list(country_names.keys())
    # COUNTRIES
    # all qitems queries
    query = 'SELECT set2descriptor, abs_value, rel_value FROM wcdo_intersections WHERE set1 = "wikidata_article_qitems" AND set1descriptor = "geolocated" AND set2 = "countries" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
        rank_dict[row[0]]=str(i)
        i=i+1
        country_names_copy.remove(row[0])

    for country_code in country_names_copy:
        rank_dict[country_code]=str(i)
        abs_rel_value_dict[country_code]=' 0 <small>(0.0%)</small>'
        i=i+1

    df = pd.DataFrame.from_dict(country_names,orient='index')
    df['Country'] = pd.Series(country_names)
    df['Nº'] = pd.Series(rank_dict)
    df['Geolocated Qitems (%)'] = pd.Series(abs_rel_value_dict)
    df = df.reset_index()
    df.rename(columns={'index': 'ISO 3166'}, inplace=True)

#    # Choosing the final columns
    columns = ['Nº','ISO 3166','Country','Geolocated Qitems (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna(0)
    df = df[df['Nº'] != '0']

    # WIKITEXT
    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'Geolocated Qitems (%)':'data-sort-type="number"|'}

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

    wikitext_countries = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'

    wiki_table_string = class_header_string + header_string + rows + closer_string
    wikitext_countries += wiki_table_string


    # SUBREGIONS
    # all qitems query
    query = 'SELECT set2descriptor, abs_value, rel_value FROM wcdo_intersections WHERE set1 = "wikidata_article_qitems" AND set1descriptor = "geolocated" AND set2 = "subregions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>'
        rank_dict[row[0]]=i
        i=i+1

    df = pd.DataFrame.from_dict(abs_rel_value_dict,orient='index')
    df['Nº'] = pd.Series(rank_dict)
    df['Geolocated Qitems (%)'] = pd.Series(abs_rel_value_dict)
    df = df.reset_index()
    df.rename(columns={'index': 'Subregion'}, inplace=True)

#    # Choosing the final columns
    columns = ['Nº','Subregion','Geolocated Qitems (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna(0)


    # WIKITEXT
    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()

    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'Geolocated Qitems (%)':'data-sort-type="number"|'}

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

    wikitext_subregions = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'

    wiki_table_string = class_header_string + header_string + rows + closer_string
    wikitext_subregions += wiki_table_string


    # REGIONS (continents)
    # all qitems query
    query = 'SELECT set2descriptor, abs_value, rel_value FROM wcdo_intersections WHERE set1 = "wikidata_article_qitems" AND set1descriptor = "geolocated" AND set2 = "regions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
        rank_dict[row[0]]=i
        i=i+1

    df = pd.DataFrame.from_dict(abs_rel_value_dict,orient='index')
    df['Nº'] = pd.Series(rank_dict)
    df['Geolocated Qitems (%)'] = pd.Series(abs_rel_value_dict)
    df = df.reset_index()
    df.rename(columns={'index': 'Region (Continent)'}, inplace=True)

#    # Choosing the final columns
    columns = ['Nº','Region (Continent)','Geolocated Qitems (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna(0)


    # WIKITEXT
    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()
    class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

    dict_data_type = {'Geolocated Qitems (%)':'data-sort-type="number"|'}

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

    wikitext_regions = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    wikitext_regions += wiki_table_string

    print ('* wikitext created.\n')

    return wikitext_countries, wikitext_subregions, wikitext_regions




### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

### PUBLISHING TO META/NOTIFYING THE WORK DONE.

# This is a function that includes all the calls to the functions in order to create the visualizations and apply them to each language.
def publish_wcdo_update_meta_pages():
    site = pywikibot.Site('meta','meta')
    main_page_name = 'Wikipedia_Cultural_Diversity_Observatory'


    # LIST OF WIKIPEDIAS BY CCC
    wiki_table = make_table_ccc_extent_all_languages()
    page = pywikibot.Page(site, main_page_name + '/' + 'List_of_Wikipedias_by_Cultural_Context_Content' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

    # CULTURE GAP COVERAGE
    wiki_table = make_table_langs_langs_ccc()
    page = pywikibot.Page(site, main_page_name + '/' + 'Culture_Gap_(coverage)' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

    # CULTURE GAP SPREAD
    wiki_table = make_table_langs_ccc_langs()
    page = pywikibot.Page(site, main_page_name + '/' + 'Culture_Gap_(spread)' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

    # ALL WIKIPEDIAS GEOLOCATED ARTICLES
    wikitext_countries, wikitext_subregions, wikitext_regions = make_table_geolocated_articles()
    page = pywikibot.Page(site, main_page_name + '/' + 'Geolocated_articles/countries_Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wikitext_countries)

    page = pywikibot.Page(site, main_page_name + '/' + 'Geolocated_articles/subregions_Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wikitext_subregions)

    page = pywikibot.Page(site, main_page_name + '/' + 'Geolocated_articles/regions_Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wikitext_regions)

    # METHOD: LANGUAGE TERRITORIES MAPPING
    wiki_table = make_table_language_territories_mapping()
    page = pywikibot.Page(site, main_page_name + '/' + 'Language_Territories_Mapping' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

#    with open('wiki_table.txt', 'w') as f: f.write(wiki_table)

#######################################################################################


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'

    # Import the language-territories mappings
    territories = wikilanguages_utils.load_languageterritories_mapping()

    # Import the Wikipedia languages characteristics / UPGRADE CASE: in case of extending the project to WMF SISTER PROJECTS, a) this should be extended with other lists for Wikimedia sister projects b) along with more get functions in the MAIN for each sister project.
    languages = wikilanguages_utils.load_wiki_projects_information(territories);
    wikilanguagecodes = languages.index.tolist()

    # Verify/Remove all languages without a replica database
#    for a in wikilanguagecodes:
#        if wikilanguages_utils.establish_mysql_connection_read(a)==None: wikilanguagecodes.remove(a)

    # Add the 'wiki' for each Wikipedia language edition
    wikilanguagecodeswiki = []
    for a in wikilanguagecodes: wikilanguagecodeswiki.append(a+'wiki')

    # Filter only those with a geographical context
    languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
    wikilanguagecodes_real = wikilanguagecodes
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # SELECTING SPECIFIC LISTS OF WIKILANGUAGES
#    wikilanguagecodes = obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
#    wikilanguagecodes = ['ca']
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('ii'):]

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'current')

    # Verify/Remove all languages without a table in ccc_ .db
    for languagecode in wikilanguagecodes:
        if languagecode not in wikipedialanguage_numberarticles: wikilanguagecodes.remove(languagecode)

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    print ('\n* Starting the WCDO UPDATE CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* WCDO UPDATE CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))

#    wikilanguages_utils.finish_email(startTime,'meta_update.out','Meta Update')