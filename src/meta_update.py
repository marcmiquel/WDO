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
        self.log = open("wcdo_update.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self): pass

# MAIN
def main():

    publish_wcdo_update_meta_pages()



######################################################################

# MAIN
######################## WCDO CREATION SCRIPT ######################## 

#    publish_wcdo_create_meta_pages()
#    publish_wcdo_update_meta_pages()


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


### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 



# TABLES
# function name composition rule: x, y, (rows, columns)

# In this function we create the table language_territories_mapping.
def make_table_language_territories_mapping(wiki_path):

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

    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    print ('* dataframe.\n')

    return wikitext



def make_table_langs_ccc_segments(wiki_path):
#   QUESTION: What is the extent of cultural context content in each language edition?

# percentatge de contingut únic (sense cap ILL) -> pensar si posar-lo a la taula de extent.  

    print ('* make_table_langs_ccc_segments')

    # OBTAIN AND FORMAT THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()



    # WP
    reformatted_wp_numberarticles = {}
    for languagecode in wikipedialanguage_numberarticles:
        reformatted_wp_numberarticles[languagecode]='{:,}'.format(int(wikipedialanguage_numberarticles[languagecode]))
    df['wp_number_articles']= pd.DataFrame(reformatted_wp_numberarticles)
    df = df.set_index(0)

    # CCC %
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "wp" AND entity_2_descriptor = "ccc" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    rank_dict = {}; i=1
    lang_dict = {}
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        lang_dict[row[0]]=languages.loc[languagecode]['languagename']
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
        rank_dict[x]=i
        i=i+1  
    df['Language'] = pd.Series(lang_dict)
    df['Nº'] = pd.Series(rank_dict)
    df['ccc_number_articles'] = pd.Series(abs_rel_value_dict)

    # CCC GL % 
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "wp" AND entity_2_descriptor = "ccc_geolocated" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['geolocated_number_articles'] = pd.Series(abs_rel_value_dict)

    # CCC KW %
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "wp" AND entity_2_descriptor = "ccc_keywords" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['keyword_title'] = pd.Series(abs_rel_value_dict)

    # CCC People %
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "wp" AND entity_2_descriptor = "ccc_people" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['people_ccc_percent'] = pd.Series(abs_rel_value_dict)


    # CCC Female %
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "ccc" AND entity_2_descriptor = "female" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    female_abs_value_dict = {}
    for row in cursor.execute(query):
        female_abs_value_dict[row[0]]=row[1]
    df['female_ccc'] = pd.Series(female_abs_value_dict)

    # CCC Male %
    query = 'SELECT entity_1, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "ccc" AND entity_2_descriptor = "male" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC'
    male_abs_value_dict = {}
    for row in cursor.execute(query):
        male_abs_value_dict[row[0]]=row[1]
    df['male_ccc'] = pd.Series(male_abs_value_dict)

    df['male_ccc'] = df.male_ccc.astype(str)
    df['female_ccc'] = df.female_ccc.astype(str)
    df['people_ccc_percent'] = df.people_ccc_percent.astype(str)

    female_male_CCC={}
    for x in df.index.values.tolist():
        sumpeople = int(df.loc[x]['male_ccc'])+int(df.loc[x]['female_ccc'])
        if sumpeople != 0:
            female_male_CCC[x] = str(round(100*int(df.loc[x]['female_ccc'])/sumpeople,1))+'%\t-\t'+str(round(100*int(df.loc[x]['male_ccc'])/sumpeople,1))+'%'
        else:
            female_male_CCC[x] = '0.0%'+'\t-\t'+'0.0%'
    df['female-male_ccc'] = pd.Series(female_male_CCC)

    df['Region']=languages.region
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

    df['Subregion']=languages.subregion
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

    # Renaming the columns
    columns_dict = {'wp_number_articles':'Articles','ccc_number_articles':'CCC (%)','geolocated_number_articles':'CCC GL (%)','keyword_title':'KW Title (%)','female-male_ccc':'CCC Female-Male %','people_ccc_percent':'CCC People (%)'}
    df=df.rename(columns=columns_dict)


    df = df.reset_index()

    # Choosing the final columns
    columns = ['Nº','Language','Wiki','Articles','CCC (%)','CCC GL (%)','KW Title (%)','CCC People (%)','CCC Female-Male %','Region']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    # WIKITEXT
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

    print ('* dataframe created.\n')

    return wikitext


def make_table_langs_ccc_territories_ccc_segments(languagecode, page_titles_qitems, qitems_page_titles_english, wiki_path):
#   QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    query = 'SELECT DISTINCT main_territory FROM ccc_'+languagecode+'wiki;'
#    print (query)
    qitems = []
    for row in cursor.execute(query):
        qitems.append(row[0])
    df = df.set_index(0)

    print ('This the number of territories for language '+languagecode+': '+str(len(df)))
    if len(df)==0: return

    # FORMAT THE DATA
    territories_adapted=territories.loc[languagecode]
    territories_adapted=territories.drop(territories[territories.index!=languagecode].index)
    territories_adapted=territories_adapted.set_index(['QitemTerritory'])
    territories_adapted=territories_adapted.rename(index={'QitemTerritory':'qitem'})

#    df['Territory Name (local)']=territories_adapted.territorynameNative
#    df['Territory Name']=territories_adapted.territoryname
    df['Country']=territories_adapted.country
#    df['ISO3166']=territories_adapted.ISO3166
#    df['ISO3166-2']=territories_adapted.ISO31662
    df['Subregion']=territories_adapted.subregion
    df['Region']=territories_adapted.region


    # CCC
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "ccc" AND entity_2 = "ccc" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
        rank_dict[x]=i
        i=i+1
    df['Nº'] = pd.Series(rank_dict)
    df['ccc_number_articles'] = pd.Series(abs_rel_value_dict)


    # GL
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "ccc" AND entity_2 = "ccc_geolocated" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['geolocated_number_articles'] = pd.Series(abs_rel_value_dict)


    # KW
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1_descriptor = "ccc" AND entity_2 = "ccc_keywords" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['geolocated_number_articles'] = pd.Series(abs_rel_value_dict)


    # PREPARING THE DATAFRAME
    # Renaming the columns
    columns_dict = {'ccc_number_articles':'CCC %','geolocated_number_articles':'CCC Geolocated (%)','keyword_title':'CCC KW Title (%)','country'}
    df=df.rename(columns=columns_dict)


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


    df = df.reset_index()
#    # Choosing the final columns
    columns = ['Nº','Territory Name','Territory Name (local)','Qitem','CCC','CCC Geolocated','Keywords Title','country','Subregion']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    # WIKITEXT
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

    print ('* wikitext created.\n')

    return wikitext


def make_table_langs_geolocated_articles_segments(wiki_path, languagecode):


    # COUNTRIES
    # language queries
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "countries" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
        rank_dict[x]=i
        i=i+1

    df = df.DataFrame(abs_rel_value_dict.keys())
    df = df.set_index(0)
    df['ISO 3166'] = df.DataFrame(abs_rel_value_dict.keys())
    df['Nº'] = pd.Series(rank_dict)
    df['Language GL (%)'] = pd.Series(abs_rel_value_dict)

    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "countries" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) AND period = "monthly";'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['Language GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)


    # all qitems queries
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "countries" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Qitems (%)'] = pd.Series(abs_rel_value_dict)

    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "countries" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)



    country_names, regions, subregions = load_iso_3166_to_geographical_regions()
    df['Country'] = pd.Series(country_names)


    df = df.reset_index()
#    # Choosing the final columns
    columns = ['Nº','ISO 3166','Country','Language GL (%)','Language GL Monthly Inc. (%)','All GL Qitems (%)','All GL Monthly Inc. (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    # WIKITEXT
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

    wikitext_countries += wiki_table_string



    # SUBREGIONS
    # language queries
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "subregions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
        rank_dict[x]=i
        i=i+1

    df = df.DataFrame(abs_rel_value_dict.keys())
    df = df.set_index(0)
    df['Subregion'] = df.DataFrame(abs_rel_value_dict.keys())
    df['Nº'] = pd.Series(rank_dict)
    df['Language GL (%)'] = pd.Series(abs_rel_value_dict)

    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "subregions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) AND period = "monthly";'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['Language GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)



    # all qitems query
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "subregions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Qitems (%)'] = pd.Series(abs_rel_value_dict)

    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "subregions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)

    df = df.reset_index()
#    # Choosing the final columns
    columns = ['Nº','Subregion','Language GL (%)','Language GL Monthly Inc. (%)','All GL Qitems (%)','All GL Monthly Inc. (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    # WIKITEXT
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

    wikitext_subregions += wiki_table_string



    # REGIONS (continents)
    # language queries
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "regions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) rel_value DESC;'
    rank_dict = {}; i=1
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
        rank_dict[x]=i
        i=i+1

    df = df.DataFrame(abs_rel_value_dict.keys())
    df = df.set_index(0)
    df['Region (Continent)'] = df.DataFrame(abs_rel_value_dict.keys())
    df['Nº'] = pd.Series(rank_dict)
    df['Language GL (%)'] = pd.Series(abs_rel_value_dict)


    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = '+languagecode+' AND entity_1_descriptor = "geolocated" AND entity_2 = "regions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) AND period = "monthly";'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['Language GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)


    # all qitems query
    query = 'SELECT entity_2_descriptor, abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "regions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Qitems (%)'] = pd.Series(abs_rel_value_dict)

    query = 'SELECT entity_2_descriptor, abs_increment, rel_increment FROM wcdo_increments INNER JOIN wcdo_intersections ON intersection_id=cur_intersection_id WHERE entity_1 = "wikidata_article_qitems" AND entity_1_descriptor = "geolocated" AND entity_2 = "regions" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY rel_value DESC;'
    abs_rel_value_dict = {}
    for row in cursor.execute(query):
        abs_rel_value_dict[row[0]]=' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(row[2])+'%)</small>')
    df['All GL Monthly Inc. (%)'] = pd.Series(abs_rel_value_dict)

    df = df.reset_index()
#    # Choosing the final columns
    columns = ['Nº','Region (Continent)','Language GL (%)','Language GL Monthly Inc. (%)','All GL Qitems (%)','All GL Monthly Inc. (%)']
    df = df[columns] # selecting the parameters to export
    df = df.fillna('')


    # WIKITEXT
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

    wikitext_regions += wiki_table_string

    print ('* wikitext created.\n')

    return wikitext_countries, wikitext_subregions, wikitext_regions



def make_table_langs_ccc_langs(wiki_path): # SPREAD

    print ('* make_table_langs_langs_ccc')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()


    #   QUESTION: How well each language edition CCC is spread in other language editions?
    # TABLE COLUMN (spread):
    # language, CCC%, RANKING TOP 5, relative spread index, total spread index, spread articles sum.
    # relative spread index: the average of the percentages it occupies in other languages.
    # total spread index: the overall percentage of spread of the own CCC articles. (sum of x-lang CCC in every language / sum of all articles in every language)
    # spread articles sum: the number of articles from this language CCC in all languages.


    language_dict = {}
    for languagecode in wikilanguagecodes:
        print (languagecode)
        query = 'SELECT abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = ? AND entity_1_descriptor="wp" AND entity_2 = ? AND entity_2_descriptor = "ccc";'
        cursor.execute(query,(languagecode,languagecode))
        row = cursor.fetchone()
        value = row[1]
        if value == None: value = 0
        ccc_number_articles = '{:,}'.format(int(value))
        value2 = row[0]
        if value2 == None: value2 = 0
        ccc_percent_wp=ccc_number_articles+' <small>'+'('+str(value2)+'%)</small>'

        query = 'SELECT entity_2, abs_value, rel_value FROM wcdo_intersections WHERE entity_1='+languagecode+' AND entity_1_descriptor="wp" AND entity_2_descriptor = "ccc" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY abs_value DESC;'


        ranking = 7
        i = 1
        spread_articles_sum=0
        sum_percentage=0
        row_dict = {}
        total_number_articles=0
        for row in cursor.execute(query):
            languagecode_covering=row[0]
            number_articles=row[1]
            percentage=round(row[2],2)
            spread_articles_sum += number_articles
            sum_percentage += percentage
            total_number_articles += wikipedialanguage_numberarticles[languagecode_covering]

            if i <= ranking:
                languagecode_covering = languagecode_covering.replace('be_tarask','be_x_old')
                languagecode_covering = languagecode_covering.replace('zh_min_nan','nan')
                languagecode_covering = languagecode_covering.replace('zh_classical','lzh')
                languagecode_covering = languagecode_covering.replace('_','-')

                value = languagecode_covering + ' ('+str(percentage)+'%)'
                row_dict[str(i)]=value
            i+=1

        for j in range(1,ranking+1):
            if str(j) in row_dict:
                row_dict[str(j)]='<small>'+row_dict[str(j)]+'</small>'
            else:
                row_dict[str(j)]='<small>0</small>'


        relative_spread_index = round(sum_percentage/i,2)
        if total_number_articles!=0: total_spread_index = round(100*spread_articles_sum/total_number_articles,2)
        else: total_spread_index=0

        row_dict['language']=languages.loc[languagecode]['languagename']
        row_dict['CCC%']=ccc_percent_wp
        row_dict['relative_spread_index']=relative_spread_index
        row_dict['total_spread_index']=total_spread_index
        row_dict['spread_articles_sum']='{:,}'.format(int(spread_articles_sum))

        language_dict[languagecode]=row_dict

        insert_db_ccc_indexs(languagecode, 'relative_spread_index', relative_spread_index)
        insert_db_ccc_indexs(languagecode, 'total_spread_index', total_spread_index)
        insert_db_ccc_indexs(languagecode, 'spread_articles_sum', spread_articles_sum)

        column_list_dict = {'language':'Language', 'CCC%':'CCC %','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','6':'nº6','7':'nº7','relative_spread_index':'R.Spread','total_spread_index':'T.Spread','spread_articles_sum':'Spread Art.'}

        column_list = ['Language','CCC %','nº1','nº2','nº3','nº4','nº5','R.Spread','T.Spread','Spread Art.']

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
                print (value)

                color = ''

                if x > 1 and x < 7:
                    scale = 10
                    color1 = '#cc0000' # red
                    color2 = '#339933' # green
                    #colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, int(value))
                    #color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.
                row_string = row_string + color + str(value) + add # here is the value
                  
            row_string = midline + row_string + '\n'
            rows = rows + row_string
        closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext



def make_table_langs_langs_ccc(wiki_path): # COVERAGE

    print ('* make_table_langs_langs_ccc')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    
    # QUESTION: How well each language edition covers the CCC of each other language edition?
    # TABLE COLUMNS (coverage):
    # language, CCC%, RANKING TOP 5, relative coverage index, total coverage index, covered articles sum.
    # relative coverage index: the average percentage of the coverage of other languages.
    # total coverage index: the overall percentage of coverage of other CCC articles.
    # covered articles sum: the number of articles from other languages CCC.

    ccc_articles_dict={}
    for languagecode in wikilanguagecodes:
        print (languagecode)
        query = 'SELECT abs_value, rel_value FROM wcdo_intersections WHERE entity_1 = ? AND entity_1_descriptor="wp" AND entity_2 = ? AND entity_2_descriptor = "ccc";'
        cursor.execute(query,(languagecode,languagecode))
        row = cursor.fetchone()[0]
        ccc_articles_dict[languagecode]=ccc_number_articles


    language_dict={}
    for languagecode in wikilanguagecodes:

        query = 'SELECT entity_2, abs_value, rel_value FROM wcdo_intersections WHERE entity_1='+languagecode+' AND entity_1_descriptor="ccc" AND entity_2_descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY abs_value DESC;'

        ranking = 7
        i = 1
        coverage_articles_sum=0
        sum_percentage=0
        total_number_articles=0
        row_dict = {}
        for row in cursor.execute(query):               
            languagecode_covered=row[0]
            number_articles=row[1]
            percentage=round(row[2],2)
            coverage_articles_sum += number_articles
            sum_percentage += percentage
            total_number_articles += ccc_articles_dict[languagecode_covered]

            if i <= ranking:
                languagecode_covered = languagecode_covered.replace('be_tarask','be_x_old')
                languagecode_covered = languagecode_covered.replace('zh_min_nan','nan')
                languagecode_covered = languagecode_covered.replace('zh_classical','lzh')
                languagecode_covered = languagecode_covered.replace('_','-')

                value = languagecode_covered + ' ('+str(percentage)+'%)'
                row_dict[str(i)]=value
            i+=1

        for j in range(1,ranking+1):
            if str(j) in row_dict:
                row_dict[str(j)]='<small>'+row_dict[str(j)]+'</small>'
            else:
                row_dict[str(j)]='<small>0</small>'


        relative_coverage_index = round(sum_percentage/i,2)
        total_coverage_index = round(100*coverage_articles_sum/total_number_articles,2)

        if coverage_articles_sum > int(wikipedialanguage_numberarticles[languagecode]):
            coverage_articles_sum = int(wikipedialanguage_numberarticles[languagecode])

        row_dict['language']=languages.loc[languagecode]['languagename']
        row_dict['WP articles']='{:,}'.format(int(wikipedialanguage_numberarticles[languagecode]))
        row_dict['relative_coverage_index']=relative_coverage_index
        row_dict['total_coverage_index']=total_coverage_index
        row_dict['coverage_articles_sum']='{:,}'.format(int(coverage_articles_sum))

        language_dict[languagecode]=row_dict

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
            print (value)

            color = ''

            if x > 1 and x < 7:
                scale = 10
                color1 = '#cc0000' # red
                color2 = '#339933' # green
                #colorhex = get_hexcolorrange(color1, color2, scale, 0, 100, int(value))
                #color = 'style="background: '+colorhex+';" |' # here we might add colors depending on the value.
            row_string = row_string + color + str(value) + add # here is the value
              
        row_string = midline + row_string + '\n'
        rows = rows + row_string
    closer_string = '|}'

    wiki_table_string = class_header_string + header_string + rows + closer_string

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext

"""
EXEMPLE: Culture_Gap_(coverage)
https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Culture_Gap_(coverage)

"""

def make_table_lang_countries_ccc_vital_articles_lists(languagecode, wiki_path):
#    for languagecode in wikilanguagecodes:
#        countries = load_countries_from_language(languagecode)

#        country_name = territories.loc[territories['ISO3166'] == country].loc[languagecode]['country']
#        if isinstance(country_name, str) != True: country_name=list(country_name)[0]
#        language_name = languages.loc[languagecode]['languagename']
#        if isinstance(language_name, str) != True: language_name=list(language_name)[0]
#        title = country_name + ' ('+language_name+' speaking territories)'

        # United States (English speaking territories)

    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    language_covering = languagecode
    country_names = load_iso_3166_to_geographical_regions()[0]

    query = 'SELECT entity_1_descriptor, entity_1, rel_value, abs_value FROM wcdo_intersections WHERE entity_2 = ? AND entity_2_descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) GROUP BY entity_1_descriptor DESC ORDER BY entity_1;'

    row_dict = {}
    language_dict = {}
    current_languagecode_covered = ''
    for row in cursor.execute(query,(language_covering,)):
        country_languagecode_covered = row[1].split('(')
        if "(" not in country_languagecode_covered: continue

        country_code_covered = country_languagecode_covered[0][:-1]
        language_name_covered = languages.loc[country_languagecode_covered[1][:-1]]['languagename']

        if current_country_languagecode_covered!=country_languagecode_covered and current_country_languagecode_covered!='':
            covered_list_articles_sum=0
            for x,y in row_dict.items(): covered_list_articles_sum += y
            row_dict['countryname'] = country_names[country_code_covered]+' '+'('+language_name_covered+')'          
            row_dict['Wiki']=language_covered
            row_dict['list_coverage_index']=list_coverage_index
            row_dict['covered_list_articles_sum']=covered_list_articles_sum

            language_dict[languagecode_covering]=row_dict
            row_dict={}

        percentage = row[2]
        list_name = row[0]
        row_dict[list_name]=percentage

        country_current_languagecode_covered = country_languagecode_covered


    column_list_dict = {'countryname':'Country (Lang. speaking territories)','CCC_Vital_articles_editors':'Editors', 'CCC_Vital_articles_featured':'Featured', 'CCC_Vital_articles_geolocated':'Geolocated', 'CCC_Vital_articles_keywords':'Keywords', 'CCC_Vital_articles_first_years':'First 3Y', 'CCC_Vital_articles_last_year':'Last Y', 'CCC_Vital_articles_women':'Women', 'CCC_Vital_articles_men':'Men', 'CCC_Vital_articles_pageviews':'Pageviews', 'CCC_Vital_articles_discussions':'Talk Edits','list_coverage_index':'List Coverage Idx.','covered_list_articles_sum':'Sum Covered Articles'}

    column_list = ['countryname','Country (Lang. speaking territories)','CCC_Vital_articles_editors','CCC_Vital_articles_featured','CCC_Vital_articles_geolocated','CCC_Vital_articles_keywords','CCC_Vital_articles_first_years','CCC_Vital_articles_last_year','CCC_Vital_articles_women','CCC_Vital_articles_men','CCC_Vital_articles_pageviews','CCC_Vital_articles_discussions','list_coverage_index','covered_list_articles_sum']

    # HTML (from dataframe)
    df=pd.Dataframe.from_dict(language_dict)

    df = df[columns] # selecting the parameters to export
    df=df.rename(columns=columns_dict)
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

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext



def make_table_lang_langs_ccc_vital_articles_lists(languagecode, wiki_path):

    # CAS: COVERAGE
    # https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/English_Wikipedia/Languages_CCC_Vital_articles_lists_(coverage)
    # how well langcode covers
    # Langcode Catalan Wikipedia - rest of Wikipedias CCC Vitals articles lists

    # FOR WHEN CREATING THE PAGE:
    # https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/English_Wikipedia/Countries_CCC_Languages_CCC_Vital_articles_lists_(coverage)

    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    language_covering = languagecode

    # QUESTION: How well does this language edition cover the lists of CCC articles from other language editions?
    # COVERAGE: how well this language covers the lists from other languages?
    # TABLE COLUMN (coverage).
    # Language Covered, Wiki, Editors,Featured, Geolocated, Keywords, First 3Y, Last Y, Women, Men, Pageviews, Talk Edits, Lists Coverage Index, Covered articles sum.
    # Lists Coverage Index is the percentage of articles covered from these lists.

    query = 'SELECT entity_1_descriptor, entity_1, rel_value, abs_value FROM wcdo_intersections WHERE entity_2 = ? AND entity_2_descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) GROUP BY entity_1_descriptor DESC ORDER BY entity_1;'

    row_dict = {}
    language_dict = {}
    current_languagecode_covered = ''
    for row in cursor.execute(query,(language_covering,)):
        languagecode_covered = row[1]
        if "(" in languagecode_covered or languagecode_covered == 'ccc_vital_articles_lists': continue

        if current_languagecode_covered!=languagecode_covered and current_languagecode_covered!='':
            covered_list_articles_sum=0
            for x,y in row_dict.items(): covered_list_articles_sum += y
            row_dict['languagename']=languages.loc[language_covered]['languagename']
            row_dict['Wiki']=language_covered
            row_dict['list_coverage_index']=list_coverage_index
            row_dict['covered_list_articles_sum']=covered_list_articles_sum

            language_dict[languagecode_covering]=row_dict
            row_dict={}

        percentage = row[2]
        list_name = row[0]
        row_dict[list_name]=percentage

        current_languagecode_covered = languagecode_covered


    column_list_dict = {'languagename':'Language','Wiki':'Wiki','CCC_Vital_articles_editors':'Editors', 'CCC_Vital_articles_featured':'Featured', 'CCC_Vital_articles_geolocated':'Geolocated', 'CCC_Vital_articles_keywords':'Keywords', 'CCC_Vital_articles_first_years':'First 3Y', 'CCC_Vital_articles_last_year':'Last Y', 'CCC_Vital_articles_women':'Women', 'CCC_Vital_articles_men':'Men', 'CCC_Vital_articles_pageviews':'Pageviews', 'CCC_Vital_articles_discussions':'Talk Edits','list_coverage_index':'List Coverage Idx.','covered_list_articles_sum':'Sum Covered Articles'}

    column_list = ['languagename','Wiki','CCC_Vital_articles_editors','CCC_Vital_articles_featured','CCC_Vital_articles_geolocated','CCC_Vital_articles_keywords','CCC_Vital_articles_first_years','CCC_Vital_articles_last_year','CCC_Vital_articles_women','CCC_Vital_articles_men','CCC_Vital_articles_pageviews','CCC_Vital_articles_discussions','list_coverage_index','covered_list_articles_sum']

    # HTML (from dataframe)
    df=pd.Dataframe.from_dict(language_dict)

    df = df[columns] # selecting the parameters to export
    df=df.rename(columns=columns_dict)
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

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext


def make_table_langs_lang_ccc_vital_articles_lists(langcode):
    return ''

    # CAS: SPREAD
    # https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/English_Wikipedia/Languages_CCC_Vital_articles_lists_(spread)
    # how well the lists of langcode are covered by the other languages
    # Catalan Wikipedia CCC Vital articles lists - rest of Wikipedias


    # - d'alguna manera he d'ensenyar quan una llengua no arriba als 100. també he d'ensenyar quan d'una llengua es cobreix la llista sencera encara que no es cobreixin els 100. 
    # 'SELECT count(qitem) FROM ccc_vital_lists_rankings WHERE langcode_original = ‘+languagecode
    # això es podria ensenyar a la primera línia de CCC Vital articles lists (spread)… que seria la pròpia llengua.
    # zu, Zulu, 90, 32, 10, 34, …,

    # QUESTION: Which of these CCC articles is or should be available in other language editions?
    # SPREAD: how well the lists of this language are spread into other languages?
    # TABLE COLUMN (spread).
    # Language Covering, Wiki, Editors 1000, Editors 100, Featured, Geolocated, Keywords, First 3Y, Last Y, Women, Men, Pageviews, Talk Edits, Lists Spread Index, Spread articles sum.
    # Lists Spread Index is the percentage of articles from this language lists spread in other languages. It must be computed here.

    language_covered = languagecode

     query = 'SELECT entity_1_descriptor, entity_2, rel_value, abs_value FROM wcdo_intersections WHERE entity_1 = ? AND entity_2_descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) GROUP BY entity_1_descriptor DESC ORDER BY entity_1;'


    row_dict = {}
    language_dict = {}
    current_languagecode_covering = ''
    for row in cursor.execute(query,(language_covered,)):
        languagecode_covering = row[1]
        if "(" in languagecode_covering or languagecode_covering == 'ccc_vital_articles_lists': continue

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

            insert_db_ccc_indexs(language_covered, 'list_spread_index', list_spread_index)
            insert_db_ccc_indexs(language_covered, 'spread_list_articles_sum', spread_list_articles_sum)

        percentage = row[2]
        list_name = row[0]
        row_dict[list_name]=percentage

        current_languagecode_covering = languagecode_covering


    column_list_dict = {'languagename':'Language','Wiki':'Wiki','CCC_Vital_articles_Top_1000':'Editors 1000', 'CCC_Vital_articles_Top_100':'Editors 100', 'CCC_Vital_articles_featured':'Featured', 'CCC_Vital_articles_geolocated':'Geolocated', 'CCC_Vital_articles_keywords':'Keywords', 'CCC_Vital_articles_first_years':'First 3Y', 'CCC_Vital_articles_last_year':'Last Y', 'CCC_Vital_articles_women':'Women', 'CCC_Vital_articles_men':'Men', 'CCC_Vital_articles_pageviews':'Pageviews', 'CCC_Vital_articles_discussions':'Talk Edits','list_spread_index':'List Spread Idx.','spread_list_articles_sum':'Sum Spread Articles'}

    column_list = ['languagename','Wiki','CCC_Vital_articles_Top_1000','CCC_Vital_articles_Top_100','CCC_Vital_articles_featured','CCC_Vital_articles_geolocated','CCC_Vital_articles_keywords','CCC_Vital_articles_first_years','CCC_Vital_articles_last_year','CCC_Vital_articles_women','CCC_Vital_articles_men','CCC_Vital_articles_pageviews','CCC_Vital_articles_discussions','list_spread_index','spread_list_articles_sum']

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

    # Returning the Wikitext
    wikitext = '* Statistics at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
    wikitext += wiki_table_string

    return wikitext
    


### --- ###

# VISUALIZATIONS
# function name composition rule: x, y, graph type.


def make_viz_languages_ccc_absolute_relative_scatter_plot():
#   QUESTION: What is the extent of Cultural Context Content in all language editions?

    print ('* make_viz_languages_ccc_absolute_relative_scatter_plot')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    query = 'SELECT * FROM wcdo_intersection'


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

    filename = 'languages_ccc_absolute_relative_scatter_plot_'+measurement_date+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path



def make_viz_langs_lang_ccc_scatter_plot(languagecode, wiki_path): # culture gap spread
#   QUESTION: How well the language editions cover the CCC of this language edition?
    # diagrama de boles

    print ('* make_viz_ccc_culture_gap_covered')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_gaps.


    filename = languagecode+'_'+'ccc_culture_gap_covered_visualization'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path



def make_viz_lang_ccc_langs_scatter_plot(languagecode, wiki_path): # culture gap coverage
#   QUESTION: How well this language edition cover the CCC of the other language editions?     

#    això és utilitzar table_ccc_gaps amb funció share.
    # diagrama de boles. stacked bar o formatget.

    print ('* make_viz_ccc_culture_gap_covered')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    
    # obtain the data from table: ccc_gaps. això abans....


    filename = languagecode+'_'+'ccc_culture_gap_covering_visualization'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path



def make_viz_all_wikipedias_all_articles_ccc_pie_chart(wiki_path):
#   QUESTION: What is the composition of the entire Wikipedia project in terms of CCC from all languages?
    return ''

    # ALERTA: això també podria ser per una llengua concreta.


def make_viz_all_wikipedias_all_articles_countries_pie_chart(wiki_path):
#   QUESTION: What is the composition of the entire Wikipedia project in terms of geographical articles?
    print ('* make_viz_geographical_overall_wikipedia_project_composition')

    # el gràfic seria semblant a ccc_extent_visualization()

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()

    # obtain data from table: ccc_entire_project


    # Formatges de tot el projecte Wikipedia amb totes les llengües CCC i el què ocupen (repetint articles).
    # Formatges de tot el projecte Wikipedia amb totes les llengües CCC i el què ocupen (sense repetir articles, és a dir, tot nou)
    # Formatges de tot Wikidata amb el què ocupen les llengües CCC.

    # El mateix per subregions. (x3)
    # El mateix per regions. (x3)


    # This should be obtained from ccc_entire_project table from wcdo_data.db

    # ALERTA: això també podria ser per una llengua concreta.


    return ''


def make_viz_all_wikipedia_all_articles_geolocated_map(languagecode, wiki_path):
#   QUESTION: How well does this language covers other language editions geolocated articles?

    print ('* make_viz_geographical_coverage_map')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    
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
    filename = languagecode+'_'+content_type+'_geolocated_coverage_map'+'.png'
    file_path = current_web_path + wiki_path + filename

    return file_path


def make_viz_lang_ccc_geolocated_map(languagecode, wiki_path):
#   QUESTION: Which are the geolocated articles with most inlinks, pageviews, etc.?

    print ('* make_viz_ccc_geolocated_articles_map')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    
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


def make_viz_ccc_last_month_articles_lang_ccc_over_time_line_chart(languagecode, wiki_path):
#   QUESTION: What is the extent of Cultural Context Content in the articles created during the last month?
    print ('* make_viz_ccc_creation_monthly')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

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


def make_viz_ccc_last_month_articles_langs_ccc_over_time_line_chart(languagecode, wiki_path):
#   QUESTION: What is the extent of articles dedicated to bridge the CCC from other language editions from those created during the last month?
#    % per cada llengua.

    print ('* make_viz_ccc_bridging_culture_gap_over_time')

    # OBTAIN THE DATA.
    print ('obtain the data.')
    conn = sqlite3.connect(databases_path + 'wcdo_data.db'); cursor = conn.cursor()
    
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


def make_viz_ccc_geolocated_segments_over_time_line_chart():
    return ''
# how much did the catalan Wikipedia increased the number of articles about France?"
"""
'ca' '' 'country' 'france', wcdo_intersections.abs_value, wcdo_intersections.rel_value, wcdo_increments.abs_increment, wcdo_increments.rel_increment, 'quarter'

SELECT wcdo_intersections.intersection_id, covering_entity, covering_entity_descriptor, covered entity, covered_entity_descriptor, abs_value, rel_value, measurement_date, abs_increment, rel_increment, period
FROM wcdo_intersections INNER JOIN wcdo_increments ON wcdo_intersections.intersection_id = wcdo_increments.intersection_id 
WHERE wcdo_intersections.covering_entity='ca' AND wcdo_intersections.covering_entity_descriptor='' AND wcdo_intersections.covered_entity='countries' AND wcdo_intersections.covered_entity_descriptor='France' AND period='quarter'
"""

def make_viz_all_wikipedia_all_articles_regions_over_time_line_chart():
    return ''
    # all wikipedias all articles - language ccc / over time

def make_viz_all_wikipedia_all_articles_regions_over_time_line_chart():
    return ''
    # all wikipedias (all articles) measures (total and averages of specific langauges):
    # all wikipedias all articles - regions-countries coverage / over time


def make_viz_all_wikipedia_all_articles_ccc_vital_articles_lists_over_time_line_chart():
    return ''
    # all wikipedias all articles - vital articles lists coverage / over time




### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- ### --- 

### PUBLISHING TO META/NOTIFYING THE WORK DONE.

# Publishes all the meta pages for all the languages in case they do not exist.
def publish_wcdo_create_meta_pages():

    for languagecode in wikilanguagecodes:

        # CATEGORY: LANGUAGE WIKIPEDIA (WCDO)
        category_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')+'_(WCDO)'
        categorypage = pywikibot.Category(site, category_page_name)
        categorypage.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text='[[Category:Wikipedia_Cultural_Diversity_Observatory]]') # introducing the parent

        language_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')+'_(WCDO)'

        # LANGUAGE WCDO MAIN PAGE
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # GEOLOCATED ARTICLES
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Geolocated_articles'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Geolocated_articles')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # CULTURE GAP
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Culture_Gap'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Culture_Gap')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # CULTURE GAP OVER TIME
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Culture_Gap_over_time'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Culture_Gap_over_time')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # LANGUAGES CCC VITAL ARTICLES COVERAGE
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Languages_CCC_Vital_articles_lists_(coverage)'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Languages_CCC_Vital_articles_lists_(coverage)')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # LANGUAGES CCC VITAL ARTICLES SPREAD
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Languages_CCC_Vital_articles_lists_(spread)'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Languages_CCC_Vital_articles_lists_(spread)')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # COUNTRIES CCC VITAL ARTICLES SPREAD
        wikitext = open(text_files_path+'Language_Wikipedia_(WCDO)/'+'Countries_CCC_Vital_articles_lists_(spread)'+'.txt',"r").read() + '\n' + '[[Category:'+category_page_name+']]'
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Languages_CCC_Vital_articles_lists_(spread)')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)



# This is a function that includes all the calls to the functions in order to create the visualizations and apply them to each language.
def publish_wcdo_update_meta_pages():
    site = pywikibot.Site('meta','meta')
    text_files_path = web_path + '/first_time_texts/'
    main_page_name = 'Wikipedia_Cultural_Diversity_Observatory'


    # METHOD: LANGUAGE TERRITORIES MAPPING
    wiki_table = make_table_language_territories_mapping(main_page_name + '/' + 'Language_Territories_Mapping')
    page = pywikibot.Page(site, main_page_name + '/' + 'Language_Territories_Mapping' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # LIST OF WIKIPEDIAS BY CCC
    wiki_table = make_table_ccc_extent_all_languages(main_page_name + '/' + 'List_of_Wikipedias_by_Cultural_Context_Content')
    page = pywikibot.Page(site, main_page_name + '/' + 'List_of_Wikipedias_by_Cultural_Context_Content' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # ALL WIKIPEDIAS CCC
    file_path = make_viz_ccc_overall_wikipedia_project_composition(wiki_path)
    page = pywikibot.Page(site, main_page_name + '/' + 'All_Wikipedias_CCC')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # ALL WIKIPEDIAS GEOLOCATED ARTICLES
    file_path = make_viz_geographical_overall_wikipedia_project_composition(wiki_path)
    page = pywikibot.Page(site, main_page_name + '/' + 'All_Wikipedias_CCC')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # ALL WIKIPEDIAS MEASURES OVER TIME
    file_path = generate_ccc_overall_measures_over_time()
    page = pywikibot.Page(site, main_page_name + '/' + 'All_Wikipedias_measures_over_time')
    wikitext = file_path
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # CULTURE GAP SPREAD
    wiki_table = make_table_ccc_culture_gap(main_page_name + '/' + 'Culture_Gap_(spread)', 'spread') #    taula amb les top 5 que més cobreix cada llengua.
    page = pywikibot.Page(site, main_page_name + '/' + 'Culture_Gap_(spread)' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)


    # CULTURE GAP COVERAGE
    wiki_table = make_table_ccc_culture_gap(main_page_name + '/' + 'Culture_Gap_(gap)', 'coverage') #    taula amb les top 5 que més cobreix cada llengua.
    page = pywikibot.Page(site, main_page_name + '/' + 'Culture_Gap_(gap)' + '/Table')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wiki_table)

    # CULTURE GAP OVER TIME
    file_path_1=make_viz_ccc_bridging_culture_gap_over_time(languagecode, main_page_name + '/' + 'Get_involved')
    wiki_text = file_path_1 + file_path_2
    page = pywikibot.Page(site, main_page_name + '/' + 'Culture_Gap_over_time')
    page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


    # LANGUAGE BY LANGUAGE
    qitems_page_titles_english = {v: k for k, v in load_dicts_page_ids_qitems('en')[0].items()}

    # project site page WCDO for each language
    for languagecode in wikilanguagecodes_real:
        print ('\n### language '+str(wikilanguagecodes.index(languagecode)+1)+'/'+str(len(wikilanguagecodes))+': \t'+languages.loc[languagecode]['languagename']+' '+languagecode+' \t| '+languages.loc[languagecode]['region']+'\t'+languages.loc[languagecode]['subregion']+'\t'+languages.loc[languagecode]['intermediateregion']+' | '+languages.loc[languagecode]['languageofficialnational']+' '+languages.loc[languagecode]['languageofficialregional'])

        (page_titles_qitems, page_titles_page_ids)=load_dicts_page_ids_qitems(languagecode)

        wikipedia_proximity_lists = obtain_proximity_wikipedia_languages_lists(languagecode)
        top19 = wikipedia_proximity_lists[0]
        upper9lower10 = wikipedia_proximity_lists[1]
        closest19 = wikipedia_proximity_lists[2]
      

        # LANGUAGE MAIN PAGE
        language_page_name = languages.loc[languagecode]['Wikipedia'].replace(' ','_')

        wikitext = make_table_ccc_extent_by_language_qitems(languagecode,page_titles_qitems,qitems_page_titles_english,main_page_name + '/' + language_page_name)
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/Table')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        file_path_1 = make_viz_ccc_extent(languagecode, top19, main_page_name + '/' + language_page_name)
        file_path_2 = make_viz_ccc_creation_monthly(languagecode,closest19, main_page_name + '/' + language_page_name)
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)


        # CCC GEOLOCATED ARTICLES PAGE
        file_path_1 = make_viz_ccc_geolocated_articles_map(languagecode, main_page_name + '/' + language_page_name + '/' + 'Geolocated_articles') # the own CCC map
        file_path_2 = make_viz_geographical_coverage_map(languagecode, main_page_name + '/' + language_page_name + '/' + 'Geolocated_articles') # world map coverage
        
        wikitext = make_table_countries_regions_gap(languagecode,'coverage')

        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Geolocated_articles')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

#---------#---------#---------#---------#---------#---------#---------#---------#---------#---------

        # CULTURE GAP PAGE
        file_path_1 = make_viz_ccc_culture_gap_covered(languagecode, wiki_path)
        file_path_2 = make_viz_ccc_culture_gap_covering(languagecode, wiki_path)
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Culture_Gap')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # CULTURE GAP OVER TIME
        file_path_1 = make_viz_ccc_bridging_culture_gap_over_time(languagecode, wiki_path)
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Culture_Gap_over_time')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # LANGUAGE VITAL ARTICLES (SPREAD) LIST PAGE / 100
        wikitext = make_table_ccc_all_vital_articles_lists(languagecode_covered, wiki_path, 'spread')        
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Languages_CCC_Languages_CCC_Vital_articles_lists_(spread)' + '/Table')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # LANGUAGE VITAL ARTICLES (COVERAGE) LIST PAGE / 100
        wikitext = make_table_ccc_all_vital_articles_lists(languagecode_covered, wiki_path, 'coverage')        
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Languages_CCC_Languages_CCC_Vital_articles_lists_(coverage)' + '/Table')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

        # COUNTRY VITAL ARTICLES (COVERAGE) LIST PAGE / 100
        wikitext = make_table_ccc_all_vital_articles_lists(languagecode_covered, wiki_path, 'coverage')        
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)
        page = pywikibot.Page(site, main_page_name + '/' + language_page_name + '/' + 'Countries_CCC_Languages_CCC_Vital_articles_lists_(coverage)' + '/Table')
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, async=False, callback=None,apply_cosmetic_changes=None, text=wikitext)

#---------#---------#---------#---------#---------#---------#---------#---------#---------#---------#---------

# TOOLS for publishing in Wikipedia (meta, language editions, commons, etc.)
def upload_publish_image(filename, description, url):
# upload pictures to a wikipedia
#https://phabricator.wikimedia.org/diffusion/PWBO/browse/master/upload.py
    print ('')


def get_hexcolorrange(color1, color2, scale, value_min, value_max, actualvalue):   
    interval=int((value_max - value_min)/scale)
    index=int(actualvalue/interval)
    colors=list(colour.Color(color1).range_to(colour.Color(color2), scale))
    choosencolor = colors[index].hex
    return choosencolor


def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

def finish_email():
    try:
        sys.stdout=None; send_email_toolaccount('WCDO UPDATE', open('wcdo_update.out', 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount('WCDO UPDATE aborted because of an error', open('wcdo_creation.out', 'r').read()+'err')


#######################################################################################


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'

    web_path = '/srv/wcdo/site/archive/'
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
    # Only those with a geographical context
    wikilanguagecodes_real = wikilanguagecodes
    for languagecode in languageswithoutterritory: wikilanguagecodes_real.remove(languagecode)

    # Get the number of articles for each Wikipedia language edition
    wikipedialanguage_numberarticles = load_wikipedia_language_editions_numberofarticles()

    # Final Wikipedia languages to process
    print (wikilanguagecodes)

    # SELECTING SPECIFIC LISTS OF WIKILANGUAGES
#    wikilanguagecodes = obtain_region_wikipedia_language_list('Oceania', '', '').index.tolist() # e.g. get the languages from a particular region.
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('cs')+1:]
#    wikilanguagecodes = ['ca']
#    wikilanguagecodes=wikilanguagecodes[wikilanguagecodes.index('ii'):]


    print ('\n* Starting the WCDO UPDATE CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* WCDO UPDATE CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
#    finish_email()