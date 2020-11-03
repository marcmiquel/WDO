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

    publish_missing_ccc_articles_lists()
    return

    while true:
        time.sleep(84600)
        print ("Good morning. It is: "+time.today()+". Let's see if today is the day to publish some stats in meta...")

        # CHAINED TO CCC CREATION (ONCE A MONTH) AND TOP CCC
        if verify_time_for_iteration():
            publish_wcdo_update_meta_pages()

######################################################################


# In this function we create the table language_territories_mapping. # CEE Spring.
def make_table_links_CEE():
    territories = wikilanguages_utils.load_languageterritories_mapping()
    languages_df = wikilanguages_utils.load_wiki_projects_information(territories);

    languages = ['en','az','ba','be','be-tarask','bs','bg','crh','de','el','eo','et','hr','hsb','hu','hy','ka','kk','lt','lv','mk','myv','pl','ro','ru','sh','sq','sr','tr','tt','uk']

    langu = ['az','ba','be','be_x_old','bs','bg','crh','de','el','et','hr','hsb','hu','hy','ka','kk','lt','lv','mk','myv','pl','ro','ru','sh','sq','sr','tr','tt','uk']

    rows_langs = {'az':'Azerbaijan','ba':'Bashkortostan','be':'Belarus','be_x_old':'Belarus','bs':'Bosnia and Herzegovina','bg':'Bulgaria','crh':'','de':'Austria','eo':'','el':'Greece','et':'Estonia','hr':'Croatia','hsb':'Germany','hu':'Hungary','hy':'Armenia','ka':'Georgia','kk':'Kazakhstan','lt':'Lithuania','lv':'Latvia','mk':'Macedonia','myv':'Russia','pl':'Poland','ro':'','ru':'Russia','sh':'','sq':'Albania','sr':'Serbia','tr':'Turkey','tt':'Tatarstan','uk':'Ukrania'}

    country_iso = {'Azerbaijan':'AZ','Belarus':'BY','Bosnia and Herzegovina':'BA','Bulgaria':'BG','Austria':'AT','Greece':'GR','Estonia':'EE','Croatia':'HR','Germany':'DE','Hungary':'HU','Armernia':'AM','Georgia':'GE','Kazakhstan':'KZ','Lithuania':'LT','Latvia':'LV','Macedonia':'MK','Russia':'RU','Poland':'PL','Albania':'AL','Serbia':'SR','Turkey':'TR'}

    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']

    lists_dict = {'editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'women':'Women', 'men':'Men', 'created_first_three_years':'Created First Three Years', 'created_last_year':'Created Last Year', 'pageviews':'Pageviews', 'discussions':'Discussions'}

    columns_final = ['List']+languages


    df_columns_list = columns_final

    wikitext = ''

    for language in langu:

        wikitext+= "==="+languages_df.loc[language]['languagename']+"===\n"

        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        header_string = '!'
        for x in range(0,len(df_columns_list)):
            if x == len(df_columns_list)-1: add = ''
            else: add = '!!'
            header_string = header_string + df_columns_list[x] + add

        header_string = header_string + '\n'

        rows = ''

        for lista in lists:
            midline = '|-\n'
            row_string = '|'
            row_string += lists_dict[lista]+'||'

            for row in languages:
                if row == 'uk': add = ''
                else: add = '||'
                # create the URL
                string = "https://wcdo.wmflabs.org/top_ccc_articles/?list="+lista
                string += "&target_lang="+row
                string += "&source_lang="+language

                if rows_langs[language] in country_iso:
                    string += "&source_country=" + country_iso[rows_langs[language]].lower()

                URL = '['+string+' '+'‌‌ '+']'

                row_string = row_string + str(URL) + add # here is the value

            row_string = midline + row_string + '\n'
            rows = rows + row_string

        closer_string = '|}'

        wiki_table_string = class_header_string + header_string + rows + closer_string
        wikitext += wiki_table_string+'\n\n'

    return wikitext


def publish_missing_ccc_articles_lists():

    glow_langs = ['sd','id', 'jv', 'su', 'hi', 'ta', 'te', 'mr', 'kn', 'ml', 'or', 'pa', 'sa', 'gu', 'en', 'ar', 'es']



#    glow_langs = ['sd']

    # Bahsa Indonesia id, Bahsa Jawa jv, Bahsa Sunda su, Hindi hi, Tamil ta, Telugu te, Marathi mr, Kannada kn, Malyalam ml, Odia or, Punjabi pa, Sanskrit sa, Gujarati gu, English - Geolocated for Nigeria en, Arabic - Jordan, Egypt and Tunisia ar, Spanish - Geolocated for Argentina es, Sindhi sd.    

    for languagecode in glow_langs:
        source_lang = 'None'


        languagename = languages.loc[languagecode]['languagename']
        try: qitems = territories.loc[languagecode]['QitemTerritory'].tolist()
        except: qitems = [territories.loc[languagecode]['QitemTerritory']]

        wikitext = ' = '+languagename+' Wikipedia Missing local articles =\n'


        line = 'Language '+languagename+' is spoken in: '
        i=0
        for qitem in qitems:
            i=i+1

            regional = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['regional']
            if regional == 'yes': regional = 'region'
            else:
                regional = 'country'
            territoryname = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['territoryname']
            ISO = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO31662']
            if ISO == '' or ISO == None:
                ISO = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode]['ISO3166']

#            if territoryname == None: territoryname = ''
            if i==len(qitems)-1:
                line = line + territoryname + ' ('+regional+' with ISO code '+ISO+') and '
            else:
                line = line + territoryname + ' ('+regional+' with ISO code '+ISO+'), '

        line = line[:len(line)-2]+'.'


        wikitext += 'This is the local content from '+languagename+' related territories that does not exist in '+languagename+' Wikipedia and yet it exists in other language editions, especially those of languages that are spoken also in these territories.\n'

        wikitext += line+'\n\n'


    # make_table_missing_ccc_articles(topic, order_by, limit, target_region, type, ccc_segment, target_lang, source_lang, target_country):


        wikitext += '== 500 Geolocated articles ==\n'
        # 500 places
        # GEOLOCATED
        # 100 amb més interwiki
        # 50 amb més inlinks from CCC
        # 25 amb més bytes
        # 25 amb més discussions
        wikitext = wikitext + '=== 100 Geolocated articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_interwiki', 100, 'None', 'None', 'geolocated', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 50 Geolocated articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_inlinks_from_original_CCC', 50, 'None', 'None', 'geolocated', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Geolocated articles sorted by number of Bytes ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_bytes', 25, 'None', 'None', 'geolocated', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Geolocated articles sorted by number of Edits in Talk Page ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_discussions', 25, 'None', 'None', 'geolocated', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # MONUMENTS AND BUILDINGS
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 Monuments and buildings articles == \n'

        wikitext = wikitext + '=== 25 Monuments and buildings articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('monuments_and_buildings', 'num_interwiki', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Monuments and buildings articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('monuments_and_buildings', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Monuments and buildings articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('monuments_and_buildings', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Monuments and buildings articles sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('monuments_and_buildings', 'num_references', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # GLAM
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 GLAM articles ==\n'

        wikitext = wikitext + '=== 25 GLAM articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('glam', 'num_interwiki', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 GLAM articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('glam', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 GLAM articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('glam', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 GLAM articles sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('glam', 'num_references', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # EARTH
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 Earth articles ==\n'

        wikitext = wikitext + '=== 25 Earth articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('earth', 'num_interwiki', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Earth articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('earth', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Earth articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('earth', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Earth articles sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('earth', 'num_references', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # 200 people
        # MEN
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 Men articles ==\n'
        wikitext = wikitext + '=== 25 Men articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('men', 'num_interwiki', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Men articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('men', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Men articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('men', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Men articles sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('men', 'num_references', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # WOMEN
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 Women articles ==\n'
        wikitext = wikitext + '=== 25 Women articles sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('women', 'num_interwiki', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Women articles sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('women', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Women articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('women', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 Women articles sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('women', 'num_references', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # 100 other topics
        wikitext += '== 100 Food, music, paintings and sports articles ==\n'

        # FOOD
        # 25 amb més pageviews
        wikitext = wikitext + '=== 25 Food articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('food', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        # MUSIC
        # 25 amb més pageviews
        wikitext = wikitext + '=== 25 Music articles sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('music_creations_and_organizations', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        # PAINTINGS
        # 25 amb més pageviews
        wikitext = wikitext + '=== 25 Paintings sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('paintings', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        # SPORTS AND TEAMS
        # 25 amb més pageviews
        wikitext = wikitext + '=== 25 Sports sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('sport_and_teams', 'num_pageviews', 25, 'None', 'None', 'None', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        # 100 general
        # KEYWORDS
        # 25 amb més interwiki
        # 25 amb més inlinks from CCC
        # 25 amb més pageviews
        # 25 amb més referències
        wikitext += '== 100 General language context-based articles ==\n'
        wikitext = wikitext + '=== 25 General articles with keywords sorted by number of Interwiki links ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_interwiki', 25, 'None', 'None', 'keywords', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 General articles with keywords sorted by number of Incoming links from Local Content (CCC) ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_inlinks_from_original_CCC', 25, 'None', 'None', 'keywords', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 General articles with keywords sorted by number of Pageviews ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_pageviews', 25, 'None', 'None', 'keywords', languagecode, source_lang, 'None')
        wikitext += '\n\n'

        wikitext = wikitext + '=== 25 General articles with keywords sorted by number of References ===\n'
        wikitext = wikitext + make_table_missing_ccc_articles('None', 'num_references', 25, 'None', 'None', 'keywords', languagecode, source_lang, 'None')
        wikitext += '\n\n'


        # new_path = languagecode+'.txt'
        # new_days = open(new_path,'w')
        # new_days.write(wikitext)

        site = pywikibot.Site('meta','meta')
        page = pywikibot.Page(site, 'User:Marcmiquel' + '/' + 'test'+'/'+languagecode)
        page.save(summary="X", watch=None, minor=False,botflag=False, force=False, asynchronous=False, callback=None,apply_cosmetic_changes=None, text=wikitext)




def make_table_missing_ccc_articles(topic, order_by, limit, target_region, type, ccc_segment, target_lang, source_lang, target_country):

    print (topic, order_by, limit, target_region, type, ccc_segment, target_lang, source_lang, target_country)

    e = (topic, order_by, limit, target_region, type, ccc_segment, target_lang, source_lang, target_country)
    charac = '_'.join(map(str,e))

    conn = sqlite3.connect(databases_path + 'missing_ccc.db'); cur = conn.cursor()

    # TARGET LANGUAGE
    target_language = languages.loc[target_lang]['languagename']

    if 'target_country' != 'None':
        target_country = target_country.upper()
        if target_country == 'NONE' or target_country == 'ALL': target_country = 'all'
    else:
        target_country = 'all'

    if 'target_region' != 'None':
        target_region = target_region.upper()
        if target_region == 'NONE' or target_region == 'ALL': target_region = 'all'
    else:
        target_region = 'all'

    # TOPIC
    type = "missing"

    # SOURCE lANGUAGE
    source_lang=source_lang.lower() #


    # CREATING THE QUERY
    query = 'SELECT '

    columns = ['num','source_lang','page_title','num_interwiki','num_pageviews']

    query += '"[[:" || languagecode || ":|" || languagecode || "]]" as source_lang, "[{{fullurl:" || languagecode || ":"|| page_title ||"}} " || REPLACE(page_title,"_"," ") || "]" as page_title, num_pageviews, num_interwiki, '

    if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_original_CCC','num_outlinks_to_original_CCC','num_bytes','num_references']: 
        query += order_by+', '
        columns = columns + [order_by]

    query += '("label" || " " || "(" || label_lang || ")" ) as label_lang, " [{{fullurl:" || "wikidata" || ":" || qitem || "}} " || REPLACE(qitem,"_"," ") || "]" as qitem '
    columns = columns + ['label_lang','qitem']

    query += 'FROM '+target_lang+'wiki '
    query += 'WHERE (page_title_original_lang IS NULL or page_id_original_lang IS NULL) '


    if ccc_segment == 'keywords':
        query += 'AND keyword_title IS NOT NULL '

    if ccc_segment == 'geolocated':
        query += 'AND (geocoordinates IS NOT NULL OR location_wd IS NOT NULL) '

    if target_country != "none" and target_country != "all":
        query += 'AND iso3166 = "'+target_country+'" '

    if target_region != "none" and target_region != "all":
        query += 'AND iso31662 = "'+target_region+'" '


    if topic != "none" and topic != "None" and topic != "all":
        if topic == 'men': # male
            query += 'AND gender = "Q6581097" '

        elif topic == 'women': # female
            query += 'AND gender = "Q6581072" '

        elif topic == 'people':
            query += 'AND gender IS NOT NULL '

        else:
            query += 'AND '+topic+' IS NOT NULL '

    if source_lang == 'coexist':
        query += 'AND non_language_pairs IS NULL '

    elif source_lang == 'nocoexist':
        query += 'AND non_language_pairs == 1 '

    elif source_lang != "none":
        query += 'AND languagecode = "'+source_lang+'" '

    query += 'AND (num_inlinks_from_original_CCC!=0 OR num_outlinks_to_original_CCC!=0) '

    if order_by == "none" or order_by == "None":
        query += 'ORDER BY num_pageviews DESC '
    else:
        query += 'ORDER BY '+order_by+' DESC '

    query += 'LIMIT 500;'


    # if limit == "none":
    #     query += 'LIMIT 100;'
    # else:
    #     query += 'LIMIT '+str(limit)+';'

    print(query)
    df = pd.read_sql_query(query, conn)#, parameters)
    df = df.fillna(0)

    if len(df)==0: return ''

    page_titles = df.page_title.tolist()
    for i in range(0,len(page_titles)-1):
        page_title = page_titles[i].split('}}')[1].strip()
        page_titles[i] = page_title[:len(page_title)-1]

#    print (page_titles)

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(target_lang); mysql_cur_read = mysql_con_read.cursor()
    page_titles_existing = []
    page_asstring = ','.join( ['%s'] * len(page_titles) )
    query = 'SELECT ll_title FROM langlinks WHERE ll_title IN (%s)' % page_asstring
    mysql_cur_read.execute(query, page_titles) # Extreure
    result = mysql_cur_read.fetchall()
    for row in result:
        page_titles_existing.append(row[0].decode('utf-8'))
    
    df.num_pageviews = df.num_pageviews.astype('int64')

    i = 0
    target_langy = '('+target_lang +')'
    qitems_list = []
    for index, row in df.iterrows():
        page_title = row['page_title'].split('}}')[1].strip()
        page_title = page_title[:len(page_title)-1]

        label_lang = row['label_lang']

        if label_lang == 0 or target_langy not in label_lang: 
            df.loc[index, 'label_lang'] = ''
        else:
            label_lang = label_lang.split('(')[0].strip()
            df.loc[index, 'label_lang'] = '[{{fullurl:'+target_lang+':'+label_lang.replace(' ','_')+'}} '+label_lang+']'

        if row['qitem'] in qitems_list or i>=limit or page_title in page_titles_existing:
            df.drop(index, inplace=True)
        else:
#            print ((row['page_title']))
            qitems_list.append(row['qitem'])
            i+=1


    column_list_dict = {'source_lang':'Wiki','page_title':'Title','num_pageviews':'Pageviews','num_interwiki':'Interwiki', 'num_inlinks_from_original_CCC':'Inlinks CCC','num_references':'References','num_bytes':'Bytes','num_discussions':'Discussions','label_lang':target_language+' WD Label','qitem':'WD Qitem'}

    df=df.rename(columns=column_list_dict)

    df_columns_list = df.columns.values.tolist()
    df_rows = df.values.tolist()


    path = '/srv/wcdo/src_viz/missing_ccc'
    if not os.path.exists(path):
        os.makedirs(path)

    path2 = path+'/'+target_lang
    if not os.path.exists(path2):
        os.makedirs(path2)

    file_name = path2+'/missing_ccc_'+target_lang+'_'+charac+'.txt'
    df.to_csv(file_name, sep='\t', encoding='utf-8')


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
    if len(df_rows)==0:
        wiki_table_string = ''

    return wiki_table_string





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
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

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
        if row[0] in languageswithoutterritory: continue
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
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor() 

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
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor() 

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

    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()

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



def verify_time_for_iteration():
    print ("Let's check it is the right time for meta update...")

    # CONDITION 1: CCC created this month.
    conn = sqlite3.connect(databases_path + ccc_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "set_production_ccc_db" AND year_month = ?;'
    cursor.execute(query,current_year_month_period)
    function_name1 = cursor.fetchone()

    # CONDITION 2: TOP CCC created this month.
    conn = sqlite3.connect(databases_path + top_ccc_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "update_top_ccc_articles_titles translations" AND year_month = ?;'
    cursor.execute(query,current_year_month_period)
    function_name2 = cursor.fetchone()

    # CONDITION 3: STATS created this month.
    conn = sqlite3.connect(databases_path + stats_db); cursor = conn.cursor()
    query = 'SELECT function_name FROM function_account WHERE function_name = "generate_pageviews_intersections" AND year_month = ?;'
    cursor.execute(query,current_year_month_period)
    function_name3 = cursor.fetchone()

    if function_name1 != None and function_name2 != None and function_name3 != None: 
        return True



#######################################################################################


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger()

    startTime = time.time()
    year_month = datetime.date.today().strftime('%Y-%m')

    databases_path = '/srv/wcdo/databases/'
    ccc_db = 'ccc.db'
    stats_db = 'stats.db'
    top_ccc_db = 'top_ccc_articles.db'


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

    print ('\n* Starting the WCDO UPDATE CYCLE '+year_month+' at this exact time: ' + str(datetime.datetime.now()))
    main()
    print ('* WCDO UPDATE CYCLE completed successfuly after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))

#    wikilanguages_utils.finish_email(startTime,'meta_update.out','Meta Update')