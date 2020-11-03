# -*- coding: utf-8 -*-
# This is the ROSETTA STONE of the project.

import wikilanguages_utils
from wikilanguages_utils import *
import eth_utils

from string import ascii_lowercase
import time
import os
import sys
import codecs
import pycountry
import requests
import babel.languages
from babel import Locale
import sqlite3
import pandas as pd
from lxml import html
import json


if sys.stdout.encoding is None: sys.stdout = codecs.open("/dev/stdout", "w", 'utf-8')
#sys.stdout=open("test.txt","w")


# INSTRUCTIONS: ****************************************************************************************************************
# MAIN
def main():


"""
  eth_utils.get_language_names_codes_url_eth()
  eth_utils.get_language_countries_mapping_eth()

  parse_languages_countries_mapping()
  parse_languages_territories_mapping()

  create_language_pairs_country_status_table()
  create_language_pairs_territory_status_table()
  create_wikipedia_language_pairs_territory_status_table()

  download_all_country_wikidata()
  download_all_country_subdivisions_wikidata()
  download_all_languages_wikidata()

  export_language_characteristics_to_complement('P31','language')
  export_language_characteristics_to_complement('P31','macrolanguage')
  export_language_characteristics_to_complement('P31','dead language')
  export_language_characteristics_to_complement('P220',1)
  export_language_characteristics_to_complement('P220',2)
  export_language_characteristics_to_complement('P1466',0)
  export_language_characteristics_to_complement('P1394',0)
  export_language_characteristics_to_complement('P625',0)
  export_language_characteristics_to_complement('P1705',0)
  export_language_characteristics_to_complement('P1627',1)
  export_language_characteristics_to_complement('P1627',2)
  export_language_characteristics_to_complement('P3823',0)
  export_language_characteristics_to_complement('P17',0)
  export_language_characteristics_to_complement('P2341',0)
  export_language_characteristics_to_complement('P1098',0)

  export_countries_language_characteristics_to_complement('P1705',0)
  export_countries_language_characteristics_to_complement('P1448',0)
  export_countries_language_characteristics_to_complement('P2936',0)
  export_countries_language_characteristics_to_complement('P37',0)

  export_countries_subdivisions_language_characteristics_to_complement('P1705',0)
  export_countries_subdivisions_language_characteristics_to_complement('P1448',0)
  export_countries_subdivisions_language_characteristics_to_complement('P2936',0)
  export_countries_subdivisions_language_characteristics_to_complement('P37',0)
"""


# Obtain all languages with a Wikipedia language edition and their characteristics.
# Main Source: WIKIDATA.
# Additional sources: Pycountry and Babel libraries for countries and their languages. Source: UNICODE: unicode.org/cldr/charts/latest/supplemental/language_territory_information.html
def download_wikipedia_language_editions_wikidata():

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?itemLabel ?language ?languageLabel ?alias ?nativeLabel ?languageISO ?languageISO3 ?languageISO5 ?languagelink ?Wikimedialanguagecode WHERE {
      ?item wdt:P31 wd:Q10876391.
      ?item wdt:P407 ?language.
      
      OPTIONAL{?language wdt:P1705 ?nativeLabel.}
      
#     ?item wdt:P856 ?officialwebsite.
#     ?item wdt:P1800 ?bbddwp.
      ?item wdt:P424 ?Wikimedialanguagecode.

      OPTIONAL {?language skos:altLabel ?alias FILTER (LANG (?alias) = ?Wikimedialanguagecode).}
      
      OPTIONAL{?language wdt:P218 ?languageISO .}
      OPTIONAL{?language wdt:P220 ?languageISO3 .}
      OPTIONAL{?language wdt:P1798 ?languageISO5 .}
      
      OPTIONAL{
      ?languagelink schema:about ?language.
      ?languagelink schema:inLanguage "en". 
      ?languagelink schema:isPartOf <https://en.wikipedia.org/>
      }
      
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?Wikimedialanguagecode'''

    
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    languages = []
    wikimedialanguagecode = ''

    Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];
    print ('COMENÇA')
    for item in data['results']['bindings']:
        
        print (item)
        #input('tell me')

        if wikimedialanguagecode != item['Wikimedialanguagecode']['value'] and wikimedialanguagecode!='':

            result = get_language_spread_unicode(languageISO, languageISO3, languageISO5)
            try:
                currentLanguagename=Locale.parse(wikimedialanguagecode).get_display_name(wikimedialanguagecode).lower()
                if currentLanguagename not in nativeLabel:
                    print ('YAHOO')
                    nativeLabel.append(currentLanguagename)
                    print(currentLanguagename)
            except:
                pass

            languages.append({
            'Qitem': ";".join(Qitem),
            'languagename': ";".join(languagename),
            'nativeLabel': ";".join(nativeLabel),
            'languageISO': ";".join(languageISO),
            'languageISO3': ";".join(languageISO3),
            'languageISO5': ";".join(languageISO5),
            'numbercountriesOfficialorRegional': result[0],
            'languageofficialnational': ";".join(result[1]),
            'languageofficialregional': ";".join(result[2]),
            'languageofficialsinglecountry': result[3],
            'WikipedialanguagearticleEnglish': englisharticle,
            'Wikipedia':wikipedia,
            'WikimediaLanguagecode': wikimedialanguagecode
            })
            #print (languages)
            #input('common')
            Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];

        Qitemcurrent = item['language']['value'].replace("http://www.wikidata.org/entity/","")
        if Qitemcurrent not in Qitem:
            Qitem.append(Qitemcurrent)

        languagenamecurrent = item['languageLabel']['value']
        if languagenamecurrent not in languagename:
            languagename.append(languagenamecurrent)

        try:
            nativeLabelcurrent = item['nativeLabel']['value'].lower()
            if nativeLabelcurrent not in nativeLabel:
                nativeLabel.append(nativeLabelcurrent)
        except:
            pass

        try: 
            aliascurrent = item['alias']['value'].lower()
            print (aliascurrent)
            if aliascurrent not in nativeLabel and len(aliascurrent)>3:
                nativeLabel.append(aliascurrent)
        except:
            pass

        try: 
            languageISOcurrent = item['languageISO']['value']
            if languageISOcurrent not in languageISO:
                languageISO.append(languageISOcurrent)
        except:
            pass

        try:
            languageISO3current = item['languageISO3']['value']
            if languageISO3current not in languageISO3:
                languageISO3.append(languageISO3current)
        except:
            pass

        try:
            languageISO5current = item['languageISO5']['value']
            if languageISO5current not in languageISO5:
                languageISO5.append(languageISO5current)
        except:
            pass

        try: englisharticle = item['languagelink']['value'] 
        except: englisharticle = 'no link'
    
        wikimedialanguagecode = item['Wikimedialanguagecode']['value'] # si 
        wikipedia = item['itemLabel']['value']

        #print (result)

    df = pd.DataFrame(languages)
    df = df.set_index(['languagename'])
    filename= 'Wikipedia_language_editions'
    newlanguages = []

    if os.path.isfile(filename+'.csv'): # FILE EXISTS: CREATE IT WITH THE NEW LANGUAGES IN THE FILENAME
        languages = pd.read_csv(filename+'.csv',sep='\t')
        languages=languages[['WikimediaLanguagecode']]
        languages = languages.set_index(['WikimediaLanguagecode'])

        languagelist = list(languages.index.values); languagelist.append('nan') # for the problem with the 'nan' code taken as a float
        languagelistcurrent = list(df['WikimediaLanguagecode'])

        newlanguages = list(set(languagelistcurrent) - set(languagelist))
        print ('These are the new languages:')
        print (newlanguages)

        filename="_".join(newlanguages)+'_'+filename
        if len(newlanguages)>0: df.to_csv(filename+'.csv',sep='\t')

    else: # FILE DOES NOT EXIST: CREATE IT WITH THE WHOLE NAME
        df.to_csv(filename+'.csv',sep='\t')

    return newlanguages


# THIS CREATES A TABLE WITH THE WIKIDATA QITEM FOR ALL LANGUAGES IN WIKIDATA
def download_all_languages_wikidata():

    for x in ['dialect','lang','natural_lang','modern_lang','isolated_lang','sign_lang','langue','creole','dead_lang','macrolanguage','']:
        lang_dialect = x

        query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?language ?languageLabel ?languagelink ?nativeLabel ?languageISO ?languageISO2 ?languageISO3 ?languageISO5 ?Ethnologuecode ?Ethnologuestatus ?speakers ?Wikimedialanguagecode ?WALS_code ?glottocode ?instanceof

        WHERE {
          ?language wdt:P31 wd:langqitem.
          OPTIONAL{?language wdt:P1705 ?nativeLabel.}
          OPTIONAL{?language wdt:P424 ?Wikimedialanguagecode.}

          OPTIONAL{?language wdt: ?languageISO5 .}
          OPTIONAL{?language wdt:P218 ?languageISO .}
          OPTIONAL{?language wdt:P219 ?languageISO2 .}
          OPTIONAL{?language wdt:P220 ?languageISO3 .}
          OPTIONAL{?language wdt:P1798 ?languageISO5 .}
          
          OPTIONAL{?language wdt:P1627 ?Ethnologuecode .}
          OPTIONAL{?language wdt:P3823 ?Ethnologuestatus .}
          OPTIONAL{?language wdt:P1098 ?speakers .}
          OPTIONAL{?language wdt:P1466 ?WALS_code .}
          OPTIONAL{?language wdt:P1394 ?glottocode .}
          OPTIONAL{?language wdt:P31 ?instanceof .}
          
          OPTIONAL{
          ?languagelink schema:about ?language.
          ?languagelink schema:inLanguage "en". 
          ?languagelink schema:isPartOf <https://en.wikipedia.org/>
          }
          
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }'''

        if lang_dialect == 'lang': lang_dialect_qitem = 'Q34770'
        if lang_dialect == 'dialect': lang_dialect_qitem = 'Q33384'
        if lang_dialect == 'natural_lang': lang_dialect_qitem = 'Q33742'
        if lang_dialect == 'modern_lang': lang_dialect_qitem = 'Q1288568'
        if lang_dialect == 'isolated_lang': lang_dialect_qitem = 'Q33648'
        if lang_dialect == 'sign_lang': lang_dialect_qitem = 'Q34228'
        if lang_dialect == 'langue': lang_dialect_qitem = 'Q4113741'
        if lang_dialect == 'creole': lang_dialect_qitem = 'Q33289'
        if lang_dialect == 'dead_lang': lang_dialect_qitem = 'Q45762'
        if lang_dialect == 'spurious': lang_dialect_qitem = 'Q568377'
        if lang_dialect == 'macrolanguage': lang_dialect_qitem = 'Q152559'

        query = query.replace('langqitem',lang_dialect_qitem)

        # with no instance of language
        if lang_dialect == '':
            query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT DISTINCT ?language ?languageLabel ?languagelink ?nativeLabel ?languageISO ?languageISO2 ?languageISO3 ?languageISO5 ?Ethnologuecode ?Ethnologuestatus ?speakers ?Wikimedialanguagecode ?WALS_code ?glottocode ?instanceof
            WHERE {
              ?language wdt:P220 ?languageISO3.

              OPTIONAL{?language wdt:P1705 ?nativeLabel.}

              OPTIONAL{?language wdt:P424 ?Wikimedialanguagecode.}

              OPTIONAL{?language wdt: ?languageISO5 .}
              OPTIONAL{?language wdt:P218 ?languageISO .}
              OPTIONAL{?language wdt:P219 ?languageISO2 .}
              OPTIONAL{?language wdt:P1798 ?languageISO5 .}
              
              OPTIONAL{?language wdt:P1627 ?Ethnologuecode .}
              OPTIONAL{?language wdt:P3823 ?Ethnologuestatus .}
              OPTIONAL{?language wdt:P1098 ?speakers .}
              OPTIONAL{?language wdt:P1466 ?WALS_code .}
              OPTIONAL{?language wdt:P1394 ?glottocode .}
              OPTIONAL{?language wdt:P31 ?instanceof .}
              
              OPTIONAL{
              ?languagelink schema:about ?language.
              ?languagelink schema:inLanguage "en". 
              ?languagelink schema:isPartOf <https://en.wikipedia.org/>
              }
              
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            }'''


        url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
        data = requests.get(url, params={'query': query, 'format': 'json'}).json()
        #print (data)

        languages = []


        params = ['languageLabel','languagelink','nativeLabel','languageISO','languageISO2','languageISO3','languageISO5','Ethnologuecode','Ethnologuestatus','speakers','WALS_code', 'glottocode','Wikimedialanguagecode', 'instanceof']

        print ('COMENÇA')
        for item in data['results']['bindings']:

            cur_lang = []
            Qitemcurrent = item['language']['value'].replace("http://www.wikidata.org/entity/","")
            cur_lang.append(Qitemcurrent)

            for param in params:
                try:
                    paramet = item[param]['value'].replace("http://www.wikidata.org/entity/","")

                    if param == 'speakers':
                        try:
                            paramet = int(paramet)
                        except:
                            paramet = ''
                except:
                    paramet = ''

                cur_lang.append(paramet)

            print ('\n')
            print (cur_lang)
            languages.append(cur_lang)

    #    print (languages)

        conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
        query = ('CREATE TABLE IF NOT EXISTS all_languages_wikidata ('+
        'Qitem text,'+
        'englishLabel text,'+
        'englishwikipediaarticle text,'+
        'nativeLabel text,'+
        'languageISO text,'+
        'languageISO2 text,'+
        'languageISO3 text,'+
        'languageISO5 text,'+
        'Ethnologuecode text,'+
        'Ethnologuestatus text,'+
        'speakers integer,'+
        'WALS_code text,'+
        'glottocode text,'+
        'Wikimedialanguagecode text,'+
        'instanceof text,'+

        'PRIMARY KEY (Qitem));')

        cursor.execute(query)
        conn.commit()

        query = 'INSERT OR IGNORE INTO all_languages_wikidata (Qitem, englishLabel, englishwikipediaarticle, nativeLabel, languageISO, languageISO2, languageISO3, languageISO5, Ethnologuecode, Ethnologuestatus, speakers, WALS_code, glottocode, Wikimedialanguagecode, instanceof) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'

        cursor.executemany(query, languages)
        conn.commit()




def download_all_country_wikidata():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = ('CREATE TABLE IF NOT EXISTS all_countries_wikidata ('+
    'Qitem text,'+
    'territoryLabel text,'+
    'englishwikipediaarticle text,'+

    'nativename text,'+
    'officialname text,'+

    'languageusedLabel text,'+
    'officiallanguageLabel text,'+
    'ISO3166 text,'+

    'PRIMARY KEY (Qitem, nativename, officialname, languageusedLabel, officiallanguageLabel));')



    cursor.execute(query)
    conn.commit()

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?territory ?territoryLabel ?nativename ?officialname ?languageusedLabel ?officiallanguageLabel ?englishwikipediaarticle ?ISO3166
    WHERE
    {
      ?territory wdt:P297 ?ISO3166.
      OPTIONAL { ?territory wdt:P1448 ?officialname }
      OPTIONAL { ?territory wdt:P1705 ?nativename }
      OPTIONAL { ?territory wdt:P2936 ?languageused }
      OPTIONAL { ?territory wdt:P37 ?officiallanguage }

      OPTIONAL{
        ?englishwikipediaarticle schema:about ?territory.
        ?englishwikipediaarticle schema:inLanguage "en". 
        ?englishwikipediaarticle schema:isPartOf <https://en.wikipedia.org/>
      }

      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }       
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    territories = []
    params = ['territoryLabel', 'englishwikipediaarticle', 'nativename', 'officialname', 'languageusedLabel', 'officiallanguageLabel','ISO3166']

    try:
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data != None: data = data.json()

        for item in data['results']['bindings']:

    #        print (item)
            cur_ter = []
            Qitemcurrent = item['territory']['value'].replace("http://www.wikidata.org/entity/","")
            cur_ter.append(Qitemcurrent)

            for param in params:
                try:
                    paramet = item[param]['value'].replace("http://www.wikidata.org/entity/","")
                except:
                    paramet = ''

                cur_ter.append(paramet)

            print ('\n')
            print (cur_ter)
            territories.append(cur_ter)
    except:
        pass

    query = 'INSERT OR IGNORE INTO all_countries_wikidata (Qitem, territoryLabel, englishwikipediaarticle, nativename, officialname, languageusedLabel, officiallanguageLabel, ISO3166) VALUES (?,?,?,?,?,?,?,?);'

    cursor.executemany(query, territories)
    conn.commit()



def download_all_country_subdivisions_wikidata():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = ('CREATE TABLE IF NOT EXISTS all_countries_subdivisions_wikidata ('+
    'Qitem text,'+
    'territoryLabel text,'+
    'englishwikipediaarticle text,'+

    'nativename text,'+
    'officialname text,'+
    'demonym text,'+

    'languageusedLabel text,'+
    'officiallanguageLabel text,'+
    'ISO31662 text,'+

    'PRIMARY KEY (Qitem, nativename, officialname, languageusedLabel, officiallanguageLabel));')

    cursor.execute(query)
    conn.commit()

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?territory ?territoryLabel ?nativename ?officialname ?languageusedLabel ?officiallanguageLabel ?englishwikipediaarticle ?ISO31662
    WHERE
    {
      ?territory wdt:P300 ?ISO31662.
      OPTIONAL { ?territory wdt:P1448 ?officialname }
      OPTIONAL { ?territory wdt:P1705 ?nativename }
      OPTIONAL { ?territory wdt:P2936 ?languageused }
      OPTIONAL { ?territory wdt:P37 ?officiallanguage }

      OPTIONAL{
        ?englishwikipediaarticle schema:about ?territory.
        ?englishwikipediaarticle schema:inLanguage "en". 
        ?englishwikipediaarticle schema:isPartOf <https://en.wikipedia.org/>
      }

      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }       
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    territories = []
    params = ['territoryLabel', 'englishwikipediaarticle', 'nativename', 'officialname', 'languageusedLabel', 'officiallanguageLabel', 'ISO31662']

    try:
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data != None: data = data.json()

        for item in data['results']['bindings']:

    #        print (item)
            cur_ter = []
            Qitemcurrent = item['territory']['value'].replace("http://www.wikidata.org/entity/","")
            cur_ter.append(Qitemcurrent)

            for param in params:
                try:
                    paramet = item[param]['value'].replace("http://www.wikidata.org/entity/","")
                except:
                    paramet = ''

                cur_ter.append(paramet)

            print ('\n')
            print (cur_ter)
            territories.append(cur_ter)
    except:
        pass

    query = 'INSERT OR IGNORE INTO all_countries_subdivisions_wikidata (Qitem, territoryLabel, englishwikipediaarticle, nativename, officialname, languageusedLabel, officiallanguageLabel, ISO31662) VALUES (?,?,?,?,?,?,?,?);'


    cursor.executemany(query, territories)
    conn.commit()





def get_language_names_codes_url_eth():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    
    cursor.execute('CREATE TABLE IF NOT EXISTS ethnologue_languages_names (language_code text, language_name text, PRIMARY KEY (language_code));')
    conn.commit()


    for x in ascii_lowercase: #ascii_lowercase

      url = 'https://web.archive.org/web/20190612162113/https://www.ethnologue.com/browse/names/'+x
      print (url)

      page = ''
      while page == '':
        try:
          page = requests.get(url)
          tree = html.fromstring(page.content)

          langs = list()
          for row in range(1,300):

              for column in range (1,5): # 1,5 
                  try:
                      language_name = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/table/tbody/tr['+str(row)+']/td['+str(column)+']/div/span/a/text()')[0]
      #               language_name = language_name.encode('utf-8')
                      language_code = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/table/tbody/tr['+str(row)+']/td['+str(column)+']/div/span/a/@href')[0]
                      language_code = str(language_code).split('/')[7]

                      langs.append((language_name,language_code)); print ((language_name,language_code))
                  except:
                      continue

          query = 'INSERT OR IGNORE INTO ethnologue_languages_names (language_name,language_code) VALUES (?,?);';
          cursor.executemany(query,langs); print (len(langs))
          conn.commit()
          time.sleep(100)
        except:
          print("Connection refused by the server..")
          print("Let me sleep for 300 seconds")
          time.sleep(500)
          print("Was a nice sleep, now let me continue...")
          continue          


def get_language_countries_mapping_eth(): # extracting from ethnologue.com
	# create table language-territories mapping

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  
    query = 'SELECT language_code, language_name FROM ethnologue_languages_names;'

    all_languages = {}
    for row in cursor.execute(query):
        language_code=str(row[0])
        language_name =str(row[1])
        all_languages[language_code]=language_name
    print ('all languages are: '+str(len(all_languages)))

    query = ('CREATE TABLE IF NOT EXISTS ethnologue_raw_language_countries_mapping ('+
    'language_code text,'
    'language_name text, '+
    'autonym text, '
    'alternate_names text, '+
    'country text,'
    'first_country integer,'
    'location text, '+
    'population text, '+
    'language_status text, '+
    'classification text, '+
    'dialects text, '+
    'typology text, '+
    'language_use text, '+
    'language_development text, '+
    'writing text, '+
    'other_comments text, '+

    'PRIMARY KEY (language_code,country));')
    cursor.execute(query); conn.commit()

    pending_languages = all_languages
    query = 'SELECT language_code FROM ethnologue_raw_language_countries_mapping;'
    for row in cursor.execute(query):
        try:
            del pending_languages[str(row[0])]
        except:
            pass

    print ('pending languages are: '+str(len(pending_languages)))

#    pending_languages = {'cat':'ah'}
    language_parameters=[]
    pending_lang_num = len(pending_languages)

    for language_code in pending_languages.keys():
        countries = []

        print ('\n'+str(pending_lang_num))
        print ('* LANGUAGE: '+language_code)
        print (pending_languages[language_code].encode('utf-8'))

        
        url = 'https://web.archive.org/web/20190612162113/https://www.ethnologue.com/language/'
        url = url + language_code
        print (url)

        page = ''
        while page == '':

          try:
            page = requests.get(url)
            print (str(page))

            if str(page) == '<Response [503]>':
                print ('Response 503. BANNED.')
                print (url)
                exit()

          except:
            print("Connection refused by the server..")
            print("Let me sleep for 300 seconds")
#            time.sleep(300)
            print("Was a nice sleep, now let me continue...")
            continue


        if str(page) == '<Response [404]>':
            print ('No codes ISO3 or ISO5. Response 404.')
            print (url)
            continue

        print (language_code)
        tree = html.fromstring(page.content)

        # START #
        ################################

        language_code = language_code;
        language_name = tree.xpath('//*[@id="page-title"]/text()')[0]
        country = '';
        first_country = 1;
        location = ''; 
        population = '';
        autonym = '';
        alternate_names = '';
        language_status = '';
        classification = '';
        dialects = '';
        typology = '';
        language_use = '';
        language_development = '';
        writing = '';
        other_comments = '';

        country = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div[1]/div/div/h2/a/text()')
        if len(country)!=0: country = country[0]
        else:
            print ((language_code)+(' code did not retrieve any result.'));
            continue;

        countries.append(country)

        num=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/text()'))
        for count in range(2,num):
            title = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[1]/text()')[0]

            if title == 'Alternate Names': alternate_names = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/text()')[0]

            if title == 'Autonym': autonym = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/text()')[0]

            if title == 'Location': location = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Population': population = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Language Status': language_status = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0] # https://www.ethnologue.com/about/language-status

            if title == 'Classification': classification = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/a/text()')[0]

            if title == 'Dialects': dialects = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Typology': typology = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Language Use': language_use = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Language Development': language_development = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/text()')[0]

            if title == 'Writing': writing = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

            if title == 'Other Comments': other_comments = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]

        language_parameters.append((language_code, language_name, autonym, country, first_country, location, population, alternate_names, language_status, classification, dialects, typology, language_use, language_development, writing, other_comments))

        othercountries=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/text()'))
        for countrycount in range(1,othercountries):
            country=tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/legend/span/span/text()')[0]
            first_country = 0;
            location = ''; 
            population = '';
            autonym = '';
            alternate_names = '';
            language_status = '';
            classification = '';
            dialects = '';
            typology = '';
            language_use = '';
            language_development = '';
            writing = '';
            other_comments = '';

#           print (country.encode('utf-8'))
            countries.append(country)

            num=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/text()'))-1
            for count in range(1,num):
                title = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/strong/text()')[0]

                if title == 'Population': population = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Alternate Names': alternate_names = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Autonym': autonym = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Language Development': language_development = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Language Use': language_use = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Writing': writing = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Location': location = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Status': language_status = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

                if title == 'Other Comments': other_comments = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]

            language_parameters.append((language_code, language_name, autonym, country, first_country, location, population, alternate_names, language_status, classification, dialects, typology, language_use, language_development, writing, other_comments))

        print ('A total number of countries: '+str((len(countries))))

        query = 'INSERT OR IGNORE INTO ethnologue_raw_language_countries_mapping (language_code, language_name, autonym, country, first_country, location, population, alternate_names, language_status, classification, dialects, typology, language_use, language_development, writing, other_comments) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
        cursor.executemany(query,language_parameters)
        conn.commit()
        pending_lang_num = pending_lang_num - 1
        sleeptime = 5
        print ('sleeping '+str(sleeptime)+' seconds...')
        time.sleep(sleeptime)
        print ('just woke up.')
        print (language_code)



def parse_languages_countries_mapping():


    country_dict = {'Tanzania':'Tanzania, United Republic of', 'Venezuela': 'Venezuela (Bolivarian Republic of)', 'United Kingdom': 'United Kingdom of Great Britain and Northern Ireland', 'United States': 'United States of America', 'São Tomé e Príncipe': 'Sao Tome and Principe', 'Laos': "Lao People's Democratic Republic", "Côte d’Ivoire": "Côte d'Ivoire", "Saint Martin": "Saint Martin (French part)", "Macedonia": "Macedonia (the former Yugoslav Republic of)", "East Timor": "Timor-Leste", "Syria": "Syrian Arab Republic", "Democratic Republic of the Congo": "Congo (Democratic Republic of the)", "Iran": "Iran (Islamic Republic of)", "Palestine": "Palestine, State of", "U.S. Virgin Islands": "Virgin Islands (U.S.)", "Taiwan": "Taiwan, Province of China", "Bolivia": "Bolivia (Plurinational State of)", "Brunei": "Brunei Darussalam", "China–Hong Kong": "Hong Kong", "China–Taiwan": "Taiwan, Province of China", "China–Macao": "Taiwan, Province of China", "Curacao":"Curaçao","Aland Islands": "Åland Islands", "Moldova":"Moldova (Republic of)", "Korea, South":"Korea (Republic of)", "Cape Verde Islands":"Cabo Verde", "Vatican State":"Holy See", "Micronesia":"Micronesia (Federated States of)","Sint Maarten":"Sint Maarten (Dutch part)","British Virgin Islands":"Virgin Islands (British)", "Caribbean Netherlands":"Bonaire, Sint Eustatius and Saba", "Czech Republic":"Czechia", "Falkland Islands": "Falkland Islands (Malvinas)","Saint Helena, Ascension, and Tristan da Cunha":"Saint Helena, Ascension and Tristan da Cunha","North Korea":"Korea (Democratic People's Republic of)"}


    # load_sil_ISO649_3_codes

    # https://iso639-3.sil.org/code_tables/download_tables
    # tables are: a) ISO 639-3 code set and b) macrolanguage mappings.
    # iso-639-3_20190125.tab and iso-639-3-macrolanguages_20190125.tab

    # LLEGINT LANGUAGE CODES
    # Id is the one we want.
    # Part2B WE DO NOT WANT IT!
    # Part2T is the ISO 639-2 we want.
    # Ref_Name is the name we want.
    languages_pd = pd.read_csv(dumps_path + 'iso-639-3_20190125'+'.tab',sep='\t',na_filter = False)
    languages_pd = languages_pd[['Id','Part2B','Part2T','Part1','Scope','Language_Type','Ref_Name']]
    """
         Id      char(3) NOT NULL,  -- The three-letter 639-3 identifier
         Part2B  char(3) NULL,      -- Equivalent 639-2 identifier of the bibliographic applications 
                                    -- code set, if there is one
         Part2T  char(3) NULL,      -- Equivalent 639-2 identifier of the terminology applications code 
                                    -- set, if there is one
         Part1   char(2) NULL,      -- Equivalent 639-1 identifier, if there is one    
         Scope   char(1) NOT NULL,  -- I(ndividual), M(acrolanguage), S(pecial)
         Type    char(1) NOT NULL,  -- A(ncient), C(onstructed),  
                                    -- E(xtinct), H(istorical), L(iving), S(pecial)
         Ref_Name   varchar(150) NOT NULL,   -- Reference language name 
         Comment    varchar(150) NULL)       -- Comment relating to one or more of the columns
    """

    languages_pd = languages_pd.set_index(['Id'])



    # LLEGINT MACROLANGUAGES
    # fem un diccionari normal allà on hi hagi A a la tercera columna.
    macrolanguage_pd = pd.read_csv(dumps_path + 'iso-639-3-macrolanguages_20190125'+'.tab',sep='\t',na_filter = False)
    macrolanguage_pd = macrolanguage_pd[['M_Id','I_Id','I_Status']]


    """
         M_Id      char(3) NOT NULL,   -- The identifier for a macrolanguage
         I_Id      char(3) NOT NULL,   -- The identifier for an individual language
                                       -- that is a member of the macrolanguage
         I_Status  char(1) NOT NULL)   -- A (active) or R (retired) indicating the
                                       -- status of the individual code element
    """

#    macrolanguage_pd = macrolanguage_pd.set_index(['M_Id'])
#    macro_pd_M_Id = macrolanguage_pd.I_Id.to_dict()

    # this is the dictionary to get a macro from a micro
    macrolanguage_pd = macrolanguage_pd.set_index(['I_Id'])
    macro_pd_I_Id = macrolanguage_pd.M_Id.to_dict()

    # this is the dictionary to get an array of micros from a macro
    new_micro = {}
    for micro,macro in macro_pd_I_Id.items():
        if macro not in new_micro:
            new_micro[macro]=[]
            new_micro[macro].append(micro)
        else:
            new_micro[macro].append(micro)


    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    query = 'SELECT WikimediaLanguagecode, languageISO, languageISO3, languageISO5 FROM wiki_projects;'
    wiki_languages = pd.read_sql_query(query, conn)

    wiki_languages = wiki_languages.set_index(['languageISO'])
    languageISOwiki = wiki_languages.WikimediaLanguagecode.to_dict()
    if '' in languageISOwiki: del languageISOwiki['']
    if None in languageISOwiki: del languageISOwiki[None]

    wiki_languages = wiki_languages.set_index(['languageISO3'])
    languageISO3wiki = wiki_languages.WikimediaLanguagecode.to_dict()
    if '' in languageISO3wiki: del languageISO3wiki['']
    if None in languageISO3wiki: del languageISO3wiki[None]

    wiki_languages = wiki_languages.set_index(['languageISO5'])
    languageISO5wiki = wiki_languages.WikimediaLanguagecode.to_dict()
    if '' in languageISO5wiki: del languageISO5wiki['']
    if None in languageISO5wiki: del languageISO5wiki[None]

    query = 'SELECT alpha_2, name, region, sub_region, intermediate_region FROM country_regions;'
    country_regions = pd.read_sql_query(query, conn)
    country_regions = country_regions.set_index(['name'])

    regions = country_regions.region.to_dict()
    subregions = country_regions.sub_region.to_dict()
    intermediate_regions = country_regions.intermediate_region.to_dict()
    alpha_2 = country_regions.alpha_2.to_dict()


    query = 'SELECT language_code, language_name, autonym, alternate_names, country, first_country, location, population, language_status, classification, dialects, typology, language_use, language_development, writing, other_comments FROM ethnologue_raw_language_countries_mapping WHERE language_code NOT IN (SELECT language_code_ISO639_3 FROM language_countries_mapping);'

    status_codes = {'0': 'International', '1':'National', '2': 'Provincial', '3': 'Wider Comomunication', '4':'Educational', '5':'Developing', '6a': 'Vigorous', '6b': 'Threatened', '7': 'Shifting', '8a': 'Moribund', '8b': 'Nearly Extinct', '9': 'Dormant', '10':'Extinct'}

    # LOAD THE DATA
    language_parameters = []
    for row in cursor.execute(query):
#        print (row)

        language_code = row[0]
        language_name = row[1]
        WikimediaLanguagecode = ''

        # ARRANGING THE NEW DATA
        language_code_ISO639_3 = language_code
        try:
            language_code_ISO639_2 = languages_pd.loc[language_code]['Part2T']
        except:
            language_code_ISO639_2 = ''
        try:
            language_code_ISO639_1 = languages_pd.loc[language_code]['Part1']
        except:
            language_code_ISO639_1 = ''


        language_name_inverted = language_name
        try:
            language_name = languages_pd.loc[language_code]['Ref_Name']
        except:
            continue
 

        try:
            macrolanguage_ISO639_3_code = macro_pd_I_Id[language_code] 
            #macrolanguage_pd.loc[language_code]['M_Id']
            com = 1
        except:
            macrolanguage_ISO639_3_code = language_code
            com = 0


        try:
            WikimediaLanguagecode = languageISOwiki[language_code_ISO639_1]
        except:
            try:
                WikimediaLanguagecode = languageISO2wiki[language_code_ISO639_2]
            except:
                try:
                    WikimediaLanguagecode = languageISO3wiki[language_code_ISO639_3]
                except:
                    try:
                        WikimediaLanguagecode = languageISO3wiki[macrolanguage_ISO639_3_code]
                    except:
                        try:
                            microlangs = new_micro[language_code_ISO639_3]
                            for micro in microlangs:
                                if micro in languageISO3wiki:
                                    WikimediaLanguagecode = languageISO3wiki[micro]
                        except:
                            WikimediaLanguagecode = ''



        if com == 1:
            composition = 'part of macrolanguage'

        if com == 0:
            composition = 'independent'

        if languages_pd.loc[language_code]['Scope'] == 'M':
            composition = 'macrolanguage'


        macrolanguage_name = ''
        macrolanguage_name = languages_pd.loc[macrolanguage_ISO639_3_code]['Ref_Name']

        lang_type = languages_pd.loc[language_code]['Language_Type']
        if lang_type == 'A': lang_type = 'Ancient'
        if lang_type == 'C': lang_type = 'Constructed'
        if lang_type == 'E': lang_type = 'Extinct'
        if lang_type == 'H': lang_type = 'Historical'
        if lang_type == 'L': lang_type = 'Living'
        if lang_type == 'S': lang_type = 'Special'

        autonym = row[2]
        alternate_names = row[3]

        language_status_code = row[8]

        language_status_code = language_status_code[0:2].replace(' ','')
        language_status_code = language_status_code.replace('*','')
        
        if language_status_code == "Un": continue # Unattested.

        if language_status_code == '' and macrolanguage_name != '':
            language_status = ''
        else:
            language_status = status_codes[language_status_code]
        
            

        other_comments = row[15].lower()
        if 'non-indigenous' in other_comments:
            indigenous = 0
        else:
            indigenous = 1

        country = row[4]
        country_original = country
        # Exceptions
        if country in country_dict: country = country_dict[country]

        try:
            population = int(row[7].split(' ')[0].replace(",",""))
        except:
            population = 0

        # WE ARE ONLY INTERESTED IN STATUS 1-3 OR INDIGENOUS.
        if indigenous == 0 and language_status_code >= '4': continue

        location = row[6]


        territory = country_original+': '+ location

        ISO3166 = alpha_2[country]
        ISO3166_2 = ''

        region = regions[country]
        sub_region = subregions[country]
        intermediate_region = intermediate_regions[country]


        # ADD
        language_parameters.append((language_code_ISO639_3,language_code_ISO639_2,language_code_ISO639_1,WikimediaLanguagecode,macrolanguage_ISO639_3_code,macrolanguage_name,composition,lang_type,language_name_inverted,language_name,autonym,alternate_names,language_status_code,language_status,indigenous,territory,country,ISO3166,ISO3166_2,population,region,sub_region,intermediate_region))

    """
        print (('language_code_ISO639_3',language_code_ISO639_3,'language_code_ISO639_2',language_code_ISO639_2,'language_code_ISO639_1',language_code_ISO639_1,'WikimediaLanguagecode',WikimediaLanguagecode,'macrolanguage_ISO639_3_code',macrolanguage_ISO639_3_code,'macrolanguage_name',macrolanguage_name,'composition',composition,'lang_type',lang_type,'language_name_inverted',language_name_inverted,'language_name',language_name,'autonym',autonym,'alternate_names',alternate_names,'language_status_code',language_status_code,'language_status',language_status,'indigenous',indigenous,'territory',territory,'country',country_original,'ISO3166',ISO3166,'ISO3166_2',ISO3166_2,'population',population,region,sub_region,intermediate_region))

        if language_code_ISO639_3 == 'nor':
#        if WikimediaLanguagecode == '': 
#            print ('\n')
            print (language_code_ISO639_1,language_code_ISO639_2,language_code_ISO639_3)
            print (WikimediaLanguagecode,language_name)
            print (row)
            input('')
    """



    # STORE THE DATA
    query = ('CREATE TABLE IF NOT EXISTS language_countries_mapping ('+
    'language_code_ISO639_3 text,'+ # this is the basic ethnologue code we have.
    'language_code_ISO639_2 text,'+ # this is sometimes 2 or 5, also three characters.
    'language_code_ISO639_1 text,'+ # this is the two character code. the original used in Wikimedia.
    'WikimediaLanguagecode text,'+

    'macrolanguage_ISO639_3_code text, '+ # this is the iso639-3 of the macrolanguage they relate to.
    'macrolanguage_name text, '+ # this is the macrolanguage name or the same languagename_sil in case there is no macrolanguage.
    'composition text, '+ # part of macrolanguage, macrolanguage, independent language. -> això està fet a partir de scope de ISO639-3.sil: collective or individual. collective means it is a macrolanguage, individual it means it is either an independent language or part of a macrolanguage.
    'lang_type text, '+ # type from iso639-3.sil.org: A(ncient), C(onstructed), E(xtinct), H(istorical), L(iving) and S(pecial).

    'language_name_inverted text, '+ # ethnologue origin. this is the one we obtain from the language_countries_mapping table.
    'language_name text,'+ #iso639-3.sil.org origin
    'autonym text, '+
    'alternate_names text, '+

    'language_status_code text, '
    'language_status text, '+
    'indigenous integer,'
    'country text,'
    'territory text, '+

    'ISO3166 text, '+
    'ISO3166_2 text, '+
    'population integer, '+

    'region text, '+
    'sub_region text, '+
    'intermediate_region text, '+

    'PRIMARY KEY (language_code_ISO639_3,territory,country));')
    cursor.execute(query); conn.commit()

    print ('inserting.')
    query = 'INSERT OR IGNORE INTO language_countries_mapping (language_code_ISO639_3,language_code_ISO639_2,language_code_ISO639_1,WikimediaLanguagecode,macrolanguage_ISO639_3_code,macrolanguage_name,composition,lang_type,language_name_inverted,language_name,autonym,alternate_names,language_status_code,language_status,indigenous,territory,country,ISO3166,ISO3166_2,population,region,sub_region,intermediate_region) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' 
    cursor.executemany(query,language_parameters)
    conn.commit()
    print ('done.')



def parse_languages_territories_mapping():


    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();


    query = ('CREATE TABLE IF NOT EXISTS language_territories_mapping ('+

    'location_text text, '+

    # TERRITORY
    'qitem text,'+
    'territoryname text, '+
    'territorynameNative text, '+
    'demonym text, '+
    'demonymNative text, '+

    'country text,'
    'ISO3166 text, '+
    'ISO3166_2 text, '+
    'regional text, '+

    # LANGUAGE CHARACTERISTICS
    'language_name text,'+ #iso639-3.sil.org origin
    'composition text, '+ # part of macrolanguage, macrolanguage, independent language. -> això està fet a partir de scope de ISO639-3.sil: collective or individual. collective means it is a macrolanguage, individual it means it is either an independent language or part of a macrolanguage.
    'language_status_code text, '
    'language_status text, '+
    'indigenous integer,'
    'population integer, '+

    # TERRITORY CHARACTERISTICS
    'region text, '+
    'sub_region text, '+
    'intermediate_region text, '+


    # LANGUAGE CHARACTERISTICS
    'language_name_inverted text, '+ # ethnologue origin. this is the one we obtain from the language_countries_mapping table.

    'language_code_ISO639_3 text,'+ # this is the basic ethnologue code we have.
    'language_code_ISO639_2 text,'+ # this is sometimes 2 or 5, also three characters.
    'language_code_ISO639_1 text,'+ # this is the two character code. the original used in Wikimedia.
    'WikimediaLanguagecode text,'+

    'macrolanguage_ISO639_3_code text, '+ # this is the iso639-3 of the macrolanguage they relate to.
    'macrolanguage_name text, '+ # this is the macrolanguage name or the same languagename_sil in case there is no macrolanguage.
    'lang_type text, '+ # type from iso639-3.sil.org: A(ncient), C(onstructed), E(xtinct), H(istorical), L(iving) and S(pecial).
    'autonym text, '+
    'alternate_names text, '+



    'PRIMARY KEY (language_code_ISO639_3,ISO3166,ISO3166_2,qitem));')
    cursor.execute(query); conn.commit()

    typeslist=[]
    subdivisions=list(pycountry.subdivisions)
    for subdivision in subdivisions:
        if subdivision.type not in typeslist: typeslist.append(subdivision.type)
    subdivisions_names = ['unit','region','province','prefecture','municipality','governorate','county','administration','zones','ward','voivodships','voivodship','urban','units','unitary','union','towns','town','tier','territory','territories','territorial','status','states','state','self','sector','republics','republican','republic','regions','rayons','quarters','quarter','provinces','prefectures','precincts','popularates','part','parishes','parish','overseas','oversea','oblasts','nations','municipalities','metropolitan','london','local','islands','island','groups','group','governorates','governing','governed','geographical','former','federal','entity','entities','economic','divisions','division','districts','district','development','dependency','dependencies','departments','department','country','countries','counties','councils','council','corporation','community','communities','communes','commune','collectivity','collectivities','city','cities','chains','capital','cantons','boroughs','autonomy','autonomous','authority','authorities','atolls','areas','area','administrative','administrations','administered','region de','region']
    typeslist = typeslist + subdivisions_names


    multilingual_ISO3166_2 = {}
    query = 'SELECT subdivision_name, subdivision_code FROM multilingual_ISO3166_2 ORDER BY subdivision_code;'
    for row in cursor.execute(query):
        name = row[0].lower()
        subdivision_code = row[1]
        for type in typeslist: name=name.replace(type,'').rstrip()

        multilingual_ISO3166_2[name]=subdivision_code


    wikidatacountrysubdivisions_all = download_all_world_subdivisions_wikidata()

    wikipedia_languages_territories_mapping = get_wikipedia_languages_territories_mapping()
    WikimediaLanguagecodes_in = []


    query = 'SELECT territory, country, ISO3166, language_name, composition, language_status_code, language_status, indigenous, population, region, sub_region, intermediate_region, language_name_inverted, language_code_ISO639_3, language_code_ISO639_2, language_code_ISO639_1, WikimediaLanguagecode, macrolanguage_ISO639_3_code, macrolanguage_name, lang_type, autonym, alternate_names FROM language_countries_mapping WHERE language_status_code != "" ORDER BY country, language_status_code DESC;'

    mapping_parameters = []
    i = 0
    for row in cursor.execute(query):
        i += 1

        location = row[0]
        country = row[1]
        ISO3166 = row[2]

        language_name = row[3]
        composition = row[4]
        language_status_code = row[5]
        language_status = row[6]
        indigenous = row[7]
        population = row[8]
        region = row[9]
        sub_region = row[10]
        intermediate_region = row[11]
        language_name_inverted = row[12]
        language_code_ISO639_3 = row[13]
        language_code_ISO639_2 = row[14]
        language_code_ISO639_1 = row[15]
        WikimediaLanguagecode = row[16]
        macrolanguage_ISO639_3_code = row[17]
        macrolanguage_name = row[18]
        lang_type = row[19]
        autonym = row[20]
        alternate_names = row[21]


        # missing: territory, ISO3166_2, region.
        print ('\n')
        print ('************************')
        print (i)
        print (language_name, language_code_ISO639_3, ISO3166)
        print (location+'\n')
        location = location.replace('‘',"'")


        # CHECK IF IT IS ALREADY IN THE OTHER DATABASE (wikipedia languages territories mapping)
        if WikimediaLanguagecode in wikipedia_languages_territories_mapping:
#            if WikimediaLanguagecode not in WikimediaLanguagecodes_in:
            print ('### WE ALREADY HAVE THIS WIKIMEDIA LANGUAGE IN THE OLD TABLE.')
            print (WikimediaLanguagecode)

            rows = wikipedia_languages_territories_mapping[WikimediaLanguagecode]

            for row in rows:

                qitem=row[1]
                territoryname=row[2]
                territorynameNative=row[3]
                demonym=row[4]
                demonymNative=row[5]
                ISO3166_wp=row[6]
                ISO31662=row[7]
                regional=row[8]
                country=row[9]
                languagestatuscountry=row[11]
                region=row[12]
                subregion=row[13]
                intermediateregion=row[14]
                languagenameEnglishethnologue=row[15]
#                location=''
#                    language_name_inverted=''

                if language_name_inverted == languagenameEnglishethnologue and ISO3166 == ISO3166_wp:

                    mapping_parameters.append((languagenameEnglishethnologue, composition, qitem, territoryname, territorynameNative, demonym, demonymNative, territoryname, country, ISO3166, ISO31662, regional, languagestatuscountry, language_status, indigenous, population, region, subregion, intermediateregion, language_name_inverted, language_code_ISO639_3, language_code_ISO639_2, language_code_ISO639_1, WikimediaLanguagecode, macrolanguage_ISO639_3_code, macrolanguage_name, lang_type, autonym, alternate_names, location))

                    print ((languagenameEnglishethnologue, composition, qitem, territoryname, territorynameNative, demonym, demonymNative, territoryname, country, ISO3166, ISO31662, regional, languagestatuscountry, language_status, indigenous, population, region, subregion, intermediateregion, language_name_inverted, language_code_ISO639_3, language_code_ISO639_2, language_code_ISO639_1, WikimediaLanguagecode, macrolanguage_ISO639_3_code, macrolanguage_name, lang_type, autonym, alternate_names, location))

#                WikimediaLanguagecodes_in.append(WikimediaLanguagecode)
#                continue # next country
            continue
#            else:
#                continue


        wikidatacountrysubdivisions = download_language_countries_and_subdivisions_wikidata(language_code_ISO639_1,ISO3166)

#        print (wikidatacountrysubdivisions)
#        input('')
        
        territories = []
        if 'widespread' in location.lower() or location[len(location)-2]==':': 
            sentence = (location.lower())
            print ('IT IS A COUNTRY!')
            print (sentence)
            territories = identify_territories_from_location_sentence(sentence,wikidatacountrysubdivisions,ISO3166, typeslist,multilingual_ISO3166_2,wikidatacountrysubdivisions_all)
            #print (territories)
#            input('')

        else: 
            print ('IT IS/THEY ARE SUBDIVISIONS!')
            location = location.lower()
            location = location.strip(' ')
            location = location.strip('.')

            location=location.replace(' and ',', ')
            location=location.replace(', and ',', ')
            location=location.replace(',',' ')
            location=location.replace('.',' .')
            location=location.replace(';','.')

            location=location.split(':',1)[1]

            sentences = location.split('.');
            print ('We just cut the sentences into: ')
            print (sentences) # cut into sentences
            print ('\n')

            for sentence in sentences:
                if sentence == '': continue
                result = identify_territories_from_location_sentence(sentence,wikidatacountrysubdivisions,ISO3166, typeslist,multilingual_ISO3166_2,wikidatacountrysubdivisions_all)

                if (len(result))!=0: 
                    territories = territories + result

        print ('\nFinal territories identified: '+str(len(territories)))
        print (territories)

        if len(territories)==0:
            print ('NO TERRITORIES. WOW!')

        
        for territory in territories:

            qitem = territory['Qitem']
            territorynameNative = territory['territorynameNative']
            territoryname = territory['territoryname']
            regional = territory['regional']
            ISO3166_2 = territory['ISO3166_2']
            demonym = territory['demonymen']
            demonymNative = territory['demonymNative']
            locationtext = territory['location']

#            print ('\n')
#            print (('language_name',language_name, 'composition',composition, 'qitem',qitem, 'territoryname',territoryname, 'territorynameNative',territorynameNative, 'demonym',demonym, 'demonymNative',demonymNative, 'territoryname',territoryname, 'country',country, 'ISO3166',ISO3166, 'ISO3166_2',ISO3166_2, 'regional',regional, 'language_status_code', language_status_code, 'language_status',language_status,'indigenous',indigenous, 'population',population, 'region',region, 'sub_region',sub_region, 'intermediate_region',intermediate_region, 'language_name_inverted',language_name_inverted, 'language_code_ISO639_3',language_code_ISO639_3, 'language_code_ISO639_2',language_code_ISO639_2, 'language_code_ISO639_1',language_code_ISO639_1, 'WikimediaLanguagecode',WikimediaLanguagecode, 'macrolanguage_ISO639_3_code',macrolanguage_ISO639_3_code, 'macrolanguage_name',macrolanguage_name, 'lang_type',lang_type, 'autonym',autonym, 'alternate_names',alternate_names, 'locationtext',locationtext))

            mapping_parameters.append((language_name, composition, qitem, territoryname, territorynameNative, demonym, demonymNative, territoryname, country, ISO3166, ISO3166_2, regional, language_status_code, language_status, indigenous, population, region, sub_region, intermediate_region, language_name_inverted, language_code_ISO639_3, language_code_ISO639_2, language_code_ISO639_1, WikimediaLanguagecode, macrolanguage_ISO639_3_code, macrolanguage_name, lang_type, autonym, alternate_names, locationtext))

#    input('')
    query = 'INSERT OR IGNORE INTO language_territories_mapping (language_name, composition, qitem, territoryname, territorynameNative, demonym, demonymNative, territoryname, country, ISO3166, ISO3166_2, regional, language_status_code, language_status, indigenous, population, region, sub_region, intermediate_region, language_name_inverted, language_code_ISO639_3, language_code_ISO639_2, language_code_ISO639_1, WikimediaLanguagecode, macrolanguage_ISO639_3_code, macrolanguage_name, lang_type, autonym, alternate_names, location_text) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)' 
#        input('')

#        if i==500: break

    cursor.executemany(query,mapping_parameters)
    conn.commit()

    print ('done.')


# This gets the territories from a sentence using WikiData and pycountry databases.
def identify_territories_from_location_sentence(sentence,wikidatacountrysubdivisions,ISO3166,typeslist,multilingual_ISO3166_2,wikidatacountrysubdivisions_all):

    subdivisionsintext=[]

    sentence=sentence.strip()
    sentence=sentence.rstrip(' ')
    if sentence=='': return subdivisionsintext
#    print ('HERE IS THE SENTENCE: ')
#    print (sentence)

    subdivisionsintext_dict = {}
    # LET'S TRY TO IDENTIFY THE TERRITORY

    if 'widespread' in sentence or sentence[len(sentence)-1]==':':
#        print ('eh gia')
#        print (wikidatacountrysubdivisions)
        for subdivision in wikidatacountrysubdivisions:

            qitem = subdivision['Qitem']
            name=subdivision['itemlabelen']
            nameNative=subdivision['itemlabelNative']
            demonym = subdivision['demonymen']
            demonymNative = subdivision['demonymNative']

            if subdivision['ISO3166_2']=='' and subdivision['ISO3166']==ISO3166: 
#                print ('\nGOLD: WIKIDATA 3')
#                print ('* TEXT: '+sentence)
                subdivisionsintext_dict[name]={}
                subdivisionsintext_dict[name]['Qitem']=qitem
                subdivisionsintext_dict[name]['territorynameNative']=nameNative
                subdivisionsintext_dict[name]['territoryname']=name
                subdivisionsintext_dict[name]['regional']='no'
                subdivisionsintext_dict[name]['ISO3166_2']=''
                subdivisionsintext_dict[name]['demonymen']=demonym
                subdivisionsintext_dict[name]['demonymNative']=demonymNative
                subdivisionsintext_dict[name]['location']=sentence
#                print ('THERE IT IS: '+subdivisionsintext_dict[name])

    else:
        # FIRST PYCOUNTRY
        pycountry_subdivisions=list(pycountry.subdivisions.get(country_code=ISO3166))
#        print(pycountry_subdivisions)

        for subdivision in pycountry_subdivisions:
            subdivision_name = subdivision.name
            subdivision_name = subdivision_name.lower()

            if subdivision_name in sentence:
#                print ('\nBINGO: PYCOUNTRY')
#                print ('* TEXT: '+sentence)
#                print (subdivision_name)
                subdivisionsintext_dict[subdivision_name]={'ISO3166_2':subdivision.code,'territoryname':subdivision_name,'location':sentence,'Qitem':'', 'territorynameNative':'','regional':'yes','demonymNative':'','demonymen':''}
#        print (subdivisionsintext_dict)


        # SECOND multilingual_ISO3166_2
        for subdivision_name in multilingual_ISO3166_2.keys():
            subdivision_name = subdivision_name.lower()

            clean_subdivision_name = subdivision_name
#            print (subdivision_name)

            if subdivision_name in sentence and multilingual_ISO3166_2[clean_subdivision_name].split('-')[0]==ISO3166:
#                print ('\nBINGO: multilingual_ISO3166_2 SUBDIVISIONS')
#                print ('* TEXT: '+sentence)
#                print (subdivision_name)
                subdivisionsintext_dict[clean_subdivision_name]={'ISO3166_2':multilingual_ISO3166_2[clean_subdivision_name],'territoryname':clean_subdivision_name,'location':sentence,'Qitem':'', 'territorynameNative':'','regional':'yes','demonymNative':'','demonymen':''}

#        print (subdivisionsintext_dict)
#        print (wikidatacountrysubdivisions)

        # THIRD WIKIDATA SUBDIVISIONS
        for subdivision in wikidatacountrysubdivisions:

            qitem = subdivision['Qitem']
            name=subdivision['itemlabelen']
            nameNative=subdivision['itemlabelNative']

            for type in typeslist:
                new_name=name.replace(type,'').rstrip()
                new_nameNative=nameNative.replace(type,'').rstrip()
#            print (name,nameNative)

            ISO3166_2 = subdivision['ISO3166_2']
            if subdivision['ISO3166_2']!='': 
                regional='yes'
            else:
                continue
            demonym = subdivision['demonymen']
            demonymNative = subdivision['demonymNative']

            # CASE 1: We find name straight
            if (new_name != '' and new_name.lower() in sentence) or (new_nameNative !='' and new_nameNative.lower() in sentence):
#                print ('\nGOLD: WIKIDATA 1')
#                print ('* TEXT: '+sentence)
#                print (name, nameNative)

                subdivisionsintext_dict[name]={}
                subdivisionsintext_dict[name]['Qitem']=qitem
                subdivisionsintext_dict[name]['territorynameNative']=nameNative
                subdivisionsintext_dict[name]['territoryname']=name
                subdivisionsintext_dict[name]['regional']=regional
                subdivisionsintext_dict[name]['demonymen']=demonym
                subdivisionsintext_dict[name]['demonymNative']=demonymNative
                subdivisionsintext_dict[name]['ISO3166_2']=ISO3166_2
                subdivisionsintext_dict[name]['location']=sentence


            coincident = []
            # CASE 2: We do not find the name but we find the ISO 3166-2
            for names_sub, dicta in subdivisionsintext_dict.items():
                if ISO3166_2 == dicta['ISO3166_2']:
#                    print ('\nGOLD: WIKIDATA 2')
#                    print ('* TEXT: '+sentence)
                    coincident.append(names_sub)

            for coinci in coincident:
                del subdivisionsintext_dict[coinci]
                subdivisionsintext_dict[name]={}
                subdivisionsintext_dict[name]['Qitem']=qitem
                subdivisionsintext_dict[name]['territorynameNative']=nameNative
                subdivisionsintext_dict[name]['territoryname']=name
                subdivisionsintext_dict[name]['regional']=regional
                subdivisionsintext_dict[name]['ISO3166_2']=ISO3166_2
                subdivisionsintext_dict[name]['demonymen']=demonym
                subdivisionsintext_dict[name]['demonymNative']=demonymNative
                subdivisionsintext_dict[name]['location']=sentence


        for name,territory in subdivisionsintext_dict.items():
            if territory['Qitem']=='':
                try:
                    subdivisionsintext_dict[name]['Qitem']=wikidatacountrysubdivisions_all[territory['ISO3166_2']]['Qitem']
                    try:
                        subdivisionsintext_dict[name]['demonymen']=wikidatacountrysubdivisions_all[territory['ISO3166_2']]['demonymen']
                    except:
                        pass
                except:
                    pass

    """
    if len(subdivisionsintext_dict)==0:
        subdivisionsintext_dict['none']={}
        subdivisionsintext_dict['none']['Qitem']=''
        subdivisionsintext_dict['none']['territorynameNative']=''
        subdivisionsintext_dict['none']['territoryname']=''
        subdivisionsintext_dict['none']['regional']=''
        subdivisionsintext_dict['none']['ISO3166_2']=''
        subdivisionsintext_dict['none']['demonymen']=''
        subdivisionsintext_dict['none']['demonymNative']=''
        subdivisionsintext_dict['none']['location']=sentence
    """

#        print (subdivision)
#    print (subdivisionsintext_dict)
#    print (len(subdivisionsintext_dict))
    return list(subdivisionsintext_dict.values())






# This obtains the WikiData on a country subdivisions in a particular language.
def download_language_countries_and_subdivisions_wikidata(language,ISO3166):
    wikidatacountrysubdivisions = []


    # FIRST THE COUNTRY SUBDIVISIONS
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?item ?ISO3166_2 ?itemlabelen ?itemlabelNative ?demonymNative ?demonymen
    WHERE
    {
      ?country wdt:P297 ?ISO3166.
      FILTER (?ISO3166 = "countrycode")
      
      ?item wdt:P17 ?country.
      ?item wdt:P300 ?ISO3166_2.

      OPTIONAL { ?item rdfs:label ?itemlabelen filter (lang(?itemlabelen) = "en"). }
      OPTIONAL { ?item rdfs:label ?itemlabelNative filter (lang(?itemlabelNative) = "language"). }

      OPTIONAL { ?item wdt:P1549 ?demonymNative;
            FILTER(LANG(?demonymNative) = "language") }
      OPTIONAL { ?item wdt:P1549 ?demonymen;
            FILTER(LANG(?demonymen) = "en") }
    }'''

    query = query.replace('language',language)
    query = query.replace('countrycode',ISO3166)

#    print (query)
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    try:
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data != None: data = data.json()


        #print (data)
        Qitem = ''; itemlabelNative = ''; itemlabelen = ''
        demonymen = []; demonymNative = [];

        #print ('COMENÇA SUBDIVISIONS')
        for item in data['results']['bindings']:    
            #print (item)
            #input('tell me')

            #print (wikidatacountrysubdivisions)

            Qitem = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            ISO3166_2 = item['ISO3166_2']['value']

            try: itemlabelen = item['itemlabelen']['value']
            except: pass

            try: itemlabelNative = item['itemlabelNative']['value']
            except: pass

            try:
                demonymencurrent = item['demonymen']['value']
                demonymencurrent = demonymencurrent.replace(',',';')
                if demonymencurrent not in demonymen:
                    demonymen.append(demonymencurrent)
            except:
                pass

            try:
                demonymNativecurrent = item['demonymNative']['value']
                demonymNativecurrent = demonymNativecurrent.replace(',',';')
                if demonymNativecurrent not in demonymNative:
                    demonymNative.append(demonymNativecurrent)
            except:
                pass

            if Qitem != item['item']['value'] and Qitem!='':
                wikidatacountrysubdivisions.append({
                'Qitem': Qitem,
                'ISO3166': ISO3166,
                'ISO3166_2': ISO3166_2,
                'itemlabelen': itemlabelen,
                'itemlabelNative': itemlabelNative,
                'demonymNative': ";".join(demonymNative),
                'demonymen': ";".join(demonymen),
                })
                demonymen = []; demonymNative = [];
                itemlabelNative = ''
                itemlabelen = ''

    except:
        pass

        #print (wikidatacountrysubdivisions)
        #input('')


    # NOW THE COUNTRY
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?item ?itemlabelen ?itemlabelNative ?demonymen ?demonymNative
    WHERE
    {
      ?item wdt:P297 ?ISO3166.
      FILTER (?ISO3166 = "countrycode")

      OPTIONAL { ?item rdfs:label ?itemlabelen filter (lang(?itemlabelen) = "en"). }
      OPTIONAL { ?item rdfs:label ?itemlabelNative filter (lang(?itemlabelNative) = "language"). }
      OPTIONAL { ?item wdt:P1549 ?demonymen filter (lang(?demonymen) = "en"). }
      OPTIONAL { ?item wdt:P1549 ?demonymNative filter (lang(?demonymNative) = "language"). }
      
    }'''

    query = query.replace('language',language)
    query = query.replace('countrycode',ISO3166)
#    print (query)

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    try:
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data != None: data = data.json()


        Qitem = ''
        demonymen = []; demonymNative = []; itemlabelen = ''; itemlabelNative = '';

        #print ('ARA EL PAÍS')
        for item in data['results']['bindings']:    
            #print (item)
            #input('tell me')

            Qitem = item['item']['value'].replace("http://www.wikidata.org/entity/","")

            try: itemlabelen = item['itemlabelen']['value']
            except: pass

            try: itemlabelNative = item['itemlabelNative']['value']
            except: pass

            try:
                demonymencurrent = item['demonymen']['value']
                demonymencurrent = demonymencurrent.replace(',',';')
                if demonymencurrent not in demonymen:
                    demonymen.append(demonymencurrent)
            except:
                pass

            try:
                demonymNativecurrent = item['demonymNative']['value']
                demonymNativecurrent = demonymNativecurrent.replace(',',';')
                if demonymNativecurrent not in demonymNative:
                    demonymNative.append(demonymNativecurrent)
            except:
                pass

        if Qitem!='':
            wikidatacountrysubdivisions.append({
            'Qitem': Qitem,
            'ISO3166': ISO3166,
            'ISO3166_2': '',
            'itemlabelen': itemlabelen,
            'itemlabelNative': itemlabelNative,
            'demonymNative': ";".join(demonymNative),
            'demonymen': ";".join(demonymen),
            })

    except:
        pass

    return (wikidatacountrysubdivisions)


def download_all_world_subdivisions_wikidata():
    wikidatacountrysubdivisions_all = {}


    # FIRST THE COUNTRY SUBDIVISIONS
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?item ?ISO3166_2 ?itemlabelen ?demonymen
    WHERE
    {
      ?item wdt:P300 ?ISO3166_2.
      OPTIONAL { ?item rdfs:label ?itemlabelen FILTER (LANG(?itemlabelen) = "en"). }
      OPTIONAL { ?item wdt:P1549 ?demonymen; FILTER(LANG(?demonymen) = "en") }
    }'''

#    print (query)
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    try:
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data != None: data = data.json()


        #print (data)
        Qitem = ''; itemlabelNative = ''; itemlabelen = ''
        demonymen = []; demonymNative = [];

        #print ('COMENÇA SUBDIVISIONS')
        for item in data['results']['bindings']:    
            #print (item)
            #input('tell me')

            #print (wikidatacountrysubdivisions_all)

            Qitem = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            ISO3166_2 = item['ISO3166_2']['value']

            try: itemlabelen = item['itemlabelen']['value']
            except: pass

            try:
                demonymencurrent = item['demonymen']['value']
                demonymencurrent = demonymencurrent.replace(',',';')
                if demonymencurrent not in demonymen:
                    demonymen.append(demonymencurrent)
            except:
                pass

            if Qitem != item['item']['value'] and Qitem!='':
                wikidatacountrysubdivisions_all[ISO3166_2]={
                'Qitem': Qitem,
                'ISO3166_2': ISO3166_2,
                'itemlabelen': itemlabelen,
                'demonymen': ";".join(demonymen),
                }
                demonymen = [];
                itemlabelen = ''
                ISO3166_2 = ''
                Qitem = ''

    except:
        pass

    return wikidatacountrysubdivisions_all



def create_language_pairs_country_status_table():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    pairs = []

    query = 'SELECT country, language_code_ISO639_3, language_status_code, indigenous, WikimediaLanguagecode, language_name, ISO3166 FROM language_countries_mapping WHERE language_status_code != "" ORDER BY country, language_status_code DESC;'

    country_rows=[]
    cur_country = ''; old_country = ''; cur_ISO3166 = ''; old_ISO3166 = ''

    for row in cursor.execute(query):
#        print (row)
        cur_country = row[0]
        cur_ISO3166 = row[6]

        if cur_country != old_country:

            num_country_rows=len(country_rows)
            pos = 0

            for rew in country_rows:
                lower_country=rew[0]
                lower_language_code_ISO639_3=rew[1]
                lower_language_status_code=rew[2]
                lower_language_status_code=lower_language_status_code.replace('a','')
                lower_language_status_code=lower_language_status_code.replace('b','.5')
                lower_language_status_code=float(lower_language_status_code)

                lower_indigenous=rew[3]
                lower_WikimediaLanguagecode=rew[4]
                lower_language_name=rew[5]

                pos = pos + 1
                for x in range(pos,len(country_rows)):
                    higher_country=country_rows[x][0]
                    higher_language_code_ISO639_3=country_rows[x][1]
                    higher_language_status_code=country_rows[x][2]
                    higher_language_status_code=higher_language_status_code.replace('a','')
                    higher_language_status_code=higher_language_status_code.replace('b','.5')
                    higher_language_status_code=float(higher_language_status_code)

                    if lower_language_status_code==higher_language_status_code:
                        equal_status = 1
                    else:
                        equal_status = 0

                    higher_indigenous=country_rows[x][3]
                    higher_WikimediaLanguagecode=country_rows[x][4]
                    higher_language_name=country_rows[x][5]

                    pairs.append((old_country,old_ISO3166,lower_language_name,higher_language_name,lower_language_code_ISO639_3,higher_language_code_ISO639_3,'territory',lower_language_status_code,higher_language_status_code,equal_status,lower_indigenous,higher_indigenous,lower_WikimediaLanguagecode,higher_WikimediaLanguagecode))
#                    print (lower_language_name,higher_language_name,lower_language_code_ISO639_3,higher_language_code_ISO639_3,'territory',lower_language_status_code,higher_language_status_code,equal_status,lower_indigenous,higher_indigenous,old_country,old_ISO3166)

            country_rows=[]

        country_rows.append(row)
        old_country = cur_country
        old_ISO3166 = cur_ISO3166


    # STORE THE DATA
    query = ('CREATE TABLE IF NOT EXISTS language_pairs_country_status ('+
    'country text,'+
    'ISO3166 text,'+
    'language_lower_name text,'+ 
    'language_higher_name text,'+ 
    'language_lower_ISOcode text,'+ 
    'language_higher_ISOcode text,'+ 
    'type_overlap text,'+ # coexistance, world language
    'status_lower float,'+ 
    'status_higher float,'+
    'equal_status integer,'+
    'indigenous_lower text,'+ 
    'indigenous_higher text,'+ 
    'wikimedia_lower text,'+
    'wikimedia_higher text,'+

    'PRIMARY KEY (language_lower_ISOcode,language_higher_ISOcode,ISO3166));')
    cursor.execute(query); conn.commit()


    query = 'INSERT OR IGNORE INTO language_pairs_country_status (country, ISO3166, language_lower_name, language_higher_name, language_lower_ISOcode, language_higher_ISOcode, type_overlap, status_lower, status_higher, equal_status, indigenous_lower, indigenous_higher, wikimedia_lower, wikimedia_higher) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);'
    cursor.executemany(query,pairs)
    conn.commit()


def create_language_pairs_territory_status_table():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    # STORE THE DATA
    query = ('CREATE TABLE IF NOT EXISTS language_pairs_territory_status ('+
    'qitem text,'+
    'territory text,'+
    'ISO3166 text,'+
    'ISO3166_2 text,'+
    'language_lower_name text,'+ 
    'language_higher_name text,'+ 
    'language_lower_ISOcode text,'+ 
    'language_higher_ISOcode text,'+ 
    'type_overlap text,'+ # coexistance, world language
    'status_lower float,'+ 
    'status_higher float,'+
    'equal_status integer,'+
    'indigenous_lower text,'+ 
    'indigenous_higher text,'+ 
    'wikimedia_lower text,'+
    'wikimedia_higher text,'+

    'PRIMARY KEY (qitem,language_lower_ISOcode,language_higher_ISOcode));')
    cursor.execute(query); conn.commit()

    all_pairs = []
    pairs = []
    query = 'SELECT territoryname, language_code_ISO639_3, language_status_code, indigenous, WikimediaLanguagecode, language_name, ISO3166_2, ISO3166, qitem, regional FROM language_territories_mapping WHERE language_status_code != "" ORDER BY ISO3166, regional, ISO3166_2, territoryname, language_status_code DESC;'
    # AND ISO3166 IN ("ES","IT")

    query_insert = 'INSERT OR IGNORE INTO language_pairs_territory_status (qitem,territory, ISO3166, ISO3166_2, language_lower_name, language_higher_name, language_lower_ISOcode, language_higher_ISOcode, type_overlap, status_lower, status_higher, equal_status, indigenous_lower, indigenous_higher, wikimedia_lower, wikimedia_higher) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'

    country_rows=[]
    cur_ISO3166 = ''; old_ISO3166 = ''; 
    i = 0
    j = 0

    for row in cursor.execute(query):
        cur_ISO3166 = row[7]
#        print (row)

        if cur_ISO3166 != old_ISO3166 and i!=0:
            j+=1
            print ('\n\n\n# ISO3166 number:  '+str(j)+'')
            country_rows,pairs=count_language_pairs(country_rows,pairs)
            all_pairs=all_pairs+pairs
            pairs=[]
            print ('all pairs accumulates: '+str(len(all_pairs)))

        old_ISO3166 = cur_ISO3166
        country_rows.append(row)
        i+=1

    print ('\n# ISO3166 number:  '+str(j)+'')
    country_rows,pairs=count_language_pairs(country_rows,pairs)
    all_pairs=all_pairs+pairs   
    cursor.executemany(query_insert,all_pairs)
    conn.commit()    

    print ('final number of pairs accumulated: '+str(len(all_pairs)))

    print ('done with the territory pairs.')


def count_language_pairs(country_rows,pairs):
    p=0

    print (country_rows[0][7])
    print ('This country has: '+str(len(country_rows))+' territories.\n')

    j=0
    for row in country_rows:
#        input('')
        j+=1
#        print (j)
#        print ('FIRST LOOP:')
#        print (row)
#        print ('row')

        row_territoryname = row[0]
        row_language_code_ISO639_3 = row[1]
        row_language_status_code = float(row[2].replace('a','').replace('b','.5'))
        row_indigenous = row[3]
        row_WikimediaLanguagecode = row[4]
        row_language_name = row[5]
        row_ISO3166_2 = row[6]
        row_ISO3166 = row[7]
        row_qitem = row[8]
        row_regional = row[9]

#        print ('CHECKING SECOND LOOP:\n')

        k=0
        # CASE B. TERRITORY LANGUAGE vs. TERRITORY LANGUAGE. NEED FOR REGIONAL NO.
        for rew in country_rows:
            k+=1
#            print (k)
#            print (rew)
#            print ('rew')

            if row == rew: continue

            # COUNTRY
            if rew[9]=='no': # no regional
                country = rew
#                print ('\nHere we have the country language:')
#                print ('eh.')

                if country != None:
                    country_status = float(country[2].replace('a','').replace('b','.5'))
        #                    print (row)
                    # CASE A: TERRITORY LANGUAGE vs. COUNTRY LANGUAGE. DIFFERENT TERRITORY. NEED FOR REGIONAL YES.
                    if country_status>row_language_status_code:
#                        print ('country has lower status')
                        equal_status = 0
                        country_situation = (row_qitem,row_territoryname,row_ISO3166,row_ISO3166_2,country[5],row_language_name,country[1],row_language_code_ISO639_3,'country level',country[2],row_language_status_code,equal_status,country[3],row_indigenous,country[4],row_WikimediaLanguagecode)

                    if country_status==row_language_status_code:
#                        print ('country equals status')
                        equal_status = 1
                        country_situation = (row_qitem,row_territoryname,row_ISO3166,row_ISO3166_2,row_language_name,country[5],row_language_code_ISO639_3,country[1],'country level',row_language_status_code,country[2],equal_status,row_indigenous,country[3],row_WikimediaLanguagecode,country[4])

                    if country_status<row_language_status_code:
#                        print ('country has higher status')
                        equal_status = 0
                        country_situation = (row_qitem,row_territoryname,row_ISO3166,row_ISO3166_2,row_language_name,country[5],row_language_code_ISO639_3,country[1],'country level',row_language_status_code,country[2],equal_status,row_indigenous,country[3],row_WikimediaLanguagecode,country[4])

#                    print (country_situation)
                    pairs.append(country_situation)
                    p+=1


            # SUBDIVISION
            else:
#                print ('subdivision')
                rew_territoryname = rew[0]
                rew_language_code_ISO639_3 = rew[1]
                rew_language_status_code = float(rew[2].replace('a','').replace('b','.5'))
                rew_indigenous = rew[3]
                rew_WikimediaLanguagecode = rew[4]
                rew_language_name = rew[5]
                rew_ISO3166_2 = rew[6]
                rew_ISO3166 = rew[7]
                rew_qitem = rew[8]
                rew_regional = rew[9]

                if row_ISO3166_2 == rew_ISO3166_2 and row_qitem == rew_qitem:
                    equal_status = ''

                    if rew_language_status_code < row_language_status_code: 
                        equal_status = 0                       
                        territory_situation = (row_qitem,row_territoryname,row_ISO3166,row_ISO3166_2,row_language_name,rew_language_name,row_language_code_ISO639_3,rew_language_code_ISO639_3,'subdivision level',row_language_status_code,rew_language_status_code,equal_status,row_indigenous,rew_indigenous,row_WikimediaLanguagecode,rew_WikimediaLanguagecode)

                        if territory_situation not in pairs:
                            p+=1
                            pairs.append(territory_situation)
#                            print ('BINGO')
 #                           print (territory_situation)

                    if rew_language_status_code == row_language_status_code: 
                        equal_status = 1

                        territory_situation0 = (row_qitem,row_territoryname,row_ISO3166,row_ISO3166_2,row_language_name,rew_language_name,row_language_code_ISO639_3,rew_language_code_ISO639_3,'subdivision level',row_language_status_code,rew_language_status_code,equal_status,row_indigenous,rew_indigenous,row_WikimediaLanguagecode,rew_WikimediaLanguagecode)

                        territory_situation1 = (rew_qitem,rew_territoryname,rew_ISO3166,rew_ISO3166_2,rew_language_name,row_language_name,rew_language_code_ISO639_3,row_language_code_ISO639_3,'subdivision level',rew_language_status_code,row_language_status_code,equal_status,rew_indigenous,row_indigenous,rew_WikimediaLanguagecode,row_WikimediaLanguagecode)

                        if territory_situation1 not in pairs and territory_situation0 not in pairs:
                            p+=1
                            pairs.append(territory_situation1)
#                            print ('BINGO')
#                            print (territory_situation1)

    country_rows=[]
    print ('total de parelles: '+str(len(pairs))+'\n')
    return country_rows,pairs





def create_wikipedia_language_pairs_territory_status_table():

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

#    closest_langs = wikilanguages_utils.obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, 4)


    # STORE THE DATA
    query = ('CREATE TABLE IF NOT EXISTS wikipedia_language_pairs_territory_status ('+
    'qitem text,'+
    'territoryname_english text,'+
    'territoryname_higher text,'+
    'ISO3166 text,'+
    'ISO3166_2 text,'+
    'language_lower_name text,'+ 
    'language_higher_name text,'+ 
    'wikimedia_lower text,'+
    'wikimedia_higher text,'+   
    'type_overlap text,'+ # coexistance, world language
    'status_lower float,'+ 
    'status_higher float,'+
    'equal_status integer,'+
    'indigenous_lower text,'+ 
    'indigenous_higher text,'+ 

    'PRIMARY KEY (qitem,wikimedia_lower,wikimedia_higher));')
    cursor.execute(query); conn.commit()

    all_pairs = []
    pairs = []
    query = 'SELECT territoryname, WikimediaLanguagecode, languagestatuscountry, indigenous, languagenameEnglishethnologue, ISO3166, ISO31662, QitemTerritory, regional FROM wikipedia_languages_territories_mapping WHERE languagestatuscountry != "" ORDER BY ISO3166, regional, ISO31662, territoryname, languagestatuscountry DESC;'
    # AND ISO3166 IN ("ES","IT")

    query_insert = 'INSERT OR IGNORE INTO wikipedia_language_pairs_territory_status (qitem,territoryname_english, territoryname_higher, ISO3166, ISO3166_2, language_lower_name, language_higher_name, wikimedia_lower, wikimedia_higher, type_overlap, status_lower, status_higher, equal_status, indigenous_lower, indigenous_higher) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'

    country_rows=[]
    cur_ISO3166 = ''; old_ISO3166 = ''; 
    i = 0
    j = 0

    for row in cursor.execute(query):
        cur_ISO3166 = row[5]

        if cur_ISO3166 != old_ISO3166 and i!=0:
            j+=1
            print ('\n\n\n# ISO3166 number:  '+str(j)+'')
            country_rows,pairs=count_wikipedia_language_pairs(country_rows,pairs)
            all_pairs=all_pairs+pairs
            pairs=[]
            print ('all pairs accumulates: '+str(len(all_pairs)))

        old_ISO3166 = cur_ISO3166
        country_rows.append(row)
        i+=1

    print ('\n# ISO3166 number:  '+str(j)+'')
    country_rows,pairs=count_wikipedia_language_pairs(country_rows,pairs)
    all_pairs=all_pairs+pairs
    cursor.executemany(query_insert,all_pairs)
    conn.commit()    

    print ('final number of pairs accumulated: '+str(len(all_pairs)))

    print ('done with the territory pairs.')




def count_wikipedia_language_pairs(country_rows,pairs):
    p=0

    print (country_rows[0][7])
    print (str(country_rows[0][0].encode('utf-8')))

    print ('This country has: '+str(len(country_rows))+' territories.\n')

    j=0
    for row in country_rows:
#        input('')
        j+=1
#        print (j)
#        print ('FIRST LOOP:')
#        print ('row')

        row_territoryname = row[0]
        row_WikimediaLanguagecode = row[1]
        row_languagestatuscountry = int(row[2])
        row_indigenous = row[3]
        row_language_name = row[4]
        row_ISO3166 = row[5]
        row_ISO3166_2 = row[6]
        row_qitem = row[7]
        row_regional = row[8]


#        print ('CHECKING SECOND LOOP:\n')

        k=0
        # CASE B. TERRITORY LANGUAGE vs. TERRITORY LANGUAGE. NEED FOR REGIONAL NO.
        for rew in country_rows:

            k+=1
#            print (k)
#            print (rew)
#            print ('rew')

            if row == rew:
                continue

            # COUNTRY
            if rew[8]=='no': # no regional
                country = rew
#                print ('\nHere we have the country language:')
#                print ('eh.')



                if country != None:
                    country_status = int(country[2])
        #                    print (row)
                    # CASE A: TERRITORY LANGUAGE vs. COUNTRY LANGUAGE. DIFFERENT TERRITORY. NEED FOR REGIONAL YES.
                    if country_status>row_languagestatuscountry:
#                        print ('country has lower status')
                        equal_status = 0
  
                        country_situation = (row_qitem,row_territoryname,get_territory_name_native(row_WikimediaLanguagecode, row_qitem),row_ISO3166,row_ISO3166_2,country[4],row_language_name,country[1],row_WikimediaLanguagecode,'country level',country[2],row_languagestatuscountry,equal_status,country[3],row_indigenous)


                    if country_status==row_languagestatuscountry:
#                        print ('country equals status')
                        equal_status = 1

                        country_situation = (row_qitem,row_territoryname,get_territory_name_native(row_WikimediaLanguagecode, row_qitem),row_ISO3166,row_ISO3166_2,row_language_name,country[4],row_WikimediaLanguagecode,country[1],'country level',row_languagestatuscountry,country[2],equal_status,row_indigenous,country[3])


                    if country_status<row_languagestatuscountry:
#                        print ('country has higher status')
                        equal_status = 0

                        country_situation = (row_qitem,row_territoryname,get_territory_name_native(country[1], row_qitem),row_ISO3166,row_ISO3166_2,row_language_name,country[4],row_WikimediaLanguagecode,country[1],'country level',row_languagestatuscountry,country[2],equal_status,row_indigenous,country[3])

#                    print (country_situation)
                    pairs.append(country_situation)
                    p+=1

            # SUBDIVISION
            else:
#                print ('subdivision')
                rew_territoryname = rew[0]
                rew_WikimediaLanguagecode = rew[1]
                rew_languagestatuscountry = int(rew[2])
                rew_indigenous = rew[3]
                rew_language_name = rew[4]
                rew_ISO3166 = rew[5]
                rew_ISO3166_2 = rew[6]
                rew_qitem = rew[7]
                rew_regional = rew[8]

                if row_ISO3166_2 == rew_ISO3166_2 and row_qitem == rew_qitem:
                    equal_status = ''

                    if rew_languagestatuscountry < row_languagestatuscountry: 
                        equal_status = 0                       
                        territory_situation = (row_qitem,row_territoryname,get_territory_name_native(rew_WikimediaLanguagecode, rew_qitem),row_ISO3166,row_ISO3166_2,row_language_name,rew_language_name,row_WikimediaLanguagecode,rew_WikimediaLanguagecode,'subdivision level',row_languagestatuscountry,rew_languagestatuscountry,equal_status,row_indigenous,rew_indigenous)

                        if territory_situation not in pairs:
                            p+=1
                            pairs.append(territory_situation)
#                            print ('BINGO')
#                            print (territory_situation)

                    if rew_languagestatuscountry == row_languagestatuscountry: 
                        equal_status = 1

                        territory_situation0 = (row_qitem,row_territoryname,get_territory_name_native(rew_WikimediaLanguagecode, rew_qitem),row_ISO3166,row_ISO3166_2,row_language_name,rew_language_name,row_WikimediaLanguagecode,rew_WikimediaLanguagecode,'subdivision level',row_languagestatuscountry,rew_languagestatuscountry,equal_status,row_indigenous,rew_indigenous)

                        territory_situation1 = (rew_qitem,rew_territoryname,get_territory_name_native(row_WikimediaLanguagecode, row_qitem),rew_ISO3166,rew_ISO3166_2,rew_language_name,row_language_name,rew_WikimediaLanguagecode,row_WikimediaLanguagecode,'subdivision level',rew_languagestatuscountry,row_languagestatuscountry,equal_status,rew_indigenous,row_indigenous)

                        if territory_situation1 not in pairs and territory_situation0 not in pairs:
                            p+=1
                            pairs.append(territory_situation1)
#                            print ('BINGO equals')
#                            print (territory_situation1)

    country_rows=[]
    print ('total de parelles: '+str(len(pairs))+'\n')
    return country_rows,pairs



def get_territory_name_native(languagecode, Qitem):

    conn = sqlite3.connect(databases_path+'wikidata.db'); cursor = conn.cursor();

    query = 'SELECT page_title FROM sitelinks WHERE qitem=? AND langcode=?'
    cursor.execute(query,(Qitem,languagecode+'wiki'))
    page_title = cursor.fetchone()
    if page_title != None:
        page_title = page_title[0]
    else:
        page_title = ''
#    print (page_title[0].encode('utf-8'))

    return page_title




### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 




# Obtain a list the territories where the language is OFFICIAL (regional or national) or INDIGENOUS. 
# MAIN SOURCES: Ethnologue and pycountry.
# Around half of the Wikipedias' languages are no official at national or regional level in any country. (!)
# Territories must be either countries (ISO 3166) or wikidatacountrysubdivisions of countries (ISO 3166-2).
def download_parse_wikipedia_languages_territories_ethnologue(languagelist):

  # parameters: file to read
  if len(languagelist)==0:
    readfilename = 'wikipedia_language_editions.csv'
  else:
    readfilename = "_".join(languagelist)+'_Wikipedia_language_editions.csv'

  languages = pd.read_csv(readfilename,sep='\t')
  print (readfilename)
  languages=languages[['WikimediaLanguagecode', 'languageISO', 'languageISO3', 'languageISO5', 'numbercountriesOfficialorRegional', 'languagename']]
  languages = languages.set_index(['WikimediaLanguagecode'])

  # parameters: file to write
  filename='Wikipedia_language_territories_mapping'
  if len(languagelist)==0:
    print ('No language list is passed. So all languages from the file are going to be used.')
    languagelist = list(languages.index.values)
  else:
    filename="_".join(languagelist)+"_Wikipedia_language_territories_mapping"

  # let's create the final Dataframe
  df = pd.DataFrame(columns=('territoryname', 'territorynameNative', 'QitemTerritory', 'languagenameEnglishethnologue','WikimediaLanguagecode', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous',  'languagestatuscountry', 'officialnationalorregional','numbercountriesOfficialorRegional','locationtext','locationfulltext'))
  df = df.set_index(['territoryname'])

  print (languagelist)
  print (('\nThere is a number of languages of: ')+str(len(languagelist))+('\t'))
  languagecount = 0
# print (languagelist)
# languagelist=languagelist[25:]
# languagelist=languagelist[184:]

  # FIRST: GROUNDTRUTH. UNICODE.
  for language in languagelist:
#   language = 'arc'

    languagecount=languagecount+1
    numbercountriesOfficialorRegional = languages.loc[language,'numbercountriesOfficialorRegional']
    languageISO = languages.loc[language,'languageISO']
    languageISO3 = languages.loc[language,'languageISO3']
    languageISO5 = languages.loc[language,'languageISO5']
    languagename = languages.loc[language,'languagename']

    position = (languagelist.index(language)+1)
    print ('\n')
    print (languagelist)
    print (('\nIT IS TIME FOR THE LANGUAGE: ')+(languagename)+(', with Wikimedia code: ')+str(language)+(', and position: ')+str(position))
    print ('############################')
#   input('')

    languageCodequery = []
    languagenameEnglishethnologue = ''
    language=str(language)

    # problems with floats
    if type(languageISO3)==str and languageISO3!='' and ';' in languageISO3:
      languageCodequery = languageISO3.split(';')
    elif type(languageISO3)==str and languageISO3!='':
      languageCode = languageISO3
      languageCodequery.append(languageCode)
    elif type(languageISO5)==str and languageISO5!='': 
      languageCode = languageISO5
      languageCodequery.append(languageCode)
    else:
      languageCodequery.append(language)


    for languageCode in languageCodequery:
      print (('\n ### RUNNING language: ')+(languagename))
      print ('Queue list for this language: ')
      print (languageCodequery)
      print (('In specific: ')+str(languageCode))
      languageCode = str(languageCode)
    

      url = 'https://web.archive.org/web/20171123221256/https://www.ethnologue.com/language/'
#     if languagecount%2: url = 'https://web.archive.org/web/20171123221256/https://www.ethnologue.com/language/'
#     else: url = 'http://webcache.googleusercontent.com/search?q=cache:https://www.ethnologue.com/language/'

#     time.sleep(20)
      page = requests.get(url+languageCode)

      # ERRORS
      # 1
      print (str(page))
      if str(page) == '<Response [404]>': 
        print ('No codes ISO3 or ISO5. Response 404.')                                                                                            
        rowdataframe = pd.DataFrame([['unknown', 'unknown', 'unknown',languagenameEnglishethnologue, language,  'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown','unknown']], columns = ['territoryname', 'territorynameNative', 'QitemTerritory', 'languagenameEnglishethnologue','WikimediaLanguagecode', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous',  'languagestatuscountry', 'officialnationalorregional','numbercountriesOfficialorRegional','locationtext','locationfulltext'])
        rowdataframe = rowdataframe.set_index(['territoryname'])
        df = df.append(rowdataframe)
        continue

      # 2
      if str(page) == '<Response [503]>':
        print ('Response 503. BANNED.')
        print (url)
        exit()

      tree = html.fromstring(page.content)
      countries={}

      firstcountryname = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div[1]/div/div/h2/a/text()')
      if len(firstcountryname)!=0: firstcountryname = firstcountryname[0]
      # 3
      else: 
        print ((languageCode)+(' code did not retrieve any result.')); 
        rowdataframe = pd.DataFrame([['unknown', 'unknown', 'unknown',languagenameEnglishethnologue, language,  'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown', 'unknown','unknown']], columns = ['territoryname', 'territorynameNative', 'QitemTerritory', 'languagenameEnglishethnologue','WikimediaLanguagecode', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous',  'languagestatuscountry', 'officialnationalorregional','numbercountriesOfficialorRegional','locationtext','locationfulltext'])
        rowdataframe = rowdataframe.set_index(['territoryname'])
        df = df.append(rowdataframe)
        continue;

      languagenameEnglishethnologue = tree.xpath('//*[@id="page-title"]/text()')[0]

      location = ''; status = ''; othercomments = '';
      num=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/text()'))
      for count in range(2,num):
        title = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[1]/text()')[0]

        if title == 'Location': 
          location = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]
          #print (title)
          #print (location)

        if title == 'Language Status': 
          status = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0] # https://www.ethnologue.com/about/language-status
          #print (title)
          #print (status)

        if title == 'Other Comments': 
          othercomments = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')[0]
          #print (title)
          #print (othercomments)
          
          if status =='':
            numcodes=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/text()')) # for macrolanguages like Arabic
            print ('Adding new languages.')
            for codecount in range(1,numcodes):
              code=tree.xpath('//*[@id="block-system-main"]/div/div/div/div[1]/div/div/div['+str(count)+']/div[2]/div/p/a['+str(codecount)+']/text()')[0]
              code=code.replace(']','');code=code.replace('[','')
              if code not in languageCodequery:
                languageCodequery.append(code)


      countries[firstcountryname]={}
      countries[firstcountryname]['status']=status
      countries[firstcountryname]['location']=location
      countries[firstcountryname]['othercomments']=othercomments
      countries[firstcountryname]['indigenous']='yes'
      
  #   input('ESPERA MAL PERIT')
      othercountries=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/text()'))
      for countrycount in range(1,othercountries):
        countryname=tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/legend/span/span/text()')[0]
        countries[countryname]={}

        location = ''; status = ''; othercomments = '';
        num=len(tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/text()'))-1

        for count in range(1,num):
          title = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/strong/text()')[0]

          
          if title == 'Location':
            location = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]
            #print (title)
            #print (location)

          if title == 'Status':
            status = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]
            #print (title)
            #print (status)
          
          if title == 'Other Comments':
            othercomments = tree.xpath('//*[@id="block-system-main"]/div/div/div/div[2]/div/div[2]/div['+str(countrycount)+']/fieldset/div/div['+str(count)+']/div/text()')[0]
            #print (title)
            #print (othercomments)

        countries[countryname]={}
        countries[countryname]['status']=status
        countries[countryname]['location']=location
        countries[countryname]['othercomments']=othercomments
        countries[countryname]['indigenous']='no' # temporary
        #print (countries)


      print ('\nThese are the countries where the language is spoken: ')
      print (countries)
      print ('A total of: '+str((len(countries))))
      #print (countries.keys())
      #input('un moment')

      # UNPACKING COUNTRIES INTO TERRITORIES AND SAVING THEM INTO THE DATAFRAME
      for countryname in countries.keys():
        print (('\nCOUNTRY: ')+(countryname))

        # Key conditions
        officialnationalorregional = 'no'
        indigenous=countries[countryname]['indigenous']

        languagestatuscountry=''
        if countries[countryname]['status']!='': languagestatuscountry = countries[countryname]['status'][0]

        if languagestatuscountry == '1': officialnationalorregional='national'
        if languagestatuscountry == '2': officialnationalorregional='regional'

        if 'non-indigenous' not in countries[countryname]['othercomments'].lower(): indigenous='yes' # fixes the temporary no

        # Get the key information about the language-country relationship
        print (('Official language in the country: ')+(officialnationalorregional))
        print (('Indigenous: ')+(indigenous))

        if officialnationalorregional=='no' and indigenous=='no': # 'OUT!'
          print ('It does not fit in: OUT OF THE DATASET')
          continue # OUT!

        # Get the location
        location = countries[countryname]['location']
        print ('LOCATION: ')
        print ((location)+'\n')
        locationfulltext = location

        # Get the country name and ISO 3166 and its subdivisions
        countrylist = list(pycountry.countries)
        if '–' in countryname: countryname=countryname.split('–')[1] # Exception for China-Hong Kong, China-Macau
        for country in countrylist:
#         print (country)
          ISO3166 = ''
          if countryname == country.name:
            ISO3166 = country.alpha_2
#           print ('som aquí 0')
            break
          if countryname in country.name:
            ISO3166 = country.alpha_2
#           print (countryname)
#           print (country.alpha_2)
#           print ('som aquí 1')
            break


         # Unpack the territories within the country
        wikidatacountrysubdivisions = download_language_countries_and_subdivisions_wikidata(language,ISO3166)
        territories = []
        if 'widespread' in location.lower() or location == '': # IT IS A COUNTRY!
          sentence = (location +' '+countryname)
          territories = identify_territories_from_location_sentence_old(sentence,wikidatacountrysubdivisions,ISO3166)
          #print (territories)
        else: # IT IS A SUBDIVISION!
          location=location.replace(' and ',', ')
          location=location.replace(';','.')

          sentences = location.split('.'); 
          print ('We just cut the sentences into: ')
          print (sentences) # cut into sentences
          print ('\n')

          for sentence in sentences:
            result = identify_territories_from_location_sentence_old(sentence,wikidatacountrysubdivisions,ISO3166)
            if (len(result))!=0: territories = territories + result

        for territory in territories: # INTRODUCING THEM INTO THE DATAFRAME
          print ('*** To the DataFrame and file: ')
          print (territory)

          # Common to all territories within the country
          language # wikimedialanguagecode
          officialnationalorregional  # related to the country info, not the specific territory
          indigenous # related to the country info, not the specific territory
          numbercountriesOfficialorRegional # related to the country info extracted from unicode
          countryname # related to the country info, not the specific territory

          # Specific to the territory
          QitemTerritory = territory['Qitem']
          territoryname = territory['territoryname']
          territorynameNative = territory['territorynameNative']
          demonym = territory['demonym']
          demonymNative = territory['demonymNative']
          ISO3166 = territory['ISO3166']
          ISO31662 = territory['ISO31662']
          region = territory['region'] # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
          locationtext = territory['locationtext']
          locationfulltext # this is for the entire country

          rowdataframe = pd.DataFrame([[ territoryname, territorynameNative, QitemTerritory, languagenameEnglishethnologue, language, demonym, demonymNative, ISO3166, ISO31662, region, countryname, indigenous, languagestatuscountry, officialnationalorregional, numbercountriesOfficialorRegional, locationtext,locationfulltext]], columns = ['territoryname', 'territorynameNative', 'QitemTerritory', 'languagenameEnglishethnologue','WikimediaLanguagecode', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous',  'languagestatuscountry', 'officialnationalorregional','numbercountriesOfficialorRegional','locationtext','locationfulltext'])
          rowdataframe = rowdataframe.set_index(['territoryname'])
          #print (rowdataframe)
          df = df.append(rowdataframe)
          #input('FINALE')

    if languagecount % 10: df.to_csv(filename+'.csv',sep='\t')
  df.to_csv(filename+'.csv',sep='\t') # TO FILE
  # this file has the name: Wikipedia_language_territories_mapping.csv and needs to be verified and converted to Wikipedia_language_territories_mapping_quality.csv



# This gets the territories from a sentence using WikiData and pycountry databases.
def identify_territories_from_location_sentence_old(sentence,wikidatacountrysubdivisions,ISO3166):

    print ('HERE IS THE SENTENCE: ')
    print (sentence)
    print ('\n')
#   print (ISO3166)

#   input(' a ')
    subdivisionsintext=[]
    sentence=sentence.strip()
    if sentence=='': return subdivisionsintext

    # PYCOUNTRY
    typeslist=[]
    subdivisions=list(pycountry.subdivisions)

    for subdivision in subdivisions:
        if subdivision.type not in typeslist: typeslist.append(subdivision.type)

    for subdivision in list(pycountry.subdivisions.get(country_code=ISO3166)):
        name=subdivision.name
#       print (name)

        if name in sentence:
           print ('\nBINGO: PYCOUNTRY')
           print (sentence)
           print (name)
           print (subdivision)
           subdivisionsintext.append({
                'Qitem': '',
                'territoryname': name,
                'territorynameNative': '',
                'demonym': '',
                'demonymNative': '',
                'ISO3166': ISO3166,
                'ISO3166_2': subdivision.code,
                'region':'yes', # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
                'parentcode':subdivision.parent_code,
                'type':subdivision.type,
                'locationtext': sentence,
                })
#   print (subdivisionsintext)

    # INTRODUCING WIKIDATA DATA
    for subdivision in wikidatacountrysubdivisions:
        name=subdivision['itemlabelen']
        nameNative=subdivision['itemlabelNative']

#       print (nameNative)
#       print (name)
        for type in typeslist:
            name=name.replace(type,'').rstrip()
            nameNative=nameNative.replace(type,'').rstrip()

        if (name != '' and name in sentence) or (nameNative !='' and nameNative in sentence):
            print ('\nBINGO 2: WIKIDATA')
            print (sentence)
            print (name, nameNative)
            print (subdivision)

            if subdivision['ISO31662']!='': region='yes' # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.

            else: region='no'
            subdivisionsintext.append({
                'Qitem': subdivision['Qitem'],
                'territoryname': name,
                'territorynameNative': nameNative,
                'demonym': subdivision['demonymen'],
                'demonymNative': subdivision['demonymNative'],
                'ISO3166': ISO3166,
                'ISO3166_2': subdivision['ISO31662'],
                'region':region,
                'locationtext': sentence
                })

#   print (subdivisionsintext)

#   unite pycountry + WikiData info.
    for subdivision in subdivisionsintext:
        ISO31662 = subdivision['ISO31662']
        for secondsubdivision in subdivisionsintext:
            if secondsubdivision['ISO31662']==ISO31662 and secondsubdivision['Qitem']=='' and subdivision['Qitem']!='':
                subdivision['parentcode']=secondsubdivision['parentcode']
                subdivision['type']=secondsubdivision['type']
                subdivisionsintext.remove(secondsubdivision)
#   print (subdivisionsintext)
#   input(' b ')

    if len(subdivisionsintext)==0:
        print (('WARNING: We could not identify any territory in this sentence: ')+sentence+'\n')       
        subdivisionsintext.append({
        'Qitem': '',
        'territoryname': '',
        'territorynameNative': '',
        'demonym': '',
        'demonymNative': '',
        'ISO3166': ISO3166,
        'ISO3166_2': '',
        'region': 'yes', # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
        'locationtext': sentence
        })

    return subdivisionsintext


def get_wikipedia_languages_territories_mapping():

    wikipedia_languages_territories_mapping = {}

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = 'SELECT WikimediaLanguagecode, QitemTerritory, territoryname, territorynameNative, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, region, subregion, intermediateregion, languagenameEnglishethnologue FROM wikipedia_languages_territories_mapping ORDER BY WikimediaLanguagecode DESC;'

    lang_territories = []
    cur_WikimediaLanguagecode = ''
    old_WikimediaLanguagecode = ''
    i = 0
    for row in cursor.execute(query):
        cur_WikimediaLanguagecode = row[0]

        if cur_WikimediaLanguagecode != old_WikimediaLanguagecode and i!=0:
            wikipedia_languages_territories_mapping[old_WikimediaLanguagecode]=lang_territories
            lang_territories=[]

        lang_territories.append(row)
        old_WikimediaLanguagecode = cur_WikimediaLanguagecode
        i+=1

    return wikipedia_languages_territories_mapping


def import_wikipedia_languages_territories_mapping_csv_store_sqlite3():
# READ FROM STORED FILE:
    territories = pd.read_csv(databases_path +'language_territories_mapping.csv',sep='\t',na_filter = False)
    territories = territories[['WikimediaLanguagecode','languagenameEnglishethnologue','territoryname','territorynameNative','QitemTerritory','demonym','demonymNative','ISO3166','ISO31662','regional','country','indigenous','languagestatuscountry','officialnationalorregional']]

    territories = territories.set_index(['WikimediaLanguagecode'])
#    territories.to_csv(databases_path +'language_territories_mapping_quality_beta.csv',sep='\t')

    territorylanguagecodes = territories.index.tolist()
    for n, i in enumerate(territorylanguagecodes): territorylanguagecodes[n]=i.replace('-','_')
    territories.index = territorylanguagecodes
    territories=territories.rename(index={'be_tarask': 'be_x_old'})
    territories=territories.rename(index={'nan': 'zh_min_nan'})

    # add regions
    ISO3166=territories['ISO3166'].tolist()
    regions = pd.read_csv(databases_path +'country_regions.csv',sep=',',na_filter = False)
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


    territories = territories.reset_index()

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS wikipedia_languages_territories_mapping ('+
    'WikimediaLanguagecode text,'+
    'languagenameEnglishethnologue text,'+
    'territoryname text,'+
    'territorynameNative text,'+
    'QitemTerritory text,'+
    'demonym text,'+
    'demonymNative text,'+
    'ISO3166 text,'+
    'ISO31662 text,'+
    'regional text,'+
    'country text,'+
    'indigenous text,'+
    'languagestatuscountry text,'+
    'officialnationalorregional text,'+
    'region text,'+
    'subregion text,'+
    'intermediateregion text,'+

    'PRIMARY KEY (WikimediaLanguagecode,QitemTerritory,ISO31662));')
    cursor.execute(query); conn.commit()

    a = territories.values.tolist()

    query = 'INSERT OR IGNORE INTO wikipedia_languages_territories_mapping (WikimediaLanguagecode, languagenameEnglishethnologue, territoryname, territorynameNative, QitemTerritory, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, officialnationalorregional, region, subregion, intermediateregion) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'


    cursor.executemany(query,a)
    conn.commit()




def import_wiki_projects_csv_store_sqlite3(territories):

    languages = pd.read_csv(databases_path + 'wikipedia_language_editions'+'.csv',sep='\t',na_filter = False)
    languages=languages[['languagename','Qitem','WikimediaLanguagecode','Wikipedia','WikipedialanguagearticleEnglish','languageISO','languageISO3','languageISO5','languageofficialnational','languageofficialregional','languageofficialsinglecountry','nativeLabel','numbercountriesOfficialorRegional']]
    languages = languages.set_index(['WikimediaLanguagecode'])
#    print (list(languages.columns.values))

    wikilanguagecodes = languages.index.tolist()
    for n, i in enumerate(wikilanguagecodes): wikilanguagecodes[n]=i.replace('-','_')
    languages.index = wikilanguagecodes
    languages=languages.rename(index={'be_tarask': 'be_x_old'})
    languages=languages.rename(index={'nan': 'zh_min_nan'})

    # add regions
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

    languages = languages.reset_index()
    languages = languages.rename(columns={'index': 'WikimediaLanguagecode'})


    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS wiki_projects ('+
    'WikimediaLanguagecode text,'+
    'languagename text,'+
    'Qitem text,'+
    'Wikipedia text,'+
    'WikipedialanguagearticleEnglish text,'+
    'languageISO text,'+
    'languageISO3 text,'+
    'languageISO5 text,'+
    'languageofficialnational text,'+
    'languageofficialregional text,'+
    'languageofficialsinglecountry text,'+
    'nativeLabel text,'+
    'numbercountriesOfficialorRegional text,'+
    'region text,'+
    'subregion text,'+
    'intermediateregion text,'+


    'PRIMARY KEY (WikimediaLanguagecode,Wikipedia));')
    cursor.execute(query); conn.commit()

    a = languages.values.tolist()
#    print (languages.head())
#    print (a[0])

    query = 'INSERT OR IGNORE INTO wiki_projects (WikimediaLanguagecode,languagename,Qitem,Wikipedia,WikipedialanguagearticleEnglish,languageISO,languageISO3,languageISO5,languageofficialnational,languageofficialregional,languageofficialsinglecountry,nativeLabel,numbercountriesOfficialorRegional,region,subregion,intermediateregion) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);'

    cursor.executemany(query,a)
    conn.commit()

    return languages



# https://cdstar.shh.mpg.de/bitstreams/EAEA0-7269-77E5-3E10-0/wals_language.csv.zip
def import_languages_wals_csv_store_sqlite3():

    wals_languages = pd.read_csv(dumps_path + 'language'+'.csv',sep=',',na_filter = False)
    wals_languages = wals_languages[['wals_code','iso_code','glottocode','Name','latitude','longitude','genus','family','macroarea','countrycodes']]
    wals_languages = wals_languages.values.tolist()

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS wals_languages ('+
    'als_code text,'+
    'iso_code text,'+
    'glottocode text,'+
    'Name text,'+
    'latitude text,'+
    'longitude text,'+
    'genus text,'+
    'family text,'+
    'macroarea text,'+
    'countrycodes text,'+
    'PRIMARY KEY (iso_code,Name));')
    cursor.execute(query); conn.commit()

    query = 'INSERT OR IGNORE INTO wals_languages (als_code,iso_code,glottocode,Name,latitude,longitude,genus,family,macroarea,countrycodes) VALUES (?,?,?,?,?,?,?,?,?,?);'

    cursor.executemany(query,wals_languages)
    conn.commit()


def import_country_regions_csv_store_sqlite3():

    country_regions = pd.read_csv(dumps_path + 'country_regions'+'.csv',sep=',',na_filter = False)
    country_regions = country_regions[['name','alpha-2','alpha-3','country-code','iso_3166-2','region','sub-region','intermediate-region','region-code','sub-region-code','intermediate-region-code']]
    country_regions = country_regions.values.tolist()

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS country_regions ('+
    'name text,'+
    'alpha_2 text,'+
    'alpha_3 text,'+
    'country_code text,'+
    'iso_3166_2 text,'+
    'region text,'+
    'sub_region text,'+
    'intermediate_region text,'+
    'region_code text,'+
    'sub_region_code text,'+
    'intermediate_region_code text,'+
    'PRIMARY KEY (name,alpha_2));')
    cursor.execute(query); conn.commit()

    query = 'INSERT OR IGNORE INTO country_regions (name, alpha_2, alpha_3, country_code, iso_3166_2, region, sub_region, intermediate_region, region_code, sub_region_code, intermediate_region_code) VALUES (?,?,?,?,?,?,?,?,?,?,?);'

    cursor.executemany(query,country_regions)
    conn.commit()


def import_world_subdivisions_csv_store_sqlite3():

    input_subdivisions_filename = dumps_path + 'world_subdivisions.csv'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = []
    for line in input_subdivisions_file: 
        info = line.strip('\n').split(',');
        subdivisions.append([info[0],info[1]])

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS world_subdivisions ('+
    'name text,'+
    'subdivision_code text,'+
    'PRIMARY KEY (name,subdivision_code));')
    cursor.execute(query); conn.commit()

    query = 'INSERT OR IGNORE INTO world_subdivisions (name, subdivision_code) VALUES (?,?);'
    cursor.executemany(query,subdivisions)
    conn.commit()


# https://www.ip2location.com/free/iso3166-2
def import_ip2location_ISO3166_2_csv_store_sqlite3():

    input_subdivisions_filename = dumps_path + 'IP2LOCATION-ISO3166-2.CSV'
    input_subdivisions_file = open(input_subdivisions_filename, 'r')
    subdivisions = []
    input_subdivisions_file.readline()
    for line in input_subdivisions_file: 
        info = line.replace('"','').strip('\n').split(',');
        print (info)
        subdivisions.append([info[0],info[1],info[2]])

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS ISO3166_2_ip2location ('+
    'country_code text,'+
    'subdivision_name text,'+
    'subdivision_code text,'+
    'PRIMARY KEY (subdivision_name,subdivision_code));')
    cursor.execute(query); conn.commit()

    query = 'INSERT OR IGNORE INTO ISO3166_2_ip2location (country_code, subdivision_name, subdivision_code) VALUES (?,?,?);'
    cursor.executemany(query,subdivisions)
    conn.commit()

# https://raw.githubusercontent.com/esosedi/3166/master/data/iso3166-2.json
def import_multilingual_ISO3166_2_csv_store_sqlite3():

    input_subdivisions_filename = dumps_path + 'iso3166-2.json'

    subdivisions = []
    with open(input_subdivisions_filename, 'r') as f:
        array = json.load(f)

    for country in array:
        country_code = array[country]['iso']

        for region in array[country]['regions']:
            iso_3166_2 = region['iso']
            if iso_3166_2 != '':
                iso_3166_2 = country_code+'-'+iso_3166_2

            for langcode, subdivision_name in region['names'].items():   
                subdivisions.append((country_code,subdivision_name,iso_3166_2))
                print ((country_code,subdivision_name,iso_3166_2))

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();  

    query = ('CREATE TABLE IF NOT EXISTS multilingual_ISO3166_2 ('+
    'country_code text,'+
    'subdivision_name text,'+
    'subdivision_code text,'+
    'PRIMARY KEY (subdivision_name));')
    cursor.execute(query); conn.commit()

    query = 'INSERT OR IGNORE INTO multilingual_ISO3166_2 (country_code, subdivision_name, subdivision_code) VALUES (?,?,?);'
    cursor.executemany(query,subdivisions)
    conn.commit()
    print ('in.')




def export_language_characteristics_to_complement(property_update,number):

    languagestatus = {'0':'Q29051543','1':'Q29051546','2':'Q29051547','3':'Q29051549','4':'Q29051550','5':'Q29051551','6a':'Q29051552','6b':'Q29051554','7':'Q29051555','8a':'Q29051556','8b':'Q29051558','9':'Q29051560','10':'Q29051561','':'Q63671741'}

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    if property_update == 'P31' and number == 'language':
      # TO CALL THEM LANGUAGES
      query = 'SELECT qitem, languageISO3 FROM all_languages_wikidata WHERE instanceof != "Q34770" AND languageISO3 IN (SELECT language_code FROM ethnologue_languages_names);' # els que s'haurien de dir llengua i encara no se'n diuen.

    elif property_update == 'P31' and number == 'macrolanguage':
      # TO CALL THEM MACROLANGUAGES
      query = 'SELECT qitem, languageISO3 FROM all_languages_wikidata WHERE instanceof != "Q152559" AND languageISO3 IN (SELECT macrolanguage_ISO639_3_code FROM language_countries_mapping WHERE composition = "macrolanguage")' # quins són macrollengua i se'ls ha de dir macrollengua.

    elif property_update == 'P31' and number == 'dead language':
      # TO CALL THEM DEAD LANGUAGE
      query = 'SELECT qitem, languageISO3 FROM all_languages_wikidata WHERE instanceof != "Q45762" AND languageISO3 IN (SELECT language_code_ISO639_3 FROM language_countries_mapping WHERE lang_type = "Extinct")' # quins són macrollengua i se'ls ha de dir macrollengua.


    elif property_update == 'P220' and number == 1:
      # TO GIVE THEM THE ISOCODE3
      query = 'SELECT DISTINCT qitem, language_code_ISO639_3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON englishLabel = language_name WHERE languageISO3 = ""'

    elif property_update == 'P220' and number == 2:

      query = 'SELECT DISTINCT qitem, language_code_ISO639_3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON englishLabel = language_name_inverted WHERE languageISO3 = ""'
      # els que no tenen isocode3 i n'haurien de tenir.

    elif property_update == 'P1466':
      # TO GIVE THEM WALS CODE
      query = 'SELECT qitem, als_code FROM all_languages_wikidata INNER JOIN wals_languages ON languageISO3 = iso_code AND wals_code = "" AND languageISO3 != ""' # els que no tenen wals code amb el seu wals code.

    elif property_update == 'P1394':
      # TO GIVE THEM GLOTTOCODE CODE
      query = 'SELECT DISTINCT qitem, wals_languages.glottocode FROM all_languages_wikidata INNER JOIN wals_languages ON languageISO3 = iso_code AND all_languages_wikidata.glottocode = "" AND languageISO3 != ""' # els que no tenen wals code amb el seu wals code.

    elif property_update == 'P625':
      # TO GIVE THEM LOCATION
      query = 'SELECT DISTINCT qitem, latitude, longitude, als_code FROM all_languages_wikidata INNER JOIN wals_languages ON languageISO3 = iso_code AND languageISO3 != "" AND als_code != ""' # els que no tenen wals code amb el seu wals code.

    elif property_update == 'P1705':
      # TO GIVE THEM NATIVE LABEL
      query = 'SELECT DISTINCT all_languages_wikidata.qitem, autonym, languageISO FROM all_languages_wikidata INNER JOIN language_territories_mapping ON languageISO3 = language_code_ISO639_3 WHERE autonym != "" AND languageISO!="";'


    elif property_update == 'P1627' and number == 1:
      # TO GIVE THEM ETHNOLOGUE CODE
      query = 'SELECT DISTINCT qitem, language_code_ISO639_3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON englishLabel = language_name_inverted WHERE Ethnologuecode = ""'

    elif property_update == 'P1627' and number == 2:
      query = 'SELECT DISTINCT qitem, language_code_ISO639_3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON englishLabel = language_name WHERE Ethnologuecode = ""'

    elif property_update == 'P3823':
      # TO GIVE THEM ETHNOLOGUE STATUS CODE
      query = 'SELECT DISTINCT all_languages_wikidata.qitem, language_status_code FROM all_languages_wikidata INNER JOIN language_countries_mapping ON languageISO3 = language_code_ISO639_3 WHERE language_status_code != "";'

    elif property_update == 'P17':
      # TO GIVE THEM THE COUNTRIES
      query = 'SELECT DISTINCT all_languages_wikidata.qitem, all_countries_wikidata.qitem, language_status_code, language_countries_mapping.language_code_ISO639_3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON languageISO3 = language_code_ISO639_3 INNER JOIN all_countries_wikidata ON language_countries_mapping.ISO3166 = all_countries_wikidata.ISO3166 WHERE language_status_code != "";'

    elif property_update == 'P2341':
      # TO GIVE THEM THE INDIGENOUS TERRITORIES
      query = 'SELECT DISTINCT all_languages_wikidata.qitem, language_territories_mapping.qitem, ISO3166_2 FROM all_languages_wikidata INNER JOIN language_territories_mapping ON languageISO3 = language_code_ISO639_3 WHERE indigenous = 1 AND language_territories_mapping.qitem!="";'

    elif property_update == 'P1098':
      # TO GIVE THEM THE NUMBER OF SPEAKERS
      query = 'SELECT DISTINCT all_languages_wikidata.qitem, population, all_languages_wikidata.languageISO3 FROM all_languages_wikidata INNER JOIN language_countries_mapping ON languageISO3 = language_code_ISO639_3;'


    path = '/srv/wcdo/datasets/export_wikidata/'+'languages_'+property_update+'_'+str(number)+'.csv'
    with open(path, 'w') as f:


      low_value = 10
      old_qitem = ''
      string = ''
      valor = 0
      for row in cursor.execute(query):
        qitem = row[0]
        value = row[1]

        if property_update == 'P31' and number == 'language':
          string = qitem+'\t'+property_update+'\t'+'Q34770'+'\t'+'S248'+'\t'+'Q14790'+'\n'

#          string = qitem+'\t'+property_update+'\t'+'Q34770'+'\t'+'S854'+'\t'+'"https://www.ethnologue.com/language/'+value+'"\n'

        elif property_update == 'P31' and number == 'macrolanguage':
          string = qitem+'\t'+property_update+'\t'+'Q152559'+'\t'+'S854'+'\t'+'"https://www.ethnologue.com/language/'+value+'"\n'

        elif property_update == 'P31' and number == 'dead language':
          string = qitem+'\t'+property_update+'\t'+'Q45762'+'\t'+'S854'+'\t'+'"https://www.ethnologue.com/language/'+value+'"\n'


        elif property_update == 'P1627' and number == 1:
          string = qitem+'\t'+property_update+'\t"'+value+'"\tS854\t'+'"https://www.ethnologue.com/language/'+value+'"\n'

        elif property_update == 'P1627' and number == 2:
          string = qitem+'\t'+property_update+'\t"'+value+'"\tS854\t'+'"https://www.ethnologue.com/language/'+value+'"\n'

        elif property_update == 'P1705':
          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"\n'

        elif property_update == 'P3823':

          if old_qitem != qitem and old_qitem!='':
            string = old_qitem+'\t'+property_update+'\t'+languagestatus[val]+'\n'
            low_value = 10

          v = float(value.replace('a','').replace('b','.5'))
          if v <= low_value:
            low_value = v
            val = value

          old_qitem = qitem
          old_value = value

        elif property_update == 'P17':
          string = qitem+'\t'+property_update+'\t'+value+'\t'+languagestatus[row[2]]+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[3]+'"\n'

        elif property_update == 'P625':
          string = qitem+'\t'+property_update+'\t@'+row[1]+'/'+row[2]+'\tS854\t'+'"https://wals.info/languoid/lect/wals_code_'+row[3]+'"\n'

        elif property_update == 'P1098':

          if old_qitem != qitem and old_qitem!='':
            string = old_qitem+'\t'+property_update+'\t'+str(valor)+'\tS854\t'+'"https://www.ethnologue.com/language/'+old_iso+'"\n'
            valor = 0

          valor += int(value)

          old_qitem = qitem
          old_iso = row[2]

        elif property_update == 'P2341':
          string = qitem+'\t'+property_update+'\t'+value+'\n'

        else:

          string = qitem+'\t'+property_update+'\t"'+value+'"\n'

        f.write(string)




def export_countries_language_characteristics_to_complement(property_update,number):

    languagestatus = {'0':'Q29051543','1':'Q29051546','2':'Q29051547','3':'Q29051549','4':'Q29051550','5':'Q29051551','6a':'Q29051552','6b':'Q29051554','7':'Q29051555','8a':'Q29051556','8b':'Q29051558','9':'Q29051560','10':'Q29051561','':'Q63671741'}

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();

    if property_update == 'P1705':
      # TO GIVE THEM A NATIVE NAME
      query = 'SELECT DISTINCT language_territories_mapping.qitem, territorynameNative, all_languages_wikidata.languageISO FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_code_ISO639_3 = languageISO3 WHERE regional = "no" AND territorynameNative != "" AND regional="no" AND indigenous="1" AND all_languages_wikidata.languageISO != "";'


    elif property_update == 'P1448':
      # TO GIVE THEM AN OFFICIAL NAME
      query = 'SELECT DISTINCT language_territories_mapping.qitem, territorynameNative, all_languages_wikidata.languageISO, language_territories_mapping.language_name, language_code_ISO639_2 FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_code_ISO639_3 = languageISO3 WHERE regional = "no" AND territorynameNative != "" AND all_languages_wikidata.languageISO != "";'


    elif property_update == 'P2936':
      query = 'SELECT DISTINCT ISO3166, qitem FROM all_countries_wikidata;'
      iso_qitem = {}
      for row in cursor.execute(query):
        code = str(row[0])
        qitem = row[1]
        iso_qitem[code]=qitem

      # TO GIVE THEM A LANGUAGE USED
      query = 'SELECT DISTINCT language_countries_mapping.ISO3166, all_languages_wikidata.Qitem, language_countries_mapping.population, language_countries_mapping.language_status_code, all_languages_wikidata.languageISO3 FROM language_countries_mapping INNER JOIN all_languages_wikidata ON language_countries_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 ORDER BY 1;'


    elif property_update == 'P37':
      query = 'SELECT DISTINCT ISO3166, qitem FROM all_countries_wikidata;'
      iso_qitem = {}
      for row in cursor.execute(query):
        code = str(row[0])
        qitem = row[1]
        iso_qitem[code]=qitem

      # TO GIVE THEM AN OFFICIAL LANGUAGE
      query = 'SELECT DISTINCT all_countries_wikidata.qitem, all_languages_wikidata.Qitem, all_languages_wikidata.languageISO3 FROM language_countries_mapping INNER JOIN all_languages_wikidata ON language_countries_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 INNER JOIN all_countries_wikidata ON all_countries_wikidata.ISO3166 = language_countries_mapping.ISO3166 WHERE language_status_code IN ("1","2");'



    path = '/srv/wcdo/datasets/export_wikidata/'+'countries_'+property_update+'.csv'
    with open(path, 'w') as f:

      old_qitem = ''
      string = ''
      valor = 0

      for row in cursor.execute(query):
        qitem = row[0]
        value = row[1]

        if property_update == 'P1705':
          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"\n'

        elif property_update == 'P1448':
          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"\n'

        elif property_update == 'P2936':
#          string = iso_qitem[str(row[0])]+'\t'+property_update+'\t'+row[1]+'\tP1098\t'+str(row[2])+'\tP3823\t'+languagestatus[row[3]]+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[4]+'"\n'

          string = iso_qitem[str(row[0])]+'\t'+property_update+'\t'+row[1]+'\n'#+'P1098\t'+str(row[2])+'\tP3823\t'+languagestatus[row[3]]+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[4]+'"\n'


        elif property_update == 'P37':
          string = qitem+'\t'+property_update+'\t'+value+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[2]+'"\n'

        else:
          string = qitem+'\t'+property_update+'\t'+value+'\n'

        f.write(string)


def export_countries_subdivisions_language_characteristics_to_complement(property_update,number):

    languagestatus = {'0':'Q29051543','1':'Q29051546','2':'Q29051547','3':'Q29051549','4':'Q29051550','5':'Q29051551','6':'Q29051552','6a':'Q29051552','6b':'Q29051554','7':'Q29051555','8':'Q29051556','8a':'Q29051556','8b':'Q29051558','9':'Q29051560','10':'Q29051561','':'Q63671741'}

    conn = sqlite3.connect(databases_path+diversity_categories_db); cursor = conn.cursor();
    query = []

    if property_update == 'P1705':
      # TO GIVE THEM A NATIVE NAME
      query = 'SELECT language_territories_mapping.qitem, territorynameNative, all_languages_wikidata.languageISO, language_territories_mapping.language_code_ISO639_3 FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 WHERE language_territories_mapping.qitem IN (SELECT Qitem FROM all_countries_subdivisions_wikidata WHERE nativename = "") AND territorynameNative!="" AND indigenous = 1 AND all_languages_wikidata.languageISO != ""'

#      query = 'SELECT DISTINCT all_countries_subdivisions_wikidata.qitem, territorynameNative,all_languages_wikidata.Qitem,  language_code_ISO639_3 FROM all_countries_subdivisions_wikidata INNER JOIN language_territories_mapping ON all_countries_subdivisions_wikidata.qitem = language_territories_mapping.qitem INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 WHERE nativename = "" AND territorynameNative!="" AND indigenous = 1')

    elif property_update == 'P1448':
      # TO GIVE THEM AN OFFICIAL NAME
      query = 'SELECT language_territories_mapping.qitem, territorynameNative, all_languages_wikidata.languageISO, language_territories_mapping.language_code_ISO639_3 FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 WHERE language_territories_mapping.qitem IN (SELECT Qitem FROM all_countries_subdivisions_wikidata WHERE nativename = "") AND territorynameNative!="" AND language_status_code IN ("1","2") AND all_languages_wikidata.languageISO != ""'

#      query = 'SELECT DISTINCT all_countries_subdivisions_wikidata.qitem, territorynameNative, all_languages_wikidata.Qitem, language_code_ISO639_3 FROM all_countries_subdivisions_wikidata INNER JOIN language_territories_mapping ON all_countries_subdivisions_wikidata.qitem = language_territories_mapping.qitem INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 WHERE nativename = "" AND territorynameNative!="" AND language_status_code IN ("1","2")')

    elif property_update == 'P2936':
      # TO GIVE THEM A LANGUAGE USED
      query = 'SELECT language_territories_mapping.qitem, all_languages_wikidata.Qitem, population, language_territories_mapping.language_status_code, all_languages_wikidata.languageISO3 FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3'

#      query = 'SELECT DISTINCT all_countries_subdivisions_wikidata.qitem, all_languages_wikidata.Qitem, language_code_ISO639_3, population FROM all_countries_subdivisions_wikidata INNER JOIN language_territories_mapping ON all_countries_subdivisions_wikidata.qitem = language_territories_mapping.qitem INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 ORDER BY 3')

# HEY: HERE THEY ARE SUBDIVISIONS. THE QUERY NEEDS A CONDICION TO AVOID COUNTRIES. REGIONAL = YES.
    elif property_update == 'P37':
      # TO GIVE THEM AN OFFICIAL LANGUAGE
      query = 'SELECT language_territories_mapping.qitem, all_languages_wikidata.Qitem, all_languages_wikidata.languageISO3 FROM language_territories_mapping INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 AND language_status_code IN ("1","2")'

#      query = 'SELECT DISTINCT all_countries_subdivisions_wikidata.qitem, all_languages_wikidata.Qitem, englishLabel FROM all_countries_subdivisions_wikidata INNER JOIN language_territories_mapping ON all_countries_subdivisions_wikidata.qitem = language_territories_mapping.qitem INNER JOIN all_languages_wikidata ON language_territories_mapping.language_code_ISO639_3 = all_languages_wikidata.languageISO3 AND language_territories_mapping.language_status_code IN ("1","2")')
 

    path = '/srv/wcdo/datasets/export_wikidata/'+'countries_subdivisions_'+property_update+'.csv'
    with open(path, 'w') as f:

      old_qitem = ''
      string = ''
      valor = 0

      for row in cursor.execute(query):
        qitem = row[0]
        value = row[1]

        if property_update == 'P1705':
          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"\n'

#          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"'+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[3]+'"\n'

        elif property_update == 'P1448':
          string = qitem+'\t'+property_update+'\t'+row[2]+':"'+value+'"\n'


        elif property_update == 'P2936':
#          string = row[0]+'\t'+property_update+'\t'+row[1]+'\tP1098\t'+str(row[2])+'\tP3823\t'+languagestatus[row[3]]+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[4]+'"\n'
          string = qitem+'\t'+property_update+'\t'+value+'\n'#


        elif property_update == 'P37':
#          string = qitem+'\t'+property_update+'\t'+value+'\tS854\t'+'"https://www.ethnologue.com/language/'+row[2]+'"\n'

          string = qitem+'\t'+property_update+'\t'+value+'\n'#+'S854\t'+'"https://www.ethnologue.com/language/'+row[2]+'"\n'

        else:
          string = qitem+'\t'+property_update+'\t'+value+'\n'

        f.write(string)



if __name__ == '__main__':

  startTime = time.time()
  dumps_path = '/srv/wcdo/dumps/'
  databases_path = '/srv/wcdo/databases/'

  main()

  end = time.time()
  print ('job completed after: ' + str(end - startTime))