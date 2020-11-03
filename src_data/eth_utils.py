# -*- coding: utf-8 -*-
# This is the ROSETTA STONE of the project.

import wikilanguages_utils
from string import ascii_lowercase
import time
import os
import requests
import sqlite3
import pandas as pd
from lxml import html
import json


def get_language_names_codes_url_ethl():

    conn = sqlite3.connect(databases_path+'diversity_categories.db'); cursor = conn.cursor();
    
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


def get_language_countries_mapping_ethl(): # extracting from ethnologue.com
  # create table language-territories mapping

    conn = sqlite3.connect(databases_path+'diversity_categories.db'); cursor = conn.cursor();  
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