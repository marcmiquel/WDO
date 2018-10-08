# -*- coding: utf-8 -*-

# time
import time
import datetime
# system
import os
# databases
#import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# requests and others
import requests
# data
import pandas as pd
import colour


databases_path = '/srv/wcdo/databases/'


# Loads language_territories_mapping.csv file
def load_languageterritories_mapping():
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
def load_wiki_projects_information(territories):
    # in case of extending the project to other WMF sister projects, it would be necessary to revise these columns and create a new file where a column would specify whether it is a language edition, a wikictionary, etc.

# READ FROM STORED FILE:
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

# READ FROM META: 
# uses pywikibot and verifies the differences with the file. 
# sends an e-mail with the difference and the 'new proposed file'
#    send_email_toolaccount('subject', 'message')
# stops.
# we verify this e-mail, we re-start the task.

#    filename = 'language_editions_with_regions'
#    languages.to_csv(databases_path + filename+'.csv',sep='\t')

    return languages


def load_wikipedia_language_editions_numberofarticles(wikilanguagecodes):
    wikipedialanguage_numberarticles = {}

    if not os.path.isfile(databases_path + 'ccc_current.db'): return
    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()

    # Obtaining CCC for all WP
    for languagecode in wikilanguagecodes:
        try:
            query = 'SELECT COUNT(*) FROM ccc_'+languagecode+'wiki;'
            cursor.execute(query)
            wikipedialanguage_numberarticles[languagecode]=cursor.fetchone()[0]
        except:
            print ('this language is not in the database yet: '+languagecode)

    return wikipedialanguage_numberarticles



def load_dicts_page_ids_qitems(printme, languagecode):
    page_titles_qitems = {}
    page_titles_page_ids = {}

    conn = sqlite3.connect(databases_path + 'ccc_current.db'); cursor = conn.cursor()
    query = 'SELECT page_title, qitem, page_id FROM ccc_'+languagecode+'wiki;'
    for row in cursor.execute(query):
        page_title=row[0].replace(' ','_')
        page_titles_page_ids[page_title]=row[2]
        page_titles_qitems[page_title]=row[1]

    if printme == 1:
        print ('page_ids loaded.')
        print ('qitems loaded.')
        print ('they are:')
        print (len(page_titles_qitems))

    return (page_titles_qitems, page_titles_page_ids)





def load_countries_from_language(languagecode,territories):
    country_territories = {}
    countries=territories.loc[languagecode].ISO3166
    if isinstance(countries,str): countries = [countries]
    else: countries = list(set(countries))

    return countries


def load_territories_from_language_country(ISO3166, languagecode, territories):
    qitems = territories.loc[territories['ISO3166'] == ISO3166].loc[languagecode]['QitemTerritory']
    if isinstance(qitems,str): qitems = [qitems]
    else: qitems = list(qitems)
    
    return qitems



def load_iso_3166_to_geographical_regions():

    country_regions = pd.read_csv(databases_path + 'country_regions.csv', sep=',',na_filter = False)
    country_regions = country_regions[['alpha-2','name','region','sub-region']]
    country_regions = country_regions.rename(columns={'sub-region': 'subregion'})
    country_regions = country_regions.set_index(['alpha-2'])

    country_names = country_regions.name.to_dict()
    regions = country_regions.region.to_dict()
    subregions = country_regions.subregion.to_dict()

#    print (country_names)
    
    return country_names, regions, subregions


def load_language_pairs_apertium(wikilanguagecodes):
    functionstartTime = time.time()
    print ('* load_language_pairs_apertium')

#    r = requests.get("https://cxserver.wikimedia.org/v1/list/languagepairs", timeout=0.5)
#    print (r.text)
    filename = 'languagepairs_apertium.csv'
    languagecode_translated_from={}
    if not os.path.isfile(databases_path + filename):
        for languagecode_1 in wikilanguagecodes:
            languagecode_translated_from[languagecode_1]=[]
            for languagecode_2 in wikilanguagecodes:
                if languagecode_1 == languagecode_2: continue
                title=''
                try:
                    r = requests.post("https://cxserver.wikimedia.org/v2/translate/"+languagecode_2+"/"+languagecode_1+"/Apertium", data={'html': '<div>'+title+'</div>'}, timeout=0.5) # https://cxserver.wikimedia.org/v2/?doc  https://codepen.io/santhoshtr/pen/zjMMrG
                    if r!= None and r.text!='Provider not supported': 
                        print (languagecode_1,'from',languagecode_2, 'paired')
                        languagecode_translated_from[languagecode_1].append(languagecode_2)
                        with open(databases_path + filename, 'a') as f: f.write(languagecode_1+'\t'+languagecode_2+'\n')
                except:
                    print (languagecode_1,languagecode_2,'timeout')
    else:
        for line in open(databases_path + filename, 'r'): 
            info = line.strip('\n').split('\t');
            if info[0] not in languagecode_translated_from:
                languagecode_translated_from[info[0]]=[info[1]]
            else:
                languagecode_translated_from[info[0]].append(info[1])
        print (languagecode_translated_from)

    print ('* load_language_pairs_apertium Function completed after: ' + str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    # for a target, it returns you the origin.
    return languagecode_translated_from


# It returns a dictionary with qitems and countries
def load_all_countries_qitems():
    country_names, regions, subregions = load_iso_3166_to_geographical_regions()

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?item ?itemLabel ?isocode WHERE {
          ?item wdt:P31 wd:Q6256.
          OPTIONAL{?item wdt:P297 ?isocode .}
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }'''


    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    iso_qitem = {}
    label_qitem = {}

    for item in data['results']['bindings']:

        try:
            qitem_value = item['item']['value'].replace("http://www.wikidata.org/entity/","")
            iso_value = item['isocode']['value']
            item_name_value = item['itemLabel']['value']

            if iso_value not in country_names: continue

            iso_qitem[iso_value]=qitem_value
            label_qitem[item_name_value]=qitem_value
        except:
            pass

    return (iso_qitem, label_qitem)


# It returns three lists with 20 languages according to different criteria.
def obtain_proximity_wikipedia_languages_lists(languagecode, wikipedialanguage_numberarticles, top_val, upperlower_vals, closest_val):

    # BIGGEST    # recommended 19
    top = []
    if top_val!=None:
        wikipedialanguage_numberarticles_sorted = sorted(wikipedialanguage_numberarticles, key=wikipedialanguage_numberarticles.get, reverse=True)

        top = wikipedialanguage_numberarticles_sorted[:top_val]
        if languagecode in top: top.append(wikipedialanguage_numberarticles_sorted[top_val+1])
        else: top.append(languagecode)

    # MIDDLE ONES     # recommended 9 and 10
    upperlower = []
    if upperlower_vals!=None:
        i=wikipedialanguage_numberarticles_sorted.index(languagecode)
        upperlower = wikipedialanguage_numberarticles_sorted[i-upperlower_vals[0]:i]+wikipedialanguage_numberarticles_sorted[i:i+upperlower_vals[1]]

    # COVERING BEST CCC    # recommended 19
    closest = []
    if closest_val!=None:
        conn = sqlite3.connect(databases_path+'wcdo_stats.db'); cursor = conn.cursor()
        query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE set1 = "'+languagecode+'" AND set1descriptor = "ccc" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY 2 DESC LIMIT '+str(closest_val)
        for row in cursor.execute(query):
            closest.append(row[0])

    return top, upperlower, closest


def obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, num):

    closest_langs = {}
    if not os.path.exists(databases_path + 'obtain_closest_for_all_languages.csv'):
        with open(databases_path + 'obtain_closest_for_all_languages.csv', 'w') as f:
            for languagecode in wikilanguagecodes:
                top, upperlower, closest= obtain_proximity_wikipedia_languages_lists(languagecode,wikipedialanguage_numberarticles, None, None, num)
                if len(closest)==0: continue
                row=''
                for x in range(0,num): row+=closest[x]+'\t'
                f.write(languagecode+'\t'+row+'\n')
                closest_langs[languagecode] = closest
    else:
        with open(databases_path + 'obtain_closest_for_all_languages.csv', 'r') as f:
            for line in f:
                line = line[:len(line)-2]
                closest=line.split('\t')
                languagecode = closest.pop(0)
                closest_langs[languagecode] = closest


    return closest_langs

# It returns a list of languages based on the region preference introduced.
def obtain_region_wikipedia_language_list(languages, region, subregion, intermediateregion):
# use as: wikilanguagecodes = wikilanguages_utils.obtain_region_wikipedia_language_list('Asia', '', '').index.tolist()
#    print('* This is the list of continents combinations: '+' | '.join(languages.region.unique())+'\n')
#    print('* This is the list of subregions (in-continents) combinations: '+' | '.join(languages.subregion.unique())+'\n')
#    print('* This is the list of intermediate regions (in continents and regions) combinations: '+' | '.join(languages.intermediateregion.unique())+'\n')
#    print (languages)
    if region!='':
        languages_region = languages.loc[languages['region'] == region]

    if subregion!='':
        languages_region = languages[languages['subregion'].str.contains(subregion)]

    if intermediateregion!='':
        languages_region = languages[languages['intermediateregion'].str.contains(intermediateregion)]

    return languages_region


# Create a database connection.
def establish_mysql_connection_read(languagecode):
#    print (languagecode)
    try: 
        mysql_con_read = mdb.connect(host=languagecode+"wiki.analytics.db.svc.eqiad.wmflabs",db=languagecode + 'wiki_p',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
        return mysql_con_read
    except:
        pass
#        print ('This language ('+languagecode+') has no mysql replica at the moment.')

# Create a database connection.
def establish_mysql_connection_write():
    mysql_con_write = mdb.connect(host="tools.db.svc.eqiad.wmflabs",db='s53619__wcdo',read_default_file=os.path.expanduser("./my.cnf"),charset='utf8mb4')
    return mysql_con_write

# Additional sources: Pycountry and Babel libraries for countries and their languages.
def extract_check_new_wiki_projects():
    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?itemLabel ?language ?languageLabel ?alias ?nativeLabel ?languageISO ?languageISO3 ?languageISO5 ?languagelink ?WikimediaLanguagecode WHERE {
      ?item wdt:P31 wd:Q10876391.
      ?item wdt:P407 ?language.
      
      OPTIONAL{?language wdt:P1705 ?nativeLabel.}
#     ?item wdt:P856 ?officialwebsite.
#     ?item wdt:P1800 ?bbddwp.
      ?item wdt:P424 ?WikimediaLanguagecode.

      OPTIONAL {?language skos:altLabel ?alias FILTER (LANG (?alias) = ?WikimediaLanguagecode).}
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
    ORDER BY ?WikimediaLanguagecode'''

    
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = requests.get(url, params={'query': query, 'format': 'json'}).json()
    #print (data)

    extracted_languages = []
    wikimedialanguagecode = ''
    Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];
    for item in data['results']['bindings']:    
#        print (item)
        #input('tell me')
        if wikimedialanguagecode != item['WikimediaLanguagecode']['value'] and wikimedialanguagecode!='':

            try:
                currentLanguagename=Locale.parse(wikimedialanguagecode).get_display_name(wikimedialanguagecode).lower()
                if currentLanguagename not in nativeLabel:
#                    print ('YAHOO')
                    nativeLabel.append(currentLanguagename)
#                    print(currentLanguagename)
            except:
                pass

            extracted_languages.append({
            'languagename': ";".join(languagename),          
            'Qitem': ";".join(Qitem),
            'WikimediaLanguagecode': wikimedialanguagecode,
            'Wikipedia':wikipedia,
            'WikipedialanguagearticleEnglish': englisharticle,
            'languageISO': ";".join(languageISO),
            'languageISO3': ";".join(languageISO3),
            'languageISO5': ";".join(languageISO5),
            'languageofficialnational': '',
            'languageofficialregional': '',
            'languageofficialsinglecountry': '',
            'nativeLabel': ";".join(nativeLabel),
            'numbercountriesOfficialorRegional':''
            })

            #print (extracted_languages)
            #input('common')
            Qitem = []; languagename = []; nativeLabel = []; languageISO = []; languageISO3 = []; languageISO5 = []; wikipedia = []; wikipedialanguagecode = [];

        Qitemcurrent = item['language']['value'].replace("http://www.wikidata.org/entity/","")
        if Qitemcurrent not in Qitem:
            Qitem.append(Qitemcurrent)
        languagenamecurrent = item['languageLabel']['value']
        if languagenamecurrent not in languagename: languagename.append(languagenamecurrent)

        try:
            nativeLabelcurrent = item['nativeLabel']['value'].lower()
            if nativeLabelcurrent not in nativeLabel: nativeLabel.append(nativeLabelcurrent)
        except:
            pass

        try: 
            aliascurrent = item['alias']['value'].lower()
#            print (aliascurrent)
            if aliascurrent not in nativeLabel and len(aliascurrent)>3: nativeLabel.append(aliascurrent)
        except:
            pass

        try: 
            languageISOcurrent = item['languageISO']['value']
            if languageISOcurrent not in languageISO: languageISO.append(languageISOcurrent)
        except:
            pass

        try:
            languageISO3current = item['languageISO3']['value']
            if languageISO3current not in languageISO3: languageISO3.append(languageISO3current)
        except:
            pass

        try:
            languageISO5current = item['languageISO5']['value']
            if languageISO5current not in languageISO5: languageISO5.append(languageISO5current)
        except:
            pass

        try: englisharticle = item['languagelink']['value'] 
        except: englisharticle = 'no link'
        wikimedialanguagecode = item['WikimediaLanguagecode']['value'] # si 
        wikipedia = item['itemLabel']['value']

        #print (result)
    df = pd.DataFrame(extracted_languages)
    df = df.set_index(['languagename'])
 
#    df = df.set_index(['WikimediaLanguagecode'])
    filename= 'Wikipedia_language_editions'
    newlanguages = []

    # CHECK IF THERE IS ANY NEW LANGUAGE
    if os.path.isfile(databases_path + filename+'.csv'): # FILE EXISTS: CREATE IT WITH THE NEW LANGUAGES IN THE FILENAME
        languages = pd.read_csv(databases_path + filename+'.csv',sep='\t')
        languages = languages.set_index(['languagename'])

        languagecodes_file = languages['WikimediaLanguagecode'].tolist(); languagecodes_file.append('nan')
        languagecodes_calculated = df['WikimediaLanguagecode'].tolist();
        newlanguages = list(set(languagecodes_calculated) - set(languagecodes_file))

        indexs = []
        for x in newlanguages: indexs = indexs + df.index[(df['WikimediaLanguagecode'] == x)].tolist()
        newlanguages = indexs

        if len(newlanguages)>0: 
            message = 'These are the new languages: '+', '.join(newlanguages)
            print (message)
            df=df.reindex(newlanguages)
            send_email_toolaccount('WCDO: New languages to introduce into the file.', message)
            print ('The new languages are in a file named: ')
            filename="_".join(newlanguages)+'_'+filename
            print (databases_path + filename+'.csv')
            print (df.head())
            df.to_csv(databases_path + filename+'.csv',sep='\t')
        else:
            print ('There are no new Wikipedia language editions.')

    else: # FILE DOES NOT EXIST: CREATE IT WITH THE WHOLE NAME
        df.to_csv(databases_path + filename+'.csv',sep='\t')

    return newlanguages


# Sends an e-mail
def send_email_toolaccount(subject, message): # https://wikitech.wikimedia.org/wiki/Help:Toolforge#Mail_from_Tools
    cmd = 'echo "Subject:'+subject+'\n\n'+message+'" | /usr/sbin/exim -odf -i tools.wcdo@tools.wmflabs.org'
    os.system(cmd)

# Finish e-mail
def finish_email(startTime, filename, title):
    try:
        sys.stdout=None; send_email_toolaccount(title + ' completed successfuly', open(filename, 'r').read())
    except Exception as err:
        print ('* Task aborted after: ' + str(datetime.timedelta(seconds=time.time() - startTime)))
        sys.stdout=None; send_email_toolaccount(title + ' aborted because of an error', open(filename, 'r').read()+'err')


def get_hexcolorrange(color1, color2, scale, value_min, value_max, actualvalue):   
    interval=int((value_max - value_min)/scale)
    index=int(actualvalue/interval)
    colors=list(colour.Color(color1).range_to(colour.Color(color2), scale))
    choosencolor = colors[index].hex
    return choosencolor