# -*- coding: utf-8 -*-
# This is the ROSETTA STONE of the project.

import time
import os
import sys
import codecs
import pycountry
import requests
from lxml import html
import babel.languages
from babel import Locale
import pandas as pd


if sys.stdout.encoding is None: sys.stdout = codecs.open("/dev/stdout", "w", 'utf-8')
#sys.stdout=open("test.txt","w")
startTime = time.time()

# INSTRUCTIONS: ****************************************************************************************************************
# First time: use it without arguments to create the language list.
# Second time: use it with the argument 'territories' to create the language territories mapping from the language list.
# Third and other times: use it without arguments to verify the language list is updated and create new files in case it is not.
# MAIN
def main():
	
	if sys.argv[1] == 'territories': extract_language_territories_unicode([]) # territories
	else: # languages and verify updates
		newlanguages = extract_wikipedia_languages(); 
		if len(newlanguages)>0: extract_language_territories_unicode(newlanguages)

# FIRST: LANGUAGES
# Obtain all languages with a Wikipedia language edition and their characteristics.
# Main Source: WIKIDATA.
# Additional sources: Pycountry and Babel libraries for countries and their languages. Source: UNICODE: unicode.org/cldr/charts/latest/supplemental/language_territory_information.html
def extract_wikipedia_languages():

	query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
	PREFIX wd: <http://www.wikidata.org/entity/>
	PREFIX wdt: <http://www.wikidata.org/prop/direct/>
	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

	SELECT DISTINCT ?itemLabel ?language ?languageLabel ?alias ?nativeLabel ?languageISO ?languageISO3 ?languageISO5 ?languagelink ?Wikimedialanguagecode WHERE {
	  ?item wdt:P31 wd:Q10876391.
	  ?item wdt:P407 ?language.
	  
	  OPTIONAL{?language wdt:P1705 ?nativeLabel.}
	  
#	  ?item wdt:P856 ?officialwebsite.
#	  ?item wdt:P1800 ?bbddwp.
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


def get_language_spread_unicode(languageISO, languageISO3, languageISO5):
	numbercountries=0
	languageofficialnational=[]
	languageofficialregional=[]
	languageofficialsinglecountry='no'

	if len(languageISO)>0 and languageISO[0]!='': languageCode=languageISO[0]
	elif len(languageISO3)>0 and languageISO3[0]!='': languageCode=languageISO3[0]
	elif len(languageISO5)>0 and languageISO5[0]!='': languageCode=languageISO5[0]
	else: languageCode=''

	#print (languageCode)

	for country in pycountry.countries:
#		for language in babel.languages.get_territory_language_info(country.alpha_2): # languages in the world countries
		#print (country)
		#print (languagesincountry)

		#countrylanguages=len(babel.languages.get_territory_language_info(country.alpha_2))
		languagesincountry=babel.languages.get_territory_language_info(country.alpha_2)
		languages={}

		for language in languagesincountry:
			position=language.find('_')
			if position != -1: newlanguage=language[0:position] 
			else: newlanguage=language
			languages[newlanguage]=languagesincountry[language]

		if languageCode in languages:
			if languages[languageCode]['official_status']=='official_regional':
				languageofficialregional.append(country.alpha_2)
				numbercountries=numbercountries+1
			if languages[languageCode]['official_status']=='official': 
				languageofficialnational.append(country.alpha_2)
				numbercountries=numbercountries+1

	if numbercountries==1 and languageofficialnational!='': languageofficialsinglecountry='yes' 
	numbercountries=str(numbercountries)

	return (numbercountries,languageofficialnational,languageofficialregional,languageofficialsinglecountry)


# SECOND: TERRITORIES
# Obtain a list the territories where the language is OFFICIAL (regional or national) or INDIGENOUS. 
# MAIN SOURCE: UNICODE (Babel) and WikiData.
# Around half of the Wikipedias' languages are no official at national or regional level in any country. (!)
# Territories are either countries (ISO 3166) or countrysubdivisions of countries (ISO 3166-2).
def extract_language_territories_unicode(languagelist):

	languages = pd.read_csv('Wikipedia_language_editions.csv',sep='\t')
	languages=languages[['WikimediaLanguagecode', 'languageISO', 'languageISO3', 'languagesISO5', 'Qitem']]
	languages = languages.set_index(['WikimediaLanguagecode'])

	if len(languagelist)==0:
		print ('No language list is passed. So all languages from the file are going to be used.')
		languagelist = list(languages.index.values)

	print (languagelist)

	# let's create the final Dataframe
	df = pd.DataFrame(columns=('WikimediaLanguagecode', 'languageISO', 'QitemTerritory', 'territoryname', 'territorynameNative', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous', 'officialnationalorregional','singlelanguageofficialcountry'))
	print (df)
	df = df.set_index(['territoryname'])

	# FIRST: GROUNDTRUTH. UNICODE.
	for language in languagelist:

		#print (language)
		#input('')

		languageISO = languages.loc[language,'languageISO']
		languageISO3 = languages.loc[language,'languageISO3']
		languageISO5 = languages.loc[language,'languageISO5']
		QitemLanguage = languages.loc[language,'Qitem']

		if languageISO != '': languageCode=languageISO 
		else: languageCode=languageISO3

#		input('tell me, baby')
		for country in pycountry.countries:
			#print (country)
			languagesincountry=babel.languages.get_territory_language_info(country.alpha_2)
			languagesnomisspellings={}

			for countrylanguage in languagesincountry: # for misspellings
				position=countrylanguage.find('_')
				if position != -1: newlanguage=countrylanguage[0:position]
				else: newlanguage=countrylanguage
				languagesnomisspellings[newlanguage]=languagesincountry[countrylanguage]

			if languageCode in languagesnomisspellings:
				#print (languagesnomisspellings)
				#print (languageCode)
				#print (('és dins al país ')+country.alpha_2)

				# PENDING FROM THE TWO IFS OR FROM OTHER SOURCES
				territoryname=''
				territorynameNative=''
				ISO3166=country.alpha_2
				ISO31662=''
				region='' # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
				countryname = Locale('EN').territories[country.alpha_2] # in English
				officialnationalorregional='no'
				singlelanguageofficialcountry='no'

				if languagesnomisspellings[languageCode]['official_status']=='official_regional': 
					territoryname = 'unknown'
					territorynameNative = 'unknown'
					region='unknown' # if it is official regionally, there is a region we do not know.
					officialnationalorregional='regional'
					ISO3166=country.alpha_2
					ISO31662='unknown'

				if languagesnomisspellings[languageCode]['official_status']=='official': 
					territoryname = Locale('EN').territories[country.alpha_2] # using babel
					try: territorynameNative = Locale(languageCode).territories[country.alpha_2]
					except: territorynameNative = ''
					region='' # if it is official nationally, there is no region.
					officialnationalorregional='national'
					count=0
					for x in languagesnomisspellings:
						if languagesnomisspellings[x]['official_status']=='official': count=count+1
					if count==1: singlelanguageofficialcountry='yes'
					ISO3166=country.alpha_2
					ISO31662='' # if it is official nationally, there is no region.

				# PENDING ALWAYS FROM OTHER SOURCES
				demonym = 'unknown' # WikiData
				demonymNative = 'unknown' # WikiData
				QitemTerritory='' # WikiData
				indigenous='' # WikiData/Ethnologue

				rowdataframe = pd.DataFrame([[language, languageISO, languageISO3, languageISO5, QitemLanguage, QitemTerritory, territoryname, territorynameNative, demonym, demonymNative, ISO3166, ISO31662, region, countryname, indigenous, officialnationalorregional, singlelanguageofficialcountry]], columns = ['WikimediaLanguagecode', 'languageISO', 'languageISO3', 'QitemLanguage', 'QitemTerritory', 'territoryname', 'territorynameNative', 'demonym', 'demonymNative', 'ISO3166', 'ISO31662', 'region','country', 'indigenous', 'officialnationalorregional','singlelanguageofficialcountry'])
				rowdataframe = rowdataframe.set_index(['territorynameNative'])
				print (rowdataframe)
				df = df.append(rowdataframe)
				#input('tell me, baby')

	df.to_csv('Wikipedia_language_territories_mapping.csv',sep='\t') # TO FILE
	# this file has the name: Wikipedia_language_territories_mapping.csv and needs to be verified and converted to Wikipedia_language_territories_mapping_quality.csv



# This gets the territories from a sentence using WikiData and pycountry databases.
def identify_territories_from_location_sentence(sentence,wikidatacountrysubdivisions,ISO3166):
#	print (sentence)
#	print (ISO3166)

#	input(' a ')
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
#		print (name)

		if name in sentence:
#			print ('BINGO: PYCOUNTRY')
#			print (sentence)
#			print (name)
#			print (subdivision)
			subdivisionsintext.append({
				'Qitem': '',
			    'territoryname': name,
			    'territorynameNative': '',
			    'demonym': '',
			    'demonymNative': '',
			    'ISO3166': ISO3166,
			    'ISO31662': subdivision.code,
			    'region':'yes', # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
			    'parentcode':subdivision.parent_code,
			    'type':subdivision.type,
			    'locationtext': sentence,
				})
#	print (subdivisionsintext)

	# WIKIDATA
	for subdivision in wikidatacountrysubdivisions:
		name=subdivision['itemlabelen']
		nameNative=subdivision['itemlabelNative']

#		print (nameNative)
#		print (name)

		for type in typeslist:
			name=name.replace(type,'').rstrip()
			nameNative=nameNative.replace(type,'').rstrip()

		if (name != '' and name in sentence) or (nameNative !='' and nameNative in sentence):
#			print ('BINGO 2: WIKIDATA')
#			print (sentence)
#			print (name)
#			print (nameNative)
#			print (subdivision)
			if subdivision['ISO31662']!='':	region='yes'
			else: region='no'
			subdivisionsintext.append({
			    'Qitem': subdivision['Qitem'],
			    'territoryname': name,
			    'territorynameNative': nameNative,
			    'demonym': subdivision['demonymen'],
			    'demonymNative': subdivision['demonymNative'],
			    'ISO3166': ISO3166,
			    'ISO31662': subdivision['ISO31662'],
			    'region':region,
			    'locationtext': sentence
				})
#	print (subdivisionsintext)

#	unite pycountry + WikiData info.
	for subdivision in subdivisionsintext:
		ISO31662 = subdivision['ISO31662']
		for secondsubdivision in subdivisionsintext:
			if secondsubdivision['ISO31662']==ISO31662 and secondsubdivision['Qitem']=='' and subdivision['Qitem']!='':
				subdivision['parentcode']=secondsubdivision['parentcode']
				subdivision['type']=secondsubdivision['type']
				subdivisionsintext.remove(secondsubdivision)
#	print (subdivisionsintext)
#	input(' b ')

	if len(subdivisionsintext)==0:
		print (('WARNING: We could not identify any territory in this sentence: ')+sentence+'\n')		
		subdivisionsintext.append({
	    'Qitem': '',
	    'territoryname': '',
	    'territorynameNative': '',
	    'demonym': '',
	    'demonymNative': '',
	    'ISO3166': ISO3166,
	    'ISO31662': '',
	    'region': 'yes', # We consider it a region when it does not have a ISO 3166. Certain regions that have ISO 3166 (usually colonies) have a 'no' in this attribute.
	    'locationtext': sentence
		})

	return subdivisionsintext


# This obtains the WikiData on a country subdivisions in a particular language.
def get_wikidata_wikidata_country_subdivisions(language,ISO3166):
	wikidatacountrysubdivisions = []

	# FIRST THE COUNTRY SUBDIVISIONS
	query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
	PREFIX wd: <http://www.wikidata.org/entity/>
	PREFIX wdt: <http://www.wikidata.org/prop/direct/>
	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

	SELECT ?item ?ISO31662 ?itemlabelen ?itemlabelNative ?demonymNative ?demonymen
	WHERE
	{
      ?country wdt:P31 wd:Q6256. 
      ?country wdt:P297 ?ISO3166.
      FILTER (?ISO3166 = "countrycode")
      
      ?item wdt:P17 ?country.
	  ?item wdt:P300 ?ISO31662.

	  OPTIONAL { ?item rdfs:label ?itemlabelen filter (lang(?itemlabelen) = "en"). }
	  OPTIONAL { ?item rdfs:label ?itemlabelNative filter (lang(?itemlabelNative) = "language"). }

	  OPTIONAL { ?item wdt:P1549 ?demonymNative;
	        FILTER(LANG(?demonymNative) = "language") }
	  OPTIONAL { ?item wdt:P1549 ?demonymen;
	        FILTER(LANG(?demonymen) = "en") }
	}'''

	query = query.replace('language',language)
	query = query.replace('countrycode',ISO3166)

	#print (query)
	url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
	data = requests.get(url, params={'query': query, 'format': 'json'}).json()

	#print (data)
	Qitem = ''; itemlabelNative = ''; itemlabelen = ''
	demonymen = []; demonymNative = [];

	#print ('COMENÇA SUBDIVISIONS')
	for item in data['results']['bindings']:	
		#print (item)
		#input('tell me')

		if Qitem != item['item']['value'] and Qitem!='':
			wikidatacountrysubdivisions.append({
	        'Qitem': Qitem,
	        'ISO3166': ISO3166,
	        'ISO31662': ISO31662,
	        'itemlabelen': itemlabelen,
	        'itemlabelNative': itemlabelNative,
	        'demonymNative': ";".join(demonymNative),
	        'demonymen': ";".join(demonymen),
	        })
			demonymen = []; demonymNative = [];
			itemlabelNative = ''
			itemlabelen = ''

			#print (wikidatacountrysubdivisions)

		Qitem = item['item']['value'].replace("http://www.wikidata.org/entity/","")
		ISO31662 = item['ISO31662']['value']

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
	  ?item wdt:P31 wd:Q6256. 
	  ?item wdt:P297 ?ISO3166.
	  FILTER (?ISO3166 = "countrycode")

	  OPTIONAL { ?item rdfs:label ?itemlabelen filter (lang(?itemlabelen) = "en"). }
	  OPTIONAL { ?item rdfs:label ?itemlabelNative filter (lang(?itemlabelNative) = "language"). }
	  OPTIONAL { ?item wdt:P1549 ?demonymen filter (lang(?demonymen) = "en"). }
	  OPTIONAL { ?item wdt:P1549 ?demonymNative filter (lang(?demonymNative) = "language"). }
	  
	}'''

	query = query.replace('language',language)
	query = query.replace('countrycode',ISO3166)
	#print (query)

	url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
	data = requests.get(url, params={'query': query, 'format': 'json'}).json()

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

	wikidatacountrysubdivisions.append({
    'Qitem': Qitem,
    'ISO3166': ISO3166,
    'ISO31662': '',
    'itemlabelen': itemlabelen,
    'itemlabelNative': itemlabelNative,
    'demonymNative': ";".join(demonymNative),
    'demonymen': ";".join(demonymen),
    })

	return (wikidatacountrysubdivisions)

if __name__ == '__main__':
	main()
	end = time.time()
	print ('job completed after: ' + str(end - startTime))