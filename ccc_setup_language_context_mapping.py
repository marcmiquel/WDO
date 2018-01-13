import time
import os
import MySQLdb as mdb
import sys
import codecs
import datetime
import urllib
import numpy
import copy
import re
import gc
import requests
import pandas as pd


if sys.stdout.encoding is None:
    sys.stdout = codecs.open("/dev/stdout", "w", 'utf-8')

startTime = time.time()
data_dir = 'data_folder/'

def main():

	# a la primera vegada, fer-ho amb arguments per anar llançant cada funció.
	# si no hi ha arguments. faria la verificació. enviaria el mail.
	# treuria els tres arxius amb un _added.

    if len(sys.argv):
		for sys.argv[0] == 'languages':
			extract_wikipedia_languages()
		for sys.argv[0] == 'territories':
			extract_territories()
		for sys.argv[0] == 'territories_rich':
			languageterritoriesmapping_rich()
	else: 


		verify_updated_mapping_file() 



# FIRST: LANGUAGES
# SOURCE: WIKIDATA

def extract_wikipedia_languages(): # no arguments
	# extracció arxiu json
	# verificació qualitativa de que tot sigui correcta i complementació


	query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
	PREFIX wd: <http://www.wikidata.org/entity/>
	PREFIX wdt: <http://www.wikidata.org/prop/direct/>
	PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

	SELECT ?item ?itemLabel ?Wikimedialanguagecode ?language ?languageLabel ?nativelabel ?languageISO ?languageISO3 ?officialwebsite ?bbddwp 
	WHERE 
	{
	  ?item wdt:P31 wd:Q10876391.
	  ?item wdt:P407 ?language.
	  
	  OPTIONAL{?language wdt:P1705 ?nativelabel.}
	  
	  
	  ?item wdt:P856 ?officialwebsite.
	  ?item wdt:P1800 ?bbddwp.
	  ?item wdt:P424 ?Wikimedialanguagecode.
	  
	  OPTIONAL{?language wdt:P218 ?languageISO .}
	  OPTIONAL{?language wdt:P220 ?languageISO3 .}
	  
	  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
	}
	}'''

	url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
	data = requests.get(url, params={'query': query, 'format': 'json'}).json()

	presidents = []
	for item in data['results']['bindings']:
	    presidents.append({
	        'name': item['president']['value'],
	        'cause of death': item['cause']['value'],
	        'date of birth': item['dob']['value'],
	        'date of death': item['dod']['value']})

	df = pd.DataFrame(presidents)

	df.head()

	df.dtypes
	df['date of birth'] = pd.to_datetime(df['date of birth'])
	df['date of death'] = pd.to_datetime(df['date of death'])
	df.sort(['date of birth', 'date of death'])
	df = df[df['date of birth'] != '1743-04-02']


# una llengua ha de tenir: Q, nom en anglès, nom en natiu, ISO 639, nom de Viquipèdia, web de Viquipèdia, base de dades de WP, Wikimedia language code.

# si no té nom en natiu, podem utilitzar babel.
                    #locale = babel.Locale.parse(langucode)
                    #languagename = locale.get_display_name()

# si no tingués nom en anglès, podríem utilitzar pycountry.languages.get(ISO639_1_code=langucode).name


	# OUTPUT: languageterritoriesmapping_rich_languages.json -> languages rich

"""
per tenir el nom de la llengua en natiu
babel
utilitzar el label en la pròpia llengua i els 'also known as'.
http://babel.pocoo.org/en/latest/api/core.html#basic-interfacehttp://babel.pocoo.org/en/latest/
http://babel.pocoo.org/en/latest/
https://github.com/mledoze/countries



language status
un paràmetre per llengua:
és oficial_en_un_país.
és single_territory

és com un flag d'alerta.




més paràametres d'una llengua:
- ISO
- noms en original












"""

	return # torna una llista de llengües



# SECOND: TERRITORIES
# Obtain a list the territories where the language is indigenous to or official. Searches for the language names and territories associated to each Wikipedia language edition and creates a rich dataset.
def extract_territories():
	# extracció dels territoris de l'arxiu anterior
	# creació json per verificació qualitativa

	# llista de subregions per cada llengua (amb el countries.json de https://github.com/mledoze/countries)

	# http://www.unicode.org/cldr/charts/latest/supplemental/territory_language_information.html

	# OUTPUT: languageterritoriesmapping_rich_territories.json -> territories
	return

"""
els territoris on és oficial o on és indigena.

Ethnologue
el territori on la llengua tingui: 
status offici == 1 OR
other comments != 'non-indigenous'








territoris - llengües

països
first-level administrative country subdivision

podem buscar:

- relació de territoris (subdivisions) en els quals la llengua consta (P37) com a oficial / funciona amb el català però en d'altres no perquè no és llengua oficial de l'estat sencer
  ?item wdt:P31/wdt:P279* wd:Q10864048.
  ?item wdt:P37 wd:Q7026.

- relació de territoris on es troba "located in the administrative territorial entity" (P131) la llengua. / funciona amb el català però amb d'altres llengües no perquè no hi consten els territoris a la seva pàgina de wikidata
   wd:Q7026 wdt:P131 ?item .

- relació de països (P17) que consten dins de la pàgina de llengua. / no funciona amb el rus, p.e., que diu que és als Estats Units o Canadà... no és això.
   wd:Q7737 wdt:P17 ?item .

- relació de països (o subclasses) en els quals la llengua consta (P37) com a oficial / aquest és un exemple amb el rus. 
  ?item wdt:P31/wdt:P279* wd:Q6256.
  ?item wdt:P37 wd:Q7737.

el Tarantino no apareix ni com a llengua als items de Puglia, Basilicata, etc. ni en l'item del Tarantino hi apareix on es parla.

sembla que ho hauré de fer manualment i basar-me en el "location map image" P242 o la pàgina de la pròpia llengua en la llengua nativa.

treure-ho tot en arxius diferents i utilitzar-ho per fer la taula qualitativa. 



	SELECT ?item ?language ?languageLabel ?territoris ?territorisLabel
    WHERE 
	{
	  ?item wdt:P31 wd:Q10876391.
	  ?item wdt:P407 ?language.
      
      ?territoris wdt:P37 ?language.
#      ?territoris wdt:P31 wd:Q3624078.
	 	  
	 	  
	  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
	}

de political territorial entity que té com a llengua oficial...


"""




"""
            string = 'afwiki,NA,ZA\n' \
                     'arwiki,DZ,BH,TD,KM,DJ,EG,ER,IQ,IL,JO,KW,LB,LY,MR,MA,OM,QA,PS,SA,SO,SD,SY,TN,AE,YE,EH\n' \
                     'cawiki,ES-CT,ES-IB,AD,ES-VC\n' \
                     'cebwiki,PH-CEB,PH-BOH,PH-NEC,PH-MAS,PH-BIL,PH-EAS,PH-LEY,PH-NSA,PH-WSA,PH-GUI,PH-CAM,PH-MAD,PH-SLU,PH-TAW\n' \
                     'cswiki,CZ\n' \
                     'dawiki,DK,FO\n' \
                     'dewiki,DE,AT,LI,LU,CH-AG,CH-AR,CH-AI,CH-BL,CH-BS,CH-GL,CH-LU,CH-NW,CH-OW,CH-SH,CH-SZ,CH-SO,CH-SG,CH-TG,CH-UR,CH-ZG,CH-ZH,CH-BE,CH-FR,CH-VS\n' \
                     'elwiki,GR,CY\n' \
                     'enwiki,GB,AU,US,NZ,CA,IN,IE,SZ,ZM,VU,TO,TV,TZ,SD,SS,ZA,SB,SL,SC,WS,LC,RW,PH,PG,PW,NU,NR,MU,MH,MT,MW,LR,KI,GM,FJ,FM,ER,CM,BW,PK,ZW,UG,TT,VC,KN,NG,NA,LS,KE,JM,GY,GD,GH,DM,CK,BZ,BB,BS,AG,MY,BN,BD,PR,TK,CC,SH,MS,GG,IO,AS,AI,BM,VG,KY,CX,FK,GI,GU,HK,IM,JE,MP,NF,SX,MP,PN,VI,TC\n' \
                     'eswiki,ES,VE,UY,PE,PY,PA,NI,HN,GT,GQ,SV,EC,DO,CL,CU,CR,CO,BO,AR,MX\n' \
                     'etwiki,EE\n' \
                     'euwiki,ES-PV,ES-NC\n' \
                     'fawiki,IR,AF,TJ\n' \
                     'fiwiki,FI\n' \
                     'frwiki,FR,BE-WAL,BE-BRU,BJ,BF,BI,CM,CA-QC,CA-ON,CA-NB,CA-MB,CF,TD,KM,CI,CD,DJ,GQ,GA,GN,HT,LU,MG,ML,MC,NE,CG,RW,SN,SC,VU,TG,CH-GE,CH-VD,CH-NE,CH-JU,CH-BE,CH-FR,CH-VS\n' \
                     'gnwiki,BO,AR-W,PY,BR\n' \
                     'hewiki,IL\n' \
                     'huwiki,HU,RS-VO\n' \
                     'idwiki,ID\n' \
                     'iswiki,IS\n' \
                     'itwiki,IT,CH-TI,VA,SM\n' \
                     'jawiki,JP\n' \
                     'kowiki,KR,KP\n' \
                     'mkwiki,MK\n' \
                     'mswiki,MY,BN,SG,CC,CC,ID\n' \
                     'newiki,NP\n' \
                     'nlwiki,NL,BE-BRU,BE-VLG,SR,SX,CW,AW\n' \
                     'nowiki,NO\n' \
                     'plwiki,PL\n' \
                     'ptwiki,PT,AO,BR,CV,TL,GW,GQ,MZ,ST\n' \
                     'rowiki,RO,MD,RS-VO\n' \
                     'ruwiki,RU,BY,KZ,KG,GE-AB,UA-43\n' \
                     'srwiki,RS,BA,XK\n' \
                     'svwiki,SE,FI-01\n' \
                     'swwiki,TZ,KE,UG\n' \
                     'trwiki,TR,CY\n' \
                     'ukwiki,UA\n' \
                     'viwiki,VN\n' \
                     'warwiki,PH-EAS,PH-NSA,PH-WSA,PH-BIL,PH-LEY,PH-MAS,PH-SOR\n' \
                     'zhwiki,CN,MO,HK,TW,SG\n'

"""



# THIRD PHASE: TERRITORIES RICH
# Create a dataset .json file named "Language-Context Mapping” with the equivalences of keywords (demonym, territory name), Wikidata Items (Q) and ISO codes per language and territory.
# SOURCE: 

"""
a l'inici del mètode cal definir les quatre sources d'information:

territories official language status
a) babel/unicode
http://www.unicode.org/cldr/charts/latest/supplemental/language_territory_information.html

territories 'regional'
* ethnologue https://en.wikipedia.org/wiki/Ethnologue
* wikipedia language pages
* unesco
http://www.unesco.org/languages-atlas/index.php# no és complert pels territoris
* wikidata: a nivell temptatiu.


territories rich: iso, demonym
* wikidata
* countries subdivisions (iso)
https://gist.github.com/mindplay-dk/4755200
https://pypi.python.org/pypi/pycountry
https://github.com/olahol/iso-3166-2.js/blob/master/data.csv

* countries: https://github.com/mledoze/countries


"""








def languageterritoriesmapping_rich():
# extracció json final 

# Aquí el què falta és la relació de territoris amb ISO, nom original, nom en anglès i demonym.
# El demonym s'ha de treure de WikiData (preguntar al país). 
# El demonym es pot treure d'aquí en anglès i després traduïr-lo amb WikiData: https://github.com/mledoze/countries

# 2 files
# languages file
# language_territories file

#	* pycountry ens permet anar de ISO 3166-2 a nom de subdivisió en natiu.
#	* pycountry (arxiu countries.csv) ens permet anar d'ISO 3166 a nom de país en natiu.

# territory:
# english_name.
# native_name.
# iso.
# demonyms
# status = official/not official.
# indigenous = yes/no.
# area = country/region

 

	# OUTPUT: languageterritoriesmapping_rich.json -> final.
	# [cawiki;Qcode,langname1,lang_ISO_639;Qcode,territori1name,territori1_ISO_3166-2,territori1_demonym1,territori1_demonim2;territori2name,territori2_ISO_3166-2,territory2_demonym;...] 
	# En el cas de la catalana és 639-1, en el cas de la veneciana és 639-3. La prioritat és el 639-1, si no hi és, tirar pel següent.
	return





# Checks whether there is a new Wikipedia language edition that is not in the dataset and includes it.
def verify_new_languages():
	# crida les funcions amb el paràmetre (verify) que fa que treguin l'arxiu amb _added. 
	# la primera funció (extract_wikipedia_languages) hauria de fer la comprovació de l'arxiu existent languageterritoriesmapping_rich.json i el resultat de la consulta WikiData.

	extract_wikipedia_languages()
	extract_territories()
	languageterritoriesmapping_rich()

	# fa cerca a wikidata de llengües i comprova si n'hi ha de noves. si n'hi ha de noves: m'envia un e-mail.
	
	# treuria els tres arxius només per les llengües noves i amb un _added.



	return



def create_mysql_connection(lang):
    try:
        mysql_cur.execute(query,args)
    except:
#        mysql_con = mdb.connect(lang + '.labsdb', 'p50380g50517', 'aiyiangahthiefay', lang + '_p')
        mysql_con = mdb.connect(host=".labsdb",db=lang + '_p',read_default_file="~/.my.cnf")
        mysql_cur = mysql_con.cursor()
#        print 'reconnect.'
        mysql_cur.execute(query,args)
#        print 'queried reconnected.'
    return mysql_cur



if __name__ == '__main__':	
	main()
	
	end = time.time()
	print 'job completed after: ' + str(end - startTime)
