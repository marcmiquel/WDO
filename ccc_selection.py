filimport time
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

if sys.stdout.encoding is None:
    sys.stdout = codecs.open("/dev/stdout", "w", 'utf-8')
    
startTime = time.time()
data_dir = 'data_folder/'


def main():


	languages = load_languagecontextmapping()
	for lang in languages:
		total = is_total(lang)
		create_CCC(lang,total)
		print ''



# DATABASE AND DATASETS MAINTENANCE FUNCTIONS
#############################################

# Check whether the current CCC selection should be incremental or total.
def is_total(lang):
	# every 10 incremental datasets, there should be a total.
	# if they have no have no database yet, it should be a total.


    """
Cada 10 setmanes fes una CCC total. Com puc saber el nombre d’incrementals? 
Comptant el nombre d’incrementals i quan sigui divisible per 10 sense residu.
Fem que el codi s’executa cada setmana des del dia 1 de l’any.

    """
	return # boolean


# Loads languagecontextmapping.json file
def load_languagecontextmapping():
	return 

# Creates a CCC database for a list of languages.
def create_database(): # se li envia una llista de llengües
	# Una taula per wiki: 
    # Qitem, article_id, article_title, CCC (binari), date_created, keyword on title, keywords category crawling distance, geolocation, number of wikidata properties, number of languages exist, number of inlinks, % inlinks from CCC, number of outlinks, % outlinks from CCC, ccc_composite_score...



    """

I si CCC ho faig sempre per cada territori? no... no sortiria bé al wikidata ni amb les categories...
tindríem articles amb dos territoris. potser no és negatiu això...és una manera de donar-li importància a un article.

potser apareixeria amb 'categories', geolocalització, keywords, wikidata lloc, wikidata....

caldria posar-los dins d'un camp amb el wikidata del territori (Q123, Q456, Q123...).
caldria posar un camp extra: número de camins.

es farien més iteracions en cadascuna de les estratègies. a cada article es faria una consulta i insert?
https://stackoverflow.com/questions/3765631/how-can-i-append-a-string-to-an-existing-field-in-mysql

potser sí que seria bo fer-ho per cada territori, així tindria sentit per fer les quotes dels prioritaris.

els territoris no han d'englobar-se els uns als altres.




    """









    query = 'DROP table u3532__.cira_'+lang
    mysql_cur.execute(query)

    query = 'CREATE table u3532__.cira_'+lang+' (page_id int(10) unsigned PRIMARY KEY,  page_title varbinary(255))'
    mysql_cur.execute(query)

    for element in selectedArticles.keys():
        sql = 'INSERT INTO u3532__.cira_'+lang+'(page_id,page_title) VALUES ('+selectedArticles[element]+', '+element+')'
        mysql_cur.execute(query)




# o bé faig la iteració dels territoris fora i passo el territori per l'argument, o la faig a dins.



# Drop the CCC database.
def drop_database(): # se li envia una llista de llengües


def create_mysql_connection(lang):
    try:
        mysql_cur.execute(query,args)
    except:
#        mysql_con = mdb.connect(lang + '.labsdb', 'p50380g50517', 'aiyiangahthiefay', lang + '_p')
#	    mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
        mysql_con = mdb.connect(host=".labsdb",db=lang + '_p',read_default_file="~/.my.cnf")

        mysql_cur = mysql_con.cursor()
#        print 'reconnect.'
        mysql_cur.execute(query,args)
#        print 'queried reconnected.'
    return mysql_cur


# Creates a dataset from the CCC database for a list of languages.
def extract_dataset_file(lang):
	return
	# col·locar els datasets en una carpeta (no sé si organtizar-ho per llengua o per data)




# ARTICLE RETRIEVING FUNCTIONS
##############################

# STRATEGY 1: GEOLOCATION
# Obtain for each language edition the articles with coordinates or which are in the territories in that language (groundtruth).
def get_articles_geolocated(lang,total):
	connection = create_mysql_connection(lang)
	# cal mecanismes per si cal tornar a començar l'script, que ho faci des d'on era i no des del principi.
	# cada llengua, que els posi primer en un arxiu i després que els posi a la bbdd finalment. després s'esborra l'arxiu.
	# comprova que siguin a la base de dades i no hi hagi l'arxiu. 
	# en cas positiu. return. surt de la funció. 
	# en cas negatiu. segueix endavant.

	# el mètode ha de fer servir les estratègies anteriors i la de WikiData. ara enganxo les anteriors.
    # Llista d’articles amb coordenades per països-regió 3166-2 i Selecció d'Articles amb Coordenades Locals (a) en un .csv (sense redirects) amb aquestes dades. MediaWiki DB (geo_tags, external links to GeoHack) / GeoData / cira_selection.py / attribute [m]== “coords” (cawiki_coords_articles.csv)
    selectedArticlesCoords = {}

    input_iso_file = data_dir + 'source/cira_iso3166.csv'
    input_iso_file = open(input_iso_file, 'r')
    for line in input_iso_file:
        page_data = line.strip('\n').split(',')
        if page_data[0] == lang: break

    # 1) GEO_TAGS BBDD
    query = 'SELECT DISTINCT(page_title), gt_page_id, gt_country, gt_region FROM page INNER JOIN geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 AND'
    for item in range(1,len(page_data)):
        if item != 1: query = query + ' OR '
        if len(page_data[item]) == 5 or len(page_data[item]) == 6:
            codis = page_data[item].split('-')
            query = query + '(gt_country="'+codis[0]+'" AND gt_region="'+codis[1]+'")'
        else: query = query + '(gt_country="'+str(page_data[item])+'")'
    print (query)
    mysql_cur.execute(query)
    result = mysql_cur.fetchall()
    for row in result:
        if not selectedArticlesCoords.has_key(str(row[0])): selectedArticlesCoords[str(row[0])] = str(row[1])

    l = 'El número d\'articles amb coordenades trobats pels territoris d\'aquesta llengua utilitzant els codis ISO a la seva bbbdd original són: '+str(len(selectedArticlesCoords))+'\n'; print (l)

    # 2) OTHERWIKIS BBDD
    articlesaltresbbdd = []
    for language in lang_list:
        language = language.replace('-', '_')
        query = 'SELECT DISTINCT(ll_title) FROM '+language+'wiki_p.geo_tags INNER JOIN '+language+'wiki_p.langlinks ON gt_page_id=ll_from INNER JOIN '+language+'wiki_p.page ON gt_page_id=page_id WHERE page_namespace=0 AND page_is_redirect=0 AND ll_lang="'+lang[0:len(lang)-4]+'" AND ('
        for item in range(1,len(page_data)):
            if item != 1: query = query + ' OR '
            if len(page_data[item]) == 5 or len(page_data[item]) == 6:
                codis = page_data[item].split('-')
                query = query + '(gt_country="'+codis[0]+'" AND gt_region="'+codis[1]+'")'
            else: query = query + '(gt_country="'+str(page_data[item])+'")'
        query = query + ')'
        # print query
        mysql_cur.execute(query)
        result = mysql_cur.fetchall()
        for row in result:
            titol = str(row[0])
            titol = titol.replace(' ', '_')
            if not selectedArticlesCoords.has_key(titol):
                articlesaltresbbdd.append(titol)
                #print 'en llengua '+language+' hem trobat l\'article '+str(row[0])+' amb bones coordenades que no estava ben geolocalitzat a l\'original '+lang
    page_asstring = ','.join( ['%s'] * len( articlesaltresbbdd ) )
    query = 'SELECT page_title, page_id FROM page WHERE page_title IN (%s) AND page_namespace=0 AND page_is_redirect=0' % page_asstring
    mysql_cur.execute(query, tuple(articlesaltresbbdd))
    result = mysql_cur.fetchall()
    for row in result:
        selectedArticlesCoords[str(row[0])] = str(row[1])
    l = 'El número d\'articles amb coordenades trobats pels territoris d\'aquesta llengua utilitzant els codis ISO amb bbdd alternatives i original són: '+str(len(selectedArticlesCoords))+'\n'; print (l)


    # KOLOSSOS BBDD
#            query = 'SELECT page_title, page_id FROM '+lang+'_p.page WHERE page_namespace=0 AND page_is_redirect=0 AND page_id IN (SELECT DISTINCT(gc_from) FROM p50380g50921__ghel_p.coord_'+lang+' WHERE '
#            for item in range(1,len(page_data)):
#                if item != 1: query = query + ' OR '
#                query = query + 'gc_region="'+page_data[item]+'"'
#            query = query + ')'
    # print query
#            try:
#                mysql_cur.execute(query)
#                result = mysql_cur.fetchall()
#                for row in result:
#                    if not selectedArticlesCoords.has_key(str(row[0])):
#                        # print 'a la bbdd del Kolossos hem trobat l\'article '+str(row[0])+' amb bones coordenades que no estava ben geolocalitzat a l\'original '+lang
#                        selectedArticlesCoords[str(row[0])] = str(row[1])
#            except mysql_cur.Error as err:
#              print("Something went wrong: {}".format(err))

#            print 'El número d\'articles amb coordenades total trobats tenint en compte la bbdd del Kolossos són: '+str(len(selectedArticlesCoords))+'\n'

    # AQUÍ EL QUÈ HAURÍEM DE FER ÉS UTILITZAR UN DETECTOR DE COORDENADES I ELIMINAR AQUELLS QUE REALMENT ESTAN FORA (GEO_DATA?) O BÉ HAVER AGAFAT DE TOTS LES LAT-LONG I HAVER VERIFICAT PER FER LA SELECCIÓ.
    output_file_coords_name = data_dir + lang + '_CIRA_coords_articles_ISO.csv'
    output_file_coords = codecs.open(output_file_coords_name, 'w', 'UTF-8')

    for key in selectedArticlesCoords.keys():
        #print key+'\t'+selectedArticlesCoords[key]+'\n'
        output_file_coords.write(key+'\t'+selectedArticlesCoords[key]+'\n')
    output_file_coords.close()


#        if attribute == "coordsreverse":

    input_subdivisions_file = data_dir + 'source/subdivisions.csv'
    input_subdiv_file = open(input_subdivisions_file, 'r')
    subdivisions = {}
    for line in input_subdiv_file:
        info = line.strip('\n').split(',')
        subdivisions[info[0]] = info[1]

    input_iso_file = data_dir + 'source/cira_iso3166.csv'
    input_iso_file = open(input_iso_file, 'r')
    for line in input_iso_file:
        codisISO = line.strip('\n').split(',')
        if codisISO[0] == lang: break
#            print codisISO
    selectedArticlesCoordsreverse = {}



    # COORDENADES REVERSE GEOCODING AL GEOTAGS BBDD DE LA PRÒPIA LLENGUA
    titolsenoriginal = []
    titols_coords = {}
    titols_country = {}
    lang_list2 = ["en", "sv", "nl", "de", "fr"]
    lang_list2.append(lang[0:len(lang)-4])
    articles_coordenades = {}

    for language in lang_list2:
        #print 'Ara toca la llengua tal per mirar les coordenades i fer l\'inversa: '+lang+'\n'
        selectedArticlesCoordsintheLang = {}
        language = language.replace('-', '_')
#                print '\n'+language+'\n'
        query = 'SELECT page_title, gt_page_id, gt_lat, gt_lon FROM '+language+'wiki_p.page INNER JOIN '+language+'wiki_p.geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_title'
        mysql_cur.execute(query)
        result = mysql_cur.fetchall()

        articles_title_id = {}

        llistacoordenades = [];
        llistatitols = [];

        for row in result:
            articles_title_id[row[0]]=row[1]
            llistatitols.append(row[0])
            llistacoordenades.append((row[2],row[3]))
            articles_coordenades[str(row[0])]=str(row[2])+'\t'+str(row[3])
            # print str(row[0])+'\t'+str(row[2])+','+str(row[3])

        l ='Per la llengua '+(language)+' hi ha tants articles geolocalitzats: '+str(len(result)); print (l)

        if llistacoordenades :
            results = rg.search(llistacoordenades)  # REVERSE GEOCODING  # REVERSE GEOCODING  # REVERSE GEOCODING  # REVERSE GEOCODING  # REVERSE GEOCODING

            for index in range(len(results)):
                cc = results[index]['cc']
                if cc in codisISO:
#                            print llistatitols[index]
                    if not selectedArticlesCoordsintheLang.has_key(str(row[0])): selectedArticlesCoordsintheLang[llistatitols[index]] = articles_title_id[llistatitols[index]]
                else:
                    territori = results[index]['admin1']
                    if territori in subdivisions:
                        cc2 = cc + '-' + subdivisions[territori]
                        if cc2 in codisISO:
#                                    print llistatitols[index]
                            if not selectedArticlesCoordsintheLang.has_key(str(row[0])): selectedArticlesCoordsintheLang[llistatitols[index]] = articles_title_id[llistatitols[index]]

            l ='Per la llengua '+language+' hi ha tants articles geolocalitzats que cauen a l\'àmbit lingüístic de la llengua original: '+str(len(selectedArticlesCoordsintheLang)); print (l)

            if selectedArticlesCoordsintheLang:
                if language+'wiki' == lang:
                    l = 'Per la llengua pròpia '+language+'wiki hi ha tants articles geolocalitzats que cauen a l\'àmbit lingüístic de la llengua original: '+str(len(selectedArticlesCoordsintheLang)); print (l)
                    mysql_con.ping(True)
                    for key in selectedArticlesCoordsintheLang:
                        if key not in titolsenoriginal:
                            titolsenoriginal.append(key)
                            titols_coords[key]=articles_coordenades[key]


                # CALDRIA MODIFICAR AQUESTA CONSULTA I POSAR-HO EN UN DICCIONARI.
                page_asstring = ','.join( ['%s'] * len( list(selectedArticlesCoordsintheLang.keys()) ) )

                query = 'SELECT DISTINCT(ll_title), gt_lat, gt_lon, page_title FROM '+language+'wiki_p.geo_tags INNER JOIN '+language+'wiki_p.langlinks ON gt_page_id=ll_from INNER JOIN '+language+'wiki_p.page ON gt_page_id=page_id WHERE ll_lang="'+lang[0:len(lang)-4]+ '" AND page_title IN (%s) GROUP BY page_title' % page_asstring
                queryreconnect(mysql_cur,query,tuple(selectedArticlesCoordsintheLang.keys()))

                result = mysql_cur.fetchall()
                for row in result:
                    titol = str(row[0])
                    titol = titol.replace(' ', '_')
                    coords = str(row[1])+','+str(row[2])
#                            print titol, coords
                    if titol not in titolsenoriginal:
                        titolsenoriginal.append(titol)
                        titols_coords[titol] = articles_coordenades[row[3]]
#                                print titol,str(row[3]),titols_coords[titol]

#                        print 'L\'àmbit lingüístic de la llengua ja suma la següent quantitat d\'articles geolocalitzats: '+str(len(titolsenoriginal))

    if len(titolsenoriginal)!=0:
        page_asstring = ','.join( ['%s'] * len( titolsenoriginal ) )
        query = 'SELECT page_title, page_id FROM page WHERE page_title IN (%s) AND page_namespace=0 AND page_is_redirect=0' % page_asstring
        mysql_cur.execute(query, tuple(titolsenoriginal))
        result = mysql_cur.fetchall()
        for row in result:
            selectedArticlesCoordsreverse[str(row[0])] = str(row[1])

    l ='El número total d\'articles amb coordenades reverse geocoding trobats pels territoris d\'aquesta llengua a totes les bbdd de la Viquipèdia són: '+str(len(selectedArticlesCoordsreverse))+'\n'; print (l)

    output_file_coords_name = data_dir + lang + '_CIRA_coords_articles.csv'
    output_file_coords = codecs.open(output_file_coords_name, 'w', 'UTF-8')

    for key in selectedArticlesCoordsreverse.keys():
        #print key+'\t'+str(selectedArticlesCoordsreverse[key])+'\t'+titols_coords[key]+'\n'
        output_file_coords.write(key+'\t'+str(selectedArticlesCoordsreverse[key])+'\t'+titols_coords[key]+'\n')

    output_file_coords.close()

    coincidents = set(selectedArticlesCoords.keys()).intersection(set(selectedArticlesCoordsreverse.keys()))
    l = 'El número de coincidents entre el grup aconseguit amb els codis ISO i el reverse geocoding és: '+str(len(coincidents)); print (l)

    #nocoincidents = {}
    #for key in selectedArticlesCoords.keys():
        #if key not in coincidents: nocoincidents[key] = selectedArticlesCoords[key]

    #for key in nocoincidents.keys():
        #print key+'\t'+selectedArticlesCoords[key]+'\n'
     #   output_file_coords.write(key+'\t'+str(nocoincidents[key])+'\n')


     # AQUÍ ENS FALTA LA NOVA ESTRATÈGIA DE WIKIDATA PER GEOLOCATION


"""
    get_articles_geolocated # Obtain for each language edition the articles with coordinates or which are in the territories in that language (groundtruth).
    aaa

    https://www.diffnow.com/
    amb articles_coord i coord data
    mirar a veure exactament a què corresponia cada cosa.
    He de comprovar si els articles coords que surten a WikiData són més o menys que els obtinguts amb altres mètodes

    obtain articles with coordinates

    SELECT DISTINCT ?item ?itemLabel ?subProperties
    WHERE { 
      ?item wdt:P625 ?coordinates.
      ?subProperties wdt:P1647* wd:P276.
      ?subProperties wikibase:directClaim ?claim .
      ?item ?claim wd:Q15950 .
      
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
           
    }
    LIMIT 10



    SELECT DISTINCT ?item ?itemLabel ?coordinates
    WHERE { 
      ?item wdt:P625 ?coordinates.
      
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
           
    }
    LIMIT 100


    SELECT ?item ?itemLabel ?coordinates
    WHERE { 
      ?item wdt:P625 ?coordinates.
      ?languagelink schema:about ?item.
      ?languagelink schema:inLanguage "ca". 
      ?languagelink schema:isPartOf <https://ca.wikipedia.org/>

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
           
    }
    LIMIT 100

    SELECT ?item ?itemLabel ?coordinates ?locatedLabel ?languagelink
    WHERE { 
      ?item wdt:P625 ?coordinates.
      ?item wdt:P276 ?located.
      
      ?languagelink schema:about ?item.
      ?languagelink schema:inLanguage "sw". 
      ?languagelink schema:isPartOf <https://sw.wikipedia.org/>

    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
           
    }
    LIMIT 100

"""




	return

# STRATEGY 2: KEYWORDS
# Obtain for each language edition the articles with a keyword in title (groundtruth).
def get_articles_keywords(lang,total):
	connection = create_mysql_connection(lang)
	# cal mecanismes per si cal tornar a començar l'script, que ho faci des d'on era i no des del principi.
	# cada llengua, que els posi primer en un arxiu i després que els posi a la bbdd finalment. després s'esborra l'arxiu.
	# comprova que siguin a la base de dades i no hi hagi l'arxiu. 
	# en cas positiu. return. surt de la funció. 
	# en cas negatiu. segueix endavant.



    # ARTICLES BY WORDS
    # level 0
    for keyword in wordlist:
        mysql_cur.execute(
            'SELECT page_id, page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND CONVERT(page_title USING utf8mb4) COLLATE utf8mb4_general_ci LIKE "%' + keyword + '%"  ORDER BY page_id')
            # AND page_is_redirect=0')  # LIMIT %d') %bucketsize  # ,%d;') %(start,bucket)  #LIMIT 100  #AND page_title IN (%s)' % page_titles_asstring, page_titles)
        # print "Número d'articles trobats per la paraula: " + wordlist[i];

        result = 1
        while result:
            result = mysql_cur.fetchall()
            # nivell, títol, id
            for row in result:
                if not selectedArticles.has_key(row[1]):
                    if attribute == 'keywords': output_file1.write(str(level) + '\t' + row[1] + '\t' + str(row[0]) + '\n')
                    selectedArticles[row[1]] = row[0]
	return

"""
get_articles_keywords # Obtain for each language edition the articles with a keyword in title (groundtruth).
He d'anar en compte amb les pàgines de desambiguació:
https://en.wikipedia.org/wiki/Barbara_Harris
"""


# STRATEGY 3: WIKIDATA PROPERTIES
# Obtain for each language with a Wikipedia language edition the articles whose WikiData items have properties linked to territories and language names (groundtruth).
def get_articles_Wikidata(lang,total):
	connection = create_mysql_connection(lang)
	# cal mecanismes per si cal tornar a començar l'script, que ho faci des d'on era i no des del principi.
	# cada llengua, que els posi primer en un arxiu i després que els posi a la bbdd finalment. després s'esborra l'arxiu.
	# comprova que siguin a la base de dades i no hi hagi l'arxiu. 
	# en cas positiu. return. surt de la funció. 
	# en cas negatiu. segueix endavant.


"""

get_articles_Wikidata # Obtain for each language with a Wikipedia language edition the articles whose WikiData items have properties linked to territories and language names (groundtruth).

Primer: Llista d’items que són la llengua i territoris.

He d'aprendre a fer funcionar Wikidata a fons abans de tornar a començar
https://www.w3.org/TR/rdf-sparql-query/
https://www.wikidata.org/wiki/Wikidata:SPARQL_tutorial
https://en.wikibooks.org/wiki/Category:Book:SPARQL
https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/queries/examples
https://en.wikibooks.org/wiki/SPARQL/WIKIDATA_Language_links_and_Badges
https://www.wikidata.org/wiki/Wikidata:List_of_properties/Summary_table
https://www.wikidata.org/wiki/Wikidata:List_of_properties
https://www.wikidata.org/wiki/Wikidata:Database_reports/List_of_properties/Top100 

Tindríem principalment quatre tipus de queries:

A# Items que tinguin estiguin fets/parlin la llengua - Persones i coses / CLEAR
- "official language" (P37) p.e. Corona d'aragó
- "languages spoken, written or signed" (P1412)
- idioma de l'obra o nom (P407)

SELECT ?item ?itemLabel ?subPropertiesLabel ?claimLabel ?languagelink
WHERE {
  ?subProperties wdt:P1647* wd:P2439.
  ?subProperties wikibase:directClaim ?claim .
  ?item ?claim wd:Q7026 .
  
  ?languagelink schema:about ?item.
  ?languagelink schema:inLanguage "ca". 
  ?languagelink schema:isPartOf <https://ca.wikipedia.org/>
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ca". }
}

B# Items que siguin llocs o estiguin ubicats/creats/succeïts a tal lloc - Llocs, esdeveniments, persones, idees/grups i coses  / CLEAR
- located in the administrative territorial entity (P131)
- té coordinate location (P625)
- applies to jursdiction (P1001) -> subproperty of location
- té codi postal
- persones que han nascut al territori (P19)
- headquarters location (P159), que és a la vegada subproperty de location (P1647)... per tant, potser no cal fer servir la subproperty
- menjar amb "país d'origen" (P495)
- lloc de fundació (location of formation)
- persona "ciutadania de" (P27)
- "work location" (P937)
- fabricat al territori
- home venue (P115) és subproperty of location  -> Camp Nou
- ciutadania de tal país


# aquí ens diu gent o coses fetes o llocs d'Igualada.... si ho féssim de Catalunya. hauríem de veure recursivitat. hauríem d'aconseguir totes les "Located in the administrative territorial entity" de Catalonia en avall.
https://www.wikidata.org/wiki/Property:P131
hi folks,
I was trying to write a query in SPARQL, perhaps someone more experienced can help me.
I wanted to get all things/people/... located (or any of its subproperties) in a place (region). The thing is that it returns me the items related to that region, but not related to the cities located in that region. How could I make a query become 'recursive'?
this is my query: https://pastebin.com/Nv4DYybR


SELECT ?item ?itemLabel ?subPropertiesLabel WHERE { 
  ?subProperties wdt:P1647* wd:P276.
  ?subProperties wikibase:directClaim ?claim .
  ?item ?claim wd:Q15950 .
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
}

C# Tots els de subproperties de País o nacionalitat / aquesta només ens serviria per països i no per regions / CLEAR
SELECT ?item ?itemLabel ?subPropertiesLabel WHERE {
  ?subProperties wdt:P1647* wd:P17.
  ?subProperties wikibase:directClaim ?claim .
  ?item ?claim wd:Q189 .
  
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}

aquest podia donar algun resultat estrany (islàndia sortia com a país de la llei de matrimoni homosexual)

D# Items que hagin estat en relació amb C. - Persones i coses (items de manera indirecta, p.e. Leo Messi, que ha jugat pel Barça i viscut a Barcelona però no hi ha nascut)
D1. Coses/idees “creades”/“pensades”/“fundades" o qualsevol de les seves subpropietats per items al país.

- fundat per una persona nascuda al territori (nintendo)

- "conferred by" (P1027) p.e. Creu de St. Jordi és conferred by Generalitat de Catalunya i a la vegada ‘applies to jurisdiction’ of Catalonia… una persona que la rep, són dos salts.

D2. Persones “formades”/“treballades”/“premiades" o qualsevol de les seves subpropietats per items al país.

- "member of" (P463)
subproperty: member of political party (P102)
subproperty: member of sports team (P54)
- "educated at" (P69)
- “employer” (ocupador per la qual ha treballat) (P108)
- "position held" (P39) com puig i cadafalch, pujol
- “award received” (P166). 
item té "Commons category" (P373) amb keyword

"""

 return




# STRATEGY 4: WIKIPEDIA CATEGORIES
# Obtain for each language edition the articles contained in the Wikipedia categories with a keyword in title (recursively).
def get_articles_categories(lang,total):
	connection = create_mysql_connection(lang)
	# cal mecanismes per si cal tornar a començar l'script, que ho faci des d'on era i no des del principi.
	# cada llengua, que els posi primer en un arxiu i després que els posi a la bbdd finalment. després s'esborra l'arxiu.
	# comprova que siguin a la base de dades i no hi hagi l'arxiu. 
	# en cas positiu. return. surt de la funció. 
	# en cas negatiu. segueix endavant.

    # get_articles_categories # Obtain for each language edition the articles contained in the Wikipedia categories with a keyword in title (recursively).

    # Selecció d’articles amb Keywords al Títol (b) (sense redirects). SQL Keyword Selection.  
    # Llista de categories que contenen Keywords en els títols per fer la Crawling Iteration i fer una Selecció de Contingut Relacionat amb les Keywords (c). SQL / Python  (cawiki_keywords_related_articles.csv / cawiki_keywords_related_categories.csv)

    array_size = 10000
    wordlist = ''

    input_list_name = data_dir + 'source/cira_list_quality.csv' # Aquest no és el què hem generat abans, sinó el que ja ha passat el filtre qualitatiu.
    input_file_list = open(input_list_name, 'r')
    output_file_name3 = data_dir + '/results/CIRA_keywords_stats.csv'
    output_file3 = codecs.open(output_file_name3, 'a', 'UTF-8')

    for line in input_file_list:
        line = line.replace(",",";")
        page_data = line.strip('\n').split(';')
        if page_data[0]==lang: break
    wordlist = page_data[1:len(page_data)-1]

    string = lang+' Starting selection of raw CIRA.'; output_file3.write(string+'\n')
    print ("\nCurrent date and time [START]: ", datetime.datetime.now())
    string = 'With language '+ lang +" and Keywords: "

    for word in wordlist: string = string + word + "\t"
    string = string + "\n"


   	output_file_name1 = data_dir + '/' + lang + '_CIRA_keywords_related_articles.csv'
    output_file_name2 = data_dir + '/' + lang + '_CIRA_keywords_related_categories.csv'
    output_file1 = codecs.open(output_file_name1, 'w', 'UTF-8')
    output_file2 = codecs.open(output_file_name2, 'w', 'UTF-8')

    output_file_name3 = data_dir + '/results/CIRA_keywords_stats.csv'
    output_file3 = codecs.open(output_file_name3, 'a', 'UTF-8')

    level = 0
    selectedCategories = dict()
    selectedArticles = dict()
    articleLevels = dict()


    # CATEGORIES BY WORDS
    # level 0
    for keyword in wordlist:
        mysql_cur.execute(
            'SELECT cat_id, cat_title FROM category INNER JOIN page ON cat_title = page_title WHERE page_namespace = 14 AND CONVERT(cat_title USING utf8mb4) COLLATE utf8mb4_general_ci LIKE "%' + keyword + '%" ORDER BY cat_id;')
        # print "Número de categories trobades per la paraula: " + wordlist[i];

        result = 1
        while result:
            result = mysql_cur.fetchall()
            for row in result:
                if attribute == 'keywords': output_file2.write(str(level) + '\t' + row[1] + '\t' + str(row[0]) + '\n')
                selectedCategories[row[1]] = row[0]

    if attribute == 'keywords': string = "El número d'articles del nivell " + str(level) + " és: " + str(len(selectedArticles))+ " i el número de categories és: " + str(len(selectedCategories)); print (string); output_file3.write(string+'\n')
    curCategories = dict()
    curCategories.update(selectedCategories)
    newCategories = dict()
    newArticles = dict()
    level = 1

    while (level <= 20): # Aquí escollim el NÚMERO DE NIVELLS QUE VOLEM. De 0 a X.
        # ARTICLES FROM CATEGORIES
        for k in curCategories.keys():
            mysql_cur.execute('SELECT page_id, page_title FROM page INNER JOIN categorylinks ON page_id = cl_from WHERE page_namespace=0 AND page_is_redirect=0 AND cl_to=%s',(k,))
            result = 1
            while result:
                result = mysql_cur.fetchall()

                for row in result:
                    if not selectedArticles.has_key(row[1]):
                        if attribute == 'keywords': output_file1.write(str(level) + '\t' + str(row[1]) + '\t' + str(row[0]) + '\n')
                        selectedArticles[row[1]] = row[0]
                        newArticles[row[1]] = row[0]

        # CATEGORIES FROM CATEGORY --> PROBLEMES DE ENCODING
        for l in curCategories.keys():

            #mysql_cur.execute('SELECT cat_id, cat_title FROM page p, categorylinks cl, category c WHERE cl.cl_from=p.page_id AND cl.cl_to=%s AND p.page_namespace=14 AND p.page_title=c.cat_title;',(l,))

            mysql_cur.execute('SELECT cat_id, cat_title FROM page INNER JOIN categorylinks ON page_id=cl_from INNER JOIN category ON page_title=cat_title WHERE page_namespace=14 AND cl_to=%s',(l,))

            result = 1
            rows_found = 0
            while result:
                result = mysql_cur.fetchall()
                # rows_found += len(result)

                for row in result:
                    if not selectedCategories.has_key(row[1]):
                        if attribute == 'keywords': output_file2.write(str(level) + '\t' + str(row[1]) + '\t' + str(row[0]) + '\n')
                        selectedCategories[row[1]] = row[0]  # per tenir la col·lecció sencera
                        newCategories[row[1]] = row[0]  # introdueix les que no són a la selecció definitiva per la propera iteració

        curCategories.clear()
        curCategories.update(newCategories)
        if attribute == 'keywords': string = "El número d'articles del nivell " + str(level) + " és: " + str(len(newArticles))+ " i el número de categories és: " + str(len(curCategories)); print (string); output_file3.write(string+'\n')

        newCategories.clear()
        newArticles.clear()
        level = level + 1

    output_file1.close()
    output_file2.close()

    mysql_cur.execute('SELECT COUNT(*) from page where page_namespace=0 AND page_is_redirect=0')
    numerototalarticles = mysql_cur.fetchone()[0]
    numeroarticlesseleccionats = len(selectedArticles)
    string = "El número total d'articles seleccionats és: " + str(numeroarticlesseleccionats); print (string); output_file3.write(string+'\n')
    string = "El número total de categories seleccionades és: " + str(len(selectedCategories)); print (string); output_file3.write(string+'\n')
    string = "El número total d'articles d'aquesta Wikipedia és: "+str(numerototalarticles)+"\n"; print (string); output_file3.write(string)
    string = "El percentatge d'articles CIRA respecte a la Wikipedia és: "+str(float(100*numeroarticlesseleccionats/numerototalarticles))+"\n"; print (string); output_file3.write(string+'\n')

    print ("Current date and time [FIN]: " +str(datetime.datetime.now())+"\n")


    # En l'anterior codi després revisava els nivells i tallava a partir d'un nivell.




    # TREURE ELS ARTICLES OBTINGUTS AMB LES CATEGORIES QUE TENEN GEOCOORDENADES FORA
    input_file_name_coords_def = data_dir + lang + '_CIRA_coords_def.csv'

    if os.path.isfile(input_file_name_coords_def):
        input_file_coords_def = open(input_file_name_coords_def, 'r')

        for line in input_file_coords_def:
            page_data = line.strip('\n').split('\t')
            page_title = str(page_data[0])
            page_id = str(page_data[1])
            page_titlescoords[page_title]=page_id
        for titol in page_titlescoords:
            #print titol
            if titol in page_titlesdict: del page_titlesdict[titol]
        print 'Coords def carregades en memòria.'
        #raw_input('mec')

    else:
        # TREURE ELS ARTICLES AMB GEOCOORDENADES RELACIONATS PEL CRAWLING
#        mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
        mysql_con = mdb.connect(host=".labsdb",db=lang + '_p',read_default_file="~/.my.cnf")


        mysql_con.set_character_set('utf8')
        with mysql_con:
            mysql_cur = mysql_con.cursor()
            query = 'SELECT page_title FROM page INNER JOIN geo_tags ON page_id=gt_page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP BY page_title'
    #        queryreconnect(mysql_cur,query,0,0)
            mysql_cur.execute(query)
            result = mysql_cur.fetchall()
        articles=[]
        for row in result:
            titol = str(row[0])
            articles.append(titol)

        coordssegurs = list(set(page_titlesdict.keys()).intersection(set(page_titlescoords_ISO.keys())))
        confirmats = list(set(coordssegurs).intersection(set(articles)))

        for titol in confirmats:
            del page_titlesdict[titol]
            page_titlescoords[titol] = page_titlescoords_ISO[titol] # Traspassem les bones del ISO al grup de Geocoordinates definitives.

        output_file_name = data_dir + lang + '_CIRA_coords_def.csv'
        output_file_name_stream = codecs.open(output_file_name, 'w', 'UTF-8')
        for element in page_titlescoords.keys():
            output_file_name_stream.write(element+'\t'+page_titlescoords[element]+'\n')
        output_file_name_stream.close()
    print 'Hem netejat el grup aconseguit baixant per les categories dels articles que tenen coordenades però són fora dels territoris.'
    maxim = len(page_titlesdict)
    print 'Tenim el següent número final d\'articles amb coordenades: '+str(len(page_titlescoords))



	return





# ARTICLE SCORING FUNCTIONS
###########################

# Give a score for a group of articles.
def calculate_score_CCC(lang,total):
	return
	# Aquí tocaria fer les consultes i anar fent updates.

    titles_interwikis = {}
    #mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
    #mysql_con.set_character_set('utf8')
    #with mysql_con:
    #    mysql_cur = mysql_con.cursor()
    #    query = "SELECT page_title, COUNT(ll_lang), ll_from FROM langlinks INNER JOIN page ON ll_from = page_id WHERE page_namespace=0 AND page_is_redirect=0 GROUP by ll_from"
    #    mysql_cur = queryreconnect(mysql_cur,query,0,0)
    #    mysql_cur.execute(query)
    #    rows = mysql_cur.fetchall()
    #    for row in rows: titles_interwikis[str(row[0])] = row[1]
    #    print 'Tots els interwiki calculats.'

"""

funcio* score estàtic (inicial):
càlcul de l'score pels aspectes que no estigui calculat.
    nombre i % categories relacionades amb CCC (keywords on title o del grup)
    nombre i % properties apuntant a CCC.
decisions perquè vagi cap al groundtruth
heuristiques per decidir quan una combinació és bona (té una property).

funcio* score dinàmic (outlinks)
càlcul de l'score per l'estat actual. actualització 'outlinks'.
decisions perquè vagi cap al groundtruth.


funcio* initial_groundtruth:
score estàtic.

funcio* additive_filter_CC:
nombre d'iteracions dins d'aquí additive filter. 
l'score dinàmic és el què faria tot de crear databases, a calcular, etc. esborrar la database.
score dinàmic.


score dinàmic:
què fem per l'score dinàmic:
0.15 d'outlinks?
per decidir el tall, què fem?

utilitzar només... 
% d'outlinks o fer un score compost que tingui en compte també el...
% de propietats de WikiData?




- nombre i % outlinks CCC (a groundtruth)
- nombre i % categories relacionades amb CCC (keywords on title o del grup)
- nombre i % properties apuntant a CCC

Actualment, l'score és un 15% dels outlinks al groundtruth.

Si és necessari, es poden utilitzar iteracions en el mètode.
Podríem mirar la ponderació o els canvis en la ponderació.
Sempre s’ha de posar un threshold (abans era del 10%). Aquest és el problema… Sempre pot fallar.

Com podem millorar CCC? donant un valor als articles i ponderant.
a cada iteració ponderem cada article en funció dels seus vincles a articles ponderats.
Es tractaria de fer una base de dades amb la ponderació de CCC i el binari de si s’inclou o no.
Quin és el valor que li donaries a la ponderació quan no ha estat calculada? 0.5?
Quantes iteracions? No seria infinit això?

L'Score també em permetria calcular l'autoreferencialitat i coses així.


Això seria score estàtic.
# Cross-verify the ‘groundtruth articles’, obtained through coordinates, keywords on title and properties are correct.
arregla la fila de la base de dades que pugui ser-hi per error…. p.e. que tingui la paraula “catalan” però parli dels números catalans. valencia i parli de química.
"""

def initial_groundtruth_filter(lang,total):

    return



# Provides a final selection for CCC given the scores of the articles.
def additive_filter_CCC(lang,total):
	# Això recorreria a la funció anterior i aniria fent iteracions.

	# si peta en aquest punt, cal tornar a començar la funció. cal revisar si ha creat l'arxiu amb extract_dataset. si no existeix, esborrar els confirmats a 'CCC' i tornar a començar.
#    mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
    mysql_con.set_character_set('utf8')
    mysql_con = mdb.connect(host=".labsdb",db=lang + '_p',read_default_file="~/.my.cnf")

    with mysql_con:
        mysql_cur = mysql_con.cursor()

        print 'first-time-in-town'

        queryreconnect(mysql_cur,'DROP TABLE IF EXISTS u3532__.'+lang+'_page_titlesdict',0,0); print 'table page_titlesdict dropped if existed'
        queryreconnect(mysql_cur,'DROP TABLE IF EXISTS u3532__.'+lang+'_pagelinks',0,0); print 'pagelinks tables dropped if existed.'
        queryreconnect(mysql_cur,'DROP TABLE IF EXISTS u3532__.'+lang+'_page_groundtruthwithredirects',0,0); print 'table groundtruth dropped if existed.'
        queryreconnect(mysql_cur,'CREATE TABLE u3532__.'+lang+'_page_groundtruthwithredirects(page_id int(8) unsigned NOT NULL, page_title varbinary(255) NOT NULL, PRIMARY KEY (page_id))',0,0); print 'page groundtruth redirects created'

        queryreconnect(mysql_cur,'CREATE TABLE u3532__.'+lang+'_page_titlesdict(page_id int(8) unsigned NOT NULL, page_title varbinary(255) NOT NULL, PRIMARY KEY (page_id))',0,0); print '* page_titles database created'
        query = 'CREATE TABLE u3532__.'+lang+'_pagelinks AS SELECT * FROM '+lang+'_p.pagelinks'
        queryreconnect(mysql_cur,query,0,0); print '* pagelinks table created with links in.'
        mysql_con.commit()
        queryreconnect(mysql_cur,'ALTER TABLE u3532__.'+lang+'_pagelinks ADD PRIMARY KEY (pl_from,pl_namespace,pl_title)',0,0); print 'pagelinks recreated indexes.'
        mysql_con.commit()



    page_titlesgroundtruth = {}
    for item in page_titlescoords.keys():page_titlesgroundtruth[item]=page_titlescoords[item]
    for item in page_keydict.keys():page_titlesgroundtruth[item]=page_keydict[item] # BASE

    print 'Això és el groundtruth inicial: '+str(len(page_titlesgroundtruth)); output_file_sum.write('Això és el groundtruth inicial: '+str(len(page_titlesgroundtruth)))

    for item in page_titlesgroundtruth.keys():
        if item in page_titlesdict: page_titlesdict.pop(item)

    print 'Tenim un grup de tants articles potencials de sumar al groundtruth de CIRA: '+str(len(page_titlesdict)); output_file_sum.write('Tenim un grup de tants articles potencials de sumar al groundtruth a CIRA: '+str(len(page_titlesdict)))
#    mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
    mysql_con = mdb.connect(host=".labsdb",db=lang + '_p',read_default_file="~/.my.cnf")

    mysql_con.set_character_set('utf8')
    with mysql_con:
        mysql_cur = mysql_con.cursor()
        queryreconnect(mysql_cur,'DROP TABLE IF EXISTS u3532__.'+lang+'_page_groundtruthwithredirects',0,0); print 'table groundtruth dropped if existed.'
        queryreconnect(mysql_cur,'CREATE TABLE u3532__.'+lang+'_page_groundtruthwithredirects(page_title varbinary(255) NOT NULL, PRIMARY KEY (page_title))',0,0); print 'page groundtruth redirects created'


## COMENÇA EL FILTRAT ####################################################################################################################################################################################################

    print ' \nCOMENÇA EL FILTRATGE:'; output_file_sum.write(' \nCOMENÇA EL FILTRATGE:'+lang+'\n')
    rounds = 3
    while rounds > 0:
        mysql_con = mdb.connect(lang + '.labsdb', 'u3532', 'titiangahcieyiph', lang + '_p')
        mysql_con.set_character_set('utf8')

        with mysql_con:
            mysql_cur = mysql_con.cursor()

            output_file_name_entra = data_dir + lang + '_CIRA_Filtered_round_GROUNDTRUTH_conser_'+str(rounds)+'.csv'
            output_file_name_nous = data_dir + lang + '_CIRA_Filtered_round_NOENTREN_conser_'+str(rounds)+'.csv'
            if rounds==1: output_file_name_entra = data_dir + lang + '_CIRA_articles.csv'
            output_file_nous = codecs.open(output_file_name_nous, 'w', 'UTF-8')
            output_file_entra = codecs.open(output_file_name_entra, 'w', 'UTF-8')
            articlesafegits = {}

            print '\n***** RONDA '+str(rounds)
            ######################################### ESPAI DE RECÀLCUL #########################################

            query = 'TRUNCATE TABLE u3532__.'+lang+'_page_titlesdict'
    #        mysql_cur = queryreconnect(mysql_cur,query,0,0)
            mysql_cur.execute(query)
            query = 'TRUNCATE TABLE u3532__.'+lang+'_page_groundtruthwithredirects'
    #        mysql_cur = queryreconnect(mysql_cur,query,0,0)
            mysql_cur.execute(query)

            print 'Taules dels articles truncades per poder-les introduïr-les.'
            print 'Total d\'articles a groundtruth: '+str(len(page_titlesgroundtruth))

            page_titlesgroundtruth = dict((v,k) for k,v in dict((v,k) for k,v in page_titlesgroundtruth.iteritems()).iteritems())
            page_titlesdict2 = dict((v,k) for k,v in dict((v,k) for k,v in page_titlesdict.iteritems()).iteritems())

            page_titlesdict_reformatted = []
            count = 1
            query = 'INSERT INTO u3532__.'+lang+'_page_titlesdict (page_title,page_id) VALUES (%s, %s)'
            for titol in page_titlesdict2.keys():
                count = count + 1
                #print (titol,page_titlesdict2[titol])
                page_titlesdict_reformatted.append((titol,page_titlesdict2[titol]))
                if count % 20000 == 0 or count >= len(page_titlesdict2):
                    mysql_cur.executemany(query,page_titlesdict_reformatted);
                    #print str(count)+' articles entrats.'
                    page_titlesdict_reformatted = []
            print '1* Pagetitles amb totes els seleccionats ja són dins a la bbdd page_titlesdict: '+str(len(page_titlesdict2))

            # LLISTA D'ARTICLES (TÍTOLS) AMB ELS SEUS REDIRECTS (pels endooutlinks dels tres tipus.)
            count = 1
            query = 'INSERT INTO u3532__.'+lang+'_page_groundtruthwithredirects (page_title) VALUES (%s)'
            pagetitles_reformatted = []
            for titol in page_titlesgroundtruth.keys():
                count = count + 1
                pagetitles_reformatted.append((titol,))
                if count % 20000 == 0 or count >= len(page_titlesgroundtruth):
                    #print str(count)+' articles entrats.'
                    mysql_cur.executemany(query,pagetitles_reformatted)
                    pagetitles_reformatted = []
            print '2* El groundtruth ja està dins de la BBDD: '+str(len(page_titlesgroundtruth))


            query = "SELECT DISTINCT page_title, page_id FROM u3532__."+lang+"_pagelinks INNER JOIN page ON pl_from=page_id WHERE pl_title IN (SELECT page_title FROM u3532__."+lang+"_page_groundtruthwithredirects) AND page_namespace=0 AND page_is_redirect=1"
#            mysql_cur.execute(query)# obtenim els redirects pels Articles Seleccionats com a groundtruth
            mysql_cur = queryreconnect(mysql_cur,query,0,0)
            rows = mysql_cur.fetchall()
            groundtruthredirects = {}
            for row in rows:
#                print str(row[0])+','+str(row[1])
                groundtruthredirects[str(row[0])]=str(row[1])
            print '3* Ja tenim els redirects del groundtruth: '+str(len(groundtruthredirects))
            groundtruthredirects = dict((v,k) for k,v in dict((v,k) for k,v in groundtruthredirects.iteritems()).iteritems())
            coincident = list(set(page_titlesgroundtruth).intersection(set(groundtruthredirects.keys())))
            for titol in coincident:
                groundtruthredirects.pop(titol)
                page_titlesgroundtruth.pop(titol)

            query = 'INSERT INTO u3532__.'+lang+'_page_groundtruthwithredirects (page_title) VALUES (%s)'
            count = 1
            pagetitleswithredirects_reformatted = []
            for titol in groundtruthredirects.keys():
                count = count + 1
                pagetitleswithredirects_reformatted.append((titol,))
                if count % 20000 == 0 or count >= len(groundtruthredirects):
                    print str(count)+' articles entrats.'
                    pagetitleswithredirects_reformatted = list(set(pagetitleswithredirects_reformatted))
                    mysql_cur.executemany(query,pagetitleswithredirects_reformatted)
                    pagetitleswithredirects_reformatted = []
            print '4* Redirects amb totes els groundtruth ja són dins a la bbdd pagegroundtruthwithredirects: '+str(len(groundtruthredirects))

            ######################################################################################################

            capcaleres = str('num\tlevel\tid\tArt\tIW\tEOL%\tEOL\tOL\t\n')
            output_file_nous.write(capcaleres)
            output_file_entra.write(capcaleres)

            print 'INICI. En el round '+str(rounds)+', CIRA GROUNDTRUTH definitiu té aquest nombre d\'articles: '+str(len(page_titlesgroundtruth)); output_file_sum.write('INICI. En el round '+str(rounds)+', CIRA GROUNDTRUTH definitiu té aquest nombre d\'articles: '+str(len(page_titlesgroundtruth)))
            print '\t Tenim un grup de tants articles POTENCIALS de pertànyer a CIRA:'+str(len(page_titlesdict)); output_file_sum.write('\tTenim un grup de tants articles POTENCIALS de pertànyer a CIRA:'+str(len(page_titlesdict)))

            titles_endooutlinks = {}
            query = 'SELECT l.page_title, count(p.pl_title) FROM u3532__.'+lang+'_pagelinks p INNER JOIN u3532__.'+lang+'_page_titlesdict l ON p.pl_from=l.page_id WHERE p.pl_title IN (SELECT page_title FROM u3532__.'+lang+'_page_groundtruthwithredirects) AND p.pl_namespace=0 GROUP by p.pl_from'
#            mysql_cur = queryreconnect(mysql_cur,query,0,0)
            mysql_cur.execute(query)
            rows = mysql_cur.fetchall()
            for row in rows: titles_endooutlinks[str(row[0])]=row[1]
            print '- obtinguts els endooutlinks pels títols de la ronda.'

            titles_outlinks = {}
            query = 'SELECT l.page_title,count(p.pl_title) FROM u3532__.'+lang+'_pagelinks p INNER JOIN u3532__.'+lang+'_page_titlesdict l ON p.pl_from=l.page_id WHERE p.pl_namespace=0 GROUP by p.pl_from'
#            mysql_cur = queryreconnect(mysql_cur,query,0,0)
            mysql_cur.execute(query)
            rows = mysql_cur.fetchall()
            for row in rows: titles_outlinks[str(row[0])]=row[1]
            print '- obtinguts els outlinks pels títols de la ronda.'

            mysql_con.ping(True)
            count = 0
            for page_title in page_titlesdict.keys():
                count = count + 1
                interwikis = 0
                endooutlinks = 0
                outlinks = 0
                percenoutlinks = 0
                page_id = page_titlesdict[page_title]
    #            if page_title in titles_interwikis: interwikis = titles_interwikis[page_title]
                if page_title in titles_outlinks: outlinks = titles_outlinks[page_title]
                if page_title in titles_endooutlinks: endooutlinks = titles_endooutlinks[page_title]
                if endooutlinks!=0: percenoutlinks = round(float(endooutlinks)/outlinks,3)

                entra = 'no entra'
                if percenoutlinks >= 0.15: entra = 'ENTRA'

                #############################################################################################################################
                # Afegir-hi les categories i les coordenades com a dades per treure en arxiu.
                dades = str(count)+'\t'
                dades = dades + str(page_leveldict[page_title] )+ '\t' + str(page_id)  +'\t'+ str(page_title) +'\t'+ str(interwikis) +'\t'+str(percenoutlinks)+'\t'+ str(endooutlinks)+'\t'+ str(outlinks)

                if count%10000 == 0: print 'article '+str(count)

                if entra == 'ENTRA':
#                    print dades + '\t' + str(entra)
                    page_titlesgroundtruth[page_title]=page_id
                    page_titlesdict.pop(page_title)

                else:
#                    print dades + '\t' + str(entra)
                    output_file_nous.write(dades + '\n') # ARXIU AMB ELS NOUS INTRODUÏTS EN AQUESTA RONDA.

            for item in page_titlesgroundtruth.keys():
                if item not in titles_interwikis: titles_interwikis[item]=0
                output_file_entra.write(item + '\t' + str(page_titlesgroundtruth[item]) +'\t'+ str(titles_interwikis[item]) +'\n') # ARXIU AMB EL GROUNDTRUTH EN AQUESTA RONDA.

            output_file_nous.close()
            output_file_entra.close()

            gc.collect()
            print 'Després de passar el filtre i afegir articles, tenim aquest nombre d\'articles a CIRA groundtruth: '+str(len(page_titlesgroundtruth))+'. D\'un possible màxim inicial de: '+str(initialcollection)+' i resten un màxim de tants articles que s\'hi poden sumar: '+str(len(page_titlesdict))+'\n'
            output_file_sum.write('\nDesprés de passar el filtre i afegir articles, tenim aquest nombre d\'articles a CIRA groundtruth: '+str(len(page_titlesgroundtruth))+'. D\'un possible màxim inicial de: '+str(initialcollection)+' i resten un màxim de tants articles que s\'hi poden sumar: '+str(len(page_titlesdict))+'\n')

            rounds = rounds - 1
            # FÍ DEL ROUND

            if rounds == 0:
                # Fí del filtre.

                output_file_sum.write('\n\n')
                print 'El número final d\'articles CIRA amb el groundtruth conservador incremental és: '+str(len(page_titlesgroundtruth))+'\n'
    #quit()
    #mysql_con.close()

    return ''

def quality_articles(lang,total):

    featuredarticles=[]
#    categorypattern = re.compile(r'(.+:+)(.*)')
    featuredarticleslangs = {}
    featuredarticleslangs['cawiki']="Llista_d'articles_de_qualitat"
    mysql_cur.execute('SELECT ll_lang,ll_title FROM cawiki_p.langlinks WHERE ll_from =379043') # SEMPRE BUSQUEM A LA VIQUIPÈDIA EN CATALÀ PER OBTENIR TOTS ELS NOMS D'ARTICLES DE QUALITAT
    rows = mysql_cur.fetchall()
    for row in rows:
        lang = row[0]+'wiki'
        lang = lang.replace('-', '_')
        title = row[1]
        title = title.replace(' ', '_')

        guions = title.index(':')
        title = title[(guions+1):len(title)]
        featuredarticleslangs[lang] = title
        if lang == 'itwiki': featuredarticleslangs[lang] = 'Voci_in_vetrina_su_it.wiki'
        if lang == 'ruwiki': featuredarticleslangs[lang] = 'Википедия:Избранные_статьи_по_алфавиту'


#    raw_input('Press start.')

    output_file_name_feat = data_dir + language + '_CIRA_featured_articles.csv'
    output_file_feat = codecs.open(output_file_name_feat, 'w', 'UTF-8')

#    lang_llista = []
#    input_iso_file = data_dir + 'source/cira_iso3166.csv'
#    input_iso_file = open(input_iso_file, 'r')
#    for line in input_iso_file:
#        page_data = line.strip('\n').split(',')
#        lang_llista.append(page_data[0])

#    for llengua in lang_llista:
#        if llengua not in featuredarticleslangs: continue
        # COMENTAT PER QUÈ NOMÉS HO HEM FET SERVIR PER VEURE EL NÚMERO D'ARTICLES DE QUALITAT PER TOTES LES LLENGÜES
#        mysql_cur.execute('SELECT page_title FROM '+llengua+'_p.categorylinks INNER JOIN page on cl_from=page_id WHERE CONVERT(cl_to USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s', featuredarticleslangs[llengua]) # Comptar
#        rows = mysql_cur.fetchall()
#        print 'Per la llengua: '+llengua+', amb '+featuredarticleslangs[llengua]+' tenim: '+str(len(rows))+' adq.'
    featuredarticleslangs['cawiki']="Llista_d'articles_de_qualitat"


    resultatqualitat = ''
    lang = language

    if language in featuredarticleslangs.keys():
        print 'Hem carregat totes les llistes de featured articles.'
        print language
        print featuredarticleslangs[lang]

        mysql_cur.execute('SELECT page_title FROM categorylinks INNER JOIN page on cl_from=page_id WHERE CONVERT(cl_to USING utf8mb4) COLLATE utf8mb4_general_ci LIKE %s', (featuredarticleslangs[lang],)) # Extreure
        rows = mysql_cur.fetchall()
        for row in rows: featuredarticles.append(str(row[0]))
        print 'ja tenim els featured de la llengua: '+language
        coincident = list(set(page_titles_cira_def.keys()).intersection(set(featuredarticles)))
        for item in coincident:
            print "Aquest és un article featured de a CIRA: %s" % item
            output_file_feat.write(item+'\n')

        percenciraqualitat = str(100*round(float(len(list(set(page_titles_cira_def.keys()).intersection(set(featuredarticles)))))/len(featuredarticles),5))
        print '\nEl nombre articles de qualitat és '+str(len(featuredarticles))+' i el nombre de CIRA és '+str(len(page_titles_cira_def.keys()))
        print 'El percentatge de CIRA a qualitat és: '+ percenciraqualitat

        resultatqualitat = percenciraqualitat + '\t' + str(len(featuredarticles))

    mysql_cur.execute('SELECT COUNT(*) from page where page_namespace=0 AND page_is_redirect=0')
    numerototalarticles = mysql_cur.fetchone()[0]
    print '\nTenim una Wikipedia amb un número total d\'articles:'+str(numerototalarticles)    



# FINAL CCC 
###########

# Creates CCC using all the previous functions.
def create_CCC(lang,total):

	# QUÈ FER AMB L'INCREMENTAL. obtenir els articles i fer les cerques... o fer les cerques amb el paràmetre de la creació de l'article?
	# funció que obtindrà els articles de manera setmanal (o bé se li passa un paràmetre a la resta de funcions)
	# l'incremental és: una iteració de l'score?
	# l'incremental és: les mateixes funcions anteriors però amb query per data (geolocated, keywords, categories)?

	if total == 1: 
		drop_database()
		create_database()

	get_articles_geolocated(lang,total) # o bé faig la iteració dels territoris fora i passo el territori per l'argument, o la faig a dins.
	get_articles_keywords(lang,total)
	get_articles_Wikidata(lang,total)
	get_articles_categories(lang,total)

	additive_filter_CCC(lang)

	extract_dataset_file(lang)

	print 'language '+(lang)+' created.'



if __name__ == '__main__':	
	main()

	end = time.time()
	print 'job completed after: ' + str(end - startTime)
