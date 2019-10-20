import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

dash_app1 = Dash(__name__, server = app, url_base_pathname= webtype + '/language_territories_mapping/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app1.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})


conn = sqlite3.connect(databases_path+'diversity_groups.db'); cursor = conn.cursor();  

query = 'SELECT WikimediaLanguagecode, languagenameEnglishethnologue, territoryname, territorynameNative, QitemTerritory, demonym, demonymNative, ISO3166, ISO31662, regional, country, indigenous, languagestatuscountry, officialnationalorregional, region, subregion, intermediateregion FROM wikipedia_languages_territories_mapping;'

df = pd.read_sql_query(query, conn)


#df = df[['territoryname','territorynameNative','QitemTerritory','WikimediaLanguagecode','demonym','demonymNative','ISO3166','ISO31662']]

df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('-','_')
df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('be_tarask', 'be_x_old')
df.WikimediaLanguagecode = df['WikimediaLanguagecode'].str.replace('nan', 'zh_min_nan')
df = df.set_index('WikimediaLanguagecode')
df['Language Name'] = pd.Series(languages[['languagename']].to_dict('dict')['languagename'])
df = df.reset_index()

columns_dict = {'Language Name':'Language','WikimediaLanguagecode':'Wiki','QitemTerritory':'WD Qitem','territoryname':'Territory','territorynameNative':'Territory (Local)','demonymNative':'Demonyms (Local)','ISO3166':'ISO 3166', 'ISO3662':'ISO 3166-2','country':'Country'}
df=df.rename(columns=columns_dict)

title = 'Language Territories Mapping'
dash_app1.title = title+title_addenda
dash_app1.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''This page contains a copy of the latest version of the **Language Territories Mapping database** (see wikipedia_language_territories_mapping.csv in [github project page](https://github.com/marcmiquel/WCDO/tree/master/language_territories_mapping)). The first version of this database has been generated using Ethnologue, 
    Wikidata and Wikipedia language pages. Wikimedians are invited to suggest changes by e-mailing [tools.wcdo@tools.wmflabs.org](mailto:tools.wcdo@tools.wmflabs.org).

    The database contains all the territories (political divisions of first and second level) in which a language 
    is spoken because it is indigeneous or official, along with some specific metadata used in the generation of 
    Cultural Context Content (CCC) dataset.

    The following table is a reduced version of the database with the Language name, wikicode, Wikidata Qitem for 
    the territory, territory in native language, demonyms in native language, ISO 3166 and ISO 3166-2, whereas 
    the full database includes the Qitem for the language, language names in Native languages among other information. 
    Additionally, the full table is extended with the database country_regions.csv, which presents an equivalence 
    table between countries, world regions (continents) and subregions (see country_regions.csv in the github).'''.replace('  ', '')),


#    containerProps={'textAlign':'center'}),
    dt.DataTable(
        columns=['Wiki','Language','WD Qitem','Territory (Local)','Demonyms (Local)','ISO3166','ISO31662'],
        rows=df.to_dict('records'),
        filterable=True,
        sortable=True,
        id='datatable-languageterritories'
    ),

    dcc.Markdown(
    '''Tags: #languages #territories #culturaldiversity #wikipedias'''.replace('  ', '')),


#    containerProps={'textAlign':'center'}),

    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###