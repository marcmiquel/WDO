# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### ARTICLES DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor() 

# s’hauria de veure fins a quin punt ocupa espai en memòria tot l’històric de monthly i què s’ha de consultar.

functionstartTime = time.time()

# MONTHLY
query = 'select set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, period from wcdo_intersections_monthly where content="articles" order by set1, period;'
df_monthly = pd.read_sql_query(query, conn).round(1)
df_monthly = df_monthly.rename(columns={'set1':'Wiki','abs_value':'Articles','rel_vale':'Extent Articles (%)'})
df_monthly['Language'] = df_monthly['Wiki'].map(language_names_full)

#print (df_monthly.head(10));
#print(str(datetime.timedelta(seconds=time.time() - functionstartTime)))
#input('')

# language CCC
df_monthly_ccc = df_monthly.loc[df_monthly['set2descriptor'] == "ccc"]
df_monthly_ccc = df_monthly.rename(columns={'set2':'Language CCC'})
df_monthly_ccc['Covered Language CCC'] = df_monthly_ccc['Language CCC'].map(language_names_full)

# gender
df_monthly_gender = df_monthly.loc[df_monthly['set2descriptor'].isin(['male','female'])]
df_monthly_gender = df_monthly_gender.rename(columns={'set2descriptor':'People'})

# geographical entities
df_monthly_country = df_monthly.loc[df_monthly['set2']=='countries']
df_monthly_country = df_monthly_country.rename(columns={'set2descriptor':'ISO 3166'})
df_monthly_country['Country'] = df_monthly_country['ISO 3166'].map(country_names)
df_monthly_country['Subregion'] = df_monthly_country['ISO 3166'].map(subregions)
df_monthly_country['Region'] = df_monthly_country['ISO 3166'].map(regions)

df_monthly_subregion = df_monthly.loc[df_monthly['set2']=='subregions']
df_monthly_subregion = df_monthly_subregion.rename(columns={'set2descriptor':'Subregions'})

df_monthly_regions = df_monthly.loc[df_monthly['set2']=='regions']
df_monthly_regions = df_monthly_regions.rename(columns={'set2descriptor':'Continents'})


# ACCUMULATED
query = 'select set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, period from wcdo_intersections_accumulated where period = (SELECT MAX(period) FROM wcdo_intersections_accumulated) and content="articles" AND set2 IN ("countries","subregions","regions") or set2descriptor = "ccc" order by set1, period;'
df_accumulated = pd.read_sql_query(query, conn)
df_accumulated = df_accumulated.rename(columns={'set1':'Wiki','abs_value':'Articles','rel_vale':'Extent Articles (%)'})
df_accumulated['Language'] = df_accumulated['Wiki'].map(language_names_full)

# language CCC
df_accumulated_ccc = df_accumulated.loc[df_accumulated['set2descriptor'] == "ccc"]
df_accumulated_ccc = df_accumulated.rename(columns={'set2':'Language CCC'})
df_accumulated_ccc['Covered Language CCC'] = df_accumulated_ccc['Language CCC'].map(language_names_full)

# gender
df_accumulated_gender = df_accumulated.loc[df_accumulated['set2descriptor'].isin(['male','female'])]
df_accumulated_gender = df_accumulated_gender.rename(columns={'set2descriptor':'People'})

# geographical entities
df_accumulated_country = df_accumulated.loc[df_accumulated['set2']=='countries']
df_accumulated_country = df_accumulated_country.rename(columns={'set2descriptor':'ISO 3166'})
df_accumulated_country['Country'] = df_accumulated_country['ISO 3166'].map(country_names)
df_accumulated_country['Subregion'] = df_accumulated_country['ISO 3166'].map(subregions)
df_accumulated_country['Region'] = df_accumulated_country['ISO 3166'].map(regions)

df_accumulated_subregion = df_accumulated.loc[df_accumulated['set2']=='subregions']
df_accumulated_subregion = df_accumulated_subregion.rename(columns={'set2descriptor':'Subregions'})

df_accumulated_regions = df_accumulated.loc[df_accumulated['set2']=='regions']
df_accumulated_regions = df_accumulated_regions.rename(columns={'set2descriptor':'Continents'})


#print (df_accumulated.head(10));
#print(str(datetime.timedelta(seconds=time.time() - functionstartTime)))

print ('monthly loaded')
print(str(datetime.timedelta(seconds=time.time() - functionstartTime)))
#input('')



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

dash_app13 = Dash(__name__, server = app, url_base_pathname= webtype + '/monthly_created_articles/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app13 = Dash()
#dash_app13.config['suppress_callback_exceptions']=True


title = "Monthly Created Articles"
dash_app13.title = title+title_addenda
text_heatmap = ''

dash_app13.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that explain the distribution of pageviews in each Wikipedia language edition. They illustrate how the different kinds of gaps also appear in the pageviews. 
        The graphs answer the following questions:
        * What is the extent of geolocated articles in countries, subregions and regions in each Wikipedia language edition created during the last month?
        * What is the extent of gender articles created in Wikipedia language editions last month?
        * What is the extent of every language CCC in the articles created during the last month in each Wikipedia language edition?


        * What is the extent of geolocated articles in countries, subregions and regions in each Wikipedia language edition created monthly?
        * What is the extent of gender articles in each Wikipedia language edition created monthly?
        * What is the extent of language CCC articles in each Wikipedia language edition created monthly?

       '''),
    html.Hr(),

###

    html.H3('Wikipedia Language Edition Last Month’s New Articles Treemap'),
    dcc.Markdown('''* **What is the extent of geolocated articles in countries, subregions and regions in each Wikipedia language edition created during the last month?**

        The following treemap graphs shows for a selected Wikipedia language edition the extent of the different types of articles created during last month. The different types of articles can be geographical entities (countries, subregions and world regions), gender (male, female) and .



        The following treemap graphs show for a selected Wikipedia language edition the extent of geographical entities (countries, subregions and world regions) in their geolocated articles. This can either be in terms of the number of articles or the pageviews they receive. The size of the tiles is according to the extent of the geographical entities take and the color simply represent the diversity of entities. When you hover on a tile you can compare both articles and pageviews extent in relative (percentage) and absolute (articles and pageviews).


        The following treemap graphs shows for a selected Wikipedia language edition the extent each language CCC take in the sum of articles and pageviews dedicated and received by all languages CCC. The size of the tiles is according to the extent the language CCC take and the color simply represent the diversity of entities. When you hover on a tile you can compare both articles and pageviews extent in relative (percentage) and absolute (articles and pageviews).

        of a selected type of geographical entity (countries, subregions and world regions) in the sum of articles and pageviews dedicated and received by this geographical entity type. The size of the tiles is according to the extent of the geographical entities take and the color simply represent the diversity of entities. When you hover on a tile you can compare both articles and pageviews extent in relative (percentage) and absolute (articles and pageviews).
     '''.replace('  ', '')),
    # last month, accumulated.

    html.Br(),
    html.Div(
    html.P('Select a Wikipedia and Geographical entity type'),
    style={'display': 'inline-block','width': '200px'}),
    html.Br(),

    html.Div(
    dcc.Dropdown(
        id='sourcelangdropdown',
        options = [{'label': k, 'value': k} for k in language_names_list],
        value = 'English (en)',
        style={'width': '240px'}
     ), style={'display': 'inline-block','width': '250px'}),

    dcc.Dropdown(
        id='entity',
        options = [{'label': k, 'value': k} for k in ['Countries','Regions','Languages CCC','Gender']],
        value = 'Countries',
        style={'width': '190px'}
     ),
#    dcc.Graph(id = 'geolocated_treemap'),

###

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 



