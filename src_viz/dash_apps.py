# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
# app
import flask
from flask import send_from_directory
from dash import Dash
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
# viz
import plotly
import plotly.plotly as py
import plotly.figure_factory as ff
# data
from urllib.parse import urlparse, parse_qs
import pandas as pd
import sqlite3
# other
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time

print ('\n\n\n*** START WCDO APP:'+str(datetime.datetime.now()))
databases_path = '/srv/wcdo/databases/'

territories = wikilanguages_utils.load_languageterritories_mapping()
languages = wikilanguages_utils.load_wiki_projects_information(territories);

wikilanguagecodes = languages.index.tolist()
wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes)
for languagecode in wikilanguagecodes:
    if languagecode not in wikipedialanguage_numberarticles: wikilanguagecodes.remove(languagecode)

languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
# Only those with a geographical context
for languagecode in languageswithoutterritory: wikilanguagecodes.remove(languagecode)

closest_langs = wikilanguages_utils.obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, 4)

country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()


# LET THE SHOW START
app = flask.Flask(__name__)

if __name__ == '__main__':
    handler = RotatingFileHandler('wcdo_app.log', maxBytes=10000, backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.run(host='0.0.0.0')

@app.route('/')
def main():
    return flask.redirect('https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory')


### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

dash_app1 = Dash(__name__, server = app, url_base_pathname='/language_territories_mapping/')
df = pd.read_csv(databases_path + 'language_territories_mapping.csv',sep='\t',na_filter = False)
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
dash_app1.title = title
dash_app1.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        columns=['Wiki','Language','WD Qitem','Territory (Local)','Demonyms (Local)','ISO3166','ISO31662'],
        rows=df.to_dict('records'),
        filterable=True,
        sortable=True,
        id='datatable-languageterritories'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app2 = Dash(__name__, server = app, url_base_pathname='/list_of_wikipedias_by_cultural_context_content/')

conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

df = pd.DataFrame(wikilanguagecodes)
df = df.set_index(0)
df['wp_number_articles']= pd.Series(wikipedialanguage_numberarticles)

# CCC %
query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE set1descriptor = "wp" AND set2descriptor = "ccc" AND content = "articles" AND set1=set2 AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY abs_value DESC;'
rank_dict = {}; i=1
lang_dict = {}
abs_rel_value_dict = {}
abs_value_dict = {}
for row in cursor.execute(query):
    lang_dict[row[0]]=languages.loc[row[0]]['languagename']
    abs_rel_value_dict[row[0]]=round(row[2],2)
    abs_value_dict[row[0]]=int(row[1])
    rank_dict[row[0]]=i
    i=i+1  
df['Language'] = pd.Series(lang_dict)
df['Nº'] = pd.Series(rank_dict)

df['ccc_percent'] = pd.Series(abs_rel_value_dict)
df['ccc_number_articles'] = pd.Series(abs_value_dict)

df['Region']=languages.region
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

# Renaming the columns
columns_dict = {'Language':'Language','wp_number_articles':'Articles','ccc_number_articles':'CCC art.','ccc_percent':'CCC %'}
df=df.rename(columns=columns_dict)
df = df.reset_index()

df = df.rename(columns={0: 'Wiki'})
df = df.fillna('')

title = 'Lists of Wikipedias by Cultural Context Content'
dash_app2.title = title
dash_app2.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        rows=df.to_dict('records'),
        columns = ['Nº','Language','Wiki','Articles','CCC art.','CCC %','Subregion','Region'],
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-cccextent'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-cccextent'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")



@dash_app2.callback(
    Output('datatable-cccextent', 'selected_row_indices'),
    [Input('graph-cccextent', 'clickData')],
    [State('datatable-cccextent', 'selected_row_indices')])
def app2_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app2.callback(
    Output('graph-cccextent', 'figure'),
    [Input('datatable-cccextent', 'rows'),
     Input('datatable-cccextent', 'selected_row_indices')])
def app2_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('CCC Articles', 'CCC %', 'Wikipedia articles',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['CCC art.'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'percent',
        'x': dff['Language'],
        'y': dff['CCC %'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['Articles'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app3 = Dash(__name__, server = app, url_base_pathname='/list_of_language_territories_by_cultural_context_content/')

#   QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?    # OBTAIN THE DATA.
conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor()

# CCC
query = 'SELECT set1 as languagecode, set2descriptor as Qitem, abs_value as CCC_articles, ROUND(rel_value,2) CCC_percent FROM wcdo_intersections WHERE set1descriptor = "ccc" AND set2 = "ccc" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set2descriptor DESC;'
df1 = pd.read_sql_query(query, conn)
# GL
query = 'SELECT set1 as languagecode2, set2descriptor as Qitem2, abs_value as CCC_articles_GL, ROUND(rel_value,2) CCC_percent_GL FROM wcdo_intersections WHERE set1descriptor = "ccc" AND set2 = "ccc_geolocated" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set2descriptor DESC;'
df2 = pd.read_sql_query(query, conn)
# KW
query = 'SELECT set1 as languagecode3, set2descriptor as Qitem3, abs_value as CCC_articles_KW, ROUND(rel_value,2) CCC_percent_KW FROM wcdo_intersections WHERE set1descriptor = "ccc" AND set2 = "ccc_keywords" AND content = "articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set2descriptor DESC;'
df3 = pd.read_sql_query(query, conn)

dfx = pd.concat([df1, df2, df3], axis=1)
dfx = dfx[['languagecode','Qitem','CCC_articles','CCC_percent','CCC_articles_GL','CCC_percent_GL','CCC_articles_KW','CCC_percent_KW']]

columns = ['territoryname','territorynameNative','country','ISO3166','ISO31662','subregion','region']
territoriesx = list()
for index in dfx.index.values:
    qitem = dfx.loc[index]['Qitem']
    languagecode = dfx.loc[index]['languagecode']
    languagename = languages.loc[languagecode]['languagename']
    current = []
    try:
        current = territories.loc[territories['QitemTerritory'] == qitem].loc[languagecode][columns].values.tolist()
        current.append(languagename)
    except:
        current = [None,None,None,None,None,None,None,languagename]
        pass
    territoriesx.append(current)
columns.append('languagename')
all_territories = pd.DataFrame.from_records(territoriesx, columns=columns)

df = pd.concat([dfx, all_territories], axis=1)

columns_dict = {'languagename':'Language','languagecode':'Wiki','Qitem':'Qitem','CCC_articles':'CCC art.','CCC_percent':'CCC %','CCC_articles_GL':'CCC GL art.','CCC_percent_GL':'CCC GL %','CCC_articles_KW':'CCC art. KW','CCC_percent_KW':'CCC KW %','territoryname':'Territory name','territorynameNative':'Territory name (native)','country':'country','ISO3166':'ISO3166','ISO31662':'ISO3166-2','subregion':'subregion','region':'region'}
df=df.rename(columns=columns_dict)

columns = ['Qitem','Territory name','Language','Wiki','CCC art.','CCC %','CCC GL art.','CCC art. KW','ISO3166','ISO3166-2']

title = 'List of Language Territories by Cultural Context Content'
dash_app3.title = title
dash_app3.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        rows=df.to_dict('records'),
        columns = columns,
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-cccextent-qitem'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-cccextent-qitem'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")



@dash_app3.callback(
    Output('datatable-cccextent-qitem', 'selected_row_indices'),
    [Input('graph-cccextent-qitem', 'clickData')],
    [State('datatable-cccextent-qitem', 'selected_row_indices')])
def app3_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app3.callback(
    Output('graph-cccextent-qitem', 'figure'),
    [Input('datatable-cccextent-qitem', 'rows'),
     Input('datatable-cccextent-qitem', 'selected_row_indices')])
def app3_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('CCC Articles by Qitems in Wikipedias', 'CCC % by Qitems in Wikipedias', 'CCC GL art. by Qitems in Wikipedias',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Territory name'],
        'y': dff['CCC art.'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'percent',
        'x': dff['Territory name'],
        'y': dff['CCC %'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Territory name'],
        'y': dff['CCC GL art.'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app4 = Dash(__name__, server = app, url_base_pathname='/ccc_coverage/')

conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 
conn2 = sqlite3.connect(databases_path + 'ccc_current.db'); cursor2 = conn2.cursor() 

avg_iw = {}
for languagecode in wikilanguagecodes:
    cursor2.execute('SELECT avg(num_interwiki) FROM ccc_'+languagecode+'wiki WHERE ccc_binary=0;')
    iw=cursor2.fetchone()[0]
    if iw == None: iw = 0
    avg_iw[languagecode]=round(iw,1)

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

        try:
            row_dict['WP articles']=int(wikipedialanguage_numberarticles[languagecode_covering])
        except:
            row_dict['WP articles']='0'

        try:
            row_dict['avg_iw']=avg_iw[languagecode_covering]
        except:
            row_dict['avg_iw']=0

        try:
            row_dict['relative_coverage_index']=r_coverage[languagecode_covering]
        except:
            row_dict['relative_coverage_index']=0

        try:
            row_dict['total_coverage_index']=t_coverage[languagecode_covering]
        except:
            row_dict['total_coverage_index']=0

        try:
            row_dict['coverage_articles_sum']=int(coverage_art[languagecode_covering])
        except:
            row_dict['coverage_articles_sum']=0

        language_dict[languagecode_covering]=row_dict
        row_dict = {}
        i = 1

    if i <= ranking:
        languagecode_covered=row[1]
        if languagecode_covered in languageswithoutterritory:
            i-=1;
        else:
            rel_value=round(row[2],2)

            languagecode_covered = languagecode_covered.replace('be_tarask','be_x_old')
            languagecode_covered = languagecode_covered.replace('zh_min_nan','nan')
            languagecode_covered = languagecode_covered.replace('zh_classical','lzh')
            languagecode_covered = languagecode_covered.replace('_','-')
            value = languagecode_covered + ' ('+str(rel_value)+'%)'

            row_dict[str(i)]=value
    i+=1

    languagecode_covering = cur_languagecode_covering


column_list_dict = {'language':'Language', 'WP articles':'Articles','avg_iw':'No CCC IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_coverage_index':'R.Coverage','total_coverage_index':'T.Coverage','coverage_articles_sum':'Covered Art.'}
column_list = ['Language','Articles','No CCC IW','nº1','nº2','nº3','nº4','nº5','R.Coverage','T.Coverage','Covered Art.','Region','Subregion']

df=pd.DataFrame.from_dict(language_dict,orient='index')

df['Region']=languages.region
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

df=df.rename(columns=column_list_dict)

df = df[column_list] # selecting the parameters to export
df = df.fillna('')


title = "Wikipedia Language Editions' CCC Coverage"
dash_app4.title = title
dash_app4.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        rows=df.to_dict('records'),
        columns = column_list,
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-ccccoverage'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-ccccoverage'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")


@dash_app4.callback(
    Output('datatable-ccccoverage', 'selected_row_indices'),
    [Input('graph-ccccoverage', 'clickData')],
    [State('datatable-ccccoverage', 'selected_row_indices')])
def app4_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app4.callback(
    Output('graph-ccccoverage', 'figure'),
    [Input('datatable-ccccoverage', 'rows'),
     Input('datatable-ccccoverage', 'selected_row_indices')])
def app4_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('All language editions CCC articles covered by Wikipedia', 'Percentual coverage of All language editions CCC articles covered by Wikipedia', 'Average number of interwiki links in non-CCC articles by Wikipedia',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['Covered Art.'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'percent',
        'x': dff['Language'],
        'y': dff['T.Coverage'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'interwiki links',
        'x': dff['Language'],
        'y': dff['No CCC IW'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig


"""    
IDEES PER DESENVOLUPAR:
# EN FUNCIÓ DEL PARÀMETRE SET LLANCEM LA TAULA O LA BUBBLE CHART.
# https://plot.ly/python/bubble-charts/
# -> per ensenyar països
# def make_viz_lang_ccc_langs_bubble_chart(languagecode, wiki_path): # culture gap coverage
#   QUESTION: How well this language edition cover the CCC of the other language editions?
# diagrama de boles. stacked bar o formatget.
print ('* make_viz_ccc_covered')
query = 'SELECT set2, abs_value, rel_value FROM wcdo_intersections WHERE set1='+languagecode+' AND set1descriptor="ccc" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY abs_value DESC;'
"""
# Posar aquests gràfics dins de wcdo_app.py. 
# https://plot.ly/python/horizontal-bar-charts/
# -> per ensenyar el què hi ha per continents, per home i dona, etc.

# https://plot.ly/python/bubble-charts/
# -> per ensenyar països


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app5 = Dash(__name__, server = app, url_base_pathname='/ccc_spread/')

# QUESTION: How well each language edition CCC is spread in other language editions?
# language, CCC%, RANKING TOP 5, relative spread index, total spread index, spread articles sum.
# relative spread index: the average of the percentages it occupies in other languages.
# total spread index: the overall percentage of spread of the own CCC articles. (sum of x-lang CCC in every language / sum of all articles in every language)
# spread articles sum: the number of articles from this language CCC in all languages.

conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 

ccc_percent_wp = {}
query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections WHERE content="articles" AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc";'
for row in cursor.execute(query):
    value = row[1]
    if value == None: value = 0
    value2 = row[2]
    if value2 == None: value2 = 0
    ccc_percent_wp[row[0]]=value
# str(value)+' '+'('+str(round(value2,2))+'%)'

inlinkszero_spread = {}
query = 'SELECT set1, rel_value FROM wcdo_intersections WHERE content="articles" AND set1=set2 AND set1descriptor="ccc" AND set2descriptor="zero_ill" ORDER BY set1;'
for row in cursor.execute(query):
    inlinkszero_spread[row[0]]=round(row[1],2)

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
            row_dict['spread_articles_sum']=spread_art[languagecode_spreading]
        except:
            row_dict['spread_articles_sum']=0

        try:
            row_dict['links_zero']=inlinkszero_spread[languagecode_spreading]
        except:
            row_dict['links_zero']=0


        language_dict[languagecode_spreading]=row_dict
        row_dict = {}
        i = 1
#            input('')

    if i <= ranking:
        languagecode_spread=row[1]
        if languagecode_spread in languageswithoutterritory:
            i-=1;
        else:
            rel_value=round(row[2],2)

            languagecode_spread = languagecode_spread.replace('be_tarask','be_x_old')
            languagecode_spread = languagecode_spread.replace('zh_min_nan','nan')
            languagecode_spread = languagecode_spread.replace('zh_classical','lzh')
            languagecode_spread = languagecode_spread.replace('_','-')
            value = languagecode_spread + ' ('+str(rel_value)+'%)'
            row_dict[str(i)]=value
    #            print (cur_languagecode_spreading,languagecode_spread,i,value)

    languagecode_spreading = cur_languagecode_spreading
    i+=1



column_list_dict = {'language':'Language', 'CCC articles':'CCC art.','links_zero':'CCC no IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_spread_index':'R.Spread','total_spread_index':'T.Spread','spread_articles_sum':'Spread Art.'}

columns = ['Language','CCC art.','CCC no IW','nº1','nº2','nº3','nº4','nº5','R.Spread','T.Spread','Spread Art.','Region','Subregion']

df=pd.DataFrame.from_dict(language_dict,orient='index')

df['Region']=languages.region
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

df=df.rename(columns=column_list_dict)

df = df[columns] # selecting the parameters to export
df = df.fillna('')

title = "Wikipedia Language Editions' CCC Spread"
dash_app5.title = title
dash_app5.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        rows=df.to_dict('records'),
        columns = columns,
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        resizable=True,
        id='datatable-cccspread'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-cccspread'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")


@dash_app5.callback(
    Output('datatable-cccspread', 'selected_row_indices'),
    [Input('graph-cccspread', 'clickData')],
    [State('datatable-cccspread', 'selected_row_indices')])
def app5_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app5.callback(
    Output('graph-cccspread', 'figure'),
    [Input('datatable-cccspread', 'rows'),
     Input('datatable-cccspread', 'selected_row_indices')])
def app5_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('Wikipedia Language CCC articles spread to other Wikipedias', 'Extent of Wikipedia Language CCC articles existing in other Wikipedias', 'Number of CCC articles with no interwiki links by Wikipedia',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['Spread Art.'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'percent',
        'x': dff['Language'],
        'y': dff['T.Spread'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['CCC no IW'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app6 = Dash(__name__, server = app, url_base_pathname='/ccc_pageviews/')

conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 

ccc_percent_wp = {}
query = 'SELECT set1, rel_value FROM wcdo_intersections WHERE content="articles" AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc";'
for row in cursor.execute(query):
    value = row[1]
    if value == None: value = 0
    ccc_percent_wp[row[0]]=round(value,2)
# str(value)+' '+'('+str(round(value2,2))+'%)'

pageviews = {}
ccc_pageviews_percent = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND set1=set2 ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del ccc propi en el total de lang1?
for row in cursor.execute(query):
    try:
        pageviews[row[0]]=round(row[2]/row[1])+1
    except:
        pageviews[row[0]]=0
    ccc_pageviews_percent[row[0]]=round(row[1],2)

own_ccc_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='ccc' AND set2descriptor='all_top_articles' AND set1=set2 ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    own_ccc_top_pageviews[row[0]]=round(row[1],2)


all_lang_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='all_top_articles' AND set2='ccc' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    all_lang_top_pageviews[row[0]]=round(row[1],2)


language_dict={}
query = "SELECT set1, set2, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND set1!=set2 ORDER BY set1, abs_value DESC;"
# quant pesen els pageviews dl ccc d'altres?

ranking = 5
row_dict = {}
i=1
languagecode_covering='aa'
for row in cursor.execute(query):

    cur_languagecode_covering=row[0]
    if cur_languagecode_covering not in wikilanguagecodes: continue
    
    if cur_languagecode_covering!=languagecode_covering: # time to save
        row_dict['language']=languages.loc[languagecode_covering]['languagename']

        row_dict['ccc_percent_wp']=ccc_percent_wp[languagecode_covering]
        row_dict['pageviews']=pageviews[languagecode_covering]
        row_dict['ccc_pageviews_percent']=ccc_pageviews_percent[languagecode_covering]
        row_dict['own_ccc_top_pageviews']=own_ccc_top_pageviews[languagecode_covering]
        row_dict['all_lang_top_pageviews']=all_lang_top_pageviews[languagecode_covering]

        language_dict[languagecode_covering]=row_dict
        row_dict = {}
        i = 1

    if i <= ranking:
        languagecode_covered=row[1]
        if languagecode_covered in languageswithoutterritory:
            i-=1;
        else:
            rel_value=round(row[2],2)

            languagecode_covered = languagecode_covered.replace('be_tarask','be_x_old')
            languagecode_covered = languagecode_covered.replace('zh_min_nan','nan')
            languagecode_covered = languagecode_covered.replace('zh_classical','lzh')
            languagecode_covered = languagecode_covered.replace('_','-')
            value = languagecode_covered + ' ('+str(rel_value)+'%)'

            row_dict[str(i)]=value
    i+=1

    languagecode_covering = cur_languagecode_covering


column_list_dict = {'language':'Language','wp_number_articles':'Articles','ccc_percent_wp':'CCC art. %','pageviews':'Pageviews','ccc_pageviews_percent':'CCC %','own_ccc_top_pageviews':'Top CCC %','all_lang_top_pageviews':'All Top %','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5'}
column_list = ['Language','Articles','CCC art. %','Pageviews','CCC %', 'Top CCC %','All Top %','nº1','nº2','nº3','nº4','nº5','Region','Subregion']

df=pd.DataFrame.from_dict(language_dict,orient='index')

df['wp_number_articles']= pd.Series(wikipedialanguage_numberarticles)

df['Region']=languages.region
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]

df=df.rename(columns=column_list_dict)

df = df[column_list] # selecting the parameters to export
df = df.fillna('')

title = "Last month pageviews in CCC by Wikipedia language edition"
dash_app6.title = title
dash_app6.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dt.DataTable(
        rows=df.to_dict('records'),
        columns = column_list,
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-cccpageviews'

    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-cccpageviews'
    ),
    html.A(html.H5('Home - Wikipedia Cultural Diverstiy Observatory'), href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

], className="container")


@dash_app6.callback(
    Output('datatable-cccpageviews', 'selected_row_indices'),
    [Input('graph-cccpageviews', 'clickData')],
    [State('datatable-cccpageviews', 'selected_row_indices')])
def app6_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app6.callback(
    Output('graph-cccpageviews', 'figure'),
    [Input('datatable-cccpageviews', 'rows'),
     Input('datatable-cccpageviews', 'selected_row_indices')])
def app6_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=('Percentage of Pageviews in CCC articles by Wikipedia','Percentage of Pageviews in Top CCC articles by Wikipedias', 'Number of pageviews by Wikipedia',),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'articles',
        'x': dff['Language'],
        'y': dff['CCC %'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'percent',
        'x': dff['Language'],
        'y': dff['Top CCC %'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'pageviews',
        'x': dff['Language'],
        'y': dff['Pageviews'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# http://wcdo.wmflabs.org/top_ccc_articles/?list=editors&lang_origin=es&lang_target=it&country=all
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app7 = Dash(__name__, server = app, url_base_pathname='/top_ccc_articles/', external_stylesheets=external_stylesheets)
dash_app7.title = 'List of Top Articles from Cultural Context Content'
dash_app7.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@dash_app7.callback(dash.dependencies.Output('page-content', 'children'), [dash.dependencies.Input('url', 'search')]) # inspired by: location-double-callback-workaround.txt https://github.com/plotly/dash-core-components/blob/c898d75a1a189943bcf92bc73b95bd08432ddd7d/test/test_integration.py#L147
def app7_display(search):
#    lang_target = 'ca'; lang_origin = 'it'; country = 'all'; list_name = 'editors'
    
    url='http://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/'+str(search)
    arguments = parse_qs(urlparse(url).query)
    print (arguments)

    lang_origin = arguments['lang_origin'][0].lower()
    lang_target = arguments['lang_target'][0].lower()
    list_name = arguments['list'][0].lower()
    
    if 'country' in arguments:
        country = arguments['country'][0]
    else:
        country = 'all'

    if country!= 'all': country=country.upper()
    print (arguments)

    conn = sqlite3.connect(databases_path + 'top_articles.db'); cur = conn.cursor()

    columns_dict = {'position':'Nº','page_title_original':'Title','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_bytes':'Bytes','num_references':'References','num_wdproperty':'Wikidata Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_discussions':'Discussion Edits','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC','other_languages':'Other Languages','page_title_target':' Title'}

    # COLUMNS
    query = 'SELECT r.position, r.qitem, f.page_title_original, '
    columns = ['Nº','Title']

    if list_name == 'editors': 
        query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

    if list_name == 'featured': 
        query+= 'f.featured_article, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki,  '
        columns+= ['Featured Article','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

    if list_name == 'geolocated': 
        query+= 'f.num_inlinks, f.editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Inlinks','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

    if list_name == 'keywords': 
        query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki,  '
        columns+= ['Editors','Pageviews','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

    if list_name == 'women': 
        query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Edits','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

    if list_name == 'men': 
        query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Edits','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

    if list_name == 'created_first_three_years': 
        query+='f.num_editors, f.num_pageviews, f.num_edits, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Editors','Pageviews','Edits','References','Featured Article','Wikidata Properties','Interwiki Links']

    if list_name == 'created_last_year': 
        query+='f.num_editors, f.num_pageviews, f.num_edits, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Editors','Pageviews','Edits','References','Featured Article','Wikidata Properties','Interwiki Links']

    if list_name == 'pageviews': 
        query+='f.num_pageviews, f.num_edits, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Pageviews','Edits','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

    if list_name == 'discussions': 
        query+='f.num_discussions, f.num_pageviews, f.num_edits, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
        columns+= ['Discussion Edits','Pageviews','Edits','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

    query += 'f.num_inlinks_from_CCC, f.date_created, p.page_title_target, p.generation_method, p0.page_title_target pt0, p1.page_title_target pt1, p2.page_title_target pt2, p3.page_title_target pt3 '

    columns+= ['Inlinks from CCC','Creation Date']
    columns+= ['Other Languages',' Title']


    query += 'FROM ccc_'+lang_origin+'wiki_top_articles_lists r '
    query += 'LEFT JOIN ccc_'+lang_target+'wiki_top_articles_page_titles p USING (qitem) '
    query += 'LEFT JOIN ccc_'+closest_langs[lang_target][0]+'wiki_top_articles_page_titles p0 USING (qitem) '
    query += 'LEFT JOIN ccc_'+closest_langs[lang_target][1]+'wiki_top_articles_page_titles p1 USING (qitem) '
    query += 'LEFT JOIN ccc_'+closest_langs[lang_target][2]+'wiki_top_articles_page_titles p2 USING (qitem) '
    query += 'LEFT JOIN ccc_'+closest_langs[lang_target][3]+'wiki_top_articles_page_titles p3 USING (qitem) '
    query += 'INNER JOIN ccc_'+lang_origin+'wiki_top_articles_features f USING (qitem) '
    query += 'WHERE r.list_name = "'+list_name+'" '
    if country: query += 'AND r.country IS "'+country+'" '
    query += 'ORDER BY r.position ASC;'

    df = pd.read_sql_query(query, conn)#, parameters)
    df = df.fillna(0)

#    df.page_title_original = df['page_title_original'].str.replace('_',' ')

    df['page_title_original'] = df['page_title_original'].apply(lambda x: html.A( x.replace('_',' '), href='https://'+lang_origin+'.wikipedia.org/wiki/'+x, target="_blank", style={'text-decoration':'none'}))

    closest_languages = closest_langs[lang_target]
    page_titles_target = ['pt0','pt1','pt2','pt3']

    cl = len(closest_languages)

    df=df.rename(columns=columns_dict)


    df_list = list()
    for index, rows in df.iterrows():
        df_row = list()
        for col in columns:

            if list_name == 'discussions' and col == 'Discussion Edits': 
                df_row.append(html.A( rows['Discussion Edits'], href='https://'+lang_origin+'.wikipedia.org/wiki/Talk:'+rows['Title'], target="_blank", style={'text-decoration':'none'}))
            elif col == 'Interwiki Links':
                df_row.append(html.A( rows['Interwiki Links'], href='https://'+lang_origin+'.wikipedia.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

            elif col == 'Wikidata Properties':
                df_row.append(html.A( rows['Wikidata Properties'], href='https://'+lang_origin+'.wikipedia.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

            elif col == 'Other Languages':

                i = 0
                text = ''
                for x in range(cl):
                    cur_title = rows[page_titles_target[x]]
                    if cur_title!= 0:
                        if i!=0 and i!=cl:
                            text+=', '
                        text+= '['+closest_languages[x]+']'+'('+'https://'+closest_languages[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'#+'{:target="_blank"}'
                        i+=1
                df_row.append(dcc.Markdown(text))

            elif col == 'Creation Date':
                date = rows[col]
                if date == 0: 
                    date = ''
                else:
                    date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                df_row.append(date)

            elif col == ' Title':
                cur_title = rows[' Title']
                if cur_title != 0:
                    cur_title = cur_title.replace('_',' ')
                    if rows['generation_method'] == 'sitelinks':
                        df_row.append(html.A(cur_title, href='https://'+lang_target+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    else:
                        df_row.append(html.A(cur_title, href='https://'+lang_target+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none',"color":"#ba0000"}))
                else:
                    df_row.append('')
            else:
                df_row.append(rows[col])

        df_list.append(df_row)

    language_origin = languages.loc[lang_origin]['languagename']
    language_target = languages.loc[lang_target]['languagename']
    if country == 'all':
        title = 'Top articles in '+language_origin+' CCC by '+list_name + ' and their availability in '+language_target
    else:
        country_origin = country_names[country]
        title = 'Top articles in '+country_origin+' in '+language_origin+' CCC by '+list_name
    dash_app7.title = title

    col_len = len(columns)
    columns[1]=language_origin+' '+columns[1]
    columns[col_len-1]=language_target+columns[col_len-1]

    dash_app7.layout = html.Div([
        html.H3(title, style={'textAlign':'center'}),
        html.Table(
        # Header
        [html.Tr([html.Th(col) for col in columns])] +
        # Body
        [html.Tr([
            html.Td(df_row[x]) for x in range(len(columns))
        ]) for df_row in df_list])

    ])

    return dash_app7.layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# http://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/?set=ca
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app8 = Dash(__name__, server = app, url_base_pathname='/languages_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets)
dash_app8.title = 'Languages top CCC articles coverage'
dash_app8.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@dash_app8.callback(dash.dependencies.Output('page-content', 'children'), [dash.dependencies.Input('url', 'search')]) # inspired by: location-double-callback-workaround.txt https://github.com/plotly/dash-core-components/blob/c898d75a1a189943bcf92bc73b95bd08432ddd7d/test/test_integration.py#L147
def app8_display(search):

    # QUESTION: How well does this language edition cover the lists of CCC articles from other language editions?
    # COVERAGE: how well this language covers the lists from other languages?
    # TABLE COLUMN (coverage).
    # Language Covered, Wiki, Editors,Featured, Geolocated, Keywords, First 3Y, Last Y, Women, Men, Pageviews, Talk Edits, Lists Coverage Index, Covered articles sum.
    # Lists Coverage Index is the percentage of articles covered from these lists.
    url='http://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/'+str(search)
    language_covering=parse_qs(urlparse(url).query)['set'][0]

#    language_covering='ca'
    conn = sqlite3.connect(databases_path + 'top_articles.db'); cursor = conn.cursor()

    query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wcdo_intersections WHERE set2 = "'+language_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set1descriptor DESC;'

    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
    list_top = lists.copy()

    row_dict = {}
    language_dict = {}
    old_languagecode_covered = ''
    covered_list_articles_sum=0
    list_coverage_index=0

    for row in cursor.execute(query):

        languagecode_covered = row[1]
        if languagecode_covered not in wikilanguagecodes: continue
#        if languagecode_covered in wikilanguagecodes: continue

        if old_languagecode_covered!=languagecode_covered and old_languagecode_covered!='':

            row_dict['languagename']=languages.loc[old_languagecode_covered]['languagename']
            row_dict['Wiki']=old_languagecode_covered
            row_dict['list_coverage_index']=round(list_coverage_index/10,2)
            row_dict['covered_list_articles_sum']=covered_list_articles_sum

            covered_list_articles_sum=0
            list_coverage_index=0

#            print (row_dict)
            for remaining_list in list_top:
                row_dict[remaining_list] = '0/0'

            list_top = lists.copy()
            language_dict[old_languagecode_covered]=row_dict
            row_dict={}

        list_name = row[0]
        percentage = round(row[2],2)
        num_articles = row[3]

        covered_list_articles_sum += num_articles
        list_coverage_index += percentage
        
        if percentage!= 0: integer = round(100*num_articles/percentage)
        else: integer = 0

        link = str(num_articles) + '/'+str(integer)
        url = '/top_ccc_articles/?list='+list_name+'&lang_origin='+languagecode_covered+'&lang_target='+language_covering+'&country=all'

#        row_dict[list_name]= dcc.Markdown('['+link+']('+url+'){:target="_blank"}')
        row_dict[list_name]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'})

        old_languagecode_covered = languagecode_covered
        list_top.remove(list_name)


    column_list_dict = {'languagename':'Language','Wiki':'Wiki','editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y', 'created_last_year':'Last Y', 'women':'Women', 'men':'Men', 'pageviews':'Pageviews', 'discussions':'Talk Edits','list_coverage_index':'List Coverage Idx.','covered_list_articles_sum':'Sum Covered Articles','World Subregion':'World Subregion'}

    df=pd.DataFrame.from_dict(language_dict,orient='index')

    columns = ['Language','Wiki','Editors', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men', 'First 3Y', 'Last Y', 'Pageviews', 'Talk Edits','List Coverage Idx.','Sum Covered Articles','World Subregion']


    df['World Subregion']=languages.subregion
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]


    df=df.rename(columns=column_list_dict)

    df = df[columns] # selecting the parameters to export
    df = df.fillna('')

    title = 'Languages top CCC articles coverage by '+languages.loc[language_covering]['languagename']+' Wikipedia'
    dash_app8.title = title

    dash_app8.layout = html.Div([
        html.H3(title, style={'textAlign':'center'}),
        html.Table(
            # Header
            [html.Tr([html.Th(col) for col in df.columns])] +

            # Body
            [html.Tr([
                html.Td(df.iloc[i][col]) for col in df.columns
            ]) for i in range(len(df))]
        ),
        html.Div(id='selected-indexes')
    ])

    return dash_app8.layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# http://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/?set=ca
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app9 = Dash(__name__, server = app, url_base_pathname='/countries_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets)
dash_app9.title = 'Countries top CCC articles coverage'
dash_app9.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@dash_app9.callback(dash.dependencies.Output('page-content', 'children'), [dash.dependencies.Input('url', 'search')]) # inspired by: location-double-callback-workaround.txt https://github.com/plotly/dash-core-components/blob/c898d75a1a189943bcf92bc73b95bd08432ddd7d/test/test_integration.py#L147
def app9_display(search):

    url='http://wcdo.wmflabs.org/countries_top_ccc_articles_coverage/'+str(search)
#    language_covering=parse_qs(urlparse(url).query)['set'][0]

    languagecode_covering = 'ca'

    conn = sqlite3.connect(databases_path + 'top_articles.db'); cursor = conn.cursor()

    query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wcdo_intersections WHERE set2 = "'+languagecode_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set1descriptor DESC;'

    list_dict = {'editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y', 'created_last_year':'Last Y', 'women':'Women', 'men':'Men', 'pageviews':'Pageviews', 'discussions':'Talk Edits'}

    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
    list_top = lists.copy()

    row_dict = {}
    country_dict = {}
    old_covered = ''
    covered_list_articles_sum=0
    list_coverage_index=0

    for row in cursor.execute(query):

        covered = row[1]
        if '(' not in covered: continue

        if old_covered!=covered and old_covered!='':
            language_name = languages.loc[languagecode_covered]['languagename']
            country_name = country_names[country_covered]
            country_language = country_name+' ('+language_name+' CCC)'

            row_dict['Country']=country_language
            row_dict['List Coverage Idx.']=round(list_coverage_index/10,2)
            row_dict['Sum Covered Articles']=covered_list_articles_sum

#            print (row_dict)
            for remaining_list in list_top:
                row_dict[list_dict[remaining_list]] = '0/0'

            country_dict[country_name+old_covered]=row_dict

            row_dict={}
            covered_list_articles_sum=0
            list_coverage_index=0
            list_top = lists.copy()


        list_name = row[0]
        percentage = row[2]
        num_articles = row[3]

        covered_list_articles_sum += num_articles
        list_coverage_index += percentage

        if covered!='':
            parts=covered.split('_')
            country_covered = parts[0]
            if len(parts)==3:
                languagecode_covered = parts[1].replace('(','')+'_'+parts[2].replace(')','')
            elif len(parts)==4:
                languagecode_covered = parts[1].replace('(','')+'_'+parts[2]+'_'+parts[3].replace(')','')
            else:
                languagecode_covered = parts[1].replace(')','').replace('(','')
        
        if percentage!= 0: integer = round(100*num_articles/percentage)
        else: integer = 0

        link = str(num_articles) + '/'+str(integer)
        url = 'https://wcdo.wmflabs.org/top_ccc_articles/?list='+list_name+'&lang_origin='+languagecode_covered+'&lang_target='+languagecode_covering+'&country='+country_covered

        row_dict[list_dict[list_name]]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'})
        row_dict['World Subregion'] = subregions[country_covered]

        old_covered = covered
        list_top.remove(list_name)

    columns = ['Country','Editors', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men', 'First 3Y', 'Last Y', 'Pageviews', 'Talk Edits','List Coverage Idx.','Sum Covered Articles','World Subregion']

    title = 'Countries top CCC articles coverage by '+languages.loc[languagecode_covering]['languagename']+' Wikipedia'
    dash_app9.title = title

    dash_app9.layout = html.Div([
        html.H3(title, style={'textAlign':'center'}),
        html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +

            # Body
            [html.Tr([
                html.Td(
                    row[col]
                    ) for col in columns
            ]) for i,row in sorted(country_dict.items())]
        ),
        html.Div(id='selected-indexes')
    ])

    return dash_app9.layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# http://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/?set=ca
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

dash_app10 = Dash(__name__, server = app, url_base_pathname='/languages_top_ccc_articles_spread/', external_stylesheets=external_stylesheets)
dash_app10.title = 'Languages top CCC articles coverage'
dash_app10.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@dash_app10.callback(dash.dependencies.Output('page-content', 'children'), [dash.dependencies.Input('url', 'search')]) # inspired by: location-double-callback-workaround.txt https://github.com/plotly/dash-core-components/blob/c898d75a1a189943bcf92bc73b95bd08432ddd7d/test/test_integration.py#L147
def app10_display(search):

#    url='http://wcdo.wmflabs.org/languages_top_ccc_articles_spread/'+str(search)
#    languagecode_spread=parse_qs(urlparse(url).query)['set'][0]

    languagecode_spread='ca'
    conn = sqlite3.connect(databases_path + 'top_articles.db'); cursor = conn.cursor()

    query = 'SELECT set1descriptor, set2, rel_value, abs_value FROM wcdo_intersections WHERE set1="'+languagecode_spread+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set2;'

    lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
    list_top = lists.copy()

    row_dict = {}
    language_dict = {}
    old_languagecode_spreadin = ''
    spread_list_articles_sum=0
    list_spread_index=0

    for row in cursor.execute(query):

        languagecode_spreadin = row[1]
        if languagecode_spreadin not in wikilanguagecodes: continue
#        if languagecode_covered in wikilanguagecodes: continue

        if old_languagecode_spreadin!=languagecode_spreadin and old_languagecode_spreadin!='':

            row_dict['languagename']=languages.loc[old_languagecode_spreadin]['languagename']
            row_dict['Wiki']=old_languagecode_spreadin
            row_dict['list_spread_index']=round(list_spread_index/10,2)
            row_dict['spread_list_articles_sum']=spread_list_articles_sum

            spread_list_articles_sum=0
            list_spread_index=0

#            print (row_dict)
            for remaining_list in list_top:
                row_dict[remaining_list] = '0/100'

            list_top = lists.copy()
            language_dict[old_languagecode_spreadin]=row_dict
            row_dict={}

        list_name = row[0]
        percentage = round(row[2],2)
        num_articles = row[3]

        spread_list_articles_sum += num_articles
        list_spread_index += percentage
        
        if percentage!= 0: integer = round(100*num_articles/percentage)
        else: integer = 0

        link = str(num_articles) + '/'+str(integer)
        url = '/top_ccc_articles/?list='+list_name+'&lang_origin='+languagecode_spread+'&lang_target='+languagecode_spreadin+'&country=all'

#        row_dict[list_name]= dcc.Markdown('['+link+']('+url+'){:target="_blank"}')
        row_dict[list_name]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'})

        old_languagecode_spreadin = languagecode_spreadin
        list_top.remove(list_name)


    column_list_dict = {'languagename':'Language','Wiki':'Wiki','editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y', 'created_last_year':'Last Y', 'women':'Women', 'men':'Men', 'pageviews':'Pageviews', 'discussions':'Talk Edits','list_spread_index':'List Spread Idx.','spread_list_articles_sum':'Sum Spread Articles','World Subregion':'World Subregion'}

    df=pd.DataFrame.from_dict(language_dict,orient='index')

    df['World Subregion']=languages.subregion
    for x in df.index.values.tolist():
        if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]

    columns = ['Language','Wiki','Editors', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men', 'First 3Y', 'Last Y', 'Pageviews', 'Talk Edits','List Spread Idx.','Sum Spread Articles','World Subregion']

    df=df.rename(columns=column_list_dict)

    df = df[columns] # selecting the parameters to export
    df = df.fillna('')

    title = languages.loc[languagecode_spread]['languagename']+' CCC articles spread into the rest of Wikipedias'
    dash_app10.title = title

    dash_app10.layout = html.Div([
        html.H3(title, style={'textAlign':'center'}),
        html.Table(
            # Header
            [html.Tr([html.Th(col) for col in df.columns])] +

            # Body
            [html.Tr([
                html.Td(df.iloc[i][col]) for col in df.columns
            ]) for i in range(len(df))]
        ),
        html.Div(id='selected-indexes')
    ])

    return dash_app10.layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###






# FLASK APP
@app.errorhandler(404)
def handling_page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404
