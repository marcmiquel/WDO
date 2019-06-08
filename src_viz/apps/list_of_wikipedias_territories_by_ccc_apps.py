import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app2 = Dash(__name__, server = app, url_base_pathname= webtype + '/list_of_wikipedias_by_cultural_context_content/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app2.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

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
dash_app2.title = title+title_addenda
dash_app2.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''This page contains a list of all the current Wikipedia language editions ordered by their number of articles from their Cultural Context Content dataset that relate to territories where the language is spoken as official or as indigeneous.

    For each language edition, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles. This is **(CCC art.)** and **CCC (%)** as the number of CCC articles and percentage, **CCC GL (%)** as the number of articles from CCC that are geolocated, **KW Title (%)** as the number of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', ''),
    containerProps={'textAlign':'center'}),
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
    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

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
dash_app3 = Dash(__name__, server = app, url_base_pathname= webtype + '/list_of_language_territories_by_cultural_context_content/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app3.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

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

columns = ['Qitem','Territory name','Language','Wiki','CCC art.','CCC %','CCC GL art.','CCC art. KW','ISO3166','ISO3166-2','subregion','region']

title = 'List of Language Territories by Cultural Context Content'
dash_app3.title = title+title_addenda
dash_app3.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''This page contains each Wikipedia language edition Cultural Context Content divided in its territories according to the language territories mapping. Articles are assigned to territories according to the different strategies that have been used to include them into CCC. The label Not Assigned is for those articles which were not possible to classify.

    For each territory, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles. This is **(CCC art.)** and **CCC (%)** as the number of CCC articles and percentage, **CCC GL (%)** as the number of articles from CCC that are geolocated, **KW Title (%)** as the number of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', ''),
    containerProps={'textAlign':'center'}),

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
    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

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