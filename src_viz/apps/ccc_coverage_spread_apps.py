import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

# M'imagino que en funció d'un paràmetre /ccc_coverage/lang=? podríem arribar a fer un dashboard personalitzat pels del cover i pels del spread.




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app4 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_coverage/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app4.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

conn = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor = conn.cursor() 
conn2 = sqlite3.connect(databases_path + 'ccc.db'); cursor2 = conn2.cursor() 

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
    t_coverage[row[0]]=round(row[2],1)

r_coverage = {}
query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE set1="all_ccc_avg" AND set1descriptor="" AND set2descriptor="wp" ORDER BY set2;'
for row in cursor.execute(query):
    r_coverage[row[0]]=round(row[1],1)


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
            rel_value=round(row[2],1)

            languagecode_covered = languagecode_covered.replace('be_tarask','be_x_old')
            languagecode_covered = languagecode_covered.replace('zh_min_nan','nan')
            languagecode_covered = languagecode_covered.replace('zh_classical','lzh')
            languagecode_covered = languagecode_covered.replace('_','-')
            value = languagecode_covered + ' ('+str(rel_value)+'%)'

            row_dict[str(i)]=value
    i+=1

    languagecode_covering = cur_languagecode_covering


column_list_dict = {'language':'Language', 'WP articles':'Articles','avg_iw':'No-CCC IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_coverage_index':'R.Coverage','total_coverage_index':'T.Coverage','coverage_articles_sum':'Covered Art.'}
column_list = ['Language','Articles','No-CCC IW','nº1','nº2','nº3','nº4','nº5','R.Coverage','T.Coverage','Covered Art.','Region','Subregion']

df=pd.DataFrame.from_dict(language_dict,orient='index')

df['Region']=languages.region
for x in df.index.values.tolist():
#    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].replace(';',', ')

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
#    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].replace(';',', ')
    
df=df.rename(columns=column_list_dict)

df = df[column_list] # selecting the parameters to export
df = df.fillna('')

df = df.sort_values('Language')


title = "Wikipedia Language Editions' CCC Coverage"
dash_app4.title = title+title_addenda
dash_app4.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows some stastistics that explain how well each Wikipedia language edition covers 
        the [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) articles from the other language editions.

        The following table is particularly useful in order to understand the content culture gap between 
        language editions, that is the imbalances across languages editions in content representing each 
        language cultural context. Specifically, it shows how well each language edition covers the other 
        language editions CCC by counting the number of CCC articles they have available.

        Languages are sorted in alphabetic order by their Wikicode, and the columns present the following 
        statistics: the number of articles in the Wikipedia language edition (**Articles**), the average number of interwiki links of not own CCC articles (**No-CCC IW**), the **first 
        five other languages CCC** in terms of most articles covered and the percentage of coverage computed 
        according to the total number of CCC articles of those language edition, the relative coverage 
        (**R. Coverage**) of all languages CCC computed as the average of each language edition CCC percentage 
        of coverage, the total coverage (**T. Coverage**) of all languages CCC computed as the percentage of 
        coverage of all the articles that belong to other language editions CCC, and the total number of articles 
        covered (**Covered Art.**) that belong other language editions CCC.
        '''.replace('  ', '')),


#    containerProps={'textAlign':'center'}),
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
    dcc.Markdown(
    '''Tags: #gaps #ccc #coverage'''.replace('  ', '')),
#    containerProps={'textAlign':'center'}),

    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

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
dash_app5 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_spread/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app5.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

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
    t_spread[row[0]]=round(row[2],1)


r_spread = {}
query = 'SELECT set2, rel_value FROM wcdo_intersections WHERE content="articles" AND set1="all_wp_avg" AND set1descriptor="" AND set2descriptor="ccc" ORDER BY set2;'
for row in cursor.execute(query):
    r_spread[row[0]]=round(row[1],1)


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
            rel_value=round(row[2],1)

            languagecode_spread = languagecode_spread.replace('be_tarask','be_x_old')
            languagecode_spread = languagecode_spread.replace('zh_min_nan','nan')
            languagecode_spread = languagecode_spread.replace('zh_classical','lzh')
            languagecode_spread = languagecode_spread.replace('_','-')
            value = languagecode_spread + ' ('+str(rel_value)+'%)'
            row_dict[str(i)]=value
    #            print (cur_languagecode_spreading,languagecode_spread,i,value)

    languagecode_spreading = cur_languagecode_spreading
    i+=1



column_list_dict = {'language':'Language', 'CCC articles':'CCC art.','links_zero':'CCC% no IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_spread_index':'R.Spread','total_spread_index':'T.Spread','spread_articles_sum':'Spread Art.'}

columns = ['Language','CCC art.','CCC% no IW','nº1','nº2','nº3','nº4','nº5','R.Spread','T.Spread','Spread Art.','Region','Subregion']

df=pd.DataFrame.from_dict(language_dict,orient='index')

df['Region']=languages.region
for x in df.index.values.tolist():
#    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]
    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].replace(';',', ')

df['Subregion']=languages.subregion
for x in df.index.values.tolist():
#    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]
    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].replace(';',', ')

df=df.rename(columns=column_list_dict)

df = df[columns] # selecting the parameters to export
df = df.fillna('')
df = df.sort_values('Language')

title = "Wikipedia Language Editions' CCC Spread"
dash_app5.title = title+title_addenda
dash_app5.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows some stastistics that explain how well each Wikipedia language edition 
        [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) articles 
        are spread across other languages.

        The following table is particularly useful in order to understand the content **culture gap** 
        between language editions, that is the imbalances across languages editions in content representing 
        cultural context. Specifically, it shows which language CCC is more popular among all Wikipedia 
        language editions by counting in each language edition the number of CCC articles spread across the other languages. 

        Languages are sorted in alphabetic order by their Wikicode, and the columns present the following 
        statistics: (**CCC art.**) the number of CCC articles and the percentage it occupies in the language 
        computed in relation to their total number of articles, the percentage of articles in a language CCC with no interwiki links (**CCC% no IW**), the **first five other languages** covering more 
         articles from the language CCC and the percentage they occupy in relation to their total number of articles, the relative spread (**R. Spread**) of a language CCC across 
        all the other languages computed as the average of the percentage they occupy in each other language 
        edition, the total spread (**T. Spread**) of a CCC across all the other languages computed as the 
        percentage in relation to all languages articles (not counting the own), and finally, the total number 
        of language CCC articles (Spread Art.) that exists across all the other language editions.'''.replace('  ', '')),
#    containerProps={'textAlign':'center'}),

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

    dcc.Markdown(
    '''Tags: #gaps #ccc #spread'''.replace('  ', '')),
#    containerProps={'textAlign':'center'}),

    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

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
        subplot_titles=('Wikipedia Language CCC articles spread across other Wikipedias', 'Extent of Wikipedia Language CCC articles existing in other Wikipedias', 'Number of CCC articles with no interwiki links by Wikipedia',),
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
        'y': dff['CCC% no IW'],
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