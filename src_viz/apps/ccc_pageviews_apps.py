import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app6 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_pageviews/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app6.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

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
    ccc_pageviews_percent[row[0]]=round(row[1],1)

own_ccc_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='ccc' AND set2='top_articles_lists' AND set2descriptor='pageviews' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    own_ccc_top_pageviews[row[0]]=round(row[1],1)

"""
all_lang_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='all_top_articles' AND set2='ccc' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    all_lang_top_pageviews[row[0]]=round(row[1],1)
"""

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
#        row_dict['all_lang_top_pageviews']=all_lang_top_pageviews[languagecode_covering]

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


column_list_dict = {'language':'Language','wp_number_articles':'Articles','ccc_percent_wp':'CCC art. %','pageviews':'Pageviews','ccc_pageviews_percent':'CCC %','own_ccc_top_pageviews':'Top CCC pageviews','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5'}
column_list = ['Language','Articles','CCC art. %','Pageviews','CCC %', 'Top CCC pageviews','nº1','nº2','nº3','nº4','nº5','Region','Subregion']

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
dash_app6.title = title+title_addenda
dash_app6.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows the distribution of last month pageviews in different groups of CCC articles for 
        all the Wikipedia language editions.

        The following table is particularly useful in order to understand the importance of cultural context content
        for each Wikipedia language edition. Specifically, it shows for each language edition the relative popularity 
        of the own CCC articles as well as that from the CCC articles originary from other language editions.

        Languages are sorted in alphabetic order by their name, and the columns present the following 
        statistics: the number of articles in the Wikipedia language edition (**Articles**), the percentage of CCC articles (**CCC art %**), the number of pageviews (**Pageviews**), the percentage of pageviews dedicated to CCC articles (**CCC %**), the percentage of pageviews dedicated to the language edition Top CCC articles (**Top CCC %**) (taking into account the first hundred articles from each list), the percentage of pageviews dedicated to all the Top CCC articles from all language editions (**All Top%**) including the own, and the percentage of pageviews dedicated to the **first five other language CCC**. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', '')),

#    containerProps={'textAlign':'center'}),
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
    dcc.Markdown(
    '''Tags: #representation #ccc #pageviews'''.replace('  ', '')),

#    containerProps={'textAlign':'center'}),
    html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

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
        subplot_titles=('Percentage of Pageviews in CCC articles by Wikipedia','Percentage of Pageviews in Top CCC articles list (Pageviews) by Wikipedia', 'Number of pageviews by Wikipedia',),
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
        'y': dff['Top CCC pageviews'],
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