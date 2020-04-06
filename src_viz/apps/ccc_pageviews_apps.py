import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app6 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_pageviews/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor() 

ccc_percent_wp = {}
query = 'SELECT set1, rel_value FROM wcdo_intersections_accumulated WHERE content="articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc";'
for row in cursor.execute(query):
    value = row[1]
    if value == None: value = 0
    ccc_percent_wp[row[0]]=round(value,2)
# str(value)+' '+'('+str(round(value2,2))+'%)'

pageviews = {}
ccc_pageviews_percent = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections_monthly WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND set1=set2 ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del ccc propi en el total de lang1?
for row in cursor.execute(query):
    try:
        pageviews[row[0]]=round(row[2]/row[1])+1
    except:
        pageviews[row[0]]=0
    ccc_pageviews_percent[row[0]]=round(row[1],1)

own_ccc_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value, period FROM wcdo_intersections_monthly WHERE period IN (SELECT MAX(period) FROM wcdo_intersections_monthly) AND content='pageviews' AND set1descriptor='ccc' AND set2='top_articles_lists' AND set2descriptor='pageviews' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    own_ccc_top_pageviews[row[0]]=round(row[1],1)

"""
all_lang_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections_accumulated WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='all_top_articles' AND set2='ccc' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    all_lang_top_pageviews[row[0]]=round(row[1],1)
"""

language_dict={}
query = "SELECT set1, set2, rel_value, abs_value FROM wcdo_intersections_monthly WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND set1!=set2 ORDER BY set1, abs_value DESC;"
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

        try: ccc_percent_value = ccc_percent_wp[languagecode_covering]
        except: ccc_percent_value = 0
        row_dict['ccc_percent_wp']=ccc_percent_value

        try: pageviews_value = pageviews[languagecode_covering]
        except: pageviews_value = 0
        row_dict['pageviews']=pageviews_value

        try: ccc_pageviews_percent_value = ccc_pageviews_percent[languagecode_covering]
        except: ccc_pageviews_percent_value = 0
        row_dict['ccc_pageviews_percent']=ccc_pageviews_percent_value

        try: own_ccc_top_pageviews_value = own_ccc_top_pageviews[languagecode_covering]
        except: own_ccc_top_pageviews_value = 0
        row_dict['own_ccc_top_pageviews']=own_ccc_top_pageviews_value

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

df['id'] = df['Language']
df.set_index('id', inplace=True, drop=False)

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


    dash_table.DataTable(
        id='datatable-cccpageviews',
        columns=[
            {'name': i, 'id': i, 'deletable': True} for i in df.columns
            # omit the id column
            if i != 'id'
        ],
        data=df.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        row_deletable=True,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 10,
        style_data={
            'whiteSpace': 'normal',
            'height': 'auto'
        },

    ),
    html.Br(),
    html.Br(),

    html.Div(id='datatable-cccpageviews-container')

], className="container")



@dash_app6.callback(
    Output('datatable-cccpageviews', 'style_data_conditional'),
    [Input('datatable-cccpageviews', 'selected_columns')]
)
def update_styles(selected_columns):

    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


@dash_app6.callback(
    Output('datatable-cccpageviews-container', 'children'),
    [Input('datatable-cccpageviews', 'derived_virtual_data'),
     Input('datatable-cccpageviews', 'derived_virtual_selected_rows')])
def update_graphs(rows, derived_virtual_selected_rows):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncracy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []


    dff = df if rows is None else pd.DataFrame(rows)

    colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff))]

    title = {'CCC %':'Percentage of Pageviews in CCC articles by Wikipedia','Top CCC pageviews':'Percentage of Pageviews in Top CCC articles list (Pageviews) by Wikipedia', 'Pageviews':'Number of pageviews by Wikipedia'}

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff["Language"],
                        "y": dff[column],
                        "type": "bar",
                        "marker": {"color": colors},
                    }
                ],
                "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                    "height": 400,
                    "margin": {"t": 60, "l": 10, "r": 10, "b": 130},
                    "title": title[column],
                },
            },
        )
        # check if column exists - user may have deleted it
        # If `column.deletable=False`, then you don't
        # need to do this check.
        for column in ['CCC %','Top CCC pageviews', 'Pageviews'] if column in dff
    ]


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###