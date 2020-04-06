import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app2 = Dash(__name__, server = app, url_base_pathname= webtype + '/list_of_wikipedias_by_cultural_context_content/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor()

df = pd.DataFrame(wikilanguagecodes)
df = df.set_index(0)
df['wp_number_articles']= pd.Series(wikipedialanguage_numberarticles)

# CCC %
query = 'SELECT set1, abs_value, rel_value FROM wcdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2descriptor = "ccc" AND content = "articles" AND set1=set2 AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY abs_value DESC;'
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

column_list = ['Nº','Language','Wiki','Articles','CCC art.','CCC %','Subregion','Region']
columns_dict = {'Language':'Language','wp_number_articles':'Articles','ccc_number_articles':'CCC art.','ccc_percent':'CCC %'}

df = df.reset_index()
df = df.rename(columns={0: 'Wiki'})
df = df.rename(columns=columns_dict)
df = df[column_list]

df = df.fillna('')

df['id'] = df['Language']
df.set_index('id', inplace=True, drop=False)

title = 'Lists of Wikipedias by Cultural Context Content'
dash_app2.title = title+title_addenda
dash_app2.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''This page contains a list of all the current Wikipedia language editions ordered by their number of articles from their Cultural Context Content dataset that relate to territories where the language is spoken as official or as indigeneous.

    For each language edition, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles. This is **(CCC art.)** and **CCC (%)** as the number of CCC articles and percentage, **CCC GL (%)** as the number of articles from CCC that are geolocated, **KW Title (%)** as the number of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', '')),
#    containerProps={'textAlign':'center'}),

    dash_table.DataTable(
        id='datatable-cccextent',
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
    html.Div(id='datatable-cccextent-container'),

], className="container")


@dash_app2.callback(
    Output('datatable-cccextent', 'style_data_conditional'),
    [Input('datatable-cccextent', 'selected_columns')]
)
def update_styles(selected_columns):
    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]

@dash_app2.callback(
    Output('datatable-cccextent-container', "children"),
    [Input('datatable-cccextent', "derived_virtual_data"),
     Input('datatable-cccextent', "derived_virtual_selected_rows")])
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


    title = {'Articles':'Wikipedia Articles', 'CCC art.':'CCC Articles', 'CCC %':'Percentage of CCC'}

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
                    "xaxis": {"automargin": False},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                    "height": 330,
                    "margin": {"t": 60, "b": 70, "l": 10, "r": 10},
                    "title":title[column],
                },
            },
        )
        # check if column exists - user may have deleted it
        # If `column.deletable=False`, then you don't
        # need to do this check.
        for column in ['Articles','CCC art.','CCC %'] if column in dff
    ]






### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app3 = Dash(__name__, server = app, url_base_pathname= webtype + '/list_of_language_territories_by_cultural_context_content/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

#   QUESTION: What is the extent of Cultural Context Content in each language edition broken down to territories?    # OBTAIN THE DATA.
conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor()

# CCC
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode, set2descriptor as Qitem, abs_value as CCC_articles, ROUND(rel_value,2) CCC_percent FROM wcdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, set2descriptor DESC;'
df1 = pd.read_sql_query(query, conn)
df1 = df1.set_index('id')

# GL
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode2, set2descriptor as Qitem2, abs_value as CCC_articles_GL, ROUND(rel_value,2) CCC_percent_GL FROM wcdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc_geolocated" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, set2descriptor DESC;'
df2 = pd.read_sql_query(query, conn)
df2 = df2.set_index('id')

# KW
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode3, set2descriptor as Qitem3, abs_value as CCC_articles_KW, ROUND(rel_value,2) CCC_percent_KW FROM wcdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc_keywords" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, set2descriptor DESC;'
df3 = pd.read_sql_query(query, conn)
df3 = df3.set_index('id')

dfx = pd.concat([df1, df3, df2], axis=1)

dfx = dfx.fillna('')
dfx = dfx.reset_index()

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
        current = ['Not Assigned',None,None,None,None,None,None,languagename]
        pass
    territoriesx.append(current)
columns.append('languagename')
all_territories = pd.DataFrame.from_records(territoriesx, columns=columns)

df = pd.concat([dfx, all_territories], axis=1)



columns_dict = {'languagename':'Language','languagecode':'Wiki','Qitem':'Qitem','CCC_articles':'CCC art.','CCC_percent':'CCC %','CCC_articles_GL':'CCC GL art.','CCC_percent_GL':'CCC GL %','CCC_articles_KW':'CCC art. KW','CCC_percent_KW':'CCC KW %','territoryname':'Territory name','territorynameNative':'Territory name (native)','country':'country','ISO3166':'ISO3166','ISO31662':'ISO3166-2','subregion':'subregion','region':'region'}
df=df.rename(columns=columns_dict)


columns = ['Qitem','Territory name','Language','Wiki','CCC art.','CCC %','CCC GL art.','CCC art. KW','ISO3166','ISO3166-2','subregion','region']
df = df[columns]




df = df.fillna('')

df['Territory name wiki'] = df['Territory name']+' ('+df['Wiki']+')'

title = 'List of Language Territories by Cultural Context Content'
dash_app3.title = title+title_addenda
dash_app3.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''This page contains each Wikipedia language edition Cultural Context Content divided in its territories according to the language territories mapping. Articles are assigned to territories according to the different strategies that have been used to include them into CCC. The label Not Assigned is for those articles which were not possible to classify.

    For each territory, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles. This is **(CCC art.)** and **CCC (%)** as the number of CCC articles and percentage, **CCC GL (%)** as the number of articles from CCC that are geolocated, **KW Title (%)** as the number of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', '')),
 
    dash_table.DataTable(
        id='datatable-cccextentqitem',
        columns=[
            {'name': i, 'id': i, 'deletable': True} for i in columns
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
    html.Div(id='datatable-cccextentqitem-container')

], className="container")



@dash_app3.callback(
    Output('datatable-cccextentqitem', 'style_data_conditional'),
    [Input('datatable-cccextentqitem', 'selected_columns')]
)
def update_styles(selected_columns):

    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


@dash_app3.callback(
    Output('datatable-cccextentqitem-container', 'children'),
    [Input('datatable-cccextentqitem', 'derived_virtual_data'),
     Input('datatable-cccextentqitem', 'derived_virtual_selected_rows')])
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

    title = {'CCC art.':'CCC Articles by Qitems in Wikipedias', 'CCC %':'CCC % by Qitems in Wikipedias', 'CCC GL art.':'CCC GL art. by Qitems in Wikipedias'}

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff["Territory name wiki"],
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
        for column in ['CCC art.','CCC %', 'CCC GL art.'] if column in dff
    ]


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###