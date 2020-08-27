import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



#### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

#### CCC DATA
conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor()

df = pd.DataFrame(wikilanguagecodes)
df = df.set_index(0)
reformatted_wp_numberarticles = {}
df['wp_number_articles']= pd.Series(wikipedialanguage_numberarticles)

# CCC %
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2descriptor = "ccc" AND content = "articles" AND set1=set2 AND period ="'+last_period+'" ORDER BY abs_value DESC;'
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

df['Continent']=languages.region
for x in df.index.values.tolist():
    try:
        if ';' in df.loc[x]['Continent']: df.at[x, 'Continent'] = df.loc[x]['Continent'].split(';')[0]
    except:
        print (x+' ccc test.')


df['Subcontinent']=languages.subregion
for x in df.index.values.tolist():
    if ';' in df.loc[x]['Subcontinent']: df.at[x, 'Subcontinent'] = df.loc[x]['Subcontinent'].split(';')[0]


# CCC (GL %) 
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2descriptor = "ccc_geolocated" AND content = "articles" AND set1=set2 AND period ="'+last_period+'" ORDER BY rel_value DESC;'
abs_rel_value_dict = {}
for row in cursor.execute(query): abs_rel_value_dict[row[0]]= round(row[2],2)# ' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
df['geolocated_number_articles'] = pd.Series(abs_rel_value_dict)

# CCC KW %
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2descriptor = "ccc_keywords" AND content = "articles" AND set1=set2 AND period ="'+last_period+'" ORDER BY rel_value DESC;'
abs_rel_value_dict = {}
for row in cursor.execute(query):
    abs_rel_value_dict[row[0]]= round(row[2],2)#' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
df['keyword_title'] = pd.Series(abs_rel_value_dict)

# CCC People %
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2descriptor = "ccc_people" AND content = "articles" AND set1=set2 AND period ="'+last_period+'" ORDER BY rel_value DESC;'
abs_rel_value_dict = {}
for row in cursor.execute(query):
    abs_rel_value_dict[row[0]]= round(row[2],2)#' '+str('{:,}'.format(int(row[1]))+' '+'<small>('+str(round(row[2],2))+'%)</small>')
df['people_ccc_wp_percent'] = pd.Series(abs_rel_value_dict)

# CCC Female %
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2descriptor = "female" AND content = "articles" AND set1=set2  AND period ="'+last_period+'" ORDER BY rel_value DESC;'
female_abs_value_dict = {}
for row in cursor.execute(query):
    female_abs_value_dict[row[0]]=row[1]
df['female_ccc'] = pd.Series(female_abs_value_dict)

# CCC Male %
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2descriptor = "male" AND content = "articles" AND set1=set2 AND period ="'+last_period+'" ORDER BY rel_value DESC'
male_abs_value_dict = {}
for row in cursor.execute(query):
    male_abs_value_dict[row[0]]=row[1]
df['male_ccc'] = pd.Series(male_abs_value_dict)


people_CCC = {}
female_male_CCC={}
for x in df.index.values.tolist():
    fccc = 0
    mccc = 0
    mccc = df.loc[x]['male_ccc']

    try:
        mccc = df.loc[x]['male_ccc']
    except:
        mccc = 0

#    fccc = df.loc[x]['female_ccc']
    try:
        fccc = df.loc[x]['female_ccc']
    except:
        fccc = 0

#    print (mccc,fccc)
    sumpeople = mccc+fccc
#    print (sumpeople)

    try:
        female_male_CCC[x] = round(100*int(fccc)/sumpeople,1)
    except:
        female_male_CCC[x] = 0

    try:
        people_CCC[x] = round(100*float(sumpeople/df.loc[x]['ccc_number_articles']),1)
    except:
        people_CCC[x] = 0

df['female-male_ccc'] = pd.Series(female_male_CCC)
df['people_ccc_percent'] = pd.Series(people_CCC)



columns_dict = {'wp_number_articles':'Articles','ccc_number_articles':'CCC art.','ccc_percent':'CCC %','geolocated_number_articles':'CCC (GL %)','keyword_title':'CCC (KW Title %)','female-male_ccc':'CCC People (Women %)','people_ccc_percent':'CCC (People %)'}
df = df.rename(columns=columns_dict)
df = df.reset_index()

df = df.rename(columns={0: 'Wiki'})
df = df.rename(columns=columns_dict)

df = df.fillna('')

df['id'] = df['Language']
df.set_index('id', inplace=True, drop=False)

df = df.fillna('')

columns = ['Nº','Language','Wiki','Articles','CCC art.','CCC %','CCC (GL %)','CCC (KW Title %)','CCC (People %)','CCC People (Women %)','Subcontinent','Continent']
df = df[columns] # selecting the parameters to export
df = df.loc[(df['Language']!='')]

df = df.sort_values(by=['CCC art.'], ascending = False)




#### TERRITORIES CCC DATA
# CCC
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode, set2descriptor as Qitem, abs_value as CCC_articles, ROUND(rel_value,2) CCC_percent FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc" AND content = "articles" AND period ="'+last_period+'" ORDER BY set1, set2descriptor DESC;'
df1 = pd.read_sql_query(query, conn)
df1 = df1.set_index('id')

# GL
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode2, set2descriptor as Qitem2, abs_value as CCC_articles_GL, ROUND(rel_value,2) CCC_percent_GL FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc_geolocated" AND content = "articles" AND period ="'+last_period+'" ORDER BY set1, set2descriptor DESC;'
df2 = pd.read_sql_query(query, conn)
df2 = df2.set_index('id')

# KW
query = 'SELECT set1 || " " || set2descriptor as id, set1 as languagecode3, set2descriptor as Qitem3, abs_value as CCC_articles_KW, ROUND(rel_value,2) CCC_percent_KW FROM wdo_intersections_accumulated WHERE set1descriptor = "ccc" AND set2 = "ccc_keywords" AND content = "articles" AND period ="'+last_period+'" ORDER BY set1, set2descriptor DESC;'
df3 = pd.read_sql_query(query, conn)
df3 = df3.set_index('id')

dfx = pd.concat([df1, df3, df2], axis=1, sort=True)

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

df_territories = pd.concat([dfx, all_territories], axis=1, sort=True)



columns_dict = {'languagename':'Language','languagecode':'Wiki','Qitem':'Qitem','CCC_articles':'CCC art.','CCC_percent':'CCC %','CCC_articles_GL':'CCC GL art.','CCC_percent_GL':'CCC (GL %)','CCC_percent_KW':'CCC (KW %)', 'CCC_articles_KW':'CCC art. KW','territoryname':'Territory name','territorynameNative':'Territory name (native)','country':'country','ISO3166':'ISO3166','ISO31662':'ISO3166-2','subregion':'Subcontinent','region':'Continent'}
df_territories=df_territories.rename(columns=columns_dict)


columns_territory = ['Qitem','Territory name','Language','Wiki','CCC art.','CCC %','CCC GL art.','CCC (KW %)','ISO3166','ISO3166-2','Subcontinent','Continent']
df_territories = df_territories[columns_territory]
df_territories = df_territories.fillna('')
df_territories['Territory name wiki'] = df_territories['Territory name']+' ('+df_territories['Wiki']+')'




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app2 = Dash(__name__, server = app, url_base_pathname= webtype + '/cultural_context_content/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

title = 'Cultural Context Content (CCC) in Wikipedia Language Editions'
dash_app2.title = title+title_addenda
dash_app2.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),

    dcc.Markdown('''
        This page shows statistics and graphs that explain the extent and composition of [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) in Wikipedia language editions. Cultural Context Content is the group of articles in a Wikipedia language edition that relates to the editors' geographical and cultural context (places, traditions, language, politics, agriculture, biographies, events, etcetera.). This is often known as **"local content"**.
        '''),


    # dcc.Markdown('''
    #     The statistics and graphs answer the following questions:
    #     * What is the extent of CCC in each Wikipedia language edition?
    #     * What is the extent of the topics about each language associated territories in the language CCC?
    #     '''),

    html.Br(),

    dcc.Tabs([
        dcc.Tab(label='Extent of Cultural Context Content in each Wikipedia (Table)', children=[
            html.Br(),

            # html.H5('Extent of Cultural Context Content in each Wikipedia', style={'textAlign':'left'}),

            dcc.Markdown(
            '''
            * **What is the extent of CCC in each Wikipedia language edition?**        

            The following table contains a list of all the current Wikipedia language editions ordered by their number of articles from their Cultural Context Content dataset that relate to territories where the language is spoken as official or as indigeneous.

            For each language edition, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles: **(CCC art.)** and **CCC (%)** as the number and percentage of CCC articles, **CCC (GL %)** as the percentage of articles from CCC that are geolocated, **CCC (KW Title %)** as the percentage of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles, CCC (People %) as the percentage of articles from CCC that are about people, CCC People (Women %) as the percentage of articles in CCC people articles that are about women. Finally, **Continent** and **Subcontinent** are introduced in order to contextualize the results.



            '''.replace('  ', '')),
            html.Br(),

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
        ]),

        dcc.Tab(label="Extent of Language Related Territories in CCC (Table)", children=[
            html.Br(),

            # html.H5('Extent of Language Related Territories in CCC (Table)', style={'textAlign':'left'}),

            dcc.Markdown(
            '''
            * **What is the extent of the topics about each language speaking territories in the language CCC?**

            The following table contains each Wikipedia language edition Cultural Context Content and the extent of content dedicated to topics related the territories where the language is spoken according to the language-territories mapping. Articles from each language CCC are assigned to territories according to the different strategies that have been used to include them into CCC. The label Not Assigned is for those articles which were not possible to classify.

            For each territory, statistics account for the number of articles of different CCC segments and their percentage computed in relation to the overall total number of Wikipedia articles. This is **(CCC art.)** and **CCC (%)** as the number of CCC articles and percentage, **CCC GL (%)** as the number of articles from CCC that are geolocated, **KW Title (%)** as the number of articles from CCC that contain specific keywords (language name, territory name or demonym) in their titles. Finally, **Region** (continent) and **Subregion** are introduced in order to contextualize the results.'''.replace('  ', '')),
            html.Br(),

            dash_table.DataTable(
                id='datatable-cccextentqitem',
                columns=[
                    {'name': i, 'id': i, 'deletable': True} for i in columns_territory
                    # omit the id column
                    if i != 'id'
                ],
                data=df_territories.to_dict('records'),
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
        ]),

    ]),

    footbar,

], className="container")



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


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
                    "title": {"text": title[column],
                             "font": {"size": 12}},
                    "xaxis": {"automargin": False},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                    "height": 330,
                    "margin": {"t": 60, "b": 70, "l": 10, "r": 10},

                },
            },
        )
        for column in ['Articles','CCC art.','CCC %'] if column in dff
    ]





@dash_app2.callback(
    Output('datatable-cccextentqitem', 'style_data_conditional'),
    [Input('datatable-cccextentqitem', 'selected_columns')]
)
def update_styles(selected_columns):

    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


@dash_app2.callback(
    Output('datatable-cccextentqitem-container', 'children'),
    [Input('datatable-cccextentqitem', 'derived_virtual_data'),
     Input('datatable-cccextentqitem', 'derived_virtual_selected_rows')])
def update_graphs(rows, derived_virtual_selected_rows):
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []


    dff2 = df if rows is None else pd.DataFrame(rows)
    colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff2))]

    title = {'CCC art.':'CCC Articles by Territories (Qitem) in Wikipedias', 'CCC %':'CCC % by Territories (Qitem) in Wikipedias', 'CCC GL art.':'CCC GL art. by Territories (Qitem) in Wikipedias'}

    cols = ['CCC art.','CCC %', 'CCC GL art.']

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff2["Territory name wiki"],
                        "y": dff2[column],
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
                    "title": {"text": title[column],
                             "font": {"size": 12}},
                },
            },
        )
        for column in cols if column in dff2
    ]


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###