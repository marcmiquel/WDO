import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



# COVERAGE APP

#### FUNCTIONS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# COVERAGE FUNCTIONS
def heatmapcoverage_values(lang_list,df_langs_map_coverage):
    lang_list2 = []
    for lg in lang_list:
        lgcode = language_names[lg]
        if lgcode in ccc_art_wp:
            lang_list2.append(lgcode)
    lang_list = sorted(lang_list2, reverse=False)

    if lang_list != None:
        df_langs_map_coverage2 = df_langs_map_coverage.loc[df_langs_map_coverage['set1'].isin(lang_list)]
        df_langs_map_coverage2 = df_langs_map_coverage2.loc[df_langs_map_coverage2['set2'].isin(lang_list)]
    else:
        df_langs_map_coverage2 = df_langs_map_coverage

    x = sorted(list(df_langs_map_coverage2.set1.unique()), reverse=False)
#    x = sorted(lang_list)
    y = sorted(x,reverse=True)
    lang_list = x

    for lang in x:
        df_langs_map_coverage2 = df_langs_map_coverage2.append(pd.Series([lang,lang,'',''], index=df_langs_map_coverage2.columns ), ignore_index=True)

    df_langs_map_coverage2 = df_langs_map_coverage2.sort_values(by=['set2', 'set1'])
    df_langs_map_coverage2 = df_langs_map_coverage2.reset_index(drop=True)
    df_langs_map_coverage2 = df_langs_map_coverage2.set_index('set2')
    df_langs_map_coverage2 = df_langs_map_coverage2.fillna(0)

    z = list()
    z_text = list()
    z_text2 = list()
    for langx in lang_list:
        z_row = []
        z_textrow = []
        z_textrow2 = []
        try:
            df_langs_map_coverage3 = df_langs_map_coverage2.loc[langx]
            df_langs_map_coverage3 = df_langs_map_coverage3.set_index('set1')
        except:
            pass

        for langy in lang_list:
            try:
                rel_value = round(df_langs_map_coverage3.loc[langy].at['rel_value'],2)
                abs_value = df_langs_map_coverage3.loc[langy].at['abs_value']
            except:
                if langx == langy:
                    abs_value = ccc_art_wp[langx]
                    rel_value = 100
                else:
                    abs_value = 0
                    rel_value = 0

            z_row.append(rel_value)
            z_textrow.append(str(abs_value)+ ' articles')
            z_textrow2.append(abs_value)

        z.append(z_row)
        z_text.append(z_textrow)
        z_text2.append(z_textrow2)

    z.reverse()
    z_text.reverse()
    z_text2.reverse()
    return x, y, z, z_text, z_text2


def treemapcoverage_values(source_lang, df_langs_map_coverage):

    if source_lang == None: return

    long_languagename = source_lang
    source_lang = language_names[source_lang]

    df_langs_map_coverage1 = df_langs_map_coverage.set_index('set2')
    df_langs_map_coverage2 = df_langs_map_coverage1.loc[source_lang]

    df_langs_map_coverage2['languagename'] = df_langs_map_coverage2['set1'].map(language_names_inv)
    df_langs_map_coverage2['languagename_full'] = df_langs_map_coverage2['set1'].map(language_names_full)

    df_langs_map_coverage2 = df_langs_map_coverage2.append({'set1':source_lang, 'rel_value':100, 'abs_value':ccc_art_wp[source_lang], 'languagename':long_languagename, 'languagename_full':languages.loc[source_lang]['languagename']}, ignore_index = True)

    # round(ccc_percent_wp[source_lang],2) -> this was the rel_value.
    
    df_langs_map_coverage2 = df_langs_map_coverage2.round(1)

    df_langs_map_coverage2['self_rel_value'] = round(100*df_langs_map_coverage2.abs_value / wikipedialanguage_numberarticles[source_lang],2)
#    print (df_langs_map_coverage2.tail(10))
#    print (df_langs_map_coverage2.head(10))
    return df_langs_map_coverage2


def scatterplotccccoverage_values(source_lang, df_langs_map_coverage):

    if source_lang == None: return

    long_languagename = source_lang
    source_lang = language_names[source_lang]

    df_langs_map_coverage = df_langs_map_coverage.set_index('set2')
    df_langs_map_coverage2 = df_langs_map_coverage.loc[source_lang]

    df_langs_map_coverage2 = df_langs_map_coverage2.set_index('set1')

    df_langs_map_coverage2['Region']=languages.region
    for x in df_langs_map_coverage2.index.values.tolist():
        if ';' in df_langs_map_coverage2.loc[x]['Region']: df_langs_map_coverage2.at[x, 'Region'] = df_langs_map_coverage2.loc[x]['Region'].split(';')[0]

    df_langs_map_coverage2['Subregion']=languages.subregion
    for x in df_langs_map_coverage2.index.values.tolist():
        if ';' in df_langs_map_coverage2.loc[x]['Subregion']: df_langs_map_coverage2.at[x, 'Subregion'] = df_langs_map_coverage2.loc[x]['Subregion'].split(';')[0]

    df_langs_map_coverage2['Language']=languages.languagename

    df_langs_map_coverage2 = df_langs_map_coverage2.reset_index()

    df_langs_map_coverage2['Language (Wiki)'] = df_langs_map_coverage2['set1'].map(language_names_inv)
    df_langs_map_coverage2['Language'] = df_langs_map_coverage2['set1'].map(language_names_full)

#    df_langs_map_coverage2 = df_langs_map_coverage2.append({'set1':source_lang, 'rel_value':round(ccc_percent_wp[source_lang],2), 'abs_value':ccc_art_wp[source_lang], 'Language':long_languagename}, ignore_index = True)

    df_langs_map_coverage2 = df_langs_map_coverage2.round(1)

    df_langs_map_coverage2 = df_langs_map_coverage2.rename(columns={"rel_value": "CCC Coverage Percentage", "abs_value": "Covered CCC Articles", "set1": "Wiki"})

    df_langs_map_coverage2 = df_langs_map_coverage2.loc[(df_langs_map_coverage2['Region']!='')]

#    print (df_langs_map_coverage2.head(10))
    return df_langs_map_coverage2


def scatterplotsum_values(df_langs_sumofCCC_coverage):

    df_langs_sumofCCC_coverage = df_langs_sumofCCC_coverage.set_index('set2')

    df_langs_sumofCCC_coverage['Region']=languages.region
    for x in df_langs_sumofCCC_coverage.index.values.tolist():
        if ';' in df_langs_sumofCCC_coverage.loc[x]['Region']: df_langs_sumofCCC_coverage.at[x, 'Region'] = df_langs_sumofCCC_coverage.loc[x]['Region'].split(';')[0]

    df_langs_sumofCCC_coverage['Subregion']=languages.subregion
    for x in df_langs_sumofCCC_coverage.index.values.tolist():
        if ';' in df_langs_sumofCCC_coverage.loc[x]['Subregion']: df_langs_sumofCCC_coverage.at[x, 'Subregion'] = df_langs_sumofCCC_coverage.loc[x]['Subregion'].split(';')[0]

    df_langs_sumofCCC_coverage['Language']=languages.languagename

    df_langs_sumofCCC_coverage = df_langs_sumofCCC_coverage.reset_index()

    df_langs_sumofCCC_coverage['Articles'] = df_langs_sumofCCC_coverage['set2'].map(wikipedialanguage_numberarticles)

    columns_dict = {'set2':'Wiki','abs_value':'Sum of All Languages CCC Articles Covered','Articles':'Wikipedia Number of Articles', 'rel_value':'Percentage of Sum of All CCC Articles'}
    df_langs_sumofCCC_coverage=df_langs_sumofCCC_coverage.rename(columns=columns_dict)

    #df_langs_sumofCCC_coverage = df_langs_sumofCCC_coverage.rename(columns={0: 'Wiki'})
    df_langs_sumofCCC_coverage = df_langs_sumofCCC_coverage.fillna('')

#    print(df_langs_sumofCCC_coverage[df_langs_sumofCCC_coverage['Region'] == ""])
    return df_langs_sumofCCC_coverage





#### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# GENERAL DATA
conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 
conn2 = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cursor2 = conn2.cursor() 

#### CCC COVERAGE DATA
ccc_percent_wp = {}
ccc_art_wp = {}
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE period = "'+last_period+'" AND content="articles" AND set1descriptor="wp" AND set2descriptor = "ccc" AND set1 = set2'
for row in cursor.execute(query):
    value = row[1]
    value2 = row[2]
    if value == None: value = 0
    if value2 == None: value2 = 0
    ccc_art_wp[row[0]]=value
    ccc_percent_wp[row[0]]=value2

avg_iw = {}
for languagecode in wikilanguagecodes:
    cursor2.execute('SELECT avg(num_interwiki) FROM '+languagecode+'wiki WHERE ccc_binary=0;')
    iw=cursor2.fetchone()[0]
    if iw == None: iw = 0
    avg_iw[languagecode]=round(iw,1)

coverage_art = {}
t_coverage = {}
query = 'SELECT set2, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1="all_ccc_articles" AND set1descriptor="" AND set2descriptor="wp" AND period = "'+last_period+'" ORDER BY set2;'
for row in cursor.execute(query):
    coverage_art[row[0]]=row[1]
    t_coverage[row[0]]=round(row[2],1)

r_coverage = {}
query = 'SELECT set2, rel_value FROM wdo_intersections_accumulated WHERE set1="all_ccc_avg" AND set1descriptor="" AND set2descriptor="wp" AND period = "'+last_period+'" ORDER BY set2;'
for row in cursor.execute(query):
    r_coverage[row[0]]=round(row[1],1)


language_dict={}
query = 'SELECT set2, set1, rel_value, period FROM wdo_intersections_accumulated WHERE content="articles" AND set1descriptor="ccc" AND set2descriptor = "wp" AND period = "'+last_period+'" ORDER BY set2, abs_value DESC;'

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


column_list_dict = {'language':'Language','Wiki':'Wiki', 'WP articles':'Articles','avg_iw':'No-CCC IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_coverage_index':'R.Coverage','total_coverage_index':'T.Coverage','coverage_articles_sum':'Covered Art.'}
column_coverage = ['Language','Wiki','Articles','No-CCC IW','nº1','nº2','nº3','nº4','nº5','R.Coverage','T.Coverage','Covered Art.','Region','Subregion']

df_coverage=pd.DataFrame.from_dict(language_dict,orient='index')


# print (df.columns)
df_coverage['Region']=languages.region
for x in df_coverage.index.values.tolist():
#    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]
    if ';' in df_coverage.loc[x]['Region']: df_coverage.at[x, 'Region'] = df_coverage.loc[x]['Region'].replace(';',', ')

df_coverage['Subregion']=languages.subregion
for x in df_coverage.index.values.tolist():
#    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]
    if ';' in df_coverage.loc[x]['Subregion']: df_coverage.at[x, 'Subregion'] = df_coverage.loc[x]['Subregion'].replace(';',', ')
    
df_coverage=df_coverage.rename(columns=column_list_dict)

# print (df.columns)
df_coverage = df_coverage.fillna('')

df_coverage = df_coverage.sort_values('Language')


df_coverage['Wiki'] = df_coverage.index

df_coverage['id'] = df_coverage['Language']
df_coverage.set_index('id', inplace=True, drop=False)

df_coverage = df_coverage[column_coverage] # selecting the parameters to export

# Coverage of all Wikipedia languages CCC (%) by all Wikipedia language editions
# set1, set1descriptor, set2, set2descriptor
query = 'SELECT set2, set1, rel_value, abs_value FROM wdo_intersections_accumulated WHERE content="articles" AND set1descriptor="ccc" AND set2descriptor = "wp" AND period = "'+last_period+'";'
df_langs_map_coverage = pd.read_sql_query(query, conn)


# Coverage of the sum of all Wikipedia languages CCC articles by all Wikipedia language editions
query = 'SELECT set2, rel_value, abs_value FROM wdo_intersections_accumulated WHERE period = "'+last_period+'" AND content="articles" AND set1="all_ccc_articles" AND set2descriptor = "wp" ORDER BY set2, abs_value DESC;'
df_langs_sumofCCC_coverage = pd.read_sql_query(query, conn)


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app4 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_coverage/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
#dash_app4.scripts.append_script()

# LAYOUT
title = "Culture Gap (CCC Coverage)"
dash_app4.title = title+title_addenda

dash_app4.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain how well each Wikipedia language edition covers 
        the [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) articles (also known as local content) from the other language editions.
        They illustrate the content culture gap between language editions, that is the imbalances across languages editions in content representing each language cultural context. 
        '''),

    # dcc.Markdown('''They answer the following questions:
    #     * How well does this group of Wikipedia language editions cover each others’ CCC?
    #     * How well does this Wikipedia language edition cover other languages CCC?
    #     * What is the extent of all language editions CCC in this Wikipedia language edition?
    #     * What Wikipedia language editions cover best the sum of all languages CCC articles?
    #     '''),

    html.Br(),


##----
    dcc.Tabs([

        dcc.Tab(label='Two Wikipedias Coverage of Lang. CCC (Treemap)', children=[
            # html.Br(),
            # html.H5("Wikipedia Language Coverage of Other Languages CCC Treemap", style={'textAlign':'left'}),
            html.Br(),

            dcc.Markdown('''
                * **What is the extent of all language editions CCC in this Wikipedia language edition?**
                '''.replace('  ', '')),

            dcc.Markdown('''
                In the following dropdown menus you can select the two Wikipedia language editions to compare how well they cover the other languages CCC and the extent they occupy.
                You can also select whether to show or hide the selected Wikipedia associated CCC.
                '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select two Wikipedias'),
            style={'display': 'inline-block','width': '400px'}),
            html.Br(),


            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_treemapcoverage',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'Spanish (es)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            html.P('Show the selected language CCC extent in the graph'),
            style={'display': 'inline-block','width': '400px'}),
            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_treemapcoverage2',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'Catalan (ca)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

#            html.Br(),

            html.Div(
            dcc.RadioItems(id='radio_exclude_ownccc',
                options=[{'label':'Yes','value':'Yes'},{'label':'No','value':'No'}],
                value='No',
                labelStyle={'display': 'inline-block', "margin": "0px 10px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            dcc.Graph(id = 'treemap_ccc_coverage'),
#            html.Hr(),
            dcc.Markdown('''
                The treemap graphs show for two selected Wikipedia language editions both the extent and the coverage of other languages CCC. The size of the tiles and the colorscale (light-dark blue) is according to the extent the other languages CCC take in the selected Wikipedia language edition. When you hover on a tile you can read the same information regarding the coverage and extent plus the number of articles. 
                '''.replace('  ', '')),

        ]),


        dcc.Tab(label='Group of Wikipedias CCC Coverage (Heatmap)', children=[
            html.Br(),

            # html.H5("Languages CCC Coverage Heatmap", style={'textAlign':'left'}),
        #    html.Br(),
            dcc.Markdown('''
                * **How well does this group of Wikipedia language editions cover each others’ CCC?**

                '''.replace('  ', '')),

            dcc.Markdown('''
                In the following menu you can choose a group of Wikipedia language editions: Top 10, 20, 30 and 40 Wikipedias according to the number of articles they have, and specific continents and subcontinents. You can manually add a language edition to the list and see how it is covered and covers the other languages CCC.

                '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='grouplangdropdown_coverage',
                options=[{'label': k, 'value': k} for k in lang_groups],
                value='Top 10',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            dcc.Dropdown(id='sourcelangdropdown_coverageheatmap',
                options = [{'label': k, 'value': k} for k in language_names_list],
                multi=True),

            html.Br(),
            html.Div(
            html.P('Show values in the cell'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),
            
            html.Div(
            dcc.RadioItems(id='radio_articlespercentage_coverage',
                options=[{'label':'Articles','value':'Articles'},{'label':'Percentage','value':'Percentage'}],
                value='Percentage',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            dcc.Graph(id = 'heatmap_coverage'),
#            html.Hr(),

            dcc.Markdown('''
                The heatmap graph shows how well a group of Wikipedia language editions cover their CCC. Each row shows the of each Wikipedia language CCC. The coverage is calculated as the number of articles in a Wikipedia language edition (row) which belong to another Wikipedia language edition CCC (column) divided by the total number of articles in the Wikipedia language edition CCC (column). For an easy identification of values, cells are coloured being purple low coverage and yellow high coverage.
                '''.replace('  ', '')),

        ]),
        dcc.Tab(label='Wikipedias Coverage of Lang. CCC (Scatterplot)', children=[
            html.Br(),

            # html.H5('Wikipedia Language Coverage of Other Language CCC Scatterplot', style={'textAlign':'left'}),

            dcc.Markdown('''
                * **How well does this Wikipedia language edition cover other languages CCC?**
             '''.replace('  ', '')),

            dcc.Markdown('''
                In the following menu you can choose a Wikipedia language edition to see the degree of coverage of other language editions CCC both in percentage and number of articles.
                '''.replace('  ', '')),


            html.Br(),

            html.Div(html.P('Select a Wikipedia'), style={'display': 'inline-block','width': '200px'}),

            dcc.Dropdown(
                id='sourcelangdropdown_coverage',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '190px'}
             ),

            dcc.Graph(id = 'scatterplot_coverage'),
        #    html.Br(),
            dcc.Markdown('''
                The scatterplot graph shows how well each Wikipedia language edition covers other languages CCC. While the Y-axis (log-scale) shows the perentage of a language CCC it covers, the X-axis shows the number of articles this equals. Wikipedia language editions are colored according to their world region (continent).
             '''.replace('  ', '')),
        ]),


        dcc.Tab(label='Wikipedias Coverage of Lang. CCC (Table)', children=[
            html.Br(),

            # html.H5("Wikipedias Coverage of Lang. CCC Summary Table", style={'textAlign':'left'}),
            dcc.Markdown('''
                * **How well does this Wikipedia language edition cover other languages CCC?**
             '''.replace('  ', '')),

            dcc.Markdown('''
                This table shows how well each language edition covers the other 
                language editions CCC by counting the number of CCC articles they have available.
                '''.replace('  ', '')),

            html.Br(),
            dash_table.DataTable(
                id='datatable-ccccoverage',
                columns=[
                    {'name': i, 'id': i, 'deletable': True} for i in df_coverage.columns
                    # omit the id column
                    if i != 'id'
                ],
                data=df_coverage.to_dict('records'),
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
            html.Br(),
            html.Br(),


            dcc.Markdown('''
                Languages are sorted in alphabetic order by their Wikicode, and the columns present the following 
                statistics: the number of articles in the Wikipedia language edition (**Articles**), the average number of interwiki links of not own CCC articles (**No-CCC IW**), the **first 
                five other languages CCC** in terms of most articles covered and the percentage of coverage computed 
                according to the total number of CCC articles of those language edition, the relative coverage 
                (**R. Coverage**) of all languages CCC computed as the average of each language edition CCC percentage 
                of coverage, the total coverage (**T. Coverage**) of all languages CCC computed as the percentage of 
                coverage of all the articles that belong to other language editions CCC, and the total number of articles 
                covered (**Covered Art.**) that belong other language editions CCC.
                '''.replace('  ', '')),

            html.Br(),
            html.Br(),
            html.Div(id='datatable-ccccoverage-container')



        ]),

        dcc.Tab(label='Wikipedias Coverage all CCC Content (Scatterplot)', children=[
            html.Br(),

            # html.H5('Wikipedia Language Editions Coverage of the Sum of All Languages CCC Articles Scatterplot', style={'textAlign':'left'}),


            dcc.Markdown('''
                * **What Wikipedia language editions cover best the sum of all languages CCC articles?**
                '''.replace('  ', '')),


            dcc.Graph(id = 'scatterplot_sum'),

            html.Div(
            dcc.RangeSlider(
                    id='rangeslider',
                    min=0,
                    max=2000000,
                    step=10000,
                    value=[500000, 2000000],
                    marks={
        #                    0: {'label': '0°C', 'style': {'color': '#77b0b1'}},
                        0: {'label': '0'},
        #                    10000: {'label': '10k'},
                        50000: {'label': '50k'},
                        100000: {'label': '100k'},
                        200000: {'label': '200k'},
                        500000: {'label': '500k'},
                        1000000: {'label': '1M'},
                        1200000: {'label': '1,2M'},
                        1400000: {'label': '1,4M'},
                        1600000: {'label': '1,6M'},
                        1800000: {'label': '1,8M'},
                        2000000: {'label': '2M'},
                    }
                ),
                style={'marginLeft': 80,'marginRight': 180}),
            html.Br(),

            dcc.Markdown('''
                The scatterplot graph shows how well each Wikipedia language edition covers the sum of all languages CCC articles. While the Y-axis (log-scale) shows the total number of articles a Wikipedia contains, the X-axis shows the Sum of all Languages CCC articles a Wikipedia covers. 

                Below the X-axis there is a range-slider that allows you to select a specific frame (by default it is set between 500,000 and 2 Million articles). Wikipedia language editions are colored according to their world region (continent). 
            '''.replace('  ', '')),
        ]),
 
    ]),

    footbar,


], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# HEATMAP COVERAGE Dropdown 
@dash_app4.callback(
    dash.dependencies.Output('sourcelangdropdown_coverageheatmap', 'value'),
    [dash.dependencies.Input('grouplangdropdown_coverage', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)

    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
#        print (item['label'])
        list_options.append(item['label'])
#    print (list_options)
    return sorted(list_options,reverse=False)


# HEATMAP COVERAGE
@dash_app4.callback(
    Output('heatmap_coverage', 'figure'),
    [Input('sourcelangdropdown_coverageheatmap', 'value'),dash.dependencies.Input('radio_articlespercentage_coverage', 'value')])
def update_heatmap_coverage(source_lang,articlespercentage):
#    print (source_lang)

    x, y, z, z_text, z_text2 = heatmapcoverage_values(sorted(source_lang,reverse=False),df_langs_map_coverage)

    if articlespercentage=='Percentage':
        fig = ff.create_annotated_heatmap(z, x=x, y=y, annotation_text=z, text=z_text, colorscale='Viridis')
    else:
        fig = ff.create_annotated_heatmap(z, x=x, y=y, annotation_text=z_text2, text=z_text, colorscale='Viridis')

    # fer servir la mitjana del valor per fer franges pel heatmap. es pot fer la mitjana i mediana al pandas i fer-la servir després a l’hora de definir variables pels colors.

    fig.update_layout(
        autosize=True,
#        height=800,
        title_font_size=12,
        paper_bgcolor="White",
        title_text='Wikipedia language editions coverage (%) of other languages CCC',
        title_x=0.5,
    )

    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].font.size = 10
    return fig


# SCATTER LANGUAGES CCC COVERAGE
@dash_app4.callback(
    Output('scatterplot_coverage', 'figure'),
    [Input('sourcelangdropdown_coverage', 'value')])
def update_scatterplot(value):
    source_lang = value

    df_langs_map_coverage2 = scatterplotccccoverage_values(source_lang, df_langs_map_coverage)

    fig = px.scatter(df_langs_map_coverage2, x="CCC Coverage Percentage", y="Covered CCC Articles", color="Region", log_x=False, log_y=True,hover_data=['Language (Wiki)'],text="Wiki") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",
    fig.update_traces(
        textposition='top center')

    return fig


# TREEMAP CCC COVERAGE
@dash_app4.callback(
    Output('treemap_ccc_coverage', 'figure'),
    [Input('sourcelangdropdown_treemapcoverage', 'value'),Input('sourcelangdropdown_treemapcoverage2', 'value'),dash.dependencies.Input('radio_exclude_ownccc', 'value')])
def update_treemap_coverage(value,value2,exclude):

#    print (df_langs_map_coverage2.head(10))
    df_langs_map_coverage2 = treemapcoverage_values(value, df_langs_map_coverage)
    df_langs_map_coverage3 = treemapcoverage_values(value2, df_langs_map_coverage)
#    print (exclude)

    if exclude=='No':
        df_langs_map_coverage2.drop(df_langs_map_coverage2.tail(1).index,inplace=True)
        df_langs_map_coverage3.drop(df_langs_map_coverage3.tail(1).index,inplace=True)


    parents = list()
    for x in df_langs_map_coverage2.index:
        parents.append('')

#    fig = make_subplots(1, 2, subplot_titles=['Size Coverage', 'Size Extent'])
    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        # subplot_titles = (value+' Wikipedia<br />&nbsp;<br />', value2+' Wikipedia<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_langs_map_coverage2['languagename_full'],
        customdata = df_langs_map_coverage2['abs_value'],
        values = df_langs_map_coverage2['self_rel_value'],
        text = df_langs_map_coverage2['rel_value'],
#        textinfo = "label+value+text",
        texttemplate = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%",
        hovertemplate='<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>',
        marker_colorscale = 'Blues',
        ),
            row=1, col=1)


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_langs_map_coverage3['languagename_full'],
        customdata = df_langs_map_coverage3['abs_value'],
        values = df_langs_map_coverage3['self_rel_value'],
        text = df_langs_map_coverage3['rel_value'],
#        textinfo = "label+value+text",
        texttemplate = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%",
        hovertemplate='<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>',
        marker_colorscale = 'Blues',
        ),
            row=1, col=2)


    fig.update_layout(
        autosize=True,
#        width=700,
        height=900,
        title_font_size=12,
#        paper_bgcolor="White",
        title_text=value+' Wikipedia (Left) and '+value2+' Wikipedia (Right)',
        title_x=0.5,

    )

    return fig


# SCATTER SUM OF CCC COVERAGE
@dash_app4.callback(
    Output('scatterplot_sum', 'figure'),
    [Input('rangeslider', 'value')])
def update_scatterplot(rangeslider):

    A, B = rangeslider
#    print (rangeslider)

    df_langs_sumofCCC = scatterplotsum_values(df_langs_sumofCCC_coverage)
    df_langs_sumofCCC = df_langs_sumofCCC.loc[(df_langs_sumofCCC['Region']!='')]
    df_langs_sumofCCC = df_langs_sumofCCC.loc[(df_langs_sumofCCC["Sum of All Languages CCC Articles Covered"] >= A) & (df_langs_sumofCCC["Sum of All Languages CCC Articles Covered"] <= B)]

#    print (df_langs_sumofCCC.head())
    fig = px.scatter(df_langs_sumofCCC, x="Sum of All Languages CCC Articles Covered", y="Wikipedia Number of Articles", color="Region",log_x=True, log_y=False,hover_data=['Language'],text="Wiki") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",

    fig.update_traces(
        textposition='top center')

    return fig


# DATATABLE
@dash_app4.callback(
    Output('datatable-ccccoverage', 'style_data_conditional'),
    [Input('datatable-ccccoverage', 'selected_columns')]
)
def update_styles(selected_columns):

    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


@dash_app4.callback(
    Output('datatable-ccccoverage-container', 'children'),
    [Input('datatable-ccccoverage', 'derived_virtual_data'),
     Input('datatable-ccccoverage', 'derived_virtual_selected_rows')])
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

    df = pd.DataFrame()
    dff = df if rows is None else pd.DataFrame(rows)

    try:
        colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
                  for i in range(len(dff))]
    except:
        pass

    title = {'Covered Art.':'All language editions CCC articles covered by Wikipedia', 'T.Coverage':'Percentual coverage of All language editions CCC articles covered by Wikipedia', 'No-CCC IW':'Average number of interwiki links in non-CCC articles by Wikipedia'}

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
                    "title": {"text": title[column],
                             "font": {"size": 12}},
                },
            },
        )
        # check if column exists - user may have deleted it
        # If `column.deletable=False`, then you don't
        # need to do this check.
        for column in ['Covered Art.','T.Coverage', 'No-CCC IW'] if column in dff
    ]

## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




# SPREAD APP

#### FUNCTIONS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# SPREAD FUNCTIONS
def heatmapspread_values(lang_list,df_langs_map_spread):
    lang_list2 = []
    for lg in lang_list:
        lgcode = language_names[lg]
        if lgcode in ccc_art_wp:
            lang_list2.append(lgcode)
    lang_list = sorted(lang_list2, reverse=False)

    if lang_list != None:
        df_langs_map_spread2 = df_langs_map_spread.loc[df_langs_map_spread['set1'].isin(lang_list)]
        df_langs_map_spread2 = df_langs_map_spread2.loc[df_langs_map_spread2['set2'].isin(lang_list)]
    else:
        df_langs_map_spread2 = df_langs_map_spread

    x = sorted(list(df_langs_map_spread2.set1.unique()), reverse=False)
#    x = sorted(lang_list)
    y = sorted(x,reverse=True)
    lang_list = x

    for lang in x:
        df_langs_map_spread2 = df_langs_map_spread2.append(pd.Series([lang,lang,'',''], index=df_langs_map_spread2.columns ), ignore_index=True)

    df_langs_map_spread2 = df_langs_map_spread2.sort_values(by=['set2', 'set1'])
    df_langs_map_spread2 = df_langs_map_spread2.reset_index(drop=True)
    df_langs_map_spread2 = df_langs_map_spread2.set_index('set2')
    df_langs_map_spread2 = df_langs_map_spread2.fillna(0)

    z = list()
    z_text = list()
    z_text2 = list()
    for langx in lang_list:
        z_row = []
        z_textrow = []
        z_textrow2 = []
        try:
            df_langs_map_spread3 = df_langs_map_spread2.loc[langx]
            df_langs_map_spread3 = df_langs_map_spread3.set_index('set1')
        except:
            pass

        for langy in lang_list:
            if langx == langy:
                rel_value = round(ccc_percent_wp[langx],2)
                abs_value = ccc_art_wp[langx]
            else:
                try:
                    rel_value = round(df_langs_map_spread3.loc[langy].at['rel_value'],2)
                    abs_value = df_langs_map_spread3.loc[langy].at['abs_value']
                except:
                    abs_value = 0
                    rel_value = 0

            z_row.append(rel_value)
            z_textrow.append(str(abs_value)+ ' articles')
            z_textrow2.append(abs_value)

        z.append(z_row)
        z_text.append(z_textrow)
        z_text2.append(z_textrow2)

    z.reverse()
    z_text.reverse()
    z_text2.reverse()
    return x, y, z, z_text, z_text2


def treemapspread_allwp_allccc_values(df_langs_allccc_wikidata_spread):

    language_names_inv = {v: k for k, v in language_names.items()}
    df_langs_allccc_wikidata_spread['languagename'] = df_langs_allccc_wikidata_spread['set2'].map(language_names_inv)
    df_langs_allccc_wikidata_spread['languagename_full'] = df_langs_allccc_wikidata_spread['set2'].map(language_names_full)
    df_langs_allccc_wikidata_spread = df_langs_allccc_wikidata_spread.round(1)

    # print (df_langs_allccc_wikidata_spread.tail(10))
    # print (df_langs_allccc_wikidata_spread.head(10))
    return df_langs_allccc_wikidata_spread


def barchartcccspread_values(source_lang, df_langs_map_coverage):

    long_languagename = source_lang
    source_lang = language_names[source_lang]

    df_langs_map_coverage = df_langs_map_coverage.set_index('set1')
    df_langs_map_coverage2 = df_langs_map_coverage.loc[source_lang]

    df_langs_map_coverage2 = df_langs_map_coverage2.set_index('set2')


    df_langs_map_coverage2['Region']=languages.region
    for x in df_langs_map_coverage2.index.values.tolist():
        if ';' in df_langs_map_coverage2.loc[x]['Region']: df_langs_map_coverage2.at[x, 'Region'] = df_langs_map_coverage2.loc[x]['Region'].split(';')[0]

    df_langs_map_coverage2['Subregion']=languages.subregion
    for x in df_langs_map_coverage2.index.values.tolist():
        if ';' in df_langs_map_coverage2.loc[x]['Subregion']: df_langs_map_coverage2.at[x, 'Subregion'] = df_langs_map_coverage2.loc[x]['Subregion'].split(';')[0]

    df_langs_map_coverage2['Language']=languages.languagename

    df_langs_map_coverage2 = df_langs_map_coverage2.reset_index()

    df_langs_map_coverage2['Language (Wiki)'] = df_langs_map_coverage2['set2'].map(language_names_inv)
    df_langs_map_coverage2['Language'] = df_langs_map_coverage2['set2'].map(language_names_full)

#    df_langs_map_coverage2 = df_langs_map_coverage2.append({'set2':source_lang, 'rel_value':round(ccc_percent_wp[source_lang],2), 'abs_value':ccc_art_wp[source_lang], 'Language':long_languagename}, ignore_index = True)

    df_langs_map_coverage2 = df_langs_map_coverage2.round(1)

    df_langs_map_coverage2['Wikipedia Articles'] = df_langs_map_coverage2['set2'].map(wikipedialanguage_numberarticles)

    df_langs_map_coverage2 = df_langs_map_coverage2.rename(columns={"rel_value": "CCC Coverage Percentage", "abs_value": "Covered CCC Articles", "set2": "Covering Wikipedia Language Edition"})

    df_langs_map_coverage2 = df_langs_map_coverage2.loc[(df_langs_map_coverage2['Region']!='')]


    df_langs_map_coverage2 = df_langs_map_coverage2.sort_values(by=['Covered CCC Articles'], ascending=False)
    df_langs_map_coverage2 = df_langs_map_coverage2.head(25)

#    print (df_langs_map_coverage2.head(10))
    return df_langs_map_coverage2



#### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# CCC SPREAD DATA
ccc_percent_wp = {}
ccc_art_wp = {}
query = 'SELECT set1, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content="articles" AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc" AND period = "'+last_period+'";'
for row in cursor.execute(query):
    value = row[1]
    value2 = row[2]
    if value == None: value = 0
    if value2 == None: value2 = 0
    ccc_art_wp[row[0]]=value
    ccc_percent_wp[row[0]]=value2


# Not spread of each Wikipedia CCC.
inlinkszero_spread = {}
query = 'SELECT set1, rel_value FROM wdo_intersections_accumulated WHERE content="articles" AND set1=set2 AND set1descriptor="ccc" AND set2descriptor="zero_ill" AND period = "'+last_period+'" ORDER BY set1;'
for row in cursor.execute(query):
    inlinkszero_spread[row[0]]=round(row[1],2)

spread_art = {}
t_spread = {}
query = 'SELECT set2, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content="articles" AND set1="all_wp_all_articles" AND set1descriptor="" AND set2descriptor = "ccc" AND period = "'+last_period+'" ORDER BY set2;'
for row in cursor.execute(query):
    spread_art[row[0]]=row[1]
    t_spread[row[0]]=round(row[2],1)


r_spread = {}
query = 'SELECT set2, rel_value FROM wdo_intersections_accumulated WHERE content="articles" AND set1="all_wp_avg" AND set1descriptor="" AND set2descriptor="ccc" AND period = "'+last_period+'" ORDER BY set2;'
for row in cursor.execute(query):
    r_spread[row[0]]=round(row[1],1)


language_dict={}
query = 'SELECT set2, set1, rel_value FROM wdo_intersections_accumulated WHERE content="articles" AND set2descriptor="ccc" AND set1descriptor = "wp" AND period = "'+last_period+'" ORDER BY set2, abs_value DESC;'

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
            row_dict['CCC articles']=ccc_art_wp[languagecode_spreading]
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


column_list_dict = {'language':'Language', 'Wiki':'Wiki', 'CCC articles':'CCC art.','links_zero':'CCC% no IW','1':'nº1','2':'nº2','3':'nº3','4':'nº4','5':'nº5','relative_spread_index':'R.Spread','total_spread_index':'T.Spread','spread_articles_sum':'Spread Art.'}

columns_spread = ['Language', 'Wiki','CCC art.','CCC% no IW','nº1','nº2','nº3','nº4','nº5','R.Spread','T.Spread','Spread Art.','Region','Subregion','World Region']

df_spread=pd.DataFrame.from_dict(language_dict,orient='index')

df_spread['World Region']=languages.region
for x in df_spread.index.values.tolist():
    if ';' in df_spread.loc[x]['World Region']: df_spread.at[x, 'World Region'] = df_spread.loc[x]['World Region'].split(';')[0]

df_spread['Region']=languages.region
for x in df_spread.index.values.tolist():
#    if ';' in df.loc[x]['Region']: df.at[x, 'Region'] = df.loc[x]['Region'].split(';')[0]
    if ';' in df_spread.loc[x]['Region']: df_spread.at[x, 'Region'] = df_spread.loc[x]['Region'].replace(';',', ')

df_spread['Subregion']=languages.subregion
for x in df_spread.index.values.tolist():
#    if ';' in df.loc[x]['Subregion']: df.at[x, 'Subregion'] = df.loc[x]['Subregion'].split(';')[0]
    if ';' in df_spread.loc[x]['Subregion']: df_spread.at[x, 'Subregion'] = df_spread.loc[x]['Subregion'].replace(';',', ')

df_spread=df_spread.rename(columns=column_list_dict)

df_spread = df_spread.fillna('')
df_spread = df_spread.sort_values('Language')

df_spread['Wiki'] = df_spread.index

df_spread['id'] = df_spread['Language']
df_spread.set_index('id', inplace=True, drop=False)
df_spread = df_spread[columns_spread] # selecting the parameters to export
df_spread = df_spread.loc[(df_spread['World Region']!='')]


# Spread of each Wikipedia language CCC (%) in all Wikipedia language editions
# set1, set1descriptor, set2, set2descriptor
query = 'SELECT set2, set1, rel_value, abs_value FROM wdo_intersections_accumulated WHERE period = "'+last_period+'" AND content="articles" AND set1descriptor="wp" AND set2descriptor = "ccc" ORDER BY set2, abs_value DESC;'
df_langs_map_spread = pd.read_sql_query(query, conn)

# Spread of each Wikipedia language CCC in all Wikidata article qitems.
query = 'SELECT set2, abs_value, rel_value FROM wdo_intersections_accumulated WHERE period = "'+last_period+'" AND content="articles" AND set2descriptor="ccc" AND set1 = "wikidata_article_qitems" ORDER BY set2, abs_value DESC;'
df_langs_wikidata_spread = pd.read_sql_query(query, conn)

# Spread of each Wikipedia language CCC in all languages CCC articles.
query = 'SELECT set2, abs_value, rel_value FROM wdo_intersections_accumulated WHERE period = "'+last_period+'" AND content="articles" AND set2descriptor="ccc" AND set1 = "all_ccc_articles" ORDER BY set2, abs_value DESC;'
df_langs_all_ccc_spread = pd.read_sql_query(query, conn)
#print (df_langs_all_ccc_spread.head(10))




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app5 = Dash(__name__, server = app, url_base_pathname= webtype + '/ccc_spread/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)


# LAYOUT
title = "Culture Gap (CCC Spread)"
dash_app5.title = title+title_addenda
dash_app5.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain how well each Wikipedia language edition 
        [Cultural Context Content (CCC)](https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content) articles (also known as local content) are spread across languages. They illustrate the content culture gap between language editions, that is the imbalances across languages editions in content representing each language cultural context.
        '''),

    # dcc.Markdown('''
    #     They answer the following questions:
    #     * What is the extent of this group of Wikipedia languages editions CCC in each others content?
    #     * What is the extent of this language CCC in other Wikipedia language editions?
    #     * What is the extent of the this language CCC articles in the sum of all languages CCC?
    #     * What is the extent of the the sum of this language CCC articles in all languages in the sum of all Wikipedia languages articles?
    #     * What is the extent of this language CCC not spread to other language editions?
    #     '''),
    html.Br(),


    dcc.Tabs([

        dcc.Tab(label='Group of Wikipedia Lang. CCC Spread (Heatmap)', children=[

            html.Br(),

            # html.H5("Languages CCC Spread Heatmap", style={'textAlign':'left'}),


            dcc.Markdown('''
                * **What is the extent of this group of Wikipedia languages editions CCC in each others content?**
                '''.replace('  ', '')),
            dcc.Markdown('''           
                In the following menu you can choose a group of Wikipedia language editions: Top 10, 20, 30 and 40 Wikipedias according to the number of articles they have, and specific continents and subcontinents. You can manually add a language edition to the list and see its CCC extent in the other Wikipedia language editions.
                '''.replace('  ', '')),


            html.Br(),

            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='grouplangdropdown_spread',
                options=[{'label': k, 'value': k} for k in lang_groups],
                value='Top 10',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            dcc.Dropdown(id='sourcelangdropdown_spreadheatmap',
                options = [{'label': k, 'value': k} for k in language_names_list],
                multi=True),

            html.Br(),
            html.Div(
            html.P('Show values in the cell'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),
            
            html.Div(
            dcc.RadioItems(id='radio_articlespercentage_spread',
                options=[{'label':'Articles','value':'Articles'},{'label':'Percentage','value':'Percentage'}],
                value='Percentage',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            dcc.Graph(id = 'heatmap_spread'),
            dcc.Markdown('''
                The heatmap graph shows the extent of each Wikipedia languages' CCC in other Wikipedia language editions. The extent is calculated as the number of articles from a Wikipedia language CCC (column) which are available in a Wikipedia language edition (row) divided by the total number of articles in the Wikipedia language edition (row). For an easy identification of values, cells are coloured being purple low extent and yellow high extent.

                '''.replace('  ', '')),


        ]),


        dcc.Tab(label='One Language CCC Spread Across Languages (Barchart)', children=[

            html.Br(),
            # html.H5('Language CCC Spread in Other Wikipedia Language Editions Barchart'),

            dcc.Markdown('''* **What is the extent of this language CCC in other Wikipedia language editions?**
             '''.replace('  ', '')),




            html.Div(html.P('Select a Language CCC'), style={'display': 'inline-block','width': '200px'}),

            dcc.Dropdown(
                id='sourcelangdropdown_spread',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '190px'}
             ),

            dcc.Graph(id = 'barchart_spread'),
#            html.Hr(),

            dcc.Markdown('''
                The following barchart graph shows for a selected Language CCC the Wikipedia language editions that cover more articles and their total number of Wikipedia articles they contain. The color relates to the total number of Wikipedia articles.
             '''.replace('  ', '')),

        # ###----
        ]),


        dcc.Tab(label='Language CCC Spread Treemap in All Content (Treemap)', children=[

            html.Br(),

            # html.H5("Language CCC Articles Spread Treemap", style={'textAlign':'left'}),
            dcc.Markdown('''* **What is the extent of the this language CCC articles in the sum of all languages CCC?**
                * **What is the extent of the the sum of this language CCC articles in all languages in the sum of all Wikipedia languages articles?**
                '''.replace('  ', '')),



            html.Br(),
            html.Div(id='none',children=[],style={'display': 'none'}),
            dcc.Graph(id = 'treemap_langccc_spreadtreemap'),
#            html.Hr(),
            dcc.Markdown('''

                The following treemap graphs show (left) the extent of all languages CCC in the sum of all languages CCC articles and (right) the sum of the extent of all languages CCC in all Wikipedia language editions articles. The two graphs show the extent both in number of articles and percentage. To calculate the percentage of extent in the left graph we divide the number of articles of a language CCC in by the sum of all languages CCC articles in their corresponding Wikipedia language editions. To calculate the percentage of extent in the right graph, for a language CCC we count the total number of articles that exist across all the language editions and divide it by the sum of all Wikipedia language editions' articles.
                '''.replace('  ', '')),


        ]),

        # ###----

        dcc.Tab(label='Languages CCC Without Interwiki (Scatterplot)', children=[


            html.Br(),

            # html.H5('Languages CCC Without Interwiki Links Scatterplot'),

            dcc.Markdown('''* **What is the extent of this language CCC not spread to other language editions?**

                The following scatterplot graph shows for all Wikipedia language editions on the Y-axis (log-scale) the number of articles in their CCC and on the X-axis the percentage of articles without any interwiki links. Wikipedia language editions are colored according to their world region (continent).
             '''.replace('  ', '')),

            dcc.Graph(id = 'scatterplot_nointerwiki'),

        ]),

        dcc.Tab(label='All Languages CCC Spread Across Languages (Table)', children=[

            html.Br(),
            # html.H5("Summary Table", style={'textAlign':'left'}),
            dcc.Markdown('''* **What is the extent of this language CCC in other Wikipedia language editions?**
             '''.replace('  ', '')),
            
            dcc.Markdown('''
                The following table shows which language CCC is more popular among all Wikipedia 
                language editions by counting in each language edition the number of CCC articles spread across the other languages.'''.replace('  ', '')),


            html.Br(),

            dash_table.DataTable(
                id='datatable-cccspread',
                columns=[
                    {'name': i, 'id': i, 'deletable': True} for i in df_spread.columns
                    # omit the id column
                    if i != 'id'
                ],
                data=df_spread.to_dict('records'),
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
            html.Br(),
            html.Br(),

            dcc.Markdown('''
                Languages are sorted in alphabetic order by their Wikicode, and the columns present the following 
                statistics: (**CCC art.**) the number of CCC articles and the percentage it occupies in the language 
                computed in relation to their total number of articles, the percentage of articles in a language CCC with no interwiki links (**CCC% Without Interwiki Links**), the **first five other languages** covering more 
                 articles from the language CCC and the percentage they occupy in relation to their total number of articles, the relative spread (**R. Spread**) of a language CCC across 
                all the other languages computed as the average of the percentage they occupy in each other language 
                edition, the total spread (**T. Spread**) of a CCC across all the other languages computed as the 
                percentage in relation to all languages articles (not counting the own), and finally, the total number 
                of language CCC articles (**Spread Art.**) that exists across all the other language editions.'''.replace('  ', '')),

            html.Br(),
            html.Br(),

            html.Div(id='datatable-cccspread-container')

            ]),


        # ###----

    ]),

    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# HEATMAP SPREAD Dropdown 
@dash_app5.callback(
    dash.dependencies.Output('sourcelangdropdown_spreadheatmap', 'value'),
    [dash.dependencies.Input('grouplangdropdown_spread', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)

    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    return sorted(list_options,reverse=False)


# HEATMAP SPREAD
@dash_app5.callback(
    Output('heatmap_spread', 'figure'),
    [Input('sourcelangdropdown_spreadheatmap', 'value'),dash.dependencies.Input('radio_articlespercentage_spread', 'value')])
def update_heatmap_spread(source_lang,articlespercentage):
#    print (source_lang)
    x, y, z, z_text, z_text2 = heatmapspread_values(sorted(source_lang,reverse=False),df_langs_map_spread)

    if articlespercentage=='Percentage':
        fig = ff.create_annotated_heatmap(z, x=x, y=y, annotation_text=z, text=z_text, colorscale='Viridis')
    else:
        fig = ff.create_annotated_heatmap(z, x=x, y=y, annotation_text=z_text2, text=z_text, colorscale='Viridis')

    fig.update_layout(
        autosize=True,
#        height=800,
        title_font_size=12,
        paper_bgcolor="White",
        title_text='Languages CCC extent (%) in Wikipedia Language editions',
        title_x=0.5,
    )

    for i in range(len(fig.layout.annotations)):
        fig.layout.annotations[i].font.size = 10
    return fig


# BARCHART CCC SPREAD
@dash_app5.callback(
    Output('barchart_spread', 'figure'),
    [Input('sourcelangdropdown_spread', 'value')])
def update_scatterplot(value):
    source_lang = value

    df_langs_map_coverage2 = barchartcccspread_values(source_lang, df_langs_map_coverage)

    fig = px.bar(df_langs_map_coverage2, x='Covering Wikipedia Language Edition', y='Covered CCC Articles',
                 hover_data=['Language','Covered CCC Articles', 'CCC Coverage Percentage', 'Region'], color='Wikipedia Articles', height=400)
    return fig


# TREEMAP CCC SPREAD ALL WP/ALL CCC
@dash_app5.callback(
    Output('treemap_langccc_spreadtreemap', 'figure'), [Input('none', 'children')])
def update_treemap_coverage_allccc_allwp(none):

    df_langs_all_ccc_spread2 = treemapspread_allwp_allccc_values(df_langs_all_ccc_spread)
    df_langs_wikidata_spread2 = treemapspread_allwp_allccc_values(df_langs_wikidata_spread)

    parents = list()
    for x in df_langs_wikidata_spread2.index:
        parents.append('')

#    fig = make_subplots(1, 2, subplot_titles=['Size Coverage', 'Size Extent'])
    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        # subplot_titles = ('CCC Coverage % (Size)<br />&nbsp;<br />', 'CCC Extent % (Size)<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_langs_all_ccc_spread2['languagename_full'],
        values = df_langs_all_ccc_spread2['abs_value'],
        customdata = df_langs_all_ccc_spread2['rel_value'],
#        text = df_langs_all_ccc_spread2['rel_value'],
#        textinfo = "label+value+text",
        texttemplate = "<b>%{label} </b><br>%{customdata}%<br>%{value} Art.<br>",
        hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Art.: %{value}<br><extra></extra>',
        marker_colorscale = 'Blues',
        ),
            row=1, col=1)


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_spread['Language'],
        customdata = df_spread['T.Spread'],
        values = df_spread['Spread Art.'],
#        text = df_langs_wikidata_spread2['rel_value'],
        texttemplate = "<b>%{label} </b><br>%{customdata}%<br>%{value} Art.<br>",
#        textinfo = "label+value+text",
#        texttemplate = "<b>%{label} </b><br>%{value} Art.<br>",
        hovertemplate = "<b>%{label} </b><br>%{customdata}%<br>%{value} Art.<br><extra></extra>",
        marker_colorscale = 'Blues',
        ),
            row=1, col=2)

    fig.update_layout(
        autosize=True,
#        width=700,
        height=900,
        title_font_size=12,
#        paper_bgcolor="White",
        title_text='Sum of All Languages CCC (Left) and Sum of All Wikipedia Languages Articles (Right)',
        title_x=0.5,
    )

    return fig



# SCATTER LANGUAGES NO INTERWIKI
@dash_app5.callback(
    Output('scatterplot_nointerwiki', 'figure'), [Input('none', 'children')])
def update_scatterplot(none):

    df_s = df_spread.rename(columns={"CCC% no IW": "CCC% Without Interwiki Links", "CCC art.": "CCC articles"})
    fig = px.scatter(df_s, x="CCC% Without Interwiki Links", y="CCC articles", color="World Region", log_x=False, log_y=True,hover_data=['Language'],text="Wiki") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",
    fig.update_traces(
        textposition='top center')

    fig.update_layout(
        autosize=True,
#        width=700,
        height=700,
#        paper_bgcolor="White",
#        title_text='Languages CCC Extent % (Left) and Languages CCC Extent % (Right)',
        title_x=0.5,

    )

    return fig


@dash_app5.callback(
    Output('datatable-cccspread', 'style_data_conditional'),
    [Input('datatable-cccspread', 'selected_columns')]
)
def update_styles(selected_columns):

    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


@dash_app5.callback(
    Output('datatable-cccspread-container', 'children'),
    [Input('datatable-cccspread', 'derived_virtual_data'),
     Input('datatable-cccspread', 'derived_virtual_selected_rows')])
def update_graphs(rows, derived_virtual_selected_rows):
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []

    df = pd.DataFrame()
    dff = df if rows is None else pd.DataFrame(rows)


    title = {'Spread Art.':'Wikipedia Language CCC articles spread across other Wikipedias', 'T.Spread':'Extent of Wikipedia Language CCC articles existing in other Wikipedias', 'CCC% no IW':'Number of CCC articles with no interwiki links by Wikipedia'}

    try:
        colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff))]
    except:
        pass

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
                    "title": {"text": title[column],
                             "font": {"size": 12}},
                },
            },
        )
        for column in ['Spread Art.','T.Spread', 'CCC% no IW'] if column in dff
    ]



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###