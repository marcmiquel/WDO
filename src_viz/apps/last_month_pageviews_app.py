import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### PAGEVIEWS DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor() 


### PAGEVIEWS
query = 'SELECT set1, set2, set2descriptor, abs_value as Pageviews, rel_value as Extent_Pageviews FROM wcdo_intersections_monthly WHERE set1descriptor = "wp" AND set2 IN ("country","region","gender") AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, rel_value DESC;'
df_pageviews = pd.read_sql_query(query, conn)


# # # # GENDER
# pageviews gender
df_gender_pageviews = df_pageviews.loc[df_pageviews['set2'] == "gender"]
df_gender_pageviews = df_gender_pageviews.rename(columns={'set1':'Wiki','set2descriptor':'Gender','Extent_Pageviews':'Extent Pageviews (%)'})
# df_gender_pageviews = df_gender_pageviews.set_index('Wiki')


# articles gender
query = 'SELECT set1 as Wiki, set2descriptor as Gender, abs_value as Articles, rel_value as Extent_Articles FROM wcdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2descriptor IN ("male","female") AND set2 = "wikidata_article_qitems" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, rel_value DESC;'
df_gender_articles = pd.read_sql_query(query, conn)
df_gender_articles = df_gender_articles.rename(columns={'Extent_Articles':'Extent Articles (%)'})
# df_gender_articles = df_gender_articles.set_index('Wiki')
# df_gender_final = pd.concat([df_gender_articles,df_gender_pageviews], axis=1, sort=False)
# print (len(df_gender_pageviews))
# print (len(df_gender_articles))

df_gender_pageviews['id'] = df_gender_pageviews['Wiki']+':'+df_gender_pageviews['Gender']
df_gender_articles['id'] = df_gender_articles['Wiki']+':'+df_gender_articles['Gender']

df_gender_final = df_gender_articles.merge(df_gender_pageviews, on='id', how='outer')
df_gender_final = df_gender_final.fillna(0).round(1)
df_gender_final.Articles = df_gender_final.Articles.astype(int)
df_gender_final.Pageviews = df_gender_final.Pageviews.astype(int)
df_gender_final = df_gender_final.rename(columns={'Wiki_x':'Wiki','Gender_x':'Gender'})
df_gender_final['Language'] = df_gender_final['Wiki'].map(language_names_full)
df_gender_final['Language (Wiki)'] = df_gender_final['Language']+' ('+df_gender_final['Wiki']+')'

df_gender_final_male = df_gender_final.loc[df_gender_final['Gender'] == 'male']
df_gender_final_male = df_gender_final_male.set_index('Wiki')
df_gender_final_female = df_gender_final.loc[df_gender_final['Gender'] == 'female']
df_gender_final_female = df_gender_final_female.set_index('Wiki')

for x in df_gender_final_male.index.values.tolist():
    try:
        male = df_gender_final_male.loc[x]['Articles']
    except:
        male = 0    
    try:
        female = df_gender_final_female.loc[x]['Articles']
    except:
        female = 0
    df_gender_final_male.at[x, 'Extent Articles (%)'] =  100*male/(male+female)
    df_gender_final_female.at[x, 'Extent Articles (%)'] =  100*female/(male+female)

    try:
        male = df_gender_final_male.loc[x]['Pageviews']
    except:
        male = 0    
    try:
        female = df_gender_final_female.loc[x]['Pageviews']
    except:
        female = 0

    pvsum = male+female
    if pvsum == 0:
        df_gender_final_male.at[x, 'Extent Pageviews (%)'] = 0
    else:
        df_gender_final_male.at[x, 'Extent Pageviews (%)'] = 100*male/pvsum

    if pvsum == 0:
        df_gender_final_male.at[x, 'Extent Pageviews (%)'] = 0
    else:
        df_gender_final_female.at[x, 'Extent Pageviews (%)'] =  100*female/pvsum

df_gender_final_male = df_gender_final_male.reset_index().round(1)
df_gender_final_female = df_gender_final_female.reset_index().round(1)


# print (len(df_gender_final))
# print (df_gender_articles.loc[df_gender_articles['Wiki'] == "ca"].head(10))
# print (df_gender_pageviews.loc[df_gender_pageviews['Wiki'] == "ca"].head(10))
# print (df_gender_final.loc[df_gender_final['Wiki_x'] == "ca"].head(10))


# GEOLOCATED
# pageviews geolocated
df_country_pageviews = df_pageviews.loc[df_pageviews['set2'] == "country"]
df_country_pageviews = df_country_pageviews.rename(columns={'set2descriptor':'ISO 3166','Extent_Pageviews':'Extent Pageviews (%)'})
df_country_pageviews['Country'] = df_country_pageviews['ISO 3166'].map(country_names)
df_country_pageviews['Subregion'] = df_country_pageviews['ISO 3166'].map(subregions)
df_country_pageviews['Region'] = df_country_pageviews['ISO 3166'].map(regions)
df_country_pageviews = df_country_pageviews.dropna()
df_country_pageviews = df_country_pageviews.rename(columns={'set1':'Wiki','set2descriptor':'Country','Extent_Pageviews':'Extent Pageviews (%)'})

df_region_pageviews = df_pageviews.loc[df_pageviews['set2'] == "region"]
df_region_pageviews = df_region_pageviews.rename(columns={'set1':'Wiki','set2descriptor':'Region','Extent_Pageviews':'Extent Pageviews (%)'})
df_region_pageviews['id'] = df_region_pageviews['Wiki']+':'+df_region_pageviews['Region']
df_country_pageviews['id'] = df_country_pageviews['Wiki']+':'+df_country_pageviews['Country']

# articles geolocated
query = 'SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wcdo_intersections_accumulated WHERE set2 IN ("countries","regions") AND set1descriptor = "geolocated" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY abs_value DESC;'
df_geolocated_articles = pd.read_sql_query(query, conn)

df_country_articles = df_geolocated_articles.loc[df_geolocated_articles['set2'] == "countries"]
df_country_articles = df_country_articles.rename(columns={'set1descriptor':'Geolocated','abs_value':'Articles','rel_value':'Extent Articles (%)','set1':'Wiki','set2descriptor':'ISO 3166'})
df_country_articles['Country'] = df_country_articles['ISO 3166'].map(country_names)
df_country_articles['Subregion'] = df_country_articles['ISO 3166'].map(subregions)
df_country_articles['Region'] = df_country_articles['ISO 3166'].map(regions)

df_region_articles = df_geolocated_articles.loc[df_geolocated_articles['set2'] == "regions"]
df_region_articles = df_region_articles.rename(columns={'set1descriptor':'Geolocated','abs_value':'Articles','rel_value':'Extent Articles (%)','set1':'Wiki','set2descriptor':'Region'})

df_region_articles['id'] = df_region_articles['Wiki']+':'+df_region_articles['Region']
df_country_articles['id'] = df_country_articles['Wiki']+':'+df_country_articles['Country']

df_region_final = df_region_articles.merge(df_region_pageviews, on='id', how='outer').fillna(0).round(1)
df_country_final = df_country_articles.merge(df_country_pageviews, on='id', how='outer').fillna(0).round(1)

df_region_final.Pageviews = df_region_final.Pageviews.astype(int)
df_country_final.Pageviews = df_country_final.Pageviews.astype(int)

df_region_final = df_region_final.rename(columns={'Region_y':'Region','Wiki_x':'Wiki'})
df_country_final = df_country_final.rename(columns={'Wiki_x':'Wiki','Region_y':'Region','Subregion_y':'Subregion','Country_y':'Country','ISO 3166_x':'ISO 3166'})

# print (df_country_final.loc[df_country_final['Wiki_x'] == "ca"].head(10))
# print (df_region_final.loc[df_region_final['Wiki_x'] == "ca"].head(10))
# input('')


### CCC PAGEVIEWS
query = "SELECT set1 as Wiki, set2 as Covered_Language_PV, rel_value as Extent_Pageviews, abs_value as Pageviews FROM wcdo_intersections_monthly WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND period IN (SELECT MAX(period) FROM wcdo_intersections_monthly) ORDER BY set1, rel_value DESC;"
df_ccc_pv = pd.read_sql_query(query, conn)
df_ccc_pv['id'] = df_ccc_pv['Wiki']+':'+df_ccc_pv['Covered_Language_PV']
df_ccc_pv = df_ccc_pv.set_index('id')

query = "SELECT set1 as Wiki, set2 as Covered_Language_Art, rel_value Extent_Articles, abs_value as Articles FROM wcdo_intersections_accumulated WHERE content='articles' AND set1descriptor='wp' AND set2descriptor='ccc' AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY set1, rel_value DESC;"
df_ccc_articles = pd.read_sql_query(query, conn)
df_ccc_articles['id'] = df_ccc_articles['Wiki']+':'+df_ccc_articles['Covered_Language_Art']
df_ccc_articles = df_ccc_articles.set_index('id')

df_ccc_final = df_ccc_articles.merge(df_ccc_pv, on='id', how='outer')
df_ccc_final = df_ccc_final.fillna(0).round(1)

df_ccc_final.Articles = df_ccc_final.Articles.astype(int)
df_ccc_final.Pageviews = df_ccc_final.Pageviews.astype(int)
df_ccc_final = df_ccc_final.rename(columns={'Covered_Language_Art':'Wiki','Extent_Articles':'Extent Articles (%)','Extent_Pageviews':'Extent Pageviews (%)'})
df_ccc_final['Covered Language'] = df_ccc_final['Wiki'].map(language_names_full)
df_ccc_final = df_ccc_final.loc[df_ccc_final['Covered_Language_PV'] != 'simple']

# print (df_ccc_pv.loc[df_ccc_pv['Wiki'] == "ca"].head(10))
# print (df_ccc_articles.loc[df_ccc_articles['Wiki'] == "ca"].head(10))
# print (df_ccc_final.loc[df_ccc_final['Wiki'] == "ca"].head(10))


#### CCC PAGEVIEWS TABLE
ccc_percent_wp = {}
query = 'SELECT set1, rel_value FROM wcdo_intersections_accumulated WHERE content="articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) AND set1 = set2 AND set1descriptor="wp" AND set2descriptor = "ccc";'
for row in cursor.execute(query):
    value = row[1]
    if value == None: value = 0
    ccc_percent_wp[row[0]]=round(value,2)


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
own_ccc_top_pageviews_abs = {}
query = "SELECT set1, rel_value, abs_value, period FROM wcdo_intersections_monthly WHERE period IN (SELECT MAX(period) FROM wcdo_intersections_monthly) AND content='pageviews' AND set1descriptor='ccc' AND set2='top_articles_lists' AND set2descriptor='pageviews' ORDER BY set1, rel_value DESC;"
for row in cursor.execute(query):
    own_ccc_top_pageviews[row[0]]=round(row[1],1)
    own_ccc_top_pageviews_abs[row[0]]=row[2]

df_own_top_ccc = pd.DataFrame.from_dict(own_ccc_top_pageviews, orient='index',columns=['Extent Pageviews (%)'])
df_own_top_ccc = df_own_top_ccc.reset_index().rename(columns={'index':'Wiki'})
df_own_top_ccc = df_own_top_ccc.set_index('Wiki')
df_own_top_ccc['Region']=languages.region
df_own_top_ccc = df_own_top_ccc.reset_index()
df_own_top_ccc['Pageviews'] = df_own_top_ccc['Wiki'].map(own_ccc_top_pageviews_abs)
df_own_top_ccc['Language'] = df_own_top_ccc['Wiki'].map(language_names_full)
for x in df_own_top_ccc.index.values.tolist():
    if ';' in df_own_top_ccc.loc[x]['Region']: df_own_top_ccc.at[x, 'Region'] = df_own_top_ccc.loc[x]['Region'].split(';')[0]
df_own_top_ccc['Language (Wiki)'] = df_own_top_ccc['Language']+' ('+df_own_top_ccc['Wiki']+')'
df_own_top_ccc = df_own_top_ccc.loc[(df_own_top_ccc['Region']!='')]
# print (df_own_top_ccc.head(10))


"""
all_lang_top_pageviews = {}
query = "SELECT set1, rel_value, abs_value FROM wcdo_intersections_accumulated WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='all_top_articles' AND set2='ccc' ORDER BY set1, rel_value DESC;"
# quant pesen els pageviews del top articles propi en el ccc propi en lang1?
for row in cursor.execute(query):
    all_lang_top_pageviews[row[0]]=round(row[1],1)
"""

language_dict={}
query = "SELECT set1, set2, rel_value, abs_value FROM wcdo_intersections_monthly WHERE content='pageviews' AND set1descriptor='wp' AND set2descriptor='ccc' AND set1!=set2 AND set2 != 'simple' ORDER BY set1, abs_value DESC;"
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



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app6 = Dash(__name__, server = app, url_base_pathname= webtype + '/last_month_pageviews/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app6.config['suppress_callback_exceptions']=True

title = "Last Month Pageviews"
dash_app6.title = title+title_addenda
text_heatmap = ''

dash_app6.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that explain the distribution of pageviews in each Wikipedia language edition and for each types of articles. Different kinds of gaps also appear in the pageviews. 
        The graphs answer the following questions:
        * What is the extent of pageviews dedicated to each country and world region in each Wikipedia language edition?
        * What is the extent of pageviews dedicated to each language CCC in each Wikipedia language edition?
        * What is the extent of pageviews dedicated to each language edition Top CCC lists in relation to their language CCC?
        * What is the gender gap in pageviews in biographies in each Wikipedia language edition? 
       '''),
    html.Br(),
#    #html.Hr(),

###
    dcc.Tabs([
        dcc.Tab(label='Extent of Geolocated Entities in Pageviews (Treemap)', children=[

            html.H5('Extent of Geolocated Entities (Countries and Regions) in Pageviews Treemap'),
            dcc.Markdown('''* **What is the extent of pageviews dedicated to each country and world region in each Wikipedia language edition?**

                The following treemap graphs show for a selected Wikipedia language edition the extent of geographical entities (countries, subregions and world regions) in their geolocated articles. This can either be in terms of the number of articles or the pageviews they receive. The size of the tiles is according to the extent of the geographical entities take and the color simply represent the diversity of entities. When you hover on a tile you can compare both articles and pageviews extent in relative (percentage) and absolute (articles and pageviews).
             '''.replace('  ', '')),
            html.Br(),
            html.Div(
            html.P('Select a Wikipedia and a type of geographical entity'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_geolocated',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '240px'}
             ), style={'display': 'inline-block','width': '250px'}),

            dcc.Dropdown(
                id='geolocateddropdown',
                options = [{'label': k, 'value': k} for k in ['Countries','Regions']],
                value = 'Countries',
                style={'width': '190px'}
             ),
            dcc.Graph(id = 'geolocated_treemap'),

        ]),

        dcc.Tab(label="Extent of Languages' CCC in Pageviews (Treemap)", children=[

            html.H5("Extent of Languages' CCC in Pageviews Treemap"),
            dcc.Markdown('''* **What is the extent of pageviews dedicated to each language CCC in each Wikipedia language edition?**

                The following treemap graphs shows for a selected Wikipedia language edition the extent each language CCC take in the sum of articles and pageviews dedicated and received by all languages CCC. The size of the tiles is according to the extent the language CCC take and the color simply represent the diversity of entities. When you hover on a tile you can compare both articles and pageviews extent in relative (percentage) and absolute (articles and pageviews).
             '''.replace('  ', '')),
            html.Br(),
            html.Div(
            html.P('Select a Wikipedia'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_languageccc',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '240px'}
             ), style={'display': 'inline-block','width': '250px'}),

            dcc.Graph(id = 'language_ccc_treemap'),
            #html.Hr(),

        ]),


###
        dcc.Tab(label="Extent of Languages' CCC in Pageviews (Table)", children=[

            html.H5("Last Month Pageviews in CCC by Wikipedia language edition"),
            dcc.Markdown('''The following table shows for each language edition the relative popularity 
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

            html.Div(id='datatable-cccpageviews-container'),

        ]),
###

        dcc.Tab(label="Extent of Languages' Top CCC in Pageviews (Scatterplot)", children=[

            html.H5("Extent of Languages' Top CCC in Pageviews Scatterplot"),
            dcc.Markdown('''
                * **What is the extent of pageviews dedicated to each language edition Top CCC lists in relation to their language CCC?**

                The following scatterplot shows for each language edition Top CCC lists articles, the number of pageviews they receive (y-axis in logscale) and the percentage of pageviews they take from all the pageviews received by the language CCC (x-axis). Wikipedia language editions are colored according to the world region (continent) where the language is spoken. You can double-click the region name in the legend to see only the Wikipedia languages editions of a single region.

                The Top CCC articles present the most rellevant articles in terms of different metrics (e.g. number of editors or pageviews) and specific content types (e.g. geolocated articles or women) from a language cultural context content. Since these articles are the most valuable part of CCC, they also collect a considerable amount of pageviews. When a Wikipedia language edition is small, often the Top CCC lists encompass most of their language CCC.
             '''.replace('  ', '')),
            html.Br(),
            html.Div(id='none',children=[],style={'display': 'none'}),    
            dcc.Graph(id = 'language_topccc_scatterplot'),

        ]),


###
        dcc.Tab(label='Gender Gap in Pageviews (Barchart)', children=[

            html.H5('Gender Gap in Pageviews Barchart'),
            dcc.Markdown('''
                * **What is the gender gap in pageviews in biographies in each Wikipedia language edition?**

                The following barchart shows for a group of selected Wikipedia language editions the gender gap in terms of articles (red for women and blue for men) and in terms of pageviews these articles receive (orange for women and pink for men). By hovering on each of the bars you can compare the percentages and the absolute number of articles and pageviews.     '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='grouplangdropdown',
                options=[{'label': k, 'value': k} for k in lang_groups],
                value='Top 10',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            dcc.Dropdown(id='sourcelangdropdown_gendergap',
                options = [{'label': k, 'value': k} for k in language_names_list],
                multi=True),

            html.Br(),
            dcc.Graph(id = 'language_gendergap_barchart'),

        ])

    ]),

    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# GEOLOCATED TREEMAP
@dash_app6.callback(
    Output('geolocated_treemap', 'figure'),
    [Input('sourcelangdropdown_geolocated', 'value'),Input('geolocateddropdown', 'value')])
def update_treemap_geolocated(language,geographicalentity):
# ['Wiki', 'Geolocated', 'set2_x', 'Region_x', 'Articles', 'Extent Articles (%)', 'id', 'Wiki_y', 'set2_y', 'Region', 'Pageviews', 'Extent Pageviews (%)']
# ['Wiki', 'Geolocated', 'set2_x', 'ISO 3166_x', 'Articles', 'Extent Articles (%)', 'Country_x', 'Subregion_x', 'Region_x', 'id', 'Wiki_y', 'set2_y', 'ISO 3166_y', 'Pageviews', 'Extent Pageviews (%)', 'Country', 'Subregion', 'Region']
    
    languageproject = language; language = language_names[language]

    texttemplate = "<b>%{label} </b><br>Extent GL Pageviews: %{text}%<br>Pageviews: %{customdata}"
    hovertemplate = '<b>%{label} </b><br>Extent GL Pageviews: %{text}%<br>Pageviews: %{customdata}<br>Extent Total Pageviews: %{value}%<br><extra></extra>'

    texttemplate2 = "<b>%{label} </b><br>Extent GL Articles: %{value}%<br>Articles: %{customdata}"
    hovertemplate2 = '<b>%{label} </b><br>Extent GL Articles: %{value}%<br>Articles: %{customdata}<br>Extent GL Pageviews: %{text}%<br><extra></extra>'


    if geographicalentity == 'Countries':
        labels = "Country"
        df = df_country_final.loc[df_country_final['Wiki'] == language]

    if geographicalentity == 'Regions':
        labels = "Region"
        df = df_region_final.loc[df_region_final['Wiki'] == language]

    # print (df.head(10))
    # input('')

    Total = df['Pageviews'].sum()
    df['Extent GL Pageviews (%)'] = 100*df['Pageviews']/Total
    df = df.round(1)

    # print (Total)
    # print (df.head(10))
    # input('')

    parents = list()
    for x in df.index: parents.append('')


    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        # subplot_titles = ('CCC Coverage % (Size)<br />&nbsp;<br />', 'CCC Extent % (Size)<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df[labels],
        customdata = df['Articles'],
        values = df['Extent Articles (%)'],
        text = df['Extent GL Pageviews (%)'],
        texttemplate = texttemplate2,
        hovertemplate= hovertemplate2,
#        marker_colorscale = 'RdBu',
        ),
            row=1, col=1)

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df[labels],
        customdata = df['Pageviews'],
        values = df['Extent Pageviews (%)'],
        text = df['Extent GL Pageviews (%)'],
        texttemplate = texttemplate,
        hovertemplate= hovertemplate,
#        marker_colorscale = 'RdBu',
        ),
            row=1, col=2)

    fig.update_layout(
        autosize=True,
    #        width=700,
        height=900,
        paper_bgcolor="White",
        title_text=geographicalentity+" Extent in Geolocated Articles (Left) and "+geographicalentity+" Extent in Geolocated Articles' Pageviews (Right)",
        title_x=0.5,
    )

    return fig



# LANGUAGE CCC TREEMAP
@dash_app6.callback(
    Output('language_ccc_treemap', 'figure'),
    [Input('sourcelangdropdown_languageccc', 'value')])
def update_treemap_ccc(language):

    languageproject = language; language = language_names[language]
    df = df_ccc_final.loc[df_ccc_final['Wiki_x'] == language]
#    df = df.rename({'Covered Language':''})

    parents = list()
    for x in df.index:
        parents.append('')

#    print (df.head(10))

#    fig = make_subplots(1, 2, subplot_titles=['Size Coverage', 'Size Extent'])
    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        # subplot_titles = ('CCC Coverage % (Size)<br />&nbsp;<br />', 'CCC Extent % (Size)<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df['Covered Language'],
        values = df['Articles'],
        customdata = df['Extent Articles (%)'],
        text = df['Extent Pageviews (%)'],
        texttemplate = "<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>",
        hovertemplate='<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>Extent Pageviews: %{text}%<br><extra></extra>',
#        marker_colorscale = 'Blues',
        ),
            row=1, col=1)

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df['Covered Language'],
        values = df['Pageviews'],
        customdata = df['Extent Pageviews (%)'],
        text = df['Extent Articles (%)'],
        texttemplate = "<b>%{label} </b><br>Extent Pageviews: %{customdata}%<br>Pageviews: %{value}<br>",
        hovertemplate='<b>%{label} </b><br>Extent Pageviews: %{customdata}%<br>Pageviews: %{value}<br>Extent Articles: %{text}%<br><extra></extra>',
#        marker_colorscale = 'Blues',
        ),
            row=1, col=2)

    fig.update_layout(
        autosize=True,
#        width=700,
        height=900,
#        paper_bgcolor="White",
        title_text="Languages CCC Extent in "+languageproject+"Wikipedia Articles (Left) and Languages CCC Extent in "+languageproject+" Articles' Pageviews (Right)",
        title_x=0.5,
    )

    return fig



# TOP CCC SCATTERPLOT
@dash_app6.callback(
    Output('language_topccc_scatterplot', 'figure'),
    [Input('none', 'children')])
def update_scatterplot(none):

    fig = px.scatter(df_own_top_ccc, y="Pageviews", x="Extent Pageviews (%)", color="Region", log_x=False, log_y=True,hover_data=['Language (Wiki)','Region'],text="Wiki") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",
    fig.update_traces(
        textposition='top center')

    return fig



# GENDER GAP Dropdown languages
@dash_app6.callback(
    dash.dependencies.Output('sourcelangdropdown_gendergap', 'value'),
    [dash.dependencies.Input('grouplangdropdown', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    return re

    # return ['Cebuano (ceb)', 'Dutch (nl)', 'English (en)', 'French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)', 'Russian (ru)', 'Spanish (es)', 'Swedish (sv)']


# GENDER GAP BARCHART
@dash_app6.callback(
    Output('language_gendergap_barchart', 'figure'),
    [Input('sourcelangdropdown_gendergap', 'value')])
def update_barchart(langs):

    languagecodes = []
    for l in langs:
        try:
            languagecodes.append(language_names[l])
        except:
            pass

    df = df_gender_final_male.loc[df_gender_final_male['Wiki'].isin(languagecodes)]
    df2 = df_gender_final_female.loc[df_gender_final_female['Wiki'].isin(languagecodes)]

    df3 = df
    df4 = df2

    df['Language (Art.)'] = df['Language'] + ' Art.'
    df2['Language (Art.)'] = df2['Language'] + ' Art.'

    df3['Language (Pv.)'] = df['Language'] + ' Pv.'
    df4['Language (Pv.)'] = df2['Language'] + ' Pv.'


    # animals=['giraffes', 'orangutans', 'monkeys']
    # fig = go.Figure(data=[
    #     go.Bar(name='SF Zoo', x=animals, y=[20, 14, 23]),
    #     go.Bar(name='LA Zoo', x=animals, y=[12, 18, 29])
    # ])
    # fig.update_layout(barmode='stack')

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['Language (Art.)'],
        y=df['Extent Articles (%)'],
        name='Men Articles',
        marker_color='blue',
#        values = df2['Extent Articles (%)'],
        customdata = df['Articles'],
        texttemplate='%{y}',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',

    ))
    fig.add_trace(go.Bar(
        x=df2['Language (Art.)'],
        y=df2['Extent Articles (%)'],
        name='Women Articles',
        marker_color='red',
#        values = df2['Extent Articles (%)'],
        customdata = df2['Articles'],
        texttemplate='%{y}',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
    ))
    fig.add_trace(go.Bar(
        x=df3['Language (Pv.)'],
        y=df3['Extent Pageviews (%)'],
        name='Men Pageviews',
        marker_color='violet',
        customdata = df3['Pageviews'],
        texttemplate='<b>%{y}</b>',
        hovertemplate='<br>Pageviews: %{customdata}<br>Extent Pageviews: %{y}%<br><extra></extra>',

    ))
    fig.add_trace(go.Bar(
        x=df4['Language (Pv.)'],
        y=df4['Extent Pageviews (%)'],
        name='Women Pageviews',
        marker_color='orange',
        customdata = df4['Pageviews'],
        texttemplate='<b>%{y}</b>',
        hovertemplate='<br>Pageviews: %{customdata}<br>Extent Pageviews: %{y}%<br><extra></extra>',
    ))    
    fig.update_layout(barmode='stack')

    # fig.add_trace(go.Bar(
    #     x=df['Language (Wiki)'],
    #     y=df['Extent Pageviews (%)'],
    #     name='Men Pageviews',
    #     marker_color='violet',
    #     customdata = df['Pageviews'],
    #     hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Pv.: %{value}<br><extra></extra>',

    # ))
    # fig.add_trace(go.Bar(
    #     x=df2['Language (Wiki)'],
    #     y=df2['Extent Pageviews (%)'],
    #     name='Women Pageviews',
    #     marker_color='orange',
    #     customdata = df2['Pageviews'],
    #     hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Pv.: %{value}<br><extra></extra>',
    # ))
    return fig


# # GENDER GAP BARCHART
# @dash_app6.callback(
#     Output('language_gendergap_barchart2', 'figure'),
#     [Input('sourcelangdropdown_gendergap', 'value')])
# def update_barchart(langs):

#     languagecodes = []
#     for l in langs:
#         languagecodes.append(language_names[l])

#     df = df_gender_final_male.loc[df_gender_final_male['Wiki'].isin(languagecodes)]
#     df2 = df_gender_final_female.loc[df_gender_final_female['Wiki'].isin(languagecodes)]

#     fig = go.Figure()
#     fig.add_trace(go.Bar(
#         x=df['Language (Wiki)'],
#         y=df['Extent Pageviews (%)'],
#         name='Men Pageviews',
#         marker_color='violet',
#         customdata = df['Pageviews'],
#         hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Pv.: %{value}<br><extra></extra>',

#     ))
#     fig.add_trace(go.Bar(
#         x=df2['Language (Wiki)'],
#         y=df2['Extent Pageviews (%)'],
#         name='Women Pageviews',
#         marker_color='orange',
#         customdata = df2['Pageviews'],
#         hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Pv.: %{value}<br><extra></extra>',
#     ))
#     fig.update_layout(barmode='stack')

#     return fig


# DATATABLE
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
