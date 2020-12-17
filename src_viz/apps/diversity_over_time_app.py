# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### ARTICLES DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

functionstartTime = time.time()
conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() # stats_prova_amb_indexs

content_types = ['Countries','Subregions','Regions','Languages CCC','Top CCC Lists','Gender']
content_types_minimal = ['Subregions','Regions','Top CCC Lists','Gender']

lists_dict = {'Editors':'editors','Featured':'featured','Geolocated':'geolocated','Keywords':'keywords','Women':'women','Men':'men','Created First Three Years':'created_first_three_years','Created Last Year':'created_last_year','Pageviews':'pageviews','Discussions':'discussions','Edits':'edits', 'Edited Last Month':'edited_last_month', 'Images':'images', 'WD Properties':'wdproperty_many', 'Interwiki':'interwiki_many', 'Least Interwiki Most Editors':'interwiki_editors', 'Least Interwiki Most WD Properties':'interwiki_wdproperty', 'Wikirank':'wikirank', 'Wiki Loves Earth':'earth', 'Wiki Loves Monuments':'monuments_and_buildings', 'Wiki Loves Sports':'sport_and_teams', 'Wiki Loves GLAM':'glam', 'Wiki Loves Folk':'folk', 'Wiki Loves Music':'music_creations_and_organizations', 'Wiki Loves Food':'food', 'Wiki Loves Paintings':'paintings', 'Wiki Loves Books':'books', 'Wiki Loves Clothing and Fashion':'clothing_and_fashion', 'Wiki Loves Industry':'industry'}
list_dict_inv = {v: k for k, v in lists_dict.items()}

content_type_dict = {'Regions':'regions','Subregions':'subregions','Countries':'countries','Gender':'gender','Top CCC Lists':'top_ccc_articles_lists', 'Languages CCC':'ccc'}

people_dict = {'Men':'male','Women':'female'}
country_names_inv = {v: k for k, v in country_names.items()}
subregions_dict = {}; regions_dict = {}
for sub,reg in subregions_regions.items():
    regions_dict[reg]=reg
    subregions_dict[sub]=sub
entities_list = []

query = 'SELECT distinct period FROM wdo_intersections_accumulated;'
df = pd.read_sql_query(query, conn)
periods = sorted(df.period.tolist(),reverse=True)
default_period = periods[2]



###############################################################################################


def params_to_df(langs, content_type, period, accumulated_monthly):

    # functionstartTime = time.time()
    db = 'FROM wdo_intersections_'+accumulated_monthly+' '
    if isinstance(langs,str): langs = [langs]

    page_asstring = ','.join( ['?'] * len(langs) )
    order = ' ORDER BY Wiki, period;'
    lass = ','.join( ['?'] * len(langs) )

    if content_type == 'gender':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2, set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content=? AND Wiki IN ('+lass+') AND "Group" = ? AND Entity IN (?,?,?,?,?,?,?,?,?,?,?,?,?)'
        if accumulated_monthly == 'monthly': content_type_conditions += ' AND set2 = Wiki'
        params = ['articles']+langs+['wp', 'male', 'female', 'transgender female', 'intersex', 'travesti', 'androgyny','transgender male', 'transfeminine', 'two-Spirit', 'hermaphrodite', 'muxe', 'māhū', 'Transgene']

    if content_type == 'countries':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2 as "Content_Type", set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content = ? AND Wiki IN ('+lass+') AND Content_Type = ? AND "Group" = ?'
        params = ['articles']+langs+['countries', 'wp']

    if content_type == 'subregions':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2 as "Content_Type", set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content = ?  AND Wiki IN ('+lass+') AND Content_Type = ? AND "Group" = ?'
        params = ['articles']+langs+['subregions', 'wp']

    if content_type == 'regions':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2 as "Content_Type", set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content = ?  AND Wiki IN ('+lass+') AND Content_Type = ? AND "Group" = ?'
        params = ['articles']+langs+['regions', 'wp']

    if content_type == 'ccc':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2 as "Entity_Extra", set2descriptor as "Content_Type", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content = ?  AND Wiki IN ('+lass+') AND Content_Type = ? AND "Group" = ?'
        params = ['articles']+langs+['ccc', 'wp']

    if content_type == 'top_ccc_articles_lists':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2 as "Content_Type", set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content=? AND Wiki IN ('+lass+') AND set2 = ? AND "Group" = ?'
        params = ['articles']+langs+['top_ccc_articles_lists', 'wp']

    if content_type == 'wp':
        cols = 'SELECT set1 as "Wiki", set1descriptor as "Group", set2, set2descriptor as "Entity", abs_value as Articles, rel_value as "Extent Articles (%)", period '
        content_type_conditions = 'WHERE content= ? AND Wiki IN ('+lass+') AND Group = ? AND Entity = "Group" AND set2 = Wiki'
        params = ['articles']+langs+['wp']


    if period != None: 
        content_type_conditions += ' AND period = ?'
        params += [period]

    query = cols + db + content_type_conditions + order

#    print (query)
    return query, params


def df_extended(df, content_type):

    if content_type == 'gender':
        df['Content_Type'] = 'gender'
        df['Entity_Extra'] = ''
        df['Entity'] = df['Entity'].map({'male':'Men','female':'Women','transgender female':'Transgender female', 'intersex':'Intersex', 'travesti':'Travesti', 'androgyny':'Androgyny','transgender male':'Transgender male', 'transfeminine':'Transfeminine', 'two-Spirit':'Two-Spirit', 'hermaphrodite':'Hermaphrodite', 'muxe':'Muxe', 'māhū':'Māhū', 'Transgene':'Transgene'})



    if content_type == 'countries':

        df['Entity_Extra'] = df['Entity'].map(regions)
        df['ISO 3166'] = df['Entity']
        df['Entity'] = df['Entity'].map(country_names)

    if content_type == 'subregions':
        df['Entity_Extra'] = df['Entity'].map(subregions_regions)

    if content_type == 'regions':
        df['Entity_Extra'] = ''

    if content_type == 'ccc':
        df['Entity'] = df['Entity_Extra'].map(language_names_full)

    if content_type == 'top_ccc_articles_lists':
        df['Entity'] = df['Entity'].map(list_dict_inv)

    df['period'] = pd.to_datetime(df['period'])
    df['Language'] = df['Wiki'].map(language_names_full)
    df['Region'] = df['Wiki'].map(languages.region)

    return df


###############################################################################################



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

dash_app13 = Dash(__name__, server = app, url_base_pathname= webtype + '/diversity_over_time/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# dash_app13 = Dash()
dash_app13.config['suppress_callback_exceptions']=True



title = "Diversity Over Time"
dash_app13.title = title+title_addenda
text_heatmap = ''

dash_app13.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows statistics and graphs that explain the creation of different types of content over time. They depict both the accumulated articles and the new articles created on a monthly basis. The different types of content used for the analysis are: geographical entities (countries, subregions and regions), languages CCC, Top CCC Lists, and gender.
       '''),

    # dcc.Markdown('''
    #     The graphs answer the following questions:
    #     * What is the extent of the different types of content created and accumulated in Wikipedia language editions over time?
    #     * What Wikipedia language editions have created and accumulated more content of the different types over time?
    #     * What is the extent of the different types of content created and accumulated in the Wikipedia language editions in a specific month?
    #     * What Wikipedia language editions have created more content of the different types in a specific month?
    #    '''),

    html.Br(),

# ###


    dcc.Tabs([


        dcc.Tab(label='One Wikipedia Over Time (Barchart)', children=[

        # 3 OVER TIME BARCHART
            html.Br(),

            # html.H5('Created and Accumulated Articles by Content Type Over Time in Wikipedia Language Editions'),
            dcc.Markdown('''* **What is the extent of the different types of content created and accumulated in Wikipedia language editions over time?**
             '''.replace('  ', '')),
            html.Br(),

            html.Div(
            html.P('Select a Wikipedia'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Content type'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Select the time aggregation'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Show absolute or relative values'),
            style={'display': 'inline-block','width': '300px'}),


            html.Br(),
            html.Div(
            dcc.Dropdown(
                id='lang_dropdown_barchart',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'French (fr)',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='content_types_barchart',
                options = [{'label': k, 'value': k} for k in content_types_minimal],
                value = 'Regions',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='time_aggregation_barchart',
                options = [{'label': k, 'value': k} for k in ['Monthly','Quarterly','Yearly']],
                value = 'Yearly',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.RadioItems(
                id='show_absolute_relative_radio_barchart',
                options=[{'label':'Absolute','value':'Absolute'},{'label':'Relative','value':'Relative'}],
                value='Absolute',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            dcc.Graph(id = 'createdaccumulatedmonthly_barchart1'),
            dcc.Graph(id = 'createdaccumulatedmonthly_barchart2'),
            #html.Hr(),

            dcc.Markdown('''The barchart graphs show for a single Wikipedia language edition and for a selected type of content, the amount of articles and the percentage of for each of its entities that is both accumulated and created over time. Time is presented in the x-axis and it is possible to select the periods in which articles are aggregated (Yearly, Quarterly and Monthly). The stacked bars can take the whole y-axis or be proportional to the number of aggregated articles for that period of time. 

              The graph contains a range-slider on the bottom to select a specific period of time especially useful when the time aggregation is set to quarterly. It is possible to use predefined specific time selections by clicking on the labels 6M, 1Y, 5Y, 10Y and ALL (last six months, last year, last five years, last ten years and all the time). The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend.
             '''.replace('  ', '')),

        ]),


        dcc.Tab(label='Multiple Wikipedias Over Time (Time Series)', children=[

            # 4 OVER TIME TIME SERIES
            html.Br(),

            # html.H5('Wikipedia Language Editions By Monthly Created Articles On Any Content Type Over Time'),
            dcc.Markdown('''* 
               **What Wikipedia language editions have created and accumulated more content of the different types over time?**
             '''.replace('  ', '')),

            html.Br(),
            html.Div(
            html.P('Select a group of Wikipedias'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('You can add or remove languages:'),
            style={'display': 'inline-block','width': '500px'}),

            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='langgroup_dropdown_timeseries',
                options = [{'label': k, 'value': k} for k in lang_groups],
                disabled =False,
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='langgroup_box_timeseries',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                multi=False,
                style={'width': '790px'}
             ), style={'display': 'inline-block','width': '800px'}),

            html.Br(),

            html.Div(
            html.P('Select a content type'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('You can add or remove entities:'),
            style={'display': 'inline-block','width': '500px'}),
            html.Br(),



            html.Div(
            dcc.Dropdown(
                id='content_types_timeseries',
                options = [{'label': k, 'value': k} for k in ['Entire Wikipedia']+content_types],
                value = 'Regions',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dcc.Dropdown(
                id='entities_box_timeseries',
                options = [{'label': k, 'value': j} for k,j in entities_list],
                multi=True,
                style={'width': '790px'}
             ), style={'display': 'inline-block','width': '800px'}),
            html.Br(),

            html.Div(
            html.P('Show absolute or relative values'),
            style={'display': 'inline-block','width': '210px'}),

            html.Div(
            html.P('Compare entities in language / entities by language'),
            style={'display': 'inline-block','width': '400px'}),

            html.Br(),

            html.Div(
            dcc.RadioItems(
                id='show_absolute_relative_radio_timeseries',
                options=[{'label':'Absolute','value':'Absolute'},{'label':'Relative','value':'Relative'}],
                value='Absolute',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '200px'}
             ), style={'display': 'inline-block','width': '210px'}),

            html.Div(
            dcc.RadioItems(
                id='show_compare_timeseries',
                options=[{'label':'Limit 1 language','value':'1Language'},{'label':'Limit 1 entity','value':'1Entity'}],
                value='1Language',
                labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),

            html.Br(),

            dcc.Graph(id = 'createdaccumulatedmonthly_timeseries1'),
            dcc.Graph(id = 'createdaccumulatedmonthly_timeseries2'),

            dcc.Markdown('''The time series / line chart graphs show for a group of selected Wikipedia language editions and for specific entities of a type of content, the amount of articles and the percentage of each entity that has been both accumulated and created over time. The graphs allow selecting either one Wikipedia language edition and more than one entity from a content type or one single entity from a content type and more than one Wikipedia language edition in order to compare them over time.

            Time is presented in the x-axis and it is possible to select the periods in which articles are aggregated (Yearly, Quarterly and Monthly). The lines can be presented in the y-axis as a result of the number of aggregated articles for that period of time or the extent they take according to the total created or accumulated articles for that content type. The graph contains a range-slider on the bottom to select a specific period of time especially useful when the time aggregation is set to quarterly. It is possible to use predefined specific time selections by clicking on the labels 6M, 1Y, 5Y, 10Y and ALL (last six months, last year, last five years, last ten years and all the time). The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend.
             '''.replace('  ', '')),
            ]),



            dcc.Tab(label='One Wikipedia At A Given Time (Treemap)', children=[

                # 1 SPECIFIC MONTH TREEMAP
                html.Br(),

                # html.H5('Articles by Content Type in a Wikipedia Language Edition On Any Specific Month Treemap'),
                dcc.Markdown('''* **What is the extent of the different types of content created and accumulated in the Wikipedia language editions in a specific month?**
                 '''.replace('  ', '')),


                html.Br(),

                html.Div(
                html.P('Select a Wikipedia'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Content type'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Select a year and month'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Show other content in the graph'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dcc.Dropdown(
                    id='lang_dropdown_treemap',
                    options = [{'label': k, 'value': k} for k in language_names_list],
                    value = 'Catalan (ca)',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dcc.Dropdown(
                    id='content_types_treemap',
                    options = [{'label': k, 'value': k} for k in content_types],
                    value = 'Languages CCC',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dcc.Dropdown(
                    id='time_aggregation_treemap',
                    options = [{'label': k, 'value': k} for k in periods],
                    value = default_period,
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dcc.RadioItems(id='showothercontent_radio_treemap',
                    options=[{'label':'Yes','value':'Yes'},{'label':'No','value':'No'}],
                    value='No',
                    labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 0px"},
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                dcc.Graph(id = 'specificmonth_treemap'),
                #html.Hr(),

                dcc.Markdown('''The treemap graphs show for a selected Wikipedia language edition the extent of the different entities in a content type in both the articles created during a specific month and in the accumulated articles to that date. The size of the tiles is according to the number of articles and the extent (%) is calculated according to the total number of articles from that type of content. By selecting "show other content" you can see in the remaining proportion of articles of existing articles that are not from that type of content.
                 '''.replace('  ', '')),


            ]),


            dcc.Tab(label='Multiple Wikipedias At A Given Time (Scatterplot)', children=[

                # 2 SPECIFIC MONTH SCATTERPLOT
                html.Br(),

                # html.H5('Wikipedia Language Editions By Created and Accumulated Articles On Content Type And Any Specific Month'),

                dcc.Markdown('''* **What Wikipedia language editions have created more content of the different types in a specific month?**'''.replace('  ', '')),                


                dcc.Markdown('''It is possible to select as many Wikipedia language editions and as many entities as preferred from a specific content type, but we recommend to select no more than three of each as the graph may become too cluttered. The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend.
                 '''.replace('  ', '')),

                html.Div(
                html.P('Select a group of Wikipedias'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('You can add or remove languages:'),
                style={'display': 'inline-block','width': '500px'}),

                html.Br(),

                html.Div(
                dcc.Dropdown(
                    id='langgroup_dropdown_scatterplot',
                    options = [{'label': k, 'value': k} for k in lang_groups],
                    value = 'Top 5',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dcc.Dropdown(
                    id='langgroup_box_scatterplot',
                    options = [{'label': k, 'value': k} for k in language_names_list],
                    multi=True,
                    style={'width': '790px'}
                 ), style={'display': 'inline-block','width': '800px'}),

                html.Br(),

                html.Div(
                html.P('Select a content type'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('You can add or remove entities:'),
                style={'display': 'inline-block','width': '500px'}),
                html.Br(),

                html.Div(
                dcc.Dropdown(
                    id='content_types_scatterplot',
                    options = [{'label': k, 'value': k} for k in content_types],
                    value = 'Regions',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dcc.Dropdown(
                    id='entities_box_scatterplot',
                    options = [{'label': k, 'value': j} for k,j in entities_list],
                    value = ['Africa','Asia','Oceania','Antarctica'],
                    multi=True,
                    style={'width': '790px'}
                 ), style={'display': 'inline-block','width': '800px'}),


                html.Br(),

                html.Div(
                html.P('Select a year and a month'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),

                html.Div(
                dcc.Dropdown(
                    id='period_scatterplot',
                    options = [{'label': k, 'value': k} for k in periods],
                    value = default_period,
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                dcc.Graph(id = 'specificmonth_scatterplot1'),
                dcc.Graph(id = 'specificmonth_scatterplot2'),
                #html.Hr(),

                dcc.Markdown('''The scatterplot graphs show for a group of selected Wikipedia language editions and type of content, the amount of articles and the percentage of it that was both accumulated and created at a specific period of time. Wikipedia language editions' entities are represented on x-axis as the number of articles and y-axis as the extent each entity take in the total number of articles created for that content type in that month or accumulated to that point in time.
                 '''.replace('  ', '')),


            ]),




    ]),

    footbar,

], className="container")

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### FUNCTIONS AND CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


def dataframe_periods(df, order, entity, units):

    # print (order, entity, units)
    # print ('oh')
    # print (df.head(10))
    # print ('eh')


    if order == 'get_last_month_of_year':
        # df.to_csv('inici.csv')
        df = df.sort_values(by=['Wiki', entity, 'period'])

        old_year = 0
        index_to_delete = []
        index_list = []
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            cur_year = row['period'].year
            cur_entity = row[entity]

            if (cur_year != old_year and old_year!=0) or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                df.at[old_index, 'period_formatted'] = str(old_year)
                index_to_delete+= index_list
                index_list = []

            cur_index = i
            index_list.append(cur_index)
            old_year = cur_year
            old_index = cur_index
            old_entity = cur_entity

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, 'period_formatted'] = str(old_year)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=[entity,'period'])
        # df.to_csv('final.csv')


    if order == 'get_last_month_of_quarter':
        df = df.sort_values(by=['Wiki', entity, 'period'])

        index_to_delete = []
        index_list = []
        old_quarter = 0
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            pe = row['period']
            cur_month = pe.month
            cur_index = i
            cur_entity = row[entity]
            cur_quarter = pe.quarter

            if (cur_quarter != old_quarter and old_quarter!=0) or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
#                print (row)

            old_quarter = pe.quarter
            old_year = pe.year
            old_entity = cur_entity

            index_list.append(cur_index)
            old_index = cur_index

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])


    if order == 'add_articles_of_year':
        df = df.sort_values(by=[entity, 'period'])

        old_year = 0
        index_to_delete = []
        index_list = []
        units_val = 0
        old_entity = 0
        for i, row in df.iterrows():
            cur_year = row['period'].year
            cur_index = i
            cur_entity = row[entity]

            if (cur_year != old_year and old_year!=0)  or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                df.at[old_index, units] = units_val
                units_val = 0
                df.at[old_index, 'period_formatted'] = str(old_year)

            index_list.append(cur_index)
            units_val += row[units]
            old_year = cur_year
            old_index = cur_index
            old_entity = cur_entity

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, units] = units_val
        df.at[old_index, 'period_formatted'] = str(old_year)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])


    if order == 'add_articles_of_quarter':
        df = df.sort_values(by=[entity, 'period'])

        index_to_delete = []
        index_list = []
        units_val = 0
        old_quarter = 0
        old_entity = 0
        old_index = 0
        for i, row in df.iterrows():
            pe = row['period']
            cur_month = pe.month
            cur_index = i
            cur_entity = row[entity]
            cur_quarter = pe.quarter

            if (cur_quarter != old_quarter and old_quarter!=0)  or (cur_entity != old_entity and old_entity!=0):
                index_list.remove(old_index)
                index_to_delete+= index_list
                index_list = []
                # print (df.loc[old_index])
                df.at[old_index, units] = units_val
                df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
                # print (df.loc[old_index])
                units_val = 0

            old_quarter = cur_quarter
            old_year = pe.year
            old_entity = cur_entity

            index_list.append(cur_index)
            units_val += row[units]
            old_index = cur_index

        index_list.remove(old_index)
        index_to_delete+= index_list
        index_list = []
        df.at[old_index, units] = units_val
        df.at[old_index, 'period_formatted'] = str(old_year) + '-Q' + str(old_quarter)
        df = df.drop(index_to_delete)
#        df = df.sort_values(by=['period'])

    df['period']=df['period_formatted']
    return df





# LANGUAGE CCC TREEMAP
@dash_app13.callback(
    Output('specificmonth_treemap', 'figure'),
    [Input('lang_dropdown_treemap', 'value'), Input('content_types_treemap', 'value'), Input('time_aggregation_treemap', 'value'), Input('showothercontent_radio_treemap','value')])
def update_specific_month_treemap(language, content_type, period, othercontent): #, ps
#    if language == None or content_type == None or period == None or othercontent == None: return None

    conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() # 
    try:
        language = language_names[language]
    except:
        language = None

    # df_created = df_created_dict[content_type]
    # df_accumulated = df_accumulated_dict[content_type]

    if language != None and period != None and content_type != None: 

        # max_period_monthly = df_created.period.max()
        # max_period_accumulated = df_accumulated.period.max()
        # max_period_monthly = '2019-05'
        # max_period_accumulated = '2019-07'
        content_type = content_type_dict[content_type]

        # period = '2019-05'
        # print(' ARA CREATED MONTHLY ')
        query, params = params_to_df(language, content_type, period, 'monthly')
        df_created = pd.read_sql_query(query, conn, params = params).round(1)
#        df_created['mode'] = 'monthly'
        # print (df_created.head(10))
        df_created = df_extended(df_created, content_type)
        # print (df_created.head(10))
        # print ('fi MONTHLY')


#        period = '2019-07'
#        print(' ARA ACCUMULATED ')
        query, params = params_to_df(language, content_type, period, 'accumulated')
        df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
#        df_accumulated['mode'] = 'accumulated'
#        print (df_accumulated.head(10))
        df_accumulated = df_extended(df_accumulated, content_type)
        # print (df_accumulated.head(10))
        # print ('fi ACCUMULATED')


        # df_created = df_created.loc[(df_created['Wiki'] == language) & (df_created['period']==max_period_monthly)]
        # df_accumulated = df_accumulated.loc[(df_accumulated['Wiki'] == language) & (df_accumulated['period']==max_period_accumulated)]

        if othercontent == 'Yes':
            topicpercent = df_created['Extent Articles (%)'].sum()
            topicarts = df_created.Articles.sum()
            totalart = topicarts/(topicpercent/100)
            remainingpercent = 100-topicpercent
            remainingart = totalart-topicarts
            row_df_created = pd.Series(data = {'Wiki':language, 'Group':'wp', 'Content_Type':content_type, 'Entity':'Other Non '+content_type+' Content', 'Articles':remainingart, 'Extent Articles (%)':remainingpercent, 'period': period, 'Language':language_names_full[language]})

            topicpercent = df_accumulated['Extent Articles (%)'].sum(); # print (topicpercent)
            topicarts = df_accumulated.Articles.sum();
            totalart = topicarts/(topicpercent/100)
            remainingpercent = 100-topicpercent; #print (remainingpercent)
            remainingart = totalart-topicarts
            row_df_accumulated = pd.Series(data = {'Wiki':language, 'Group':'wp', 'Content_Type':content_type, 'Entity':'Other Non '+content_type+' Content', 'Articles':remainingart, 'Extent Articles (%)':remainingpercent, 'period': period, 'Language':language_names_full[language]})

            df_created = df_created.append(row_df_created, ignore_index=True)
            df_accumulated = df_accumulated.append(row_df_accumulated, ignore_index=True)

            df_created.Articles = df_created.Articles.astype(int)
            df_accumulated.Articles = df_accumulated.Articles.astype(int)
            df_created = df_created.round(1)
            df_accumulated = df_accumulated.round(1)



    fig = go.Figure()
    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
#        subplot_titles = ('Accumulated Articles'+'<br />&nbsp;<br />', 'Created Articles'+'<br />&nbsp;<br />'),
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )

    if language != None and period != None and content_type != None: 

        parents = list()
        for x in df_created.index:
            parents.append('')

        parents2 = list()
        for x in df_accumulated.index:
            parents2.append('')

        fig.add_trace(go.Treemap(
            parents = parents,
            labels = df_created['Entity'],
            values = df_created['Articles'],
            customdata = df_created['Extent Articles (%)'],
            text = df_created['Entity_Extra'],
            texttemplate = "<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>",
            hovertemplate='<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>%{text}<br><extra></extra>',
            ),
                row=1, col=1)

        fig.add_trace(go.Treemap(
            parents = parents2,
            labels = df_accumulated['Entity'],
            values = df_accumulated['Articles'],
            customdata = df_accumulated['Extent Articles (%)'],
            text = df_accumulated['Entity_Extra'],
            texttemplate = "<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>",
            hovertemplate='<b>%{label} </b><br>Extent Articles: %{customdata}%<br>Articles: %{value}<br>%{text}<br><extra></extra>',
            ),
                row=1, col=2)

        extra = ""
    else:
        if period != None:
            extra = " in A Specific Month ("+period+")"

    fig.update_layout(
        autosize=True,
#        width=700,
        height=900,
        title_font_size=12,
        title_text="Created Articles (Left) and Accumulated Articles (Right)"+extra,
        title_x=0.5,
    )

    return fig

########################


# GIVEN MONTH SCATTERPLOT
@dash_app13.callback(
    [Output('specificmonth_scatterplot1', 'figure'), Output('specificmonth_scatterplot2', 'figure')],
    [Input('langgroup_box_scatterplot', 'value'), Input('content_types_scatterplot', 'value'), Input('entities_box_scatterplot','value'), Input('period_scatterplot','value')])
def update_specific_month_scatterplot(languages, content_type, entities, period):
#    if languages == None or content_type == None or entities == None or period == None: return (None,None)
        
    if languages == None:
        languages = []

    langs = []
    if type(languages) != str:
        for x in languages: langs.append(language_names[x])
    else:
        langs.append(language_names[languages])

    ents = []
    if type(entities) == str:
        try:
            ents = [language_name_wiki[entities]]
        except:
            ents = [entities]
    elif entities != None:
        try:
            for x in entities: ents.append(language_name_wiki[x])
        except:
            ents = entities

    if period == None: 
        period = ''
        extra = ' ('+period+')'
    else:
        extra = ''

    df_created = pd.DataFrame()
    df_accumulated = pd.DataFrame()

    fig = px.scatter()
    fig2 = px.scatter()

    content_type = content_type_dict[content_type]

    if content_type != None:
        conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor()

        # df_created = df_created_dict[content_type]
        # df_accumulated = df_accumulated_dict[content_type]

        query, params = params_to_df(langs, content_type, period, 'monthly')
        df_created = pd.read_sql_query(query, conn, params = params).round(1)
        df_created = df_extended(df_created, content_type)

        query, params = params_to_df(langs, content_type, period, 'accumulated')
        df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
        df_accumulated = df_extended(df_accumulated, content_type)


        # df_created = df_created.loc[(df_created['period']==period) & (df_created['Wiki'].isin(langs)) & (df_created['Entity'].isin(ents))]

        # df_accumulated = df_accumulated.loc[(df_accumulated['period']==period) & (df_accumulated['Wiki'].isin(langs)) & (df_accumulated['Entity'].isin(ents))]

        df_created["Entity (Wiki)"] = df_created["Entity"]+" ("+df_created["Wiki"]+")"
        df_accumulated["Entity (Wiki)"] = df_accumulated["Entity"]+" ("+df_accumulated["Wiki"]+")"

        df_created = df_created.loc[(df_created['Entity'].isin(ents))]
        df_accumulated = df_accumulated.loc[(df_accumulated['Entity'].isin(ents))]

        # print (df_created.head(10))
        # print (df_accumulated.head(10))

        fig = px.scatter(df_accumulated, x="Articles", y="Extent Articles (%)", color="Wiki", log_x=True, log_y=False,hover_data=['Language','Region'], text="Entity (Wiki)", size_max=60)
        fig.update_traces(
            textposition='top center')

        fig2 = px.scatter(df_created, x="Articles", y="Extent Articles (%)", color="Wiki", log_x=True, log_y=False,hover_data=['Language','Region'], text="Entity (Wiki)", size_max=60)
        fig2.update_traces(
            textposition='top center')


    fig2.update_layout(
        height=600,
        title_font_size=12,
        xaxis=dict(
#            title=time_aggregation,
            titlefont_size=12,
            tickfont_size=10,
        ),
        yaxis=dict(
#            title=created_accumulated,
            titlefont_size=12,
            tickfont_size=10,
        ),
        title_text='Comparison of Wikipedias by Content Types Created in A Specific Month'+extra
    )

    fig.update_layout(
        height=600,
        title_font_size=12,
        xaxis=dict(
#            title=time_aggregation,
            titlefont_size=12,
            tickfont_size=10,
        ),
        yaxis=dict(
#            title=created_accumulated,
            titlefont_size=12,
            tickfont_size=10,
        ),
        title_text='Comparison of Wikipedias by Content Types Accumulated in A Specific Month'+extra
    )

    return (fig, fig2)

########################


def create_fig_barchart(created_accumulated, df, time_aggregation, content_type, absolute_relative, entities_list):

    fig = go.Figure()

    if absolute_relative == 'Absolute':
        customdata = 'Extent Articles (%)'
        y = 'Articles'
        hovertemplate_extent = '<br>Extent Articles: %{customdata}%'
        hovertemplate_articles = '<br>Articles: %{y}'
        yaxis = content_type+' Entities by Number of Articles'
    else:
        customdata = 'Articles'
        y = 'Extent Articles (%)'
        hovertemplate_extent = '<br>Extent Articles: %{y}%'
        hovertemplate_articles = '<br>Articles: %{customdata}'
        yaxis = content_type+' Entities by Percentage of Articles'


    if time_aggregation != '':
        titletext = time_aggregation+' '+created_accumulated+' on '+content_type
        title = 'Periods of Time ('+time_aggregation+')'


        for entity_name in entities_list:
            d = df.loc[(df['Entity'] == entity_name)]

            fig.add_trace(go.Bar(
                customdata=d[customdata],
                y = d[y],
                x=d['period'],
                name=entity_name,
                texttemplate='%{y}',
                hovertemplate=str(entity_name)+hovertemplate_articles+hovertemplate_extent+'<br>Period: %{x}<br><extra></extra>',
            ))

    else:
        if content_type != '':
            titletext = created_accumulated+' on '+content_type
        else:
            titletext = created_accumulated            
        title = 'Periods of Time'
#        df = df.drop(df.index, inplace=True)

    fig.update_layout(
        xaxis=dict(
            title=title,
            titlefont_size=12,
            tickfont_size=10,

            rangeselector=dict(
                buttons=list([
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(count=5,
                         label="5y",
                         step="year",
                         stepmode="backward"),
                    dict(count=10,
                         label="10y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(
            visible = True
            ),
            type="date"
        ),
        yaxis=dict(
            title=yaxis,
            titlefont_size=12,
            tickfont_size=10,
        ),
        title_font_size=12,
        title_text=titletext,
        legend = dict(
            font=dict(
#                family="sans-serif",
                size=12
#                color="black"
            ),
            traceorder="normal"
            ),
        autosize=True,
        height = 500,
        width=1200,
        barmode='stack')

    return fig



# MONTHLY BARCHART
@dash_app13.callback(
    [Output('createdaccumulatedmonthly_barchart1', 'figure'), Output('createdaccumulatedmonthly_barchart2', 'figure')], [Input('lang_dropdown_barchart', 'value'), Input('content_types_barchart', 'value'), Input('time_aggregation_barchart', 'value'), Input('show_absolute_relative_radio_barchart','value')])
def update_monthly_barchart(language, content_type, time_aggregation, absolute_relative):

#    print (language, content_type, time_aggregation, absolute_relative)

    if time_aggregation == None: time_aggregation = ''
    if language == None: language = ''
    if content_type == None: content_type = ''

    functionstartTime = time.time()

    if content_type != '':
        ct = content_type_dict[content_type]
        conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() #

#        print (content_type)

        query, params = params_to_df(language_names[language], ct, None, 'monthly')
#        print (query, params)
        df_created = pd.read_sql_query(query, conn, params = params).round(1)
        df_created = df_extended(df_created, ct)

        query, params = params_to_df(language_names[language], ct, None, 'accumulated')
#        print (query)
        df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
        df_accumulated = df_extended(df_accumulated, ct)

        # print('abans de periods'+str(datetime.timedelta(seconds=time.time() - functionstartTime)))
        # print (df_created.head(10))
        # print (df_accumulated.head(10))

        # df_created.to_csv('cr.csv')
        # df_accumulated.to_csv('acc.csv')


        if time_aggregation == 'Quarterly': df_created = dataframe_periods(df_created, 'add_articles_of_quarter','Entity','Articles') # CREATED QUARTER

        if time_aggregation == 'Quarterly': df_accumulated = dataframe_periods(df_accumulated, 'get_last_month_of_quarter','Entity','Articles') # ACCUMULATED QUARTER

        if time_aggregation == 'Yearly': df_created = dataframe_periods(df_created, 'add_articles_of_year','Entity','Articles') # CREATED YEAR

        if time_aggregation == 'Yearly': df_accumulated = dataframe_periods(df_accumulated, 'get_last_month_of_year','Entity','Articles') # ACCUMULATED YEAR

        # print (df_created.head(10))
        # print (df_accumulated.head(10))


        df_accumulated = df_accumulated.sort_values(by=['Articles','Entity','period'], ascending = False)
    
        entities_list = list(df_accumulated.Entity.unique())

        df_accumulated['Period Articles'] = df_accumulated['period'].map(df_accumulated.groupby('period').Articles.sum())
        df_accumulated['extent'] = 100*df_accumulated['Articles']/df_accumulated['Period Articles']
        df_accumulated['Extent Articles (%)'] = df_accumulated['extent'].round(1)
    
        df_accumulated = df_accumulated.loc[(df_accumulated['Entity'].isin(entities_list[:15]))]

        df_created = df_created.sort_values(by=['Articles','Entity','period'], ascending = False)#, reverse=True)
        df_created['Period Articles'] = df_created['period'].map(df_created.groupby('period').Articles.sum())
        df_created['Extent Articles (%)'] = 100*df_created['Articles']/df_created['Period Articles']
        df_created = df_created.round(1)


        df_created['period'] = pd.to_datetime(df_created['period'])
        df_accumulated['period'] = pd.to_datetime(df_accumulated['period'])


    fig = create_fig_barchart('Accumulated Articles', df_accumulated, time_aggregation, content_type, absolute_relative, entities_list)
    fig2 = create_fig_barchart('Created Articles', df_created, time_aggregation, content_type, absolute_relative, entities_list)

#    print('després de generar figs'+str(datetime.timedelta(seconds=time.time() - functionstartTime)))

    return (fig,fig2)



########################

def create_fig_timeseries(created_accumulated, df, content_type, absolute_relative):
    fig = go.Figure()

    if absolute_relative == 'Absolute':
        customdata = 'Extent Articles (%)'
        y = 'Articles'
        hovertemplate_extent = '<br>Extent Articles: %{customdata}%'
        hovertemplate_articles = '<br>Articles: %{y}'
        yaxis = 'Number of Articles ' + created_accumulated
    else:
        customdata = 'Articles'
        y = 'Extent Articles (%)'
        hovertemplate_extent = '<br>Extent Articles: %{y}%'
        hovertemplate_articles = '<br>Articles: %{customdata}'
        yaxis = 'Percentage of Articles ' + created_accumulated

    fig = go.Figure()
    for entity_name in list(df["Entity (Wiki)"].unique()):
#        print (entity_name)
        d = df.loc[(df["Entity (Wiki)"] == entity_name)]

        fig.add_trace(go.Scatter(
            customdata=d[customdata],
            y = d[y],
            x=d['period'],
            name=entity_name,
            hovertemplate=str(entity_name)+hovertemplate_articles+hovertemplate_extent+'<br>Period: %{x}<br><extra></extra>'
        ))

    fig.update_layout(
        xaxis=dict(
            title='Time (Monthly)',
            titlefont_size=12,
            tickfont_size=10,

            rangeselector=dict(
                buttons=list([
                    dict(count=6,
                         label="6m",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="1y",
                         step="year",
                         stepmode="backward"),
                    dict(count=5,
                         label="5y",
                         step="year",
                         stepmode="backward"),
                    dict(count=10,
                         label="10y",
                         step="year",
                         stepmode="backward"),
                    dict(step="all")
                        ])
                    ),
                    rangeslider=dict(
                        visible=False
                    ),
                    type="date"

        ),
        yaxis=dict(
            title=yaxis,
            titlefont_size=12,
            tickfont_size=10,
        ),

        title_font_size=12,
        title_text=created_accumulated+' Articles on '+content_type,
        legend = dict(
            font=dict(
#                family="sans-serif",
                size=12
#                color="black"
            ),
            traceorder="normal"
            ),
        autosize=True,
        height = 500,
        width=1200)

    return fig


# MONTHLY TIME SERIES
@dash_app13.callback(
    [Output('createdaccumulatedmonthly_timeseries1', 'figure'), Output('createdaccumulatedmonthly_timeseries2', 'figure')],
    [Input('langgroup_box_timeseries', 'value'),
    Input('content_types_timeseries', 'value'), 
    Input('entities_box_timeseries', 'value'), 
    Input('show_absolute_relative_radio_timeseries','value')])
def update_monthly_time_series(languages, content_type, entities, absolute_relative):

    if entities == None:
        entities = []
    if languages == None:
        languages = []

#    print (languages, content_type, entities, absolute_relative, 'update_monthly_time_series')
    langs = []
    if type(languages) != str:
        for x in languages: langs.append(language_names[x])
    else:
        langs.append(language_names[languages])

    ents = []
    if type(entities) != str:
        try:
            for x in entities: ents.append(language_name_wiki[x])
        except:
            ents = entities
    else:
        try:
            ents = [language_name_wiki[entities]]
        except:
            ents = [entities]



    ct = content_type_dict[content_type]
    conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() #

    query, params = params_to_df(langs, ct, None, 'monthly')
    df_created = pd.read_sql_query(query, conn, params = params).round(1)
    df_created = df_extended(df_created, ct)

    query, params = params_to_df(langs, ct, None, 'accumulated')
    df_accumulated = pd.read_sql_query(query, conn, params = params).round(1)
    df_accumulated = df_extended(df_accumulated, ct)

    # # ACCUMULATED
    # df_accumulated = pd.DataFrame()
    # df_accumulated = df_accumulated_dict[content_type]
    # df_accumulated = df_accumulated.loc[(df_accumulated['Wiki'].isin(langs)) & (df_accumulated['Entity'].isin(ents))]
    df_accumulated = df_accumulated.loc[(df_accumulated['Entity'].isin(ents))]

    # # CREATED
    # df_created = pd.DataFrame()
    # df_created = df_created_dict[content_type]
    # df_created = df_created.loc[(df_created['Wiki'].isin(langs)) & (df_created['Entity'].isin(ents))]
    df_created = df_created.loc[(df_created['Entity'].isin(ents))]

    df_accumulated["Entity (Wiki)"] = df_accumulated["Entity"]+" ("+df_accumulated["Wiki"]+")"
    df_created["Entity (Wiki)"] = df_created["Entity"]+" ("+df_created["Wiki"]+")"

    fig = create_fig_timeseries('Accumulated', df_accumulated, content_type, absolute_relative)
    fig2 = create_fig_timeseries('Created', df_created, content_type, absolute_relative)

    return fig, fig2


########################


# Enable entities options
# Dropdown entities
def entities_group_options(selected_group):

    if selected_group == 'Countries': entities_list = country_names_inv

    if selected_group == 'Subregions': entities_list = subregions_dict

    if selected_group == 'Regions': entities_list = regions_dict

    if selected_group == 'Languages CCC': entities_list = language_names

    if selected_group == 'Top CCC Lists': entities_list = lists_dict

    if selected_group == 'Gender': entities_list = people_dict

    if selected_group == 'Entire Wikipedia': entities_list = {'Entire Wikipedia':'Entire Wikipedia'}

    if len(entities_list) <= 10:
        selected_entities = random.sample(list(entities_list.keys()),len(entities_list))
    else:
        selected_entities = []

    if len(entities_list) > 9:
        sample = 9
    else:
        sample = len(entities_list)

    if selected_group != None:
        selected_entities = random.sample(list(entities_list.keys()),sample)
    else:
        selected_entities = []

    entities_list = [{'label': k, 'value': k} for k,j in entities_list.items()]

    return entities_list, selected_entities


@dash_app13.callback(
    [Output('entities_box_timeseries', 'options'),
    Output('entities_box_timeseries', 'value')],
    [Input('content_types_timeseries', 'value'),
    Input('show_compare_timeseries','value')
    ])
def set_entities_group_options_given_timeseries(selected_group, compare):
    entities_list, selected_entities = entities_group_options(selected_group)
    if compare == '1Entity': selected_entities = []
    return entities_list, selected_entities


# Dropdown languages and entities
@dash_app13.callback(
    [Output('langgroup_box_timeseries','multi'), 
    Output('entities_box_timeseries','multi'),
    Output('langgroup_dropdown_timeseries', 'value'),
    Output('langgroup_dropdown_timeseries', 'disabled')
    ],
    [Input('show_compare_timeseries','value')])
def limit_langs_entities(compare):

    if compare == '1Language':
        languages = False
        entities = True
        group_disabled = True
    else:
        entities = False
        languages = True
        group_disabled = False

    return (languages,entities,[],group_disabled)


# Dropdown languages
@dash_app13.callback(
    Output('langgroup_box_timeseries', 'value'),
    [Input('langgroup_dropdown_timeseries', 'value'), 
    Input('show_compare_timeseries','value')])
def set_langs_group_options_time_series(selected_group, compare):

#    print (selected_group, compare, 'set_langs_group_options_time_series')
    if compare == '1Language':
        return []

    if compare == '1Entity' and selected_group != None and len(selected_group)!=0:
        langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
        available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
        list_options = []
        for item in available_options: list_options.append(item['label'])
        return sorted(list_options,reverse=False)
#        return ['Catalan (ca)','French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)']


########################


# Dropdown entities
@dash_app13.callback(
    [Output('entities_box_scatterplot', 'options'),Output('entities_box_scatterplot', 'value')],
    [Input('content_types_scatterplot', 'value')])
def set_entities_group_options_given_scatterplot(selected_language_group):
    return entities_group_options(selected_language_group)

# Dropdown languages
@dash_app13.callback(
    Output('langgroup_box_scatterplot', 'value'),
    [Input('langgroup_dropdown_scatterplot', 'value')])
def set_langs_group_options_scatterplot(selected_language_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_language_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options: list_options.append(item['label'])
    return sorted(list_options,reverse=False)

#    return ['Catalan (ca)','French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)']


# ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

# if __name__ == '__main__':
#     dash_app13.run_server(debug=True)
