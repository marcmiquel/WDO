import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

eg = group_labels.loc[(group_labels["lang"] == 'en') & (group_labels["category_label"] == 'religious_groups')][['qitem','label','page_title']]

religious_groups_dict = {}
for index, rows in eg.iterrows():
    qitem = rows['qitem']
    label = rows['label']
    page_title = rows['page_title']

    if page_title != '':
        value = page_title
    elif label != '':
        value = label
    else:
        value = qitem
    religious_groups_dict[value] = qitem

eg = group_labels.loc[(group_labels["category_label"] == 'religious_groups')][['qitem','label','page_title']]

for index, rows in eg.iterrows():
    qitem = rows['qitem']   
    if qitem not in religious_groups_dict.values():
        religious_groups_dict[qitem] = qitem

religious_groups_dict_inv = {v: k for k, v in religious_groups_dict.items()}

#print (religious_groups_dict)

people_dict = {'People in Wikipedia':'wikipediapeople','People in CCC':'wikipediacccpeople'} # 'People in Wikidata':'wikidatapeople',
people_inv = {v: k for k, v in people_dict.items()}



def params_to_df(langs, content_type):

    conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 
    # lang = language_names[lang]
    functionstartTime = time.time()
    if isinstance(langs,str): langs = [langs]
    lass = ','.join( ['?'] * len(langs) )



# 1 stackedbar


	# NO FUNCIONA
    if content_type == 'religious_group_names_wikidata_extent':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 = 'wikidata_article_qitems' AND set1descriptor = 'people' AND set2 = 'religious_group' AND period = '"+last_period+"' ORDER BY abs_value DESC;"

        df = pd.read_sql_query(query, conn)


    # FUNCIONA
    if content_type == 'religious_group_names_wikipedia_extent':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 IN ("+lass+") AND set1descriptor = 'people' AND set2 = 'religious_group' AND period = '"+last_period+"' ORDER BY abs_value DESC;"

        df = pd.read_sql_query(query, conn, params = langs)


    # FUNCIONA
    if content_type == 'religious_group_names_ccc_extent':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 IN ("+lass+") AND set1descriptor = 'ccc_people' AND set2 = 'religious_group' AND period = '"+last_period+"' ORDER BY abs_value DESC;"

        df = pd.read_sql_query(query, conn, params = langs)



# 2 scatterplot
	
	# NO FUNCIONA
    if content_type == 'religious_group_names_coverage':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 = 'wikidata_article_qitems' AND set2descriptor = 'wp' AND period = '"+last_period+"' AND set2 IN ("+lass+") ORDER BY abs_value DESC;" # aquí potser falta que passem els tipus de sexual orientation per paràmetre.

        df = pd.read_sql_query(query, conn, params = langs)



# 3 barchart
	
	# NO FUNCIONA
    if content_type == 'religious_group_wikidata_extent':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 = 'wikidata_article_qitems' AND set1descriptor = 'people' AND set2 = 'wikidata_article_qitems' AND set2descriptor = 'religious_group' AND period = '"+last_period+"' ORDER BY abs_value DESC;"

        df = pd.read_sql_query(query, conn)


	# NO FUNCIONA
    if content_type == 'religious_group_wikipedia_extent':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = 'articles' AND set1 IN ("+lass+") AND set1descriptor = 'people' AND set2 = 'wikidata_article_qitems' AND set2descriptor = 'religious_group' AND period = '"+last_period+"' ORDER BY abs_value DESC;"

        df = pd.read_sql_query(query, conn, params = langs)

    return df



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
# dash_app16 = Dash(url_base_pathname = '/religious_groups_gap/', external_stylesheets=external_stylesheets, suppress_callback_exceptions = True)

dash_app16 = Dash(__name__, server = app, url_base_pathname= webtype + '/religious_groups_gap/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app16.config['suppress_callback_exceptions']=True

title = "Religious Groups Gap"
dash_app16.title = title+title_addenda



dash_app16.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that illustrate the Religious Groups gap in Wikipedia language editions content. They depict both Religious Groups understood as biographies of people with a non-heterosexual orientational and topics that relate to the Religious Groups cultural contexts (e.g. places, monuments, traditions, etc.). For more information on the gaps, you can retrieve lists of Religious Groups articles in the [Top CCC Lists Dashboard](https://wdo.wmflabs.org).

        This dashboard is in **Alpha stage**, more measures will be incorporated.
        '''),


    # dcc.Tabs([

        # 1 Stacked Bars
        # dcc.Tab(label='Extent of the Religious Groups Biographies (Stacked Bars)', children=[


    html.Br(),
    html.Br(),

    html.H6('Extent of the Religious Groups Biographies (Stacked Bars)', style={'textAlign':'left'}),

    html.Br(),
    dcc.Markdown('''* **What is the extent of the different types of Religious group in the language editions people's articles, in their CCC and in all the people in Wikidata?**
     '''.replace('  ', '')),

    dcc.Markdown('''
        It is possible to select as many Wikipedia language editions as preferred from a specific content type, but we recommend to select groups of 40 max. as there may be too many bars.
        '''),

    html.Br(),

    html.Div(
    html.P('Select people'),
    style={'display': 'inline-block','width': '200px'}),

    html.Div(
    html.P('Filter out'),
    style={'display': 'inline-block','width': '200px'}),

    html.Div(
    html.P('Select the y-axis'),
    style={'display': 'inline-block','width': '200px'}),

    html.Div(
    html.P('Minimal extent (%) to appear'),
    style={'display': 'inline-block','width': '200px'}),



    html.Br(),
    html.Div(
    dcc.Dropdown(
        id='peopledrowndown',
        options=[{'label': k, 'value': l} for k,l in people_dict.items()],
        value='wikipediapeople',
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),


    html.Div(
    dcc.Dropdown(
        id='sodropdown',
        options=[{'label': k, 'value': l} for k,l in religious_groups_dict.items()],
        value='',
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),

    html.Div(
    dcc.RadioItems(
        id='stacked_absolute_relative_radio_y',
        options=[{'label':'Number of Articles','value':'articles'},{'label':'% People','value':'percentage'}],
        value='articles',
        labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 5px"},
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),


   html.Div(
    dcc.Input(
        id='minimal_percentage',                    
        placeholder='Extent to appear',
        type='text',
        value=0.02,
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),



    html.Br(),
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

    dcc.Dropdown(id='sourcelangdropdown_religious_groups_gap',
        options = [{'label': k, 'value': k} for k in language_names_list],
        multi=True),


    dcc.Graph(id = 'religious_groups_names_stacked_bar'),
    dcc.Markdown('''
        The barchart shows the selected Wikipedia language editions by the number of articles for people with a Religious group Wikidata property. Each color represents a Religious group. You can filter out any type to see better with a dropdown menu or you can select a single type by double-clicking on it in the legend.'''.replace('  ', '')),



        # ]),






        # # 2 Scatterplot
        # dcc.Tab(label='Religious Groups Coverage (Scatterplot)', children=[ # 'Religious Groups Coverage and Extent (Scatterplot)'
        #     html.Br(),
        #     dcc.Markdown('''* **What is the coverage of the articles related to Religious Groups by the different Wikipedia language editions?**
        #      '''.replace('  ', '')),


        #     dcc.Markdown('''
        #         It is possible to select as many Wikipedia language editions as preferred from a specific content type, but we recommend to select groups of 40 max. as the graph may become too cluttered. The graph provides additional information on each point by hovering as well as it allows selecting a specific language and exclude the rest by clicking on it on the legend..
        #         '''),

        #     html.Br(),
        #     html.Div(
        #     html.P('Select a group of Wikipedias'),
        #     style={'display': 'inline-block','width': '200px'}),

        #     html.Br(),
        #     html.Div(
        #     dcc.Dropdown(
        #         id='grouplangdropdown2',
        #         options=[{'label': k, 'value': k} for k in lang_groups],
        #         value='Top 10',
        #         style={'width': '190px'}
        #      ), style={'display': 'inline-block','width': '200px'}),
        #     html.Br(),

        #     dcc.Dropdown(id='sourcelangdropdown_religious_groups_gap_scatter',
        #         options = [{'label': k, 'value': k} for k in language_names_list],
        #         multi=True),

        #     html.Br(),

        #     html.Div(
        #     html.P('Show only specific Religious Groups (optional)'),
        #     style={'display': 'inline-block','width': '300px'}),

        #     html.Br(),

        #     dcc.Dropdown(
        #         id='rgdropdown',
        #         options=[{'label': k, 'value': l} for k,l in religious_groups_dict.items()],
        #         value='',
        #         multi=True),
        #      #    style={'width': '190px'}
        #      # ), style={'display': 'inline-block','width': '200px'}),


        #     html.Br(),
   
        #     html.Div(id='none',children=[],style={'display': 'none'}),

        #     dcc.Graph(id = 'religious_groups_coverage_extent_scatterplot'),

        #     dcc.Markdown('''This scatterplot graph shows the coverage of the articles related to Religious Groups for a group of selected Wikipedia language editions. On the y-axis we see the number of articles and on the x-axis the percentage of coverage by each of the selected Wikipedia language editions.
        #      '''.replace('  ', '')),


        # ]),










    	# 3 Barchart
      #   dcc.Tab(label='Religious Groups Articles Extent in People (Barchart)', children=[
      #       html.Br(),
      #       dcc.Markdown('''* **What is the extent of biographies with a religious groups marker?**
      #        '''.replace('  ', '')),

      #       html.Div(
      #       html.P('Select a group of Wikipedias'),
      #       style={'display': 'inline-block','width': '200px'}),

      #       html.Br(),
      #       html.Div(
      #       dcc.Dropdown(
      #           id='grouplangdropdown',
      #           options=[{'label': k, 'value': k} for k in lang_groups],
      #           value='Top 10',
      #           style={'width': '190px'}
      #        ), style={'display': 'inline-block','width': '200px'}),
      #       html.Br(),

      #       dcc.Dropdown(id='sourcelangdropdown_2',
      #           options = [{'label': k, 'value': k} for k in language_names_list],
      #           multi=True),



		    # html.Br(),
		    # html.Br(),

      #       html.Div(
      #       dcc.RadioItems(
      #           id='barchart_absolute_relative_radio_y',
      #           options=[{'label':'Absolute','value':'absolute'},{'label':'Relative','value':'relative'}],
      #           value='relative',
      #           labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 5px"},
      #           style={'width': '390px'}
      #        ), style={'display': 'inline-block','width': '400px'}),



		    # dcc.Graph(id = 'religious_group_wikipedia_extent_barchart'),


            # dcc.Markdown('''The barchart graph shows for a selected geographical entity (country, world subregion or region) the degree of coverage of its geolocated articles/Qitems in them by all Wikipedia language editions or a selected group. By hovering in each Wikipedia you can see the coverage percentage as well, the extent it takes in percentage of their total number of articles, the total number of articles geolocated in that geographical entity as well as other geographical information.'''.replace('  ', '')),



			# * Select Group (on Wikidata pot ser un)
			# * Select Languages
			# * Select % or Absolute for Y

			# Bars:
			# X bars: Languages
			# Y: % People with religious Groups
			# Color: Número d’articles

			# s’ha de veure si canviar

        # ]),


    # ]),


	footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 



# Dropdown languages
@dash_app16.callback(
    dash.dependencies.Output('sourcelangdropdown_religious_groups_gap_scatter', 'value'),
    [dash.dependencies.Input('grouplangdropdown2', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    return re

@dash_app16.callback(
    dash.dependencies.Output('sourcelangdropdown_religious_groups_gap', 'value'),
    [dash.dependencies.Input('grouplangdropdown', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    return re




# 1 Stacked Bars
# Extent of Religious Group Names (Stacked Bars)
@dash_app16.callback(
    Output('religious_groups_names_stacked_bar', 'figure'),
    [Input('sodropdown', 'value'),Input('peopledrowndown', 'value'),Input('stacked_absolute_relative_radio_y', 'value'),Input('sourcelangdropdown_religious_groups_gap', 'value'),Input('minimal_percentage', 'value')])
def religious_group_types_stacked_bar(ethnicgroup, content, absolute_relative, langs, minimal_percentage):
    functionstartTime = time.time()
    languagecodes = []
    for l in langs:
        languagecodes.append(language_names[l])

    if content == 'wikipediacccpeople': content_type = 'religious_group_names_ccc_extent'

    if content == 'wikipediapeople': content_type = 'religious_group_names_wikipedia_extent'

    # print (languagecodes)
    # print (content_type)
    # print (minimal_percentage)

    df = params_to_df(languagecodes, content_type).round(3)

    df['Religious Group'] = df['set2descriptor'].map(religious_groups_dict_inv)
    df = df.rename(columns={"set1": "Wiki", "abs_value": "Biographies with Religious Group", "rel_value":"% People with a Religious Group", "set2descriptor":"Qitem"}).dropna()
    df['Language'] = df['Wiki'].map(language_names_full)

    try:
        minimal_percentage = float(minimal_percentage)
    except:
        minimal_percentage = None

    if minimal_percentage != None:
        df = df.loc[df['% People with a Religious Group'] > minimal_percentage]

    df = df.sort_values(by=['Language','% People with a Religious Group'], ascending=False)
#'Biographies with Religious Group',

    # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')

    # print (df.head(10))

    if ethnicgroup != None:
        df = df[df.Qitem != ethnicgroup]

    if absolute_relative == "articles":
        y = "Biographies with Religious Group"
    else:
        y = "% People with a Religious Group"

    fig = px.bar(df, x="Language", y=y, color="Religious Group", title="Types of Religious Group", hover_data=['Wiki','Religious Group','% People with a Religious Group','Biographies with Religious Group','Qitem'], text='Biographies with Religious Group')

    fig.update_layout(
#        barmode='stack', 

        font=dict(size=12),
        xaxis={'categoryorder':'category ascending','titlefont_size':12, 'tickfont_size':12},

        yaxis=dict(
#            title='Covered Articles',
            titlefont_size=12,
            tickfont_size=10,
        ))

    return fig



# 2 Scatterplot
# Religious Groups Coverage and Extent (Scatterplot)
@dash_app16.callback(Output('religious_groups_coverage_extent_scatterplot', 'figure'), # Output('religious_groups_coverage_extent_scatterplot1', 'figure'),Output('religious_groups_coverage_extent_scatterplot2', 'figure')
    [Input('rgdropdown', 'value'),Input('sourcelangdropdown_religious_groups_gap_scatter', 'value')])
def religious_groups_coverage_extent_scatterplot(ethnicgroups, langs):
    functionstartTime = time.time()

    languagecodes = []
    for l in langs:
        languagecodes.append(language_names[l])


    df2 = params_to_df(languagecodes, "religious_group_names_coverage").round(3)

    print (df2.set1descriptor.unique())

    df2 = df2.rename(columns={"set1": "Wiki", "abs_value": "Religious Groups Articles", "rel_value":"Extent Articles (%) in Wikipedia", "set2descriptor":"Qitem"}).dropna()

    if isinstance(ethnicgroups, str) and ethnicgroups != '':
        ethnicgroups = [ethnicgroups]

    if len(ethnicgroups) > 0:
        df2 = df2.loc[(df2['Qitem'].isin(ethnicgroups))]
    else:
        df2 = df2.loc[df2['Extent Articles (%) in Wikipedia'] > 0.02]

    df2['Language'] = df2['Wiki'].map(language_names_full)
    df2['Religious Group'] = df2['Qitem'].map(religious_groups_dict_inv)
    df2 = df2.sort_values(by=['Language'], ascending=False)
    df2["Religious Group (Wiki)"] = df2["Religious Group"]+" ("+df2["Wiki"]+")"

    # print (df2.head(10))
    # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')


    fig2 = px.scatter(df2, x="Extent Articles (%) in Wikipedia", y="Religious Groups Articles", color="Wiki", log_x=True, log_y=True,hover_data=['Wiki','Language'], text='Religious Group (Wiki)', size_max=60) # text="Entity (Wiki)",
    # ,'Region'

    fig2.update_traces(
        textposition='top center')

    return fig2









# # Dropdown entities
# @dash_app16.callback(
#     [Output('entities_box_scatterplot', 'options'),Output('entities_box_scatterplot', 'value')],
#     [Input('content_types_scatterplot', 'value')])
# def set_entities_group_options_given_scatterplot(selected_language_group):
#     return entities_group_options(selected_language_group)

# # Dropdown languages
# @dash_app16.callback(
#     Output('langgroup_box_scatterplot', 'value'),
#     [Input('langgroup_dropdown_scatterplot', 'value')])
# def set_langs_group_options_scatterplot(selected_language_group):
#     langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_language_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
#     available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
#     list_options = []
#     for item in available_options: list_options.append(item['label'])
#     return sorted(list_options,reverse=False)





# 3 Barchart
# Sexual Orientation Articles Extent in People (Barchart)
# @dash_app16.callback(Output('religious_group_wikipedia_extent_barchart', 'figure'),
#     [Input('grouplangdropdown', 'value'), Input('sourcelangdropdown_2', 'value'), Input('barchart_absolute_relative_radio_y', 'value')])
# def religious_group_wikipedia_extent_barchart(entityspecific, entitytype, languages):

	# INPUTS
	# * Select Group (on Wikidata pot ser un)
	# * Select Languages
	# * Select % or Absolute for Y


	# FIGURES
	# Bars:
	# X bars: Languages
	# Y: % People from religious Group
	# Color: Número d’articles

	# s’ha de veure si canviar

	# fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)


    # langs = []
    # if type(languages) == list and len(languages) != 0:
    # 	for x in languages: 
    # 		langs.append(language_names[x])

    # elif type(languages) == str:
    # 	langs.append(language_names[languages])

    # else:
    # 	langs = wikilanguagecodes


    # if entitytype == 'Countries':
    #     df = params_to_df(langs, 'lang geographical entities', 'countries')
    #     df = df.loc[(df['Country'] == entityspecific)]
    #     df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

    #     hover_data = ['Country','ISO 3166','Subregion','Region','Coverage (%)','Country Total Articles','Covered Articles','Language','Period']
    #     fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)

    # if entitytype == 'Subregions':
    #     # df = params_to_df(langs, 'lang geographical entities', 'subregions')
    #     df = params_to_df(langs, 'lang geographical entities', 'countries')
    #     df = df.loc[(df['Subregion'] == entityspecific)]
    #     df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

    #     df['Subregion Covered Articles'] = df.groupby('Language')['Covered Articles'].transform('sum')
    #     df['Subregion Total Articles'] = df.groupby('Language')['Country Total Articles'].transform('sum')
    #     df['Subregion Coverage (%)'] = (100*df['Subregion Covered Articles']/df['Subregion Total Articles']).round(1)
    #     hover_data = ['Country','ISO 3166','Subregion','Region','Country Total Articles','Covered Articles','Coverage (%)','Subregion Covered Articles','Subregion Total Articles','Subregion Coverage (%)']

    #     df['Region Covered Articles'] = df.groupby('Language')['Articles'].transform('sum')
    #     df['Region Total Articles'] = df.groupby('Language')['Articles'].transform('sum')

    #     fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)


    # if entitytype == 'Regions':
    #     # df = params_to_df(langs, 'lang geographical entities', 'regions')
    #     df = params_to_df(langs, 'lang geographical entities', 'countries')
    #     df = df.loc[(df['Region'] == entityspecific)]
    #     df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

    #     df['Region Covered Articles'] = df.groupby('Language')['Covered Articles'].transform('sum')
    #     df['Region Total Articles'] = df.groupby('Language')['Country Total Articles'].transform('sum')
    #     df['Region Coverage (%)'] = (100*df['Region Covered Articles']/df['Region Total Articles']).round(1)
    #     hover_data = ['Country','ISO 3166','Subregion','Region','Country Total Articles','Covered Articles','Coverage (%)','Region Covered Articles','Region Total Articles','Region Coverage (%)']

    #     fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)

    # fig.update_layout(
    #     barmode='stack', 
    #     xaxis={'categoryorder':'total descending','titlefont_size':12, 'tickfont_size':12
    #     },
    #     yaxis=dict(
    #         title='Covered Articles',
    #         titlefont_size=12,
    #         tickfont_size=10,
    #     ))


    # return fig

