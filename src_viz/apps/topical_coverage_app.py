import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 


query = 'SELECT set1, set1descriptor, set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = "articles" AND set2="topic" AND period = "'+last_period+'" ORDER BY set1;'
df_topics = pd.read_sql_query(query, conn)

df_topics = df_topics.rename(columns={'Extent':'Extent (%)','rel_value':'Extent (%)','abs_value':'Articles','set1descriptor':'Content', 'set2descriptor':'Topic2','set1':'Wiki'})
df_topics['Language'] = df_topics['Wiki'].map(language_names_full)
df_topics = df_topics.round(1)

topic_names = {'books':'Books','clothing_and_fashion':'Clothing and Fashion','earth':'Earth','folk':'Folk','food':'Food','glam':'GLAM','industry':'Industry','monuments_and_buildings':'Monuments and Buildings','music_creations_and_organizations':'Music','paintings':'Paintings','sport_and_teams':'Sports and Teams', 'people':'People','no_topic':'No Topic Info.'}
df_topics['Topic'] = df_topics['Topic2'].map(topic_names)

df_topics_books = df_topics.loc[df_topics['Topic2'] == 'books']
df_topics_clothing = df_topics.loc[df_topics['Topic2'] == 'clothing_and_fashion']
df_topics_earth = df_topics.loc[df_topics['Topic2'] == 'earth']
df_topics_folk = df_topics.loc[df_topics['Topic2'] == 'folk']
df_topics_food = df_topics.loc[df_topics['Topic2'] == 'food']
df_topics_glam = df_topics.loc[df_topics['Topic2'] == 'glam']
df_topics_industry = df_topics.loc[df_topics['Topic2'] == 'industry']
df_topics_monuments_and_buildings = df_topics.loc[df_topics['Topic2'] == 'monuments_and_buildings']
df_topics_music_creations_and_organizations = df_topics.loc[df_topics['Topic2'] == 'music_creations_and_organizations']
df_topics_paintings = df_topics.loc[df_topics['Topic2'] == 'paintings']
df_topics_sport_and_teams = df_topics.loc[df_topics['Topic2'] == 'sport_and_teams']
df_topics_people = df_topics.loc[df_topics['Topic2'] == 'people']


### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

dash_app14 = Dash(__name__, server = app, url_base_pathname= webtype + '/topical_coverage/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app14.config['suppress_callback_exceptions']=True

title = "Topical Coverage"
dash_app14.title = title+title_addenda
text_heatmap = ''

dash_app14.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that explain the distribution of topics in Wikipedia language editions and in their CCC based on Wikidata properties. 
             '''.replace('  ', '')),

    # dcc.Markdown('''    
    #     The graphs of this page answer the following questions:
    #     * What is the topical distribution in a Wikipedia Language Edition and in its CCC?
    #     * What is the topical distribution in various Wikipedia Language Edition and their CCC?
    #    '''),

    html.Br(),

###

    dcc.Tabs([
        dcc.Tab(label='Extent of Topics in a Wikipedia and its CCC (Treemap)', children=[
            html.Br(),

            # html.H5('Extent of Topics in a Wikipedia Language Edition and in Languages CCC'),
            dcc.Markdown('''* **What is the topical distribution in a Wikipedia Language Edition and in its CCC?**'''.replace('  ', '')),

            dcc.Markdown('''This graph allows comparing how different the cultural context content is from the sum of all the language edition content.
             '''.replace('  ', '')),
            # last month, accumulated.

            html.Div(
            html.P('Select a Wikipedia'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dcc.Dropdown(
                id='sourcelangdropdown_languages',
                options = [{'label': k, 'value': k} for k in language_names_list],
                value = 'English (en)',
                style={'width': '240px'}
             ), style={'display': 'inline-block','width': '250px'}),

            dcc.Graph(id = 'topics_treemap'),
#            html.Hr(),

            dcc.Markdown('''The treemap graphs show for a selected Wikipedia language edition the extent of articles that have been identified as related to one of the aforementioned topics both in all the Wikipedia language edition articles and in the selection of Cultural Context Content (CCC) articles. The size of the tiles is according to the extent in number of articles. In many language editions, a considerable extent of content is not "instance of" these topics.'''.replace('  ', '')),

            dcc.Markdown('''
                The topics are Earth, Monuments and Buildings, GLAM (Galleries, Libraries, Archives and Museums), Folk, Food, Books, Paintings, Clothing and Fashion, Sports and Teams, Music Creations and Organizations, and People. Articles have been identified with containing the Wikidata property "instance of" assigned the following Qitems: Earth (Q271669 landform, Q205895 landmass), GLAM (Q33506 museum, Q166118 archive, library Q7075), Monuments and Buildings (Q811979 architectural structure), Folk (Q132241  festival, Q288514  fair, Q4384751 folk culture, Q36192 folklore), Food (Q2095 food), Books (Q7725634 literary work, Q571 book, Q234460 text, Q47461344 written work), Paintings (Q3305213 painting), Clothing and Fashion (Q11460  clothing, Q3661311 clothing_and_fashion house, Q1618899clothing_and_fashion label), Sports and Teams (Q327245 team, Q41323 type of sport, Q349 sport), Music Creations and Organizations (Q188451 musical genre, Q2088357 musical organization, Q482994 musical term), People (articles have been identified using the P21 gender property).

                If you want to find relevant articles in each of these topics for any language and focused on their cultural context, you can access the [Top CCC articles lists](https://wcdo.wmflabs.org/top_ccc_articles/).
                     '''.replace('  ', '')),


    ###
        ]),
        dcc.Tab(label='Extent of Topics in Several Language Editions and their CCC (Stacked Bars)', children=[
            html.Br(),

            # html.H5('Extent of Topics in Wikipedia Language Editions and in Languages CCC'),
            dcc.Markdown('''* **What is the topical distribution in various Wikipedia Language Edition and their CCC?**
             '''.replace('  ', '')),


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

            dcc.Dropdown(id='sourcelangdropdown_topicalcoverage',
                options = [{'label': k, 'value': k} for k in language_names_list],
                multi=True),

            html.Br(),
            dcc.Graph(id = 'language_topics_barchart'),
            dcc.Graph(id = 'language_topics_barchart2'),

            dcc.Markdown('''These barcharts show for a group of selected Wikipedia language editions the topical distribution. In the first row you can see the extent of each topic for the entire Wikipedia language edition and in the second row for their CCC. By hovering on each topic you can see the extent in percentage and number of articles.'''),

            dcc.Markdown('''The topics are Earth, Monuments and Buildings, GLAM (Galleries, Libraries, Archives and Museums), Folk, Food, Books, Paintings, Clothing and Fashion, Sports and Teams, Music Creations and Organizations, and People. Articles have been identified with containing the Wikidata property "instance of" assigned the following Qitems: Earth (Q271669 landform, Q205895 landmass), GLAM (Q33506 museum, Q166118 archive, library Q7075), Monuments and Buildings (Q811979 architectural structure), Folk (Q132241  festival, Q288514  fair, Q4384751 folk culture, Q36192 folklore), Food (Q2095 food), Books (Q7725634 literary work, Q571 book, Q234460 text, Q47461344 written work), Paintings (Q3305213 painting), Clothing and Fashion (Q11460  clothing, Q3661311 clothing_and_fashion house, Q1618899clothing_and_fashion label), Sports and Teams (Q327245 team, Q41323 type of sport, Q349 sport), Music Creations and Organizations (Q188451 musical genre, Q2088357 musical organization, Q482994 musical term), People (articles have been identified using the P21 gender property).

                If you want to find relevant articles in each of these topics for any language and focused on their cultural context, you can access the [Top CCC articles lists](https://wcdo.wmflabs.org/top_ccc_articles/).
                     '''.replace('  ', '')),

            ]),

    ]),

    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# TOPICS TREEMAP
@dash_app14.callback(
    Output('topics_treemap', 'figure'),
    [Input('sourcelangdropdown_languages', 'value')])
def update_treemap_ccc(language):

    languageproject = language; language = language_names[language]
    df_topics_lang = df_topics.loc[df_topics['Wiki'] == language]

    df_topics_lang_wp = df_topics_lang.loc[df_topics_lang['Content'] == 'wp']
    df_topics_lang_ccc = df_topics_lang.loc[df_topics_lang['Content'] == 'ccc']

    wp_notopic = df_topics_lang_wp.loc[df_topics_lang_wp['Topic'] == 'No Topic Info.']['Extent (%)'].tolist()[0]
    ccc_notopic = df_topics_lang_ccc.loc[df_topics_lang_ccc['Topic'] == 'No Topic Info.']['Extent (%)'].tolist()[0]

    df_topics_lang_wp = df_topics_lang_wp[df_topics_lang_wp.Topic != 'No Topic Info.']
    df_topics_lang_ccc = df_topics_lang_ccc[df_topics_lang_ccc.Topic != 'No Topic Info.']

    parents = ['','','','','','','','','','','']

    fig = make_subplots(
        cols = 2, rows = 1,
        column_widths = [0.45, 0.45],
        specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
    )

    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_topics_lang_wp['Topic'],
        values = df_topics_lang_wp['Articles'],
        customdata = df_topics_lang_wp['Extent (%)'],
        texttemplate = "<b>%{label} </b><br>%{customdata}%<br>%{value} Art.<br>",
        hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Art.: %{value}<br><extra></extra>',
#        marker_colorscale = 'Blues',
        ),
            row=1, col=1)


    fig.add_trace(go.Treemap(
        parents = parents,
        labels = df_topics_lang_ccc['Topic'],
        values = df_topics_lang_ccc['Articles'],
        customdata = df_topics_lang_ccc['Extent (%)'],
        texttemplate = "<b>%{label} </b><br>%{customdata}%<br>%{value} Art.<br>",
        hovertemplate='<b>%{label} </b><br>Extent: %{customdata}%<br>Art.: %{value}<br><extra></extra>',
#        marker_colorscale = 'Blues',
        ),
            row=1, col=2)

    fig.update_layout(
        autosize=True,
#        width=700,
        titlefont_size=12,
        height=900,
#        paper_bgcolor="White",
        title_text="Topics Extent in Wikipedia (Left) and in CCC (Right)",
        title_x=0.5

    )

    return fig




# LANGUAGE DROPDOWN BARCHART
@dash_app14.callback(
    dash.dependencies.Output('sourcelangdropdown_topicalcoverage', 'value'),
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


# TOPICS WP BARCHART
@dash_app14.callback(
    Output('language_topics_barchart', 'figure'),

    [Input('sourcelangdropdown_topicalcoverage', 'value')])
def update_barchart_wp(langs):

    languagecodes = []
    for l in langs:
        languagecodes.append(language_names[l])

    df_books = df_topics_books.loc[df_topics_books['Wiki'].isin(languagecodes)]
    df_clothing = df_topics_clothing.loc[df_topics_clothing['Wiki'].isin(languagecodes)]
    df_earth = df_topics_earth.loc[df_topics_earth['Wiki'].isin(languagecodes)]
    df_folk = df_topics_folk.loc[df_topics_folk['Wiki'].isin(languagecodes)]
    df_food = df_topics_food.loc[df_topics_food['Wiki'].isin(languagecodes)]
    df_glam = df_topics_glam.loc[df_topics_glam['Wiki'].isin(languagecodes)]
    df_industry = df_topics_industry.loc[df_topics_industry['Wiki'].isin(languagecodes)]
    df_monuments_and_buildings = df_topics_monuments_and_buildings.loc[df_topics_monuments_and_buildings['Wiki'].isin(languagecodes)]
    df_music_creations_and_organizations = df_topics_music_creations_and_organizations.loc[df_topics_music_creations_and_organizations['Wiki'].isin(languagecodes)]
    df_paintings = df_topics_paintings.loc[df_topics_paintings['Wiki'].isin(languagecodes)]
    df_sports_and_teams = df_topics_sport_and_teams.loc[df_topics_sport_and_teams['Wiki'].isin(languagecodes)]
    df_people = df_topics_people.loc[df_topics_people['Wiki'].isin(languagecodes)]


    df_books_wp = df_books.loc[df_books['Content'] == 'wp']
    df_clothing_wp = df_clothing.loc[df_clothing['Content'] == 'wp']
    df_earth_wp = df_earth.loc[df_earth['Content'] == 'wp']
    df_folk_wp = df_folk.loc[df_folk['Content'] == 'wp']
    df_food_wp = df_food.loc[df_food['Content'] == 'wp']
    df_glam_wp = df_glam.loc[df_glam['Content'] == 'wp']
    df_industry_wp = df_industry.loc[df_industry['Content'] == 'wp']
    df_monuments_and_buildings_wp = df_monuments_and_buildings.loc[df_monuments_and_buildings['Content'] == 'wp']
    df_music_creations_and_organizations_wp = df_music_creations_and_organizations.loc[df_music_creations_and_organizations['Content'] == 'wp']
    df_paintings_wp = df_paintings.loc[df_paintings['Content'] == 'wp']
    df_sport_and_teams_wp = df_sports_and_teams.loc[df_sports_and_teams['Content'] == 'wp']
    df_people_wp = df_people.loc[df_people['Content'] == 'wp']


    df_books_ccc = df_books.loc[df_books['Content'] == 'ccc']
    df_clothing_ccc = df_clothing.loc[df_clothing['Content'] == 'ccc']
    df_earth_ccc = df_earth.loc[df_earth['Content'] == 'ccc']
    df_folk_ccc = df_folk.loc[df_folk['Content'] == 'ccc']
    df_food_ccc = df_food.loc[df_food['Content'] == 'ccc']
    df_glam_ccc = df_glam.loc[df_glam['Content'] == 'ccc']
    df_industry_ccc = df_industry.loc[df_industry['Content'] == 'ccc']
    df_monuments_and_buildings_ccc = df_monuments_and_buildings.loc[df_monuments_and_buildings['Content'] == 'ccc']
    df_music_creations_and_organizations_ccc = df_music_creations_and_organizations.loc[df_music_creations_and_organizations['Content'] == 'ccc']
    df_paintings_ccc = df_paintings.loc[df_paintings['Content'] == 'ccc']
    df_sport_and_teams_ccc = df_sports_and_teams.loc[df_sports_and_teams['Content'] == 'ccc']
    df_people_ccc = df_people.loc[df_people['Content'] == 'ccc']


    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_people_wp['Language'],
        y=df_people_wp['Extent (%)'],
        name='People',
#        marker_color='orange',
#        values = df2['Extent Articles (%)'],
        customdata = df_people_wp['Articles'],
        text='%{y}',
        hovertemplate='People<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_monuments_and_buildings_wp['Language'],
        y=df_monuments_and_buildings_wp['Extent (%)'],
        name='Monuments and Buildings',
#        marker_color='#636EFA',
#        values = df2['Extent Articles (%)'],
        customdata = df_monuments_and_buildings_wp['Articles'],
        text='%{y}',
        hovertemplate='Monuments and Buildings<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_music_creations_and_organizations_wp['Language'],
        y=df_music_creations_and_organizations_wp['Extent (%)'],
        name='Music, Creations and Organizations',
#        marker_color='#EF553B',
#        values = df2['Extent Articles (%)'],
        customdata = df_music_creations_and_organizations_wp['Articles'],
        text='%{y}',
        hovertemplate='Music, Creations and Organizations<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_books_wp['Language'],
        y=df_books_wp['Extent (%)'],
        name='Books',
#        marker_color='#00CC96',
#        values = df2['Extent Articles (%)'],
        customdata = df_books_wp['Articles'],
        text='%{y}',
        hovertemplate='Books<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_earth_wp['Language'],
        y=df_earth_wp['Extent (%)'],
        name='Earth',
#        marker_color='#AB63FA',
#        values = df2['Extent Articles (%)'],
        customdata = df_earth_wp['Articles'],
        text='%{y}',
        hovertemplate='Earth<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_food_wp['Language'],
        y=df_food_wp['Extent (%)'],
        name='Food',
#        marker_color='#B6E880',
#        values = df2['Extent Articles (%)'],
        customdata = df_food_wp['Articles'],
        text='%{y}',
        hovertemplate='Food<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_glam_wp['Language'],
        y=df_glam_wp['Extent (%)'],
        name='GLAM',
#        marker_color='#FF6692',
#        values = df2['Extent Articles (%)'],
        customdata = df_glam_wp['Articles'],
        text='%{y}',
        hovertemplate='GLAM<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_industry_wp['Language'],
        y=df_industry_wp['Extent (%)'],
        name='Industry',
#        marker_color='#19D3F3',
#        values = df2['Extent Articles (%)'],
        customdata = df_industry_wp['Articles'],
        text='%{y}',
        hovertemplate='Industry<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))

    fig.add_trace(go.Bar(
        x=df_paintings_wp['Language'],
        y=df_paintings_wp['Extent (%)'],
        name='Paintings',
#        marker_color='#FECB52',
#        values = df2['Extent Articles (%)'],
        customdata = df_paintings_wp['Articles'],
        text='%{y}',
        hovertemplate='Paintings<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_folk_wp['Language'],
        y=df_folk_wp['Extent (%)'],
        name='Folk',
#        marker_color='#FF97FF',
#        values = df2['Extent Articles (%)'],
        customdata = df_folk_wp['Articles'],
        text='%{y}',
        hovertemplate='Folk<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_sport_and_teams_wp['Language'],
        y=df_sport_and_teams_wp['Extent (%)'],
        name='Sports and Teams',
#        marker_color='orange',
#        values = df2['Extent Articles (%)'],
        customdata = df_sport_and_teams_wp['Articles'],
        text='%{y}',
        hovertemplate='Sports and Teams<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_clothing_wp['Language'],
        y=df_clothing_wp['Extent (%)'],
        name='Clothing and Fashion',
#        marker_color='#7E7DCD',
#        values = df2['Extent Articles (%)'],
        customdata = df_clothing_wp['Articles'],
        text='%{y}',
        hovertemplate='Clothing and Fashion<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))


    fig.update_layout(
        barmode='stack',
        autosize=True,
#        width=700,
#        height=900,
#        paper_bgcolor="White",
        titlefont_size=12,
        title_text="Topics Extent in Selected Wikipedia Language Editions (Top) and in Languages CCC (Bottom)",
        title_x=0.5,
    )

    return fig



# TOPICS WP BARCHART
@dash_app14.callback(
    Output('language_topics_barchart2', 'figure'),
    [Input('sourcelangdropdown_topicalcoverage', 'value')])
def update_barchart_ccc(langs):

    languagecodes = []
    for l in langs:
        languagecodes.append(language_names[l])

    df_books = df_topics_books.loc[df_topics_books['Wiki'].isin(languagecodes)]
    df_clothing = df_topics_clothing.loc[df_topics_clothing['Wiki'].isin(languagecodes)]
    df_earth = df_topics_earth.loc[df_topics_earth['Wiki'].isin(languagecodes)]
    df_folk = df_topics_folk.loc[df_topics_folk['Wiki'].isin(languagecodes)]
    df_food = df_topics_food.loc[df_topics_food['Wiki'].isin(languagecodes)]
    df_glam = df_topics_glam.loc[df_topics_glam['Wiki'].isin(languagecodes)]
    df_industry = df_topics_industry.loc[df_topics_industry['Wiki'].isin(languagecodes)]
    df_monuments_and_buildings = df_topics_monuments_and_buildings.loc[df_topics_monuments_and_buildings['Wiki'].isin(languagecodes)]
    df_music_creations_and_organizations = df_topics_music_creations_and_organizations.loc[df_topics_music_creations_and_organizations['Wiki'].isin(languagecodes)]
    df_paintings = df_topics_paintings.loc[df_topics_paintings['Wiki'].isin(languagecodes)]
    df_sports_and_teams = df_topics_sport_and_teams.loc[df_topics_sport_and_teams['Wiki'].isin(languagecodes)]
    df_people = df_topics_people.loc[df_topics_people['Wiki'].isin(languagecodes)]


    df_books_ccc = df_books.loc[df_books['Content'] == 'ccc']
    df_clothing_ccc = df_clothing.loc[df_clothing['Content'] == 'ccc']
    df_earth_ccc = df_earth.loc[df_earth['Content'] == 'ccc']
    df_folk_ccc = df_folk.loc[df_folk['Content'] == 'ccc']
    df_food_ccc = df_food.loc[df_food['Content'] == 'ccc']
    df_glam_ccc = df_glam.loc[df_glam['Content'] == 'ccc']
    df_industry_ccc = df_industry.loc[df_industry['Content'] == 'ccc']
    df_monuments_and_buildings_ccc = df_monuments_and_buildings.loc[df_monuments_and_buildings['Content'] == 'ccc']
    df_music_creations_and_organizations_ccc = df_music_creations_and_organizations.loc[df_music_creations_and_organizations['Content'] == 'ccc']
    df_paintings_ccc = df_paintings.loc[df_paintings['Content'] == 'ccc']
    df_sport_and_teams_ccc = df_sports_and_teams.loc[df_sports_and_teams['Content'] == 'ccc']
    df_people_ccc = df_people.loc[df_people['Content'] == 'ccc']


    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_people_ccc['Language'],
        y=df_people_ccc['Extent (%)'],
        name='People',
#        marker_color='orange',
#        values = df2['Extent Articles (%)'],
        customdata = df_people_ccc['Articles'],
        text='%{y}',
        hovertemplate='People<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_monuments_and_buildings_ccc['Language'],
        y=df_monuments_and_buildings_ccc['Extent (%)'],
        name='Monuments and Buildings',
#        marker_color='#636EFA',
#        values = df2['Extent Articles (%)'],
        customdata = df_monuments_and_buildings_ccc['Articles'],
        text='%{y}',
        hovertemplate='Monuments and Buildings<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_music_creations_and_organizations_ccc['Language'],
        y=df_music_creations_and_organizations_ccc['Extent (%)'],
        name='Music, Creations and Organizations',
#        marker_color='#EF553B',
#        values = df2['Extent Articles (%)'],
        customdata = df_music_creations_and_organizations_ccc['Articles'],
        text='%{y}',
        hovertemplate='Music, Creations and Organizations<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_books_ccc['Language'],
        y=df_books_ccc['Extent (%)'],
        name='Books',
#        marker_color='#00CC96',
#        values = df2['Extent Articles (%)'],
        customdata = df_books_ccc['Articles'],
        text='%{y}',
        hovertemplate='Books<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_earth_ccc['Language'],
        y=df_earth_ccc['Extent (%)'],
        name='Earth',
#        marker_color='#AB63FA',
#        values = df2['Extent Articles (%)'],
        customdata = df_earth_ccc['Articles'],
        text='%{y}',
        hovertemplate='Earth<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_food_ccc['Language'],
        y=df_food_ccc['Extent (%)'],
        name='Food',
#        marker_color='#B6E880',
#        values = df2['Extent Articles (%)'],
        customdata = df_food_ccc['Articles'],
        text='%{y}',
        hovertemplate='Food<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_glam_ccc['Language'],
        y=df_glam_ccc['Extent (%)'],
        name='GLAM',
#        marker_color='#FF6692',
#        values = df2['Extent Articles (%)'],
        customdata = df_glam_ccc['Articles'],
        text='%{y}',
        hovertemplate='GLAM<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_industry_ccc['Language'],
        y=df_industry_ccc['Extent (%)'],
        name='Industry',
#        marker_color='#19D3F3',
#        values = df2['Extent Articles (%)'],
        customdata = df_industry_ccc['Articles'],
        text='%{y}',
        hovertemplate='Industry<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))

    fig.add_trace(go.Bar(
        x=df_paintings_ccc['Language'],
        y=df_paintings_ccc['Extent (%)'],
        name='Paintings',
#        marker_color='#FECB52',
#        values = df2['Extent Articles (%)'],
        customdata = df_paintings_ccc['Articles'],
        text='%{y}',
        hovertemplate='Paintings<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_folk_ccc['Language'],
        y=df_folk_ccc['Extent (%)'],
        name='Folk',
#        marker_color='#FF97FF',
#        values = df2['Extent Articles (%)'],
        customdata = df_folk_ccc['Articles'],
        text='%{y}',
        hovertemplate='Folk<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_sport_and_teams_ccc['Language'],
        y=df_sport_and_teams_ccc['Extent (%)'],
        name='Sports and Teams',
#        marker_color='orange',
#        values = df2['Extent Articles (%)'],
        customdata = df_sport_and_teams_ccc['Articles'],
        text='%{y}',
        hovertemplate='Sports and Teams<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))
    fig.add_trace(go.Bar(
        x=df_clothing_ccc['Language'],
        y=df_clothing_ccc['Extent (%)'],
        name='Clothing and Fashion',
#        marker_color='#7E7DCD',
#        values = df2['Extent Articles (%)'],
        customdata = df_clothing_ccc['Articles'],
        text='%{y}',
        hovertemplate='Clothing and Fashion<br>Articles: %{customdata}<br>Extent Articles: %{y}%<br><extra></extra>',
        ))

    fig.update_layout(barmode='stack')

    return fig

