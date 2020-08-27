import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 

gender_dict = {'Q6581097':'male','Q6581072':'female', 'Q1052281':'transgender female','Q1097630':'intersex','Q1399232':"fa'afafine",'Q17148251':'travesti','Q19798648':'unknown value','Q207959':'androgyny','Q215627':'person','Q2449503':'transgender male','Q27679684':'transfeminine','Q27679766':'transmasculine','Q301702':'two-Spirit','Q303479':'hermaphrodite','Q3177577':'muxe','Q3277905':'māhū','Q430117':'Transgene','Q43445':'female non-human organism'}
lang_groups.insert(3, 'All languages')

# articles gender
query = 'SELECT set1, set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE content = "articles" AND set1descriptor = "wp" AND set2descriptor IN ("male","female") AND set2 = "wikidata_article_qitems" AND period = "'+last_period+'" ORDER BY set1, rel_value DESC;'
df_gender_articles = pd.read_sql_query(query, conn)
df_gender_articles = df_gender_articles.rename(columns={'set1':'Wiki', 'set2descriptor':'Gender', 'abs_value':'Articles', 'rel_value':'Extent Articles (%)'})
df_gender_articles = df_gender_articles.fillna(0).round(1)

df_gender_articles['Language'] = df_gender_articles['Wiki'].map(language_names_full)
df_gender_articles['Language (Wiki)'] = df_gender_articles['Language']+' ('+df_gender_articles['Wiki']+')'
df_gender_articles['Extent Articles WP (%)'] = df_gender_articles['Extent Articles (%)']


df_gender_articles_male = df_gender_articles.loc[df_gender_articles['Gender'] == 'male']
df_gender_articles_male = df_gender_articles_male.set_index('Wiki')
df_gender_articles_female = df_gender_articles.loc[df_gender_articles['Gender'] == 'female']
df_gender_articles_female = df_gender_articles_female.set_index('Wiki')

for x in df_gender_articles_male.index.values.tolist():
    try:
        male = df_gender_articles_male.loc[x]['Articles']
    except:
        male = 0  
    try:
        female = df_gender_articles_female.loc[x]['Articles']
    except:
        female = 0
    df_gender_articles_male.at[x, 'Extent Articles (%)'] =  100*male/(male+female)
    df_gender_articles_female.at[x, 'Extent Articles (%)'] =  100*female/(male+female)

df_gender_articles_male = df_gender_articles_male.reset_index().round(1)
df_gender_articles_female = df_gender_articles_female.reset_index().round(1)



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app12 = Dash(__name__, server = app, url_base_pathname= webtype + '/gender_gap/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app12.config['suppress_callback_exceptions']=True

title = "Gender Gap"
dash_app12.title = title+title_addenda

dash_app12.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that illustrate the gender gap in Wikipedia language editions content. For a detailed analysis on the evolution of gender gap over time or the pageviews women articles receive, you can check [Diversity Over Time](http://wcdo.wmflabs.org/diversity_over_time) and [Last Month Pageviews](https://wcdo.wmflabs.org/last_month_pageviews/).
        '''),

    # html.H5('Gender Gap in Wikipedia Language Editions'),

    dcc.Markdown('''* **What is the gender gap in specific groups of Wikipedia language editions?**'''.replace('  ', '')),


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
    html.Div(
    html.P('You can add or remove languages:'),
    style={'display': 'inline-block','width': '500px'}),

    dcc.Dropdown(id='sourcelangdropdown_gender_gap',
        options = [{'label': k, 'value': k} for k in language_names_list],
        multi=True),


    dcc.Graph(id = 'language_gendergap_barchart'),

    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# GENDER GAP Dropdown languages
@dash_app12.callback(
    dash.dependencies.Output('sourcelangdropdown_gender_gap', 'value'),
    [dash.dependencies.Input('grouplangdropdown', 'value')])
def set_langs_options_spread(selected_group):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    return re

#    return ['Cebuano (ceb)', 'Dutch (nl)', 'English (en)', 'French (fr)', 'German (de)', 'Italian (it)', 'Polish (pl)', 'Russian (ru)', 'Spanish (es)', 'Swedish (sv)']


# GENDER GAP BARCHART
@dash_app12.callback(
    Output('language_gendergap_barchart', 'figure'),
    [Input('sourcelangdropdown_gender_gap', 'value')])
def update_barchart(langs):

    languagecodes = []
    for l in langs:
        try:
            languagecodes.append(language_names[l])
        except:
            pass

    df = df_gender_articles_male.loc[df_gender_articles_male['Wiki'].isin(languagecodes)].set_index('Wiki').sort_values(by ='Extent Articles (%)', ascending=False)
    df2 = df_gender_articles_female.loc[df_gender_articles_female['Wiki'].isin(languagecodes)].set_index('Wiki').sort_values(by ='Extent Articles (%)', ascending=True)

    height = len(df2)*25
    if len(languagecodes)==10: height = 500

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df['Language'],
        x=df['Extent Articles (%)'],
        name='Men Articles',
        marker_color='blue',
#        values = df2['Extent Articles (%)'],
        customdata = df['Articles'],
        texttemplate='%{y}',
        orientation='h',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{x}%<br><extra></extra>',

    ))
    fig.add_trace(go.Bar(
        y=df2['Language'],
        x=df2['Extent Articles (%)'],
        name='Women Articles',
        marker_color='red',
#        values = df2['Extent Articles (%)'],
        customdata = df2['Articles'],
        texttemplate='%{y}',
        orientation='h',
        hovertemplate='<br>Articles: %{customdata}<br>Extent Articles: %{x}%<br><extra></extra>',
    ))

    fig.update_layout(
#        autosize=True,
        title_font_size=12,
        height = height,
        titlefont_size=12,
        width=700,
        barmode='stack')



    return fig
