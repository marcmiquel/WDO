import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### DATA
conn = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor = conn.cursor()
# countries Top CCC article lists totals
query = 'SELECT set1, set1descriptor, abs_value FROM wdo_intersections WHERE set1 LIKE "%(" ||  set2 || ")%" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) ORDER BY set1;'
df_countries = pd.read_sql_query(query, conn)
df_countries = df_countries.set_index('set1')

# languages Top CCC article lists totals
query = 'SELECT set1, set1descriptor, abs_value FROM wdo_intersections WHERE set1 = set2 AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) ORDER BY set1;'    
df_langs = pd.read_sql_query(query, conn)
df_langs = df_langs.set_index('set1')




conn2 = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor2 = conn2.cursor()
query = 'SELECT set2, set1, abs_value FROM wdo_intersections WHERE set1descriptor ="all_top_ccc_articles" ORDER BY set2;' # spread

# query = 'SELECT set2, set1, abs_value FROM wdo_intersections WHERE set1descriptor ="all_top_ccc_articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) ORDER BY set2;' # spread
df_cover = pd.read_sql_query(query, conn2)
df_cover = df_cover.set_index('set2')



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/?lang=ca
dash_app8 = Dash(__name__, server = app, url_base_pathname= webtype + '/languages_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app8.config['suppress_callback_exceptions']=True


dash_app8.title = 'Languages Top 100 CCC article lists coverage'+title_addenda
dash_app8.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

column_list_dict = {'languagename':'Language','Wiki':'Wiki','editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y. A. Edits', 'created_last_year':'Last Y. A. Edits', 'women':'Women', 'men':'Men', 'pageviews':'Page views', 'discussions':'Talk Edits','edits':'Edits', 'edited_last_month':'Edited Last Month', 'images':'Images', 'wdproperty_many':'WD Properties', 'interwiki_many':'Interwiki', 'interwiki_editors':'Least Interwiki Most Editors', 'interwiki_wdproperty':'Least Interwiki Most WD Properties', 'wikirank':'Wikirank', 'earth':'Wiki Loves Earth', 'monuments_and_buildings':'Wiki Loves Monuments', 'sport_and_teams':'Wiki Loves Sports', 'glam':'Wiki Loves GLAM', 'folk':'Wiki Loves Folk', 'music_creations_and_organizations':'Wiki Loves Music', 'food':'Wiki Loves Food', 'paintings':'Wiki Loves Paintings', 'books':'Wiki Loves Books', 'clothing_and_fashion':'Wiki Loves Clothing and Fashion', 'industry':'Wiki Loves Industry','religion':'Wiki Loves Religion','religious_group':'Religious Group','sexual_orientation':'LGBT+','ethnic_group':'Ethnic Group','list_coverage_index':'Coverage Idx.','covered_list_articles_sum':'Covered Articles','list_spread_index':'List Spread Idx.','spread_list_articles_sum':'Spread Articles','World Subregion':'World Subregion'}

lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions','edits','edited_last_month','images','wdproperty_many','interwiki_many','interwiki_editors','interwiki_wdproperty','wikirank','earth','monuments_and_buildings','sport_and_teams','glam','folk','music_creations_and_organizations','food','paintings','books','clothing_and_fashion','industry','religion','religious_group','sexual_orientation','ethnic_group']

default_lists = ['pageviews', 'editors', 'discussions', 'created_first_three_years', 'created_last_year', 'geolocated', 'keywords', 'women', 'men']

lists_dict = {'Editors':'editors', 'Featured':'featured', 'Geolocated':'geolocated', 'Keywords':'keywords', 'Women':'women', 'Men':'men', 'First 3Y. A. Edits':'created_first_three_years', 'Last Y. A. Edits':'created_last_year', 'Pageviews':'pageviews', 'Talk Edits':'discussions','Edits':'edits','Edited Last Month':'edited_last_month','Images':'images','WD Properties':'wdproperty_many','Interwiki':'interwiki_many','Least Interwiki Most Editors':'interwiki_editors','Least Interwiki Most WD Properties':'interwiki_wdproperty','Wikirank':'wikirank','Wiki Loves Earth':'earth','Wiki Loves Monuments':'monuments_and_buildings','Wiki Loves Sports':'sport_and_teams','Wiki Loves GLAM':'glam','Wiki Loves Folk':'folk','Wiki Loves Music':'music_creations_and_organizations','Wiki Loves Food':'food','Wiki Loves Paintings':'paintings','Wiki Loves Books':'books','Wiki Loves Clothing and Fashion':'clothing_and_fashion','Wiki Loves Industry':'industry', 'Wiki Loves Religion':'religion','Religious Group':'religious_group','LGBT+':'sexual_orientation','Ethnic Group':'ethnic_group'}

# Languages Top CCC Coverage
text_app8 = '''This page shows some statistics that explain how well a Wikipedia language edition covers the Top 100 of the [Top CCC articles lists](https://wcdo.wmflabs.org/top_ccc_articles/) from other Wikipedia language editions.
        '''

# These lists are created by ranking the articles according to different criteria. They can be grouped into the following groups:


text_lists = '''
* Lists of CCC diversity articles based on relevance features: most pageviews during the last month (**Pageviews**), most number of editors (**Editors**), most number of edits (**Edits**), most number of edits during the last month (**Edited Last Month**), most number of edits in talk pages (**Discussions**), created during the first three years and with most edits (**First 3Y.**), created during the last year and with most edits **Last Y.**), the highest Wikirank (**Wikirank**), featured article distinction (**Featured**) most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), most number of images (**Images**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), most number of properties in Wikidata (**WD Properties**), most number of number of Interwiki links (**Interwiki**), least number of interwiki and most number of editors (**Least Interwiki Most Editors**), and least number interwiki and most number of properties (**Least Interwiki Most With Wikidata Properties**).
* Lists of CCC articles based on localness characteristics: geolocation with the most number of links coming from CCC (**Geolocated**), and keywords on title with the largest number of bytes (**Keywords**).
* Lists of CCC articles and diversity topics: women and men biographies and with most edits (**Women**) and (**Men**), biographies identified with lgbt+ sexual orientation with most edits (**LGBT+**), biographies identified as belonging to an ethnic group with most edits (**Ethnic Group**) and biographies identified with an affiliation to a religious group (**Religious Group**).
* Lists of CCC articles and with most pageviews during the last month and **"Wiki Loves"** topics based on WD properties: *Books*, *Clothing and fashion*, *Earth*, *Folk*, *Food*, *GLAM*, *Industry*, *Monuments and buildings*, *Music creation and organizations*, *Paintings*, *Religion* and *Sports and teams*.
'''    

text2_app8 = '''
**The challenge is to reach 100 articles covered (Covered Articles) from each language CCC!**
'''


def dash_app8_build_layout(params):
    functionstartTime = time.time()

    if len(params)!=0 and params['lang'].lower()!='none' and 'lists' in params:      

        language_covering=params['lang'].lower()
        language_covering=language_covering.lower()

        df_cv = df_cover.loc[language_covering]
        df_cv = df_cv.set_index('set1')

        lists_p = params['lists']
        lists_p = lists_p.split(',')

    #    language_covering='ca'
        conn = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor = conn.cursor()

#        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wdo_intersections WHERE set2 = "'+language_covering+'" AND set2descriptor = "wp" AND set1 NOT LIKE "%(%" ORDER BY set1, set1descriptor DESC;'
        a = lists_p.copy()
        a.append("%(%")
        page_asstring = ','.join( ['?'] * (len(a)-1) )
        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wdo_intersections WHERE set2 = "'+language_covering+'" AND set2descriptor = "wp" AND set1descriptor IN (%s) ' % page_asstring
        query += 'AND set1 NOT LIKE ? ORDER BY set1, set1descriptor DESC;' 

#        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wdo_intersections WHERE set2 = "'+language_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) AND set1 NOT LIKE "%(%" ORDER BY set1, set1descriptor DESC;'

        list_top = lists.copy()

        for x in range(0,len(lists_p)):
            lists_p[x]=column_list_dict[lists_p[x]]

        row_dict = {}
        language_dict = {}
        old_languagecode_covered = ''
        list_coverage_index=0

        i = 0
        for row in cursor.execute(query,a):
            i+=1
            languagecode_covered = row[1]
            # print (languagecode_covered)

            if languagecode_covered not in wikilanguagecodes: continue

            if old_languagecode_covered!=languagecode_covered and old_languagecode_covered!='':

                row_dict['languagename']=str(languages.loc[old_languagecode_covered])

                row_dict['Wiki']=html.A(old_languagecode_covered.replace('_','-'), href='https://'+old_languagecode_covered.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

                row_dict['list_coverage_index']=round(list_coverage_index/(len(lists)-len(list_top)),1)

                try:
                    value = int(df_cv.loc[old_languagecode_covered]['abs_value'])
                except:
                    value = 0

#                print (languagecode_covered,value)


                if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                row_dict['covered_list_articles_sum']=color
                row_dict['covered_list_articles']=value

                list_coverage_index=0

    #            print (row_dict)
                for remaining_list in list_top:
                    row_dict[remaining_list] = '0/0'

                list_top = lists.copy()
                language_dict[old_languagecode_covered]=row_dict
                row_dict={}

            # accumulate
            list_name = row[0]
            percentage = round(row[2],2)
            num_articles = row[3]

            list_coverage_index += percentage

            if languagecode_covered != old_languagecode_covered:
                df_lang = df_langs.loc[languagecode_covered]
                df_lang = df_lang.set_index('set1descriptor')
                # print (languagecode_covered)

            total_list = df_lang.loc[list_name,'abs_value']
            if total_list == 100:
                link = str(num_articles) + '%'
            else:
                link = str(num_articles) + '/'+str(total_list)

            link = html.Abbr(link,title=column_list_dict[list_name]+' List',style={'text-decoration':'none'})

            url = '/top_ccc_articles/?list='+list_name+'&source_lang='+languagecode_covered+'&source_country=all&target_lang='+language_covering+'&order_by=none&filter=None'
            row_dict[list_name]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'}) # target="_blank", 

            old_languagecode_covered = languagecode_covered
            list_top.remove(list_name)


        # last iteration
        row_dict['languagename']=str(languages.loc[languagecode_covered]['languagename'])
        row_dict['languagename_sort']=str(languagecode_covered)


        row_dict['Wiki']=html.A(languagecode_covered.replace('_','-'), href='https://'+languagecode_covered.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})
        row_dict['list_coverage_index']=round(list_coverage_index/(len(lists)-len(list_top)),1)


        try:
            val = df_cv.loc[languagecode_covered]['abs_value']
        except:
            val = 0

        value = int(val)
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['covered_list_articles_sum']=color
        row_dict['covered_list_articles']=value

        for remaining_list in list_top:
            row_dict[remaining_list] = '0/0'

        language_dict[languagecode_covered]=row_dict
        # duration = 'data is ready'+' '+str(datetime.timedelta(seconds=time.time() - functionstartTime))

        df=pd.DataFrame.from_dict(language_dict,orient='index')


        # df['Wiki'] = df.languagename.astype(str)



        languagename={}
        for x in df.index.values: 
            englishwpname = languages.loc[x]['WikipedialanguagearticleEnglish']
            if englishwpname != 'no link':
                englishwpname = englishwpname.split('/')[4].replace(' ','_')
            languagename[x]=html.A(languages.loc[x]['languagename'], href='https://en.wikipedia.org/wiki/'+englishwpname, target="_blank", style={'text-decoration':'none'})
        df['languagename'] = pd.Series(languagename)


        columns = ['Language','Wiki'] + ['World Subregion','Coverage Idx.','Covered Articles'] + lists_p


        df['World Subregion']=languages.subregion
        for x in df.index.values.tolist():
            if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]


        df=df.rename(columns=column_list_dict)

        df = df.sort_values(by=['covered_list_articles'], ascending = False)


        # print (columns)
        # print (df.columns)
        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        # df = df.sort_index(axis = 0) 


        language_cover = languages.loc[language_covering]['languagename']
        title = 'Languages Top CCC Articles Lists Coverage by '+language_cover+' Wikipedia'
#        dash_app8.title = title+title_addenda
        
        results_text = '''
                The following table shows how well '''+language_cover+''' Wikipedia covers the Top 100 CCC articles from the lists generated from all the other language editions CCC. Languages are sorted in alphabetic order by their Wikicode, and columns present the number of articles from each list covered by English language. The third and fourth columns, All Top CCC Lists **Coverage Idx.** and All Top CCC Lists **Covered Articles** present for each language, the average percentage of articles from the selected lists and the overall sum of articles from all the Top CCC lists covered by '''+language_cover+''' Wikipedia. When Covered Articles has 100 or more articles it will turn green. In some languages, it is not be possible to cover them because unfortunately the lists do not have enough articles. Every cell of the table is a clickable link to a Top CCC list.
                '''
                # The values of this table are *updated once per day* counting the new articles created.

        

        layout = html.Div([
            navbar,
            html.H3('Languages Top CCC Articles Lists Coverage', style={'textAlign':'center'}),
            dcc.Markdown(
                text_app8.replace('  ', '')),


            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),
            html.Br(),



            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),

            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),


            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(results_text.replace('  ', '')),
            dcc.Markdown(
                text2_app8.replace('  ', '')),

            html.Br(),
            html.H6(title, style={'textAlign':'center'}),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +

                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(len(df))]
            ),

            footbar,

            ], className="container")

    else:
        layout = html.Div([
            navbar,

            html.H3('Languages Top CCC Articles Lists Coverage', style={'textAlign':'center'}),
            dcc.Markdown(
                text_app8.replace('  ', '')),


            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),


            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),

            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),


            footbar,
            ], className="container")        

    duration = 'return layout'+' '+str(datetime.timedelta(seconds=time.time() - functionstartTime))
#    print (duration)
    return layout


# callback update URL
component_ids8 = ['lang','lists']
@dash_app8.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids8])
def update_url_state(*values):
    if not isinstance(values[1], str):
        values = values[0],','.join(values[1])

    state = urlencode(dict(zip(component_ids8, values)))
    return '?'+state

# callback update page layout
@dash_app8.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app8_build_layout(state)

@dash_app8.callback(
    Output("collapse", "is_open"),
    [Input("collapse-button", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/countries_top_ccc_articles_coverage/?lang=ca
dash_app9 = Dash(__name__, server = app, url_base_pathname= webtype + '/countries_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

#dash_app9.config.supress_callback_exceptions = True
dash_app9.config['suppress_callback_exceptions']=True

dash_app9.title = 'Countries Top 100 CCC article lists coverage'+title_addenda
dash_app9.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

text_default_app9 = '''
        This page shows some statistics that explain how well a Wikipedia language edition covers the [Top CCC articles lists](https://wcdo.wmflabs.org/top_ccc_articles/) generated for the countries associated to each language edition (when it is a region or several regions located in the same country, it appears as one row with the country name).

        Some languages are mapped to several countries, some countries are mapped to several languages and sometimes the language spread remains at a regional level. Therefore, in order to create lists for countries, articles from a language CCC has been associated to specific territories (regions or countries) according to their features obtained during the CCC selection process, and later, they have been aggregated by countries (ISO 3166). For instance, for the French CCC, the resulting final list of countries includes France (French speaking territories), whose territory equates directly to a country, and Canada (French speaking territories), whose use of French is only limited to Québec.
        '''

        # These lists are created by ranking the articles according to different criteria. They can be grouped into the following groups:

text2_app9 = '''
**The challenge is to reach 100 articles covered (Covered Articles) from each country!**
'''


def dash_app9_build_layout(params):

    if len(params)!=0 and params['lang'].lower()!='none' and 'lists' in params:      

        languagecode_covering=params['lang'].lower()
        df_cv = df_cover.loc[languagecode_covering]
        df_cv = df_cv.set_index('set1')

        lists_p = params['lists']
        lists_p = lists_p.split(',')

        conn = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor = conn.cursor()

        a = lists_p.copy()
        page_asstring = ','.join( ['?'] * (len(a)) )

        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wdo_intersections WHERE set2 = "'+languagecode_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) '
        query+= 'AND set1descriptor IN (%s) ORDER BY set1, set1descriptor DESC;' % page_asstring

        list_top = lists.copy()

        for x in range(0,len(lists_p)):
            lists_p[x]=column_list_dict[lists_p[x]]

        row_dict = {}
        country_dict = {}
        old_covered = ''
        list_coverage_index=0

        for row in cursor.execute(query, a):

            covered = row[1]
            if '(' not in covered: continue

            if old_covered!=covered and old_covered!='':
                language_name = languages.loc[languagecode_covered]['languagename']
                country_name = country_names[country_covered]
                country_language = language_name+' CCC ('+ country_name+')'
    #            country_language = country_name+' ('+language_name+' CCC)'

                try:
#                    print (country_covered, languagecode_covered, covered)
                    territorynames = wikilanguages_utils.load_territories_names_from_language_country(country_covered, languagecode_covered, territories)
                    territorynames_label = ", ".join(territorynames)
                    territorynames_label+= '.'



                    abbr_label = html.Abbr(country_language,title='Territories: '+territorynames_label)

                    row_dict['Country']=abbr_label
                    row_dict['Coverage Idx.']=round(list_coverage_index/(len(lists)-len(list_top)),1)

                    value = int(df_cv.loc[old_covered]['abs_value'])
                    if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                    else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                    row_dict['Covered Articles']=color
                    row_dict['covered_list_articles_sum']=value

        #            print (row_dict)
                    for remaining_list in list_top:
                        row_dict[column_list_dict[remaining_list]] = '0/0'

                    country_dict[country_language]=row_dict

                except:
                    territorynames_label = ''
                    # print ('this is not covered anymore: '+country_language)
#                    continue

                row_dict={}
                list_coverage_index=0
                list_top = lists.copy()

            list_name = row[0]
            percentage = row[2]
            num_articles = row[3]

            list_coverage_index += percentage

            if covered != '':
                parts=covered.split('_')
                country_covered = parts[0]

                if len(parts)==3:
                    languagecode_covered = parts[1].replace('(','')+'_'+parts[2].replace(')','')
                elif len(parts)==4:
                    languagecode_covered = parts[1].replace('(','')+'_'+parts[2]+'_'+parts[3].replace(')','')
                else:
                    languagecode_covered = parts[1].replace(')','').replace('(','')
            

            if covered != old_covered:
                df_country = df_countries.loc[covered]
                df_country = df_country.set_index('set1descriptor')


            total_list = df_country.loc[list_name,'abs_value']
            if total_list == 100:
                link = str(num_articles) + '%'
            else:
                link = str(num_articles) + '/'+str(total_list)

            link = html.Abbr(link,title=column_list_dict[list_name]+' List',style={'text-decoration':'none'})

            url = 'https://wcdo.wmflabs.org/top_ccc_articles/?list='+list_name+'&source_lang='+languagecode_covered+'&source_country='+country_covered.lower()+'&target_lang='+languagecode_covering+'&order_by=none&filter=None'

            row_dict[column_list_dict[list_name]]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'})
            row_dict['World Subregion'] = subregions[country_covered]

            old_covered = covered
            list_top.remove(list_name)


        # last iteration
        language_name = languages.loc[languagecode_covered]['languagename']
        country_name = country_names[country_covered]

    #    country_language = country_name+' ('+language_name+' CCC)'
        country_language = language_name+' CCC ('+ country_name+')'

        row_dict['Country']=country_language
        row_dict['Coverage Idx.']=round(list_coverage_index/(len(lists)-len(list_top)),1)

        value = int(df_cv.loc[covered]['abs_value'])
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['Covered Articles']=color
        row_dict['covered_list_articles_sum']=value

        for remaining_list in list_top:
            row_dict[column_list_dict[remaining_list]] = '0/0'
        country_dict[country_language]=row_dict



        # READY TO PRINT
        columns = ['Country']+['World Subregion','Coverage Idx.','Covered Articles']+lists_p

        df=pd.DataFrame.from_dict(country_dict,orient='index')
        df = df.sort_values(by=['covered_list_articles_sum'], ascending = False)
        df = df[columns] # selecting the parameters to export
        df = df.fillna('')        





        language_cover = languages.loc[languagecode_covering]['languagename']
        main_title = 'Countries Top 100 CCC article lists coverage by '+language_cover+' Wikipedia'
#        dash_app9.title = title+title_addenda

        results_text = '''
                The following table shows how well '''+language_cover+''' Wikipedia covers the Top 100 CCC articles from all the lists generated from the other language editions CCC down at country level. Languages CCC are sorted in alphabetic order with the country subselection between parenthesis - e.g. Italian CCC (Switzerland) row would be for Ticino, while Catalan CCC (Spain) row would be for Catalonia, Balearic Islands and Valencia. When a language is spoken in several countries there is one row per country and by hovering on it you can see the regions where it is spoken. If you detect any territory which you do not consider correct, please contact us as we may need to modify the Language-Territories Mapping table used to obtain the CCC for this particular language.

                Columns present the number of articles from each list covered by English language. The third and fourth columns, All Top CCC Lists **Coverage Idx.** and All Top CCC Lists **Covered Articles** present for each language, the average percentage of articles from the selected lists and the overall sum of articles from all the Top CCC lists covered by '''+language_cover+''' Wikipedia. When Covered Articles has 100 or more articles it will turn green. In some languages, it is not be possible to cover them because unfortunately the lists do not have enough articles. Every cell of the table is a clickable link to a Top CCC list. Every cell of the table is a clickable link to a Top CCC list.'''
                # The values of this table are *updated once per day* counting the new articles created.


        layout = html.Div([
            navbar,
            html.H3('Countries Top 100 CCC article lists coverage', style={'textAlign':'center'}),
            dcc.Markdown(
                text_default_app9.replace('  ', '')),

            # html.Div([
            #     dcc.Markdown(text_lists.replace('  ', '')),
            #     ],
            #     style={'font-size': 12}),


            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),



            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),


            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),

            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(results_text.replace('  ', '')),

            dcc.Markdown(
                text2_app9.replace('  ', '')),

            html.Br(),
            html.H6(main_title, style={'textAlign':'center'}),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +

                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(len(df))]
            ),

            footbar,

            ], className="container")

    else:


        layout = html.Div([
            navbar,
            html.H3('Countries Top 100 CCC Articles Lists Coverage', style={'textAlign':'center'}),
            dcc.Markdown(text_default_app9.replace('  ', '')),

            # html.Div([
            #     dcc.Markdown(text_lists.replace('  ', '')),
            #     ],
            #     style={'font-size': 12}),


            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),

            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),

            footbar,

            ], className="container")

    return layout

# callback update URL
component_ids9 = ['lang','lists']
@dash_app9.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids9])
def update_url_state(*values):
    if not isinstance(values[1], str):
        values = values[0],','.join(values[1])
    state = urlencode(dict(zip(component_ids9, values)))
    return '?'+state

# callback update page layout
@dash_app9.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app9_build_layout(state)


@dash_app9.callback(
    Output("collapse", "is_open"),
    [Input("collapse-button", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open





### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###





### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/languages_top_ccc_articles_spread/?lang=ca
dash_app10 = Dash(__name__, server = app, url_base_pathname= webtype + '/languages_top_ccc_articles_spread/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app10.config['suppress_callback_exceptions']=True

title = 'Top 100 CCC article lists spread across the Wikipedias'
dash_app10.title = title+title_addenda

conn2 = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor2 = conn2.cursor()
query = 'SELECT set1, set2, abs_value FROM wdo_intersections WHERE set1descriptor ="all_top_ccc_articles" ORDER BY set1;' # spread


# conn2 = sqlite3.connect(databases_path + 'stats_production.db'); cursor2 = conn2.cursor()
# query = 'SELECT set1, set2, abs_value FROM wdo_intersections_accumulated WHERE set1descriptor ="all_top_ccc_articles" AND period IN (SELECT max(period) FROM wdo_intersections_accumulated) ORDER BY set1;' # spread


df_spread = pd.read_sql_query(query, conn2)
df_spread = df_spread.set_index('set1')

dash_app10.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


text_app10 = '''
        This page shows some statistics that explain how well the first 100 of the Wikipedia [Top CCC articles lists](https://wcdo.wmflabs.org/top_ccc_articles/) are spread across the other language editions.
        '''

text2_app10 = '''
**The challenge is to reach 100 articles (Spread Articles) in each other Wikipedia language edition!**
'''


def dash_app10_build_layout(params):

    if len(params)!=0 and params['lang'].lower()!='none' and 'lists' in params:      

        languagecode_spread=params['lang'].lower()
#        print (languagecode_spread)

        df_sp = df_spread.loc[languagecode_spread]
        df_sp = df_sp.set_index('set2')

        df_lang = df_langs.loc[languagecode_spread]
        df_lang = df_lang.set_index('set1descriptor')

        conn = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cursor = conn.cursor()
        query = 'SELECT set1descriptor, set2, rel_value, abs_value FROM wdo_intersections WHERE set1="'+languagecode_spread+'" AND set2descriptor = "wp" AND set1descriptor != "all_top_ccc_articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) ORDER BY set2;'

        # SELECT set1descriptor, set2, rel_value, abs_value FROM wdo_intersections WHERE set1="ca" AND set2descriptor = "wp" AND set1descriptor != "all_top_ccc_articles" AND measurement_date IN (SELECT MAX(measurement_date) FROM wdo_intersections) ORDER BY set2;

        # SELECT set1descriptor, set2, rel_value, abs_value FROM wdo_intersections WHERE set1="ca" AND set2descriptor = "wp" AND set1descriptor != "all_top_ccc_articles" ORDER BY set2;


        lists_p = params['lists']
        lists_p = lists_p.split(',')

        for x in range(0,len(lists_p)):
            lists_p[x]=column_list_dict[lists_p[x]]

        row_dict = {}
        language_dict = {}
        old_languagecode_spreadin = ''

        for row in cursor.execute(query):

            languagecode_spreadin = row[1]
            if languagecode_spreadin not in wikilanguagecodes: continue
    #        if languagecode_covered in wikilanguagecodes: continue

            if old_languagecode_spreadin!=languagecode_spreadin and old_languagecode_spreadin!='':


                # row_dict['languagename']=html.A(languages.loc[old_languagecode_spreadin]['languagename'], href='https://en.wikipedia.org/wiki/'+englishwpname, target="_blank", style={'text-decoration':'none'})

                row_dict['languagename']=languages.loc[old_languagecode_spreadin]['languagename']

                row_dict['Wiki']=html.A(old_languagecode_spreadin.replace('_','-'), href='https://'+old_languagecode_spreadin.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

                try:
                    value = int(df_sp.loc[old_languagecode_spreadin]['abs_value'])
                except:
                    value = 0
                if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                row_dict['spread_list_articles_sum']=color
                row_dict['spread_list_articles_sum_sort']=value

                list_spread_index=0

                language_dict[old_languagecode_spreadin]=row_dict
                row_dict={}

            list_name = row[0]
            percentage = round(row[2],2)
            num_articles = row[3]

            total_articles = df_lang.loc[list_name,'abs_value']

            if total_articles == 100:
                link = str(num_articles) + '%'
            else:
                link = str(num_articles) + '/'+str(total_articles)

            try:
                link = html.Abbr(link,title=column_list_dict[list_name]+' List',style={'text-decoration':'none'})
            except:
                continue

            url = '/top_ccc_articles/?list='+list_name+'&source_lang='+languagecode_spread+'&target_lang='+languagecode_spreadin+'&country=all'+'&order_by=none&filter=None'

    #        row_dict[list_name]= dcc.Markdown('['+link+']('+url+'){:target="_blank"}')
            row_dict[list_name]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'}) # target="_blank",

            old_languagecode_spreadin = languagecode_spreadin



        # last iteration
        row_dict['languagename']=languages.loc[languagecode_spreadin]['languagename']


        row_dict['Wiki']=html.A(languagecode_spreadin.replace('_','-'), href='https://'+languagecode_spreadin.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

        value = int(df_sp.loc[languagecode_spreadin]['abs_value'])
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['spread_list_articles_sum']=color
        row_dict['spread_list_articles_sum_sort']=value


        language_dict[languagecode_spreadin]=row_dict


        df=pd.DataFrame.from_dict(language_dict,orient='index')

#        df['languagename'] = df.languagename.astype(str)
        df = df.sort_values('languagename')
        languagename={}
        for x in df.index.values: 
            englishwpname = languages.loc[x]['WikipedialanguagearticleEnglish']
            if englishwpname != 'no link':
                englishwpname = englishwpname.split('/')[4].replace(' ','_')
            languagename[x]=html.A(languages.loc[x]['languagename'], href='https://en.wikipedia.org/wiki/'+englishwpname, target="_blank", style={'text-decoration':'none'})
        df['languagename'] = pd.Series(languagename)


        df['World Subregion']=languages.subregion
        for x in df.index.values.tolist():
            if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]

        df=df.rename(columns=column_list_dict)

        columns = ['Language','Wiki']+['World Subregion','Spread Articles']+lists_p
        columns_list = columns.copy()

        for column in columns: 
            if column not in df.columns.values:
                df[column]='0/0'


        df = df.sort_values(by=['spread_list_articles_sum_sort'], ascending = False)
        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        language_spread = languages.loc[languagecode_spread]['languagename']

        main_title = language_spread+' Wikipedia Top 100 CCC article lists spread across the rest of Wikipedias'
#        dash_app10.title = title+title_addenda

        results_text = '''
                The following table is useful in order to assess how known the Top 100 CCC articles from '''+language_spread+''' Wikipedia language are in the entire Wikipedia project. Languages are sorted in alphabetic order by their Wikicode, and columns present the number of articles from each list covered by the language. The last column **Spread Articles** the overall sum of articles from the Top 100 of each list spread across a specific language. The maximum Spread Articles can be 1000 (there are 100 in each of the 10 lists), although it may be lower since some lists have articles in common. When Spread Articles has 100 or more articles it will turn green. Every cell of the table is a clickable link to a Top CCC list.
                '''
                # The values of this table are *updated once per day* counting the new articles created.

        layout = html.Div([
            navbar,
            html.H3('Any Wikipedia Top 100 CCC article lists spread across the rest of Wikipedias', style={'textAlign':'center'}),
            dcc.Markdown(
                text_app10.replace('  ', '')),

            # html.Div([
            #     dcc.Markdown(text_lists.replace('  ', '')),
            #     ],
            #     style={'font-size': 12}),

            # dcc.Markdown(
            #     text2_app10.replace('  ', '')),

            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),
            html.Br(),


            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),

            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),

            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(results_text.replace('  ', '')),
            dcc.Markdown(
                text2_app10.replace('  ', '')),


            html.Br(),
            html.H6(main_title, style={'textAlign':'center'}),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +

                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(len(df))]
            ),
            footbar,
            ], className="container")


    else:
        layout = html.Div([
            navbar,
            html.H3('Any Wikipedia Top 100 CCC article lists spread across the rest of Wikipedias', style={'textAlign':'center'}),
            dcc.Markdown(
                text_app10.replace('  ', '')),

            html.Br(),
            html.H5('Select the language and lists'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Lists'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'lists',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value=default_lists,
                multi=True,
                placeholder="Select lists",           
                style={'width': '900px'}
             ), style={'display': 'inline-block','width': '200px'}),
            html.Br(),


            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dbc.Button(
                "Lists",
                id="collapse-button",
                className="mb-3",
                color="primary",
            ),
            style={'display': 'inline-block','width': '200px'}),

            dbc.Collapse(
                dbc.Card(      
                html.P(
                    dcc.Markdown(text_lists.replace('  ', '')),
                    # style={'font-size': 12},
                    style={'font-size': 12, 'marginLeft': 10, 'marginRight': 10, 'marginTop': 10, 'marginBottom': 10, 
                           'backgroundColor':'#F7FBFE',
                           'padding': '6px 0px 0px 8px'}),
                ),
                id="collapse",
            ),


            footbar,
            ], className="container")

    return layout



# callback update URL
component_ids10 = ['lang','lists']
@dash_app10.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids10])
def update_url_state(*values):

    if not isinstance(values[1], str):
        values = values[0],','.join(values[1])

    state = urlencode(dict(zip(component_ids10, values)))
    return '?'+state


# callback update page layout
@dash_app10.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app10_build_layout(state)



@dash_app10.callback(
    Output("collapse", "is_open"),
    [Input("collapse-button", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open
