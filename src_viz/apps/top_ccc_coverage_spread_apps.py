import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/?lang=ca
dash_app8 = Dash(__name__, server = app, url_base_pathname= webtype + '/languages_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app8.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})
dash_app8.config['suppress_callback_exceptions']=True

#dash_app8.config.supress_callback_exceptions = True

dash_app8.title = 'Languages Top 100 CCC article lists coverage'+title_addenda
dash_app8.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
query = 'SELECT set2, set1, abs_value FROM wcdo_intersections WHERE set1descriptor ="all_top_ccc_articles" ORDER BY set2;' # spread
df_cover = pd.read_sql_query(query, conn2)
df_cover = df_cover.set_index('set2')


# callback update URL
component_ids = ['lang']
@dash_app8.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids, values)))
    return '?'+state

# callback update page layout
@dash_app8.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app8_build_layout(state)


def dash_app8_build_layout(params):

    if len(params)!=0 and params['lang'].lower()!='none':

        language_covering=params['lang'].lower()
        language_covering=language_covering.lower()

        df_cv = df_cover.loc[language_covering]
        df_cv = df_cv.set_index('set1')

    #    language_covering='ca'
        conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()

        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wcdo_intersections WHERE set2 = "'+language_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set1descriptor DESC;'

        column_list_dict = {'languagename':'Language','Wiki':'Wiki','editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y. A. Edits', 'created_last_year':'Last Y. A. Edits', 'women':'Women', 'men':'Men', 'pageviews':'Page views', 'discussions':'Talk Edits','list_coverage_index':'List Coverage Idx.','covered_list_articles_sum':'Sum Covered Articles','World Subregion':'World Subregion'}

        lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
        list_top = lists.copy()

        row_dict = {}
        language_dict = {}
        old_languagecode_covered = ''
        list_coverage_index=0

        for row in cursor.execute(query):

            languagecode_covered = row[1]
            if languagecode_covered not in wikilanguagecodes: continue

            if old_languagecode_covered!=languagecode_covered and old_languagecode_covered!='':

                row_dict['languagename']=html.A(languages.loc[old_languagecode_covered]['languagename'], href='https://en.wikipedia.org/wiki/'+languages.loc[old_languagecode_covered]['WikipedialanguagearticleEnglish'].split('/')[4].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                row_dict['Wiki']=html.A(old_languagecode_covered.replace('_','-'), href='https://'+old_languagecode_covered.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

                row_dict['list_coverage_index']=round(list_coverage_index/10,1)

                value = int(df_cv.loc[old_languagecode_covered]['abs_value'])
                if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                row_dict['covered_list_articles_sum']=color

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
        row_dict['languagename']=html.A(languages.loc[languagecode_covered]['languagename'], href='https://en.wikipedia.org/wiki/'+languages.loc[languagecode_covered]['WikipedialanguagearticleEnglish'].split('/')[4].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
        row_dict['Wiki']=html.A(languagecode_covered.replace('_','-'), href='https://'+languagecode_covered.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})
        row_dict['list_coverage_index']=round(list_coverage_index/10,1)

        value = int(df_cv.loc[languagecode_covered]['abs_value'])
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['covered_list_articles_sum']=color

        for remaining_list in list_top:
            row_dict[remaining_list] = '0/0'

        language_dict[languagecode_covered]=row_dict

        df=pd.DataFrame.from_dict(language_dict,orient='index')

        columns = ['Language','Wiki', 'Page views', 'Editors', 'Talk Edits', 'First 3Y. A. Edits', 'Last Y. A. Edits', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men','List Coverage Idx.','Sum Covered Articles','World Subregion']


        df['World Subregion']=languages.subregion
        for x in df.index.values.tolist():
            if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]


        df=df.rename(columns=column_list_dict)

        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        language_cover = languages.loc[language_covering]['languagename']
        title = 'Languages Top 100 CCC articles lists coverage by '+language_cover+' Wikipedia'
#        dash_app8.title = title+title_addenda

        text = '''This page shows some statistics that explain how well '''+language_cover+''' Wikipedia language edition covers the Top 100 of the Top CCC articles lists from other Wikipedia language editions. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

                The following table is useful to assess how well '''+language_cover+''' Wikipedia covers the Top 100 CCC articles from the lists generated from all the other language editions CCC. Languages are sorted in alphabetic order by their Wikicode, and columns present the number of articles from each list covered by English language. The last two columns, **Lists Coverage Idx.** and **Sum Covered Articles** present the percentage of articles from the lists covered and the overall sum of articles from the lists covered by '''+language_cover+''' Wikipedia for each language. The maximum Sum Covered Articles can be 1000 (there are 100 in each of the 10 lists), although it may be lower since some lists have articles in common. When Sum Covered Articles has 100 or more articles it will turn green. In some languages, it is not be possible to cover them because unfortunately the lists do not have enough articles.

                **The challenge is to reach 100 articles covered (Sum Covered Articles) from each language CCC!**
                '''

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', ''), containerProps={'textAlign':'center'}),

            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +

                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(len(df))]
            )
            ], className="container")

    else:
        title = 'Languages Top 100 CCC articles lists coverage'
#        dash_app8.title = title+title_addenda

        text = '''This page shows some statistics that explain how well a Wikipedia language edition covers the Top 100 of the Top CCC articles lists from other Wikipedia language editions. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

                **The challenge is to reach 100 articles covered (Sum Covered Articles) from each language CCC!**
                '''

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', ''), containerProps={'textAlign':'center'}),

            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),

            ], className="container")        

    return layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/countries_top_ccc_articles_coverage/?lang=ca
dash_app9 = Dash(__name__, server = app, url_base_pathname= webtype + '/countries_top_ccc_articles_coverage/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
dash_app9.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

#dash_app9.config.supress_callback_exceptions = True
dash_app9.config['suppress_callback_exceptions']=True

dash_app9.title = 'Countries Top 100 CCC article lists coverage'+title_addenda
dash_app9.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
query = 'SELECT set2, set1, abs_value FROM wcdo_intersections WHERE set1descriptor ="all_top_ccc_articles" ORDER BY set2;' # spread
df_cover = pd.read_sql_query(query, conn2)
df_cover = df_cover.set_index('set2')



# callback update URL
component_ids = ['lang']
@dash_app9.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids, values)))
    return '?'+state

# callback update page layout
@dash_app9.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app9_build_layout(state)


def dash_app9_build_layout(params):

    if len(params)!=0 and params['lang'].lower()!='none':

        languagecode_covering=params['lang'].lower()

        df_cv = df_cover.loc[languagecode_covering]
        df_cv = df_cv.set_index('set1')

        conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()

        query = 'SELECT set1descriptor, set1, rel_value, abs_value FROM wcdo_intersections WHERE set2 = "'+languagecode_covering+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1, set1descriptor DESC;'

        list_dict = {'editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y. A. Edits', 'created_last_year':'Last Y. A. Edits', 'women':'Women', 'men':'Men', 'pageviews':'Page views', 'discussions':'Talk Edits'}

        lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']
        list_top = lists.copy()

        row_dict = {}
        country_dict = {}
        old_covered = ''
        list_coverage_index=0

        for row in cursor.execute(query):

            print (row)

            covered = row[1]
            if '(' not in covered: continue

            if old_covered!=covered and old_covered!='':
                language_name = languages.loc[languagecode_covered]['languagename']
                country_name = country_names[country_covered]
                country_language = language_name+' CCC ('+ country_name+')'
    #            country_language = country_name+' ('+language_name+' CCC)'

                try:
                    print (country_covered, languagecode_covered, covered)

                    territorynames = wikilanguages_utils.load_territories_names_from_language_country(country_covered, languagecode_covered, territories)
                    territorynames_label = ", ".join(territorynames)
                    territorynames_label+= '.'
                    print (territorynames_label)



                    abbr_label = html.Abbr(country_language,title='Territories: '+territorynames_label)

                    row_dict['Country']=abbr_label
                    row_dict['List Coverage Idx.']=round(list_coverage_index/10,1)

                    value = int(df_cv.loc[old_covered]['abs_value'])
                    if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                    else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                    row_dict['Sum Covered Articles']=color

        #            print (row_dict)
                    for remaining_list in list_top:
                        row_dict[list_dict[remaining_list]] = '0/0'

                    country_dict[country_language]=row_dict



                except:
                    territorynames_label = ''
                    print ('this is not covered anymore: '+country_language)
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

            link = html.Abbr(link,title=list_dict[list_name]+' List',style={'text-decoration':'none'})

            url = 'https://wcdo.wmflabs.org/top_ccc_articles/?list='+list_name+'&source_lang='+languagecode_covered+'&source_country='+country_covered+'&target_lang='+languagecode_covering+'&order_by=none&filter=None'

            row_dict[list_dict[list_name]]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'})
            row_dict['World Subregion'] = subregions[country_covered]

            print (covered,old_covered)
            print ('chicken')

            old_covered = covered
            list_top.remove(list_name)




    





        # last iteration
        language_name = languages.loc[languagecode_covered]['languagename']
        country_name = country_names[country_covered]

    #    country_language = country_name+' ('+language_name+' CCC)'
        country_language = language_name+' CCC ('+ country_name+')'

        row_dict['Country']=country_language
        row_dict['List Coverage Idx.']=round(list_coverage_index/10,1)

        value = int(df_cv.loc[covered]['abs_value'])
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['Sum Covered Articles']=color

        for remaining_list in list_top:
            row_dict[list_dict[remaining_list]] = '0/0'
        country_dict[country_language]=row_dict

        columns = ['Country', 'Page views', 'Editors', 'Talk Edits', 'First 3Y. A. Edits', 'Last Y. A. Edits', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men','List Coverage Idx.','Sum Covered Articles','World Subregion']

        language_cover = languages.loc[languagecode_covering]['languagename']
        title = 'Countries Top 100 CCC article lists coverage by '+language_cover+' Wikipedia'
#        dash_app9.title = title+title_addenda
        text = '''
                This page shows some statistics that explain how well '''+language_cover+'''' Wikipedia language edition covers the Top 100 CCC articles lists generated for the countries associated to each language edition (when it is a region or several regions located in the same country, it appears as one row with the country name). It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

                Some languages are mapped to several countries, some countries are mapped to several languages and sometimes the language spread remains at a regional level. Therefore, in order to create lists for countries, articles from a language CCC has been associated to specific territories (regions or countries) according to their features obtained during the CCC selection process, and later, they have been aggregated by countries (ISO 3166). For instance, for the French CCC, the resulting final list of countries includes France (French speaking territories), whose territory equates directly to a country, and Canada (French speaking territories), whose use of French is only limited to Québec.

                The following table is useful to assess how well '''+language_cover+'''' Wikipedia covers the Top 100 CCC articles from all the lists generated from the other language editions CCC down at country level. Languages CCC are sorted in alphabetic order with the country subselection between parenthesis - e.g. Italian CCC (Switzerland) row would be for Ticino, while Catalan CCC (Spain) row would be for Catalonia, Balearic Islands and Valencia. When a language is spoken in several countries there is one row per country and by hovering on it you can see the regions where it is spoken. If you detect any territory which you do not consider correct, please contact us as we may need to modify the Language-Territories Mapping table used to obtain the CCC for this particular language.

                The rest of columns present the number of articles from each list covered by English language. The last two columns, **Lists Coverage Idx.** and **Sum Covered Articles** present the percentage of articles from the lists covered and the overall sum of articles by '''+language_cover+'''' Wikipedia. The maximum Sum Covered Articles can be 1000 (there are 100 in each of the 10 lists), although it may be lower since some lists have articles in common. When Sum Covered Articles has 100 or more articles it will turn green. In some languages and countries, it is not be possible to cover them because unfortunately the lists do not have enough articles.

                **The challenge is to reach 100 articles covered (Sum Covered Articles) from each country!**
            
                _Please, be aware this interface is in its Beta phase. If it does not respond to your queries, refresh the page several times until you see Updating..._.\
                '''
        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', ''),
            containerProps={'textAlign':'center'}),

            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in columns])] +

                # Body
                [html.Tr([
                    html.Td(
                        row[col]
                        ) for col in columns
                ]) for i,row in sorted(country_dict.items())]
            ),

            dcc.Markdown('''Tags: #sharing #ccc #culturaldiversity'''.replace('  ', ''),
            containerProps={'textAlign':'center'}),

            html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})

            ], className="container")

    else:

        title = 'Countries Top 100 CCC article lists coverage'
#        dash_app9.title = title+title_addenda
        text = '''
                This page shows some statistics that explain how well a Wikipedia language edition covers the Top 100 CCC articles lists generated for the countries associated to each language edition (when it is a region or several regions located in the same country, it appears as one row with the country name). It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

                Some languages are mapped to several countries, some countries are mapped to several languages and sometimes the language spread remains at a regional level. Therefore, in order to create lists for countries, articles from a language CCC has been associated to specific territories (regions or countries) according to their features obtained during the CCC selection process, and later, they have been aggregated by countries (ISO 3166). For instance, for the French CCC, the resulting final list of countries includes France (French speaking territories), whose territory equates directly to a country, and Canada (French speaking territories), whose use of French is only limited to Québec.

                **The challenge is to reach 100 articles covered (Sum Covered Articles) from each country!**
                '''
        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', ''),
            containerProps={'textAlign':'center'}),

            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),

            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in columns])] +

                # Body
                [html.Tr([
                    html.Td(
                        row[col]
                        ) for col in columns
                ]) for i,row in sorted(country_dict.items())]
            ),

            dcc.Markdown('''Tags: #sharing #countries #ccc #culturaldiversity'''.replace('  ', ''),
            containerProps={'textAlign':'center'}),

            html.A('Home - Wikipedia Cultural Diverstiy Observatory', href='https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory', target="_blank", style={'textAlign': 'right', 'text-decoration':'none'})


            ], className="container")



    return layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###


### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# https://wcdo.wmflabs.org/languages_top_ccc_articles_spread/?lang=ca
dash_app10 = Dash(__name__, server = app, url_base_pathname= webtype + '/languages_top_ccc_articles_spread/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app10.scripts.append_script({"external_url": "https://wcdo.wmflabs.org/assets/gtag.js"})

#dash_app10.config.supress_callback_exceptions = True
dash_app10.config['suppress_callback_exceptions']=True

title = 'Top 100 CCC article lists spread across the Wikipedias'
dash_app10.title = title+title_addenda

conn2 = sqlite3.connect(databases_path + 'wcdo_stats.db'); cursor2 = conn2.cursor()
query = 'SELECT set1, set2, abs_value FROM wcdo_intersections WHERE set1descriptor ="all_top_ccc_articles" ORDER BY set1;' # spread
df_spread = pd.read_sql_query(query, conn2)
df_spread = df_spread.set_index('set1')

dash_app10.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])




# callback update URL
component_ids = ['lang']
@dash_app10.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids, values)))
    return '?'+state

# callback update page layout
@dash_app10.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app10_build_layout(state)


def dash_app10_build_layout(params):

    if len(params)!=0 and params['lang'].lower()!='none':

        languagecode_spread=params['lang'].lower()
#        print (languagecode_spread)

        df_sp = df_spread.loc[languagecode_spread]
        df_sp = df_sp.set_index('set2')

        df_lang = df_langs.loc[languagecode_spread]
        df_lang = df_lang.set_index('set1descriptor')

        conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()
        query = 'SELECT set1descriptor, set2, rel_value, abs_value FROM wcdo_intersections WHERE set1="'+languagecode_spread+'" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set2;'

        lists = ['editors', 'featured', 'geolocated', 'keywords', 'women', 'men', 'created_first_three_years', 'created_last_year', 'pageviews', 'discussions']

        column_list_dict = {'languagename':'Language','Wiki':'Wiki','editors':'Editors', 'featured':'Featured', 'geolocated':'Geolocated', 'keywords':'Keywords', 'created_first_three_years':'First 3Y. A. Edits', 'created_last_year':'Last Y. A. Edits', 'women':'Women', 'men':'Men', 'pageviews':'Page views', 'discussions':'Talk Edits','list_spread_index':'List Spread Idx.','spread_list_articles_sum':'Sum Spread Articles','World Subregion':'World Subregion'}

        row_dict = {}
        language_dict = {}
        old_languagecode_spreadin = ''

        for row in cursor.execute(query):

            languagecode_spreadin = row[1]
            if languagecode_spreadin not in wikilanguagecodes: continue
    #        if languagecode_covered in wikilanguagecodes: continue

            if old_languagecode_spreadin!=languagecode_spreadin and old_languagecode_spreadin!='':

                row_dict['languagename']=html.A(languages.loc[old_languagecode_spreadin]['languagename'], href='https://en.wikipedia.org/wiki/'+languages.loc[old_languagecode_spreadin]['WikipedialanguagearticleEnglish'].split('/')[4].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                row_dict['Wiki']=html.A(old_languagecode_spreadin.replace('_','-'), href='https://'+old_languagecode_spreadin.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

                value = int(df_sp.loc[old_languagecode_spreadin]['abs_value'])
                if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
                else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
                row_dict['spread_list_articles_sum']=color

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

            link = html.Abbr(link,title=column_list_dict[list_name]+' List',style={'text-decoration':'none'})

            url = '/top_ccc_articles/?list='+list_name+'&source_lang='+languagecode_spread+'&target_lang='+languagecode_spreadin+'&country=all'+'&order_by=none&filter=None'

    #        row_dict[list_name]= dcc.Markdown('['+link+']('+url+'){:target="_blank"}')
            row_dict[list_name]= html.A(link, href=url, target="_blank", style={'text-decoration':'none'}) # target="_blank",

            old_languagecode_spreadin = languagecode_spreadin

        # last iteration
        row_dict['languagename']=html.A(languages.loc[languagecode_spreadin]['languagename'], href='https://en.wikipedia.org/wiki/'+languages.loc[languagecode_spreadin]['WikipedialanguagearticleEnglish'].split('/')[4].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
        row_dict['Wiki']=html.A(languagecode_spreadin.replace('_','-'), href='https://'+languagecode_spreadin.replace('_','-')+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none','font-weight': 'bold','color':'#000000'})

        value = int(df_sp.loc[languagecode_spreadin]['abs_value'])
        if value < 99: color = html.Div(str(value), style={'color': 'red','font-weight': 'bold'})
        else: color = html.Div(str(value), style={'color': 'green','font-weight': 'bold'})
        row_dict['spread_list_articles_sum']=color

        language_dict[languagecode_spreadin]=row_dict

        df=pd.DataFrame.from_dict(language_dict,orient='index')

        df['World Subregion']=languages.subregion
        for x in df.index.values.tolist():
            if ';' in df.loc[x]['World Subregion']: df.at[x, 'World Subregion'] = df.loc[x]['World Subregion'].split(';')[0]

        df=df.rename(columns=column_list_dict)
        columns = ['Language','Wiki','Page views', 'Editors', 'Talk Edits', 'First 3Y. A. Edits', 'Last Y. A. Edits', 'Featured', 'Geolocated', 'Keywords', 'Women', 'Men','Sum Spread Articles','World Subregion']
        columns_list = columns.copy()

        for column in columns: 
            if column not in df.columns.values:
                df[column]='0/0'

        df = df[columns] # selecting the parameters to export
        df = df.fillna('')

        language_spread = languages.loc[languagecode_spread]['languagename']

        title = language_spread+' Wikipedia Top 100 CCC article lists spread across the rest of Wikipedias'
#        dash_app10.title = title+title_addenda

        text = '''
                This page shows some statistics that explain how well the first '''+language_spread+''' Wikipedia Top 100 CCC articles are spread across the other language editions. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

                The following table is useful in order to assess how known the Top 100 CCC articles from '''+language_spread+''' Wikipedia language are in the entire Wikipedia project. Languages are sorted in alphabetic order by their Wikicode, and columns present the number of articles from each list covered by the language. The last column **Sum Spread Articles** the overall sum of articles from the Top 100 of each list spread across a specific language. The maximum Sum Spread Articles can be 1000 (there are 100 in each of the 10 lists), although it may be lower since some lists have articles in common. When Sum Spread Articles has 100 or more articles it will turn green.

                **The challenge is to reach 100 articles (Sum Spread Articles) in each other Wikipedia language edition!**
                '''

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', '')),
#            containerProps={'textAlign':'center'}),

            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),


            html.Table(
                # Header
                [html.Tr([html.Th(col) for col in df.columns])] +

                # Body
                [html.Tr([
                    html.Td(df.iloc[i][col]) for col in df.columns
                ]) for i in range(len(df))]
            )
            ], className="container")
    else:
        title = 'Any Wikipedia Top 100 CCC article lists spread across the rest of Wikipedias'
#        dash_app10.title = title+title_addenda

        text = '''
                This page shows some statistics that explain how well the first Wikipedia Top 100 CCC articles are spread across the other language editions. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

                These lists are created by ranking the articles according to specific features and sometimes giving them weights. These different features are based on the _**article characteristics**_ (number of Editors) or _**content type**_ (e.g. geolocated articles). 

                The Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles created during the first three years and with most edits (**First 3Y. A. Edits**) and list of CCC articles created during the last year and with most edits (**Last Y. A. Edits**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).
                '''

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', ''),
            containerProps={'textAlign':'center'}),


            html.H5('Select the language'),

            html.Div(
            html.P('Language'),
            style={'display': 'inline-block','width': '200'}),
            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.A(html.Button('Query Results!'),
                href=''),

            ], className="container")


    return layout

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
