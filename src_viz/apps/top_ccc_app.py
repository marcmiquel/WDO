import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app7 = Dash(server = app, url_base_pathname = webtype + '/top_ccc_articles/', external_stylesheets = external_stylesheets)
dash_app7.config['suppress_callback_exceptions']=True

dash_app7.title = 'Top CCC Diversity Lists'+title_addenda
dash_app7.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])


text_default = '''
In this page you can retrieve lists of Top CCC Diversity articles from any Wikipedia language edition and check its availability in other Wikipedia language editions.
'''    

text_default2 = '''
Select a Top CCC Diversity list or it will use the default list "editors". These lists are created by ranking the articles according to different criteria. They can be grouped into the following groups:
* Lists of CCC diversity articles based on relevance: most pageviews during the last month (**Pageviews**), most number of editors (**Editors**), most number of edits (**Edits**), most number of edits during the last month (**Edited Last Month**), most number of edits in talk pages (**Discussions**), created during the first three years and with most edits (**First 3Y.**), created during the last year and with most edits **Last Y.**), the highest Wikirank (**Wikirank**), featured article distinction (**Featured**) most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), most number of images (**Images**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), most number of properties in Wikidata (**WD Properties**), most number of number of Interwiki links (**Interwiki**), least number of interwiki and most number of editors (**Least Interwiki Most Editors**), and least number interwiki and most number of properties (**Least Interwiki Most With Wikidata Properties**).
* Lists of CCC articles based on localness characteristics: geolocation with the most number of links coming from CCC (**Geolocated**), and keywords on title with the largest number of bytes (**Keywords**).
* Lists of CCC articles and diversity topics: women and men biographies and with most edits (**Women**) and (**Men**), biographies identified with lgbt+ sexual orientation with most edits (**LGBT+**), biographies identified as belonging to an ethnic group with most edits (**Ethnic Group**) and biographies identified with an affiliation to a religious group (**Religious Group**).
* Lists of CCC articles and with most pageviews during the last month and **"Wiki Loves"** topics based on WD properties: *Books*, *Clothing and fashion*, *Earth*, *Folk*, *Food*, *GLAM*, *Industry*, *Monuments and buildings*, *Music creation and organizations*, *Paintings*, *Religion* and *Sports and teams*.
'''



interface_row1 = html.Div([

    html.Div(
    [
    html.P(
        [
            "Source ",
            html.Span(
                "language",
                id="tooltip-target-language",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a source language to retrieve a list of articles from its 'local content' or CCC.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-language",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    html.Div([
    html.P(
        [
            "Source ",
            html.Span(
                "country",
                id="tooltip-target-country",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Selecting a source country is an optional choice. It is used to filter some part of the language context, in case it encompasses more than one country (i.e. English is used in several countries besides the United Kingdom, you can choose among them). In case no country is selected, the default choice is all the language-related countries.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-country",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div(
    [
    html.P(
        [
            "Source ",
            html.Span(
                "list",
                id="tooltip-target-list",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '200px'},
    ),

    dbc.Tooltip(
        html.Div(
            dcc.Markdown(text_default2.replace('  ', '')),
        style={"width": 870, 'font-size': 12, 'text-align':'left','backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-list",
        placement="top",
        style={'color':'black','backgroundColor':'transparent'},
    ),

    html.Div(
    [
    html.P(
        [
            "Target ",
            html.Span(
                "language",
                id="tooltip-target-targetlanguage",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a target language where you want to check the availability of the retrieved list of articles.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-targetlanguage",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])


interface_row2 = html.Div([
    html.Div(
    [
    html.P(
        [
            "Order ",
            html.Span(
                "by",
                id="tooltip-target-orderby",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature and use it to sort the list of articles.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-orderby",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    html.Div(
    [
    html.P(
        [
            html.Span(
                "Exclude",
                id="tooltip-target-exclude",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Exclude the articles which already exist or do not exist in the target language.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-exclude",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])


covered = {'Existing articles':'existing', 'Non-existing articles':'non-existing'}

def dash_app7_build_layout(params):
    features_dict = {'Number of Editors':'num_editors','Number of Edits':'num_edits','Number of Images':'num_images','Wikirank':'wikirank','Number of Pageviews':'num_pageviews','Number of Inlinks':'num_inlinks','Number of References':'num_references','Number of Bytes':'num_bytes','Number of Outlinks':'num_outlinks','Number of Interwiki':'num_interwiki','Number of WDProperties':'num_wdproperty','Number of Discussions':'num_discussions','Creation Date':'date_created','Number of Inlinks from CCC':'num_inlinks_from_CCC'}

    lists_dict = {'Editors':'editors','Featured':'featured','Geolocated':'geolocated','Keywords':'keywords','Women':'women','Men':'men','Created First Three Years':'created_first_three_years','Created Last Year':'created_last_year','Pageviews':'pageviews','Discussions':'discussions','Edits':'edits', 'Edited Last Month':'edited_last_month', 'Images':'images', 'WD Properties':'wdproperty_many', 'Interwiki':'interwiki_many', 'Least Interwiki Most Editors':'interwiki_editors', 'Least Interwiki Most WD Properties':'interwiki_wdproperty', 'Wikirank':'wikirank', 'Wiki Loves Earth':'earth', 'Wiki Loves Monuments':'monuments_and_buildings', 'Wiki Loves Sports':'sport_and_teams', 'Wiki Loves GLAM':'glam', 'Wiki Loves Folk':'folk', 'Wiki Loves Music':'music_creations_and_organizations', 'Wiki Loves Food':'food', 'Wiki Loves Paintings':'paintings', 'Wiki Loves Books':'books', 'Wiki Loves Clothing and Fashion':'clothing_and_fashion', 'Wiki Loves Industry':'industry', 'Wiki Loves Religion':'religion', 'Religious Group':'religious_group','LGBT+':'sexual_orientation','Ethnic Group':'ethnic_group'}


    list_dict_inv = {v: k for k, v in lists_dict.items()}


    if len(params)!=0 and params['source_lang'].lower()!='none' and params['target_lang']!='none':


        if params['list'].lower()=='none':
            list_name='editors'
        else:
            list_name=params['list'].lower()
    
        source_lang=params['source_lang'].lower()
        target_lang=params['target_lang'].lower()

        if 'source_country' in params:
            country=params['source_country'].upper()
            if country == 'NONE' or country == 'ALL': country = 'all'
        else:
            country = 'all'

        if 'exclude' in params:
            exclude_articles=params['exclude'].lower()
        else:
            exclude_articles='none'

        if 'order_by' in params:
            order_by=params['order_by']#.lower()
        else:
            order_by='none'

        source_language = languages.loc[source_lang]['languagename']
        target_language = languages.loc[target_lang]['languagename']


    #    lists = ['editors','featured','geolocated','keywords','women','men','created_first_three_years','created_last_year','pageviews','discussions']

        conn = sqlite3.connect(databases_path + 'top_diversity_articles_production.db'); cur = conn.cursor()

        columns_dict = {'position':'Nº','page_title_original':'Article Title','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_bytes':'Bytes','num_images':'Images','wikirank':'Wikirank','num_references':'References','num_inlinks':'Inlinks','num_wdproperty':'Wikidata Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_discussions':'Discussions','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC','related_languages':'Related Languages','page_title_target':' Article Title'}

        columns_dict_abbr = {'References':'Refs.', 'Pageviews':'PV', 'Editors':'Edtrs','num_inlinks':'Inlinks','Wikidata Properties':'WD.P.','Interwiki Links':'IW.L.','Featured Article':'F.A.','Creation Date':'Created','Inlinks from CCC':'IL CCC','Related Languages':'Rel. Lang.','Ethnic Group':'Ethnia','Religious Group':'Religion'}

        # falten introduïr les columnes de sexual orientation, ethnic group i religious group.

        # COLUMNS
        query = 'SELECT r.qitem, f.page_title_original, '
        columns = ['Nº','Article Title']

        if list_name == 'editors': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'featured': 
            query+= 'f.featured_article, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki,  '
            columns+= ['Featured Article','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'geolocated': 
            query+= 'f.num_inlinks, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Inlinks','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'keywords': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki,  '
            columns+= ['Editors','Pageviews','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'women': 
            query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Edits','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'men': 
            query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Edits','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'sexual_orientation': 

            query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_interwiki, f.sexual_orientation, '
            columns+= ['Edits','Editors','Pageviews','Bytes','References','Interwiki Links','LGBT+']

        if list_name == 'ethnic_group': 

            query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_interwiki, f.ethnic_group, '
            columns+= ['Edits','Editors','Pageviews','Bytes','References','Interwiki Links', 'Ethnic Group']

        if list_name == 'religious_group': 

            query+= 'f.num_edits, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_interwiki, f.religious_group, '
            columns+= ['Edits','Editors','Pageviews','Bytes','References','Interwiki Links', 'Religious Group']

        if list_name == 'created_first_three_years': 
            query+='f.num_editors, f.num_pageviews, f.num_edits, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Edits','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'created_last_year': 
            query+='f.num_editors, f.num_pageviews, f.num_edits, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Edits','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'pageviews': 
            query+='f.num_pageviews, f.num_edits, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Pageviews','Edits','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'discussions': 
            query+='f.num_discussions, f.num_pageviews, f.num_edits, f.num_bytes, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Discussions','Pageviews','Edits','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'edits': 
            query+='f.num_edits, f.num_bytes, f.num_discussions, f.num_pageviews, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Edits','Discussions','Bytes','Pageviews','References','Wikidata Properties','Interwiki Links']

        if list_name == 'edited_last_month': 
            query+='f.num_edits, f.num_bytes, f.num_discussions, f.num_pageviews, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Edits','Bytes','Discussions','Pageviews','References','Wikidata Properties','Interwiki Links']

        if list_name == 'images': 
            query+='f.num_images, f.num_bytes, f.num_edits, f.num_pageviews, f.num_references, f.featured_article, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Images','Bytes','Edits','Pageviews','References','Featured Article','Wikidata Properties','Interwiki Links']

        if list_name == 'wdproperty_many': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'interwiki_many': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'interwiki_editors': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'interwiki_wdproperty': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'wikirank': 
            query+= 'f.wikirank, f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Wikirank','Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'earth': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'monuments_and_buildings': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'sport_and_teams': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'glam': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'folk': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'music_creations_and_organizations': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'food': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'paintings': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'books': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'clothing_and_fashion': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'industry': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']

        if list_name == 'religion': 
            query+= 'f.num_editors, f.num_pageviews, f.num_bytes, f.num_references, f.num_wdproperty, f.num_interwiki, '
            columns+= ['Editors','Pageviews','Bytes','References','Wikidata Properties','Interwiki Links']



        if order_by != 'none':
            feat = columns_dict[order_by]
            if feat not in columns:
                columns+= [feat]
                query+= 'f.'+order_by+', '

#        print (columns)

        # NEW LISTS
        query += 'f.num_inlinks_from_CCC, f.date_created, p.page_title_target, p.generation_method, p0.page_title_target pt0, p0.generation_method pg0, p1.page_title_target pt1, p1.generation_method pg1, p2.page_title_target pt2, p2.generation_method pg2, p3.page_title_target pt3, p3.generation_method pg3 '

        if 'Inlinks from CCC' not in columns: columns+= ['Inlinks from CCC']
        columns+= ['Creation Date']
        columns+= ['Related Languages',' Article Title']
#        columns= list(dict.fromkeys(columns))

        query += 'FROM '+source_lang+'wiki_top_articles_lists r '
        query += 'LEFT JOIN '+target_lang+'wiki_top_articles_page_titles p USING (qitem) '
        query += 'LEFT JOIN '+closest_langs[target_lang][0]+'wiki_top_articles_page_titles p0 USING (qitem) '
        query += 'LEFT JOIN '+closest_langs[target_lang][1]+'wiki_top_articles_page_titles p1 USING (qitem) '
        query += 'LEFT JOIN '+closest_langs[target_lang][2]+'wiki_top_articles_page_titles p2 USING (qitem) '
        query += 'LEFT JOIN '+closest_langs[target_lang][3]+'wiki_top_articles_page_titles p3 USING (qitem) '
        query += 'INNER JOIN '+source_lang+'wiki_top_articles_features f USING (qitem) '
        query += "WHERE r.list_name = '"+list_name+"' "
        if country: query += 'AND r.country IS "'+country+'" '

        if exclude_articles == 'existing': 
            query += 'AND p.generation_method != "sitelinks" '
        elif exclude_articles == 'non-existing':
            query += 'AND p.generation_method = "sitelinks" '


        if order_by != 'none':
            query += 'ORDER BY f.'+order_by+' DESC;'
        else:
            query += 'ORDER BY r.position ASC;'


        # print (query)
        # print (columns)

        df = pd.read_sql_query(query, conn)#, parameters)
        df = df.fillna(0)
        # print (df.columns)

        if country == 'all':
            main_title = source_language + ' Top CCC articles list "'+list_dict_inv[list_name]+'" and its coverage by '+target_language+' Wikipedia'

            source_country = ' '
        else:
            source_country = country_names[country]

            main_title = source_language + ' Top CCC articles list "'+list_dict_inv[list_name]+'" related to '+source_country+' and its coverage by '+target_language+' Wikipedia'
            source_country = '('+source_country+')'

        results_text = '''
        The following table shows the Top 500 articles list '''+list_dict_inv[list_name] + ''' from '''+source_language+''' CCC '''+source_country+''' and its article availability in '''+target_language+''' Wikipedia. The columns present complementary features that are explicative of the article relevance (number of editors, edits, pageviews, Bytes, Wikidata properties or Interwiki links). In particular, number of Inlinks from CCC (incoming links from the CCC group of articles) highlights the article importance in terms of how much it is required by other articles. The column named Related Languages present Interwiki links to the article version when available in the four languages closer to the target language (those that cover best this language and therefore it is likely their editors consult it).

        The table's last column shows the article title in its target language, in ***blue*** when it exists, in ***red*** as a proposal generated with the Wikimedia Content Translation tool or as an existing Wikidata label in the same language, and ***empty*** when the article does not exist or there is no title proposal available. This column is *updated once per day* with the new articles created.
        '''    

        if len(df) == 0: # there are no results.

            # PAGE NO RESULTS

            layout = html.Div([
                navbar,
                html.H3('Top CCC articles lists', style={'textAlign':'center'}),
                html.Br(),
                dcc.Markdown(
                    text_default.replace('  ', '')),

                # html.Div([
                #     dcc.Markdown(text_default2.replace('  ', '')),
                #     ],
                #     style={'font-size': 12}),

                html.Br(),

                html.H5('Select the parameters'),

                interface_row1,


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='none',
                    placeholder="Select a source language",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_country',
#                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a source country (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='list',
                    options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                    value='none',
                    placeholder="Select a list",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
                #        dcc.Link('Query',href=""),

                html.Br(),

                interface_row2,

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='exclude',
                    options=[{'label': i, 'value': covered[i]} for i in sorted(covered)],
                    value='none',
                    placeholder="Exclude (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                html.A(html.Button('Query Results!'),
                    href=''),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Br(),

                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                footbar,

            ], className="container")

            return layout


        closest_languages = closest_langs[target_lang]
        page_titles_target = ['pt0','pt1','pt2','pt3']
        generation_method_target = ['pg0','pg1','pg2','pg3']
        cl = len(closest_languages)

        # Excel file
        filename = 'top_ccc_articles_'+source_lang+'_'+list_name+'_'+country
        workbook = xlsxwriter.Workbook('/srv/wcdo/src_viz/downloads/top_ccc_lists/'+filename+'.xlsx')
        worksheet = workbook.add_worksheet(target_language)
        columns_excel = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P']
        for pos_ex in range(0,len(columns)):
            col_ex = columns_excel[pos_ex]
            label = columns[pos_ex]
            worksheet.write(col_ex+'1', label)
        worksheet.set_column(1, 1, 30)
        len_columns = len(columns)-1
        worksheet.set_column(len_columns, len_columns, 30)
        worksheet.set_column(len_columns-2, len_columns-2, 10)

        k = 0
        # print (df.columns)

        df=df.rename(columns=columns_dict)



        # group labels
        if list_name in ('sexual_orientation', 'ethnic_group', 'religious_group'):

            qitem_labels_target_lang = group_labels.loc[(group_labels["lang"] == target_lang) & (group_labels["group_label"] == list_name)][['qitem','label','lang']]
            qitem_labels_en = group_labels.loc[(group_labels["lang"] == "en") & (group_labels["group_label"] == list_name)][['qitem','label','lang']]

            qitem_labels_en = qitem_labels_en.set_index('qitem')
            qitem_labels_target_lang = qitem_labels_target_lang.set_index('qitem')

            qitem_labels_en = qitem_labels_en['label'].str.replace('_',' ')
            qitem_labels_target_lang = qitem_labels_target_lang['label'].str.replace('_',' ')

            # print (len(qitems_labels))
            # print (qitems_labels.head(10))

        tl = target_lang.replace('_','-')

        df_list = list()

        df_list_wt = list()
        for index, rows in df.iterrows():
            df_row = list()
            df_row_wt = list()

            for col in columns:

                l = columns_excel[columns.index(col)]
                pos = l+str(k)

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))
                    df_row_wt.append(str(k))

                elif col == 'Featured Article': 
                    fa = rows['Featured Article']
                    if fa == 0:
                        df_row.append('No')
                        df_row_wt.append('No')
                        worksheet.write(pos, u'No')

                    else:
                        df_row.append('Yes')
                        df_row_wt.append('Yes')
                        worksheet.write(pos, u'Yes')

                elif col == 'Interwiki Links':
                    df_row.append(html.A( rows['Interwiki Links'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[wikidata:'+rows['qitem']+'|'+str(rows['Interwiki Links'])+']]')
                    worksheet.write_url(pos, 'https://www.wikidata.org/wiki/'+str(rows['qitem']), string=str(rows['Interwiki Links']))

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_')+'|'+str(rows['Inlinks'])+']]')
                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), string=str(rows['Inlinks']))

                elif col == 'Inlinks from CCC':
                    df_row.append(html.A( rows['Inlinks from CCC'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_')+'|'+str(rows['Inlinks from CCC'])+']]')
                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), string=str(rows['Inlinks from CCC']))


                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':'+rows['Article Title'].replace(' ','_')+'|'+str(rows['Editors'])+']]')

                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', string=str(rows['Editors']))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                    df_row_wt.append('[[:'+source_lang+':'+rows['Article Title'].replace(' ','_')+'|'+str(rows['Edits'])+']]')
                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', string=str(rows['Edits']))

                elif col == 'Discussions':
                    df_row.append(html.A( rows['Discussions'], href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':'+rows['Article Title'].replace(' ','_')+'|'+str(rows['Discussions'])+']]')

                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), string=str(rows['Discussions']))


                elif col == 'Wikirank':
                    df_row.append(html.A( rows['Wikirank'], href='https://wikirank.net/'+source_lang+'/'+rows['Article Title'], target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append(str(rows['Wikirank']))
                    worksheet.write_url(pos, 'https://wikirank.net/'+source_lang+'/'+rows['Article Title'], string=str(rows['Wikirank']))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+source_lang+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append(str( int(rows['Pageviews'])))
                    worksheet.write_url(pos, 'https://tools.wmflabs.org/pageviews/?project='+source_lang+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', string=str(rows['Pageviews']))


                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[wikidata:'+rows['qitem']+'|'+str(rows['Wikidata Properties'])+']]')
                    worksheet.write_url(pos, 'https://www.wikidata.org/wiki/'+str(rows['qitem']), string=str(rows['Wikidata Properties']))

                elif col == 'LGBT+' or col == 'Ethnic Group' or col == 'Religious Group':

                    if col == 'LGBT+':
                        qit = str(rows['sexual_orientation'])
                    elif col == 'Religious Group':
                        qit = str(rows['religious_group'])
                    elif col == 'Ethnic Group':
                        qit = str(rows['ethnic_group'])

                    if ';' in qit:
                        qlist = qit.split(';')
                    else:
                        qlist = [qit]

                    c = len(qlist)

                    text = ''
                    text_ex = ''
                    text_wt = ''

                    i = 0
                    for ql in qlist:
                        i+= 1
                        try:
                            label = qitem_labels_target_lang.loc[ql]
                            text+= '['+label+']'+'('+'http://'+target_lang+'.wikipedia.org/wiki/'+ label.replace(' ','_')+')'
                            text_wt+= '[[:'+tl+':|'+label.replace(' ','_')+']]'
                        except:                            
                            try:
                                label = qitem_labels_en.loc[ql]
                                text+= '['+label+' (en)'+']'+'('+'http://en.wikipedia.org/wiki/'+ label.replace(' ','_')+')'
                                text_wt+= '[[:'+tl+':|'+label.replace(' ','_')+']]'

                            except:
                                label = ql
                                text+= '['+label+']'+'('+'https://www.wikidata.org/wiki/'+ label+')'
                                text_wt+= '[[wikidata:'+label+'|'+label+']]'

                        if i<c:
                            text+=', '
                            text_ex+=', '
                            text_wt+=', '

                    df_row.append(dcc.Markdown(text))
                    df_row_wt.append(text_wt)
                    worksheet.write(pos, text_ex)

                elif col == 'Related Languages':
                    i = 0
                    text = ''
                    text_ex = ''
                    text_wt = ''

                    for x in range(cl):
                        cur_generation_method = rows[generation_method_target[x]]
                        if cur_generation_method != 'sitelinks': continue
                        cur_title = rows[page_titles_target[x]]
                        try:
                            cur_title = cur_title.decode('utf-8')
                        except:
                            pass

                        if cur_title!= 0:
                            if i!=0 and i!=cl:
                                text+=', '
                                text_ex+=', '
                                text_wt+=', '

                            text+= '['+closest_languages[x]+']'+'('+'http://'+closest_languages[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'
                            text_wt+= '[[:'+closest_languages[x]+':'+cur_title.replace(' ','_')+'|'+closest_languages[x]+']]'

                            #+'{:target="_blank"}'
                            text_ex+= closest_languages[x]
                            i+=1


                    df_row.append(dcc.Markdown(text))
                    df_row_wt.append(text_wt)
                    worksheet.write(pos, text_ex)

                elif col == 'Bytes':
#                    print (rows[col])
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')
                    df_row_wt.append('[[:'+source_lang+':'+rows['Article Title'].replace(' ','_')+'|'+str(value)+'k]]')
                    worksheet.write(pos, str(value)+'k')

                elif col == 'Images':
                    title = rows['Article Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':'+title+'|'+str(rows[col])+']]')
                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), string=str(rows['Images']))


                elif col == 'Creation Date':
                    date = rows[col]
                    if date == 0: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)
                    df_row_wt.append(date)
                    worksheet.write(pos, date)

                elif col == 'Article Title':
                    title = rows['Article Title']
                    df_row.append(html.A(title.replace('_',' '), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    df_row_wt.append('[[:'+source_lang+':'+title+'|'+title.replace('_',' ')+']]')
                    worksheet.write_url(pos, 'https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), string=title)

                elif col == ' Article Title':
                    cur_title = rows[' Article Title']
                    if cur_title != 0:
                        try:
                            cur_title = cur_title.decode('utf-8')
                        except:
                            pass
                        cur_title = cur_title.replace('_',' ')
                        if rows['generation_method'] == 'sitelinks':
                            df_row.append(html.A(cur_title, href='https://'+target_lang+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                            df_row_wt.append('[[:'+tl+':'+cur_title+'|'+cur_title+']]')
                            worksheet.write_url(pos, 'https://'+target_lang+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), string=cur_title)
                        else:
                            df_row.append(html.A(cur_title+' ('+rows['generation_method']+')',href='https://'+target_lang+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none',"color":"#ba0000"}))
                            df_row_wt.append('[[:'+tl+':'+cur_title+'|'+cur_title+']]')

                    else:
                        df_row.append('')
                        df_row_wt.append('')


                else:
                    df_row.append(rows[col])
                    # print (col)
                    # print (rows[col])
                    df_row_wt.append(rows[col])
                    worksheet.write(pos,rows[col])

            df_list.append(df_row)

            # print (df_row_wt)
            # print (len(df_row_wt))
            df_list_wt.append(df_row_wt)


        workbook.close()

        col_len = len(columns)
        columns[1]=source_language+' '+columns[1]
        columns[col_len-1]=target_language+columns[col_len-1]


        df1 = pd.DataFrame(df_list_wt)
        df1.columns = columns
        todelete = ['Nº','IL CCC']
        if 'Wikidata Properties' in columns: todelete.append('WD.P.')
        df1=df1.rename(columns=columns_dict_abbr)
        df1=df1.drop(columns=todelete)



        # CREATING THE WIKITEXT TABLE
        # WIKITEXT
        df_columns_list = df1.columns.values.tolist()
        df_rows = df1.values.tolist()

        class_header_string = '{| border="1" cellpadding="2" cellspacing="0" style="width:100%; background: #f9f9f9; border: 1px solid #aaaaaa; border-collapse: collapse; white-space: nowrap; text-align: right" class="sortable"\n'

        dict_data_type = {}
        header_string = '!'
        for x in range(0,len(df_columns_list)):
            if x == len(df_columns_list)-1: add = ''
            else: add = '!!'
            data_type = ''
            if df_columns_list[x] in dict_data_type: data_type = ' '+dict_data_type[df_columns_list[x]]
            header_string = header_string + data_type + df_columns_list[x] + add

        header_string = header_string + '\n'

        rows = ''
        for row in df_rows:
            midline = '|-\n'
            row_string = '|'
            for x in range(0,len(row)):
                if x == len(row)-1: add = ''
                else: add = '||'
                value = row[x]
                row_string = row_string + str(value) + add # here is the value

                # here we might add colors.
            row_string = midline + row_string + '\n'
            rows = rows + row_string
        closer_string = '|}'

        wikitext = '* '+'Diversity Observatory table:'+main_title+'. Generated at '+datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S')+'\n'
        wikitext += 'Legend: Edtrs (number of editors), PV (number of page views), Bytes (number of Bytes), Refs. (number of References), IW.L. (number of interwiki links), Created (date of the first edit) and Rel. Lang. (interwiki link to relevant related languages).\n\n'

        wiki_table_string = class_header_string + header_string + rows + closer_string
        wikitext += wiki_table_string

        f = open('/srv/wcdo/src_viz/downloads/top_ccc_lists/'+filename+'.txt','w')
        f.write(str(wikitext))
        f.close()
        ###


        ## LAYOUT
        # RESULTS PAGE
        layout = html.Div([
            navbar,
            html.H3('Top CCC articles lists', style={'textAlign':'center'}),
            dcc.Markdown(
                text_default.replace('  ', '')),


            html.Br(),
            html.H5('Select the parameters'),

            interface_row1,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a source language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_country',
#                options=[{'label': i, 'value': countries_sel[i]} for i in sorted(countries_sel)],
                value='none',
                placeholder="Select a source country (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Br(),

            interface_row2,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='exclude',
                options=[{'label': i, 'value': covered[i]} for i in sorted(covered)],
                value='none',
                placeholder="Exclude (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(results_text.replace('  ', '')),

            html.Div(

            html.P([
            html.P('Download Table ', style= {'font-weight':'bold'}),

            html.A('Wikitext',
                id='download-link-txt',
                download=filename+".txt",
                href='/downloads/top_ccc_lists/'+filename+'.txt',
                target="_blank"),                

            '  ',

            html.A('Excel',
                id='download-link-Excel',
                download=filename+".xlsx",
                href='/downloads/top_ccc_lists/'+filename+'.xlsx',
                target="_blank"),                

            ]), style={'display': 'inline-block', 'float': 'right', 'text-align':'right','width': '300px'}),

            html.Br(),
            html.Br(),
            html.Br(),
            html.H6(main_title, style={'textAlign':'center'}),

            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(df_row[x]) for x in range(len(columns))
            ]) for df_row in df_list]),

            footbar,

        ], className="container")

    else:

        # FIRST PAGE
        layout = html.Div([
            navbar,
            html.H3('Top CCC Diversity Lists', style={'textAlign':'center'}),
            dcc.Markdown(
                text_default.replace('  ', '')),


            html.Br(),

            html.H5('Select the parameters'),

            interface_row1,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_country',
#                options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                value='none',
                placeholder="Select a country (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),


            html.Br(),

            interface_row2,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='exclude',
                options=[{'label': i, 'value': covered[i]} for i in sorted(covered)],
                value='none',
                placeholder="Exclude (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            html.A(html.Button('Query Results!'),
                href=''),
            style={'display': 'inline-block','width': '200px'}),


            footbar,

        ], className="container")

    return layout



@dash_app7.callback(
    Output('source_country', 'options'),
    [Input('source_lang', 'value')])
def set_countries(source_lang):

    try:
        countries_sel = language_countries[source_lang]
    except:
        countries_sel = {}

    countries_list = [{'label': i, 'value': countries_sel[i]} for i in sorted(countries_sel)]
    if countries_list != None:
        return countries_list
    else:
        return


# callback update URL
component_ids_app7 = ['list','source_lang','source_country','target_lang','order_by','exclude']
@dash_app7.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app7])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids_app7, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app7.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app7_build_layout(state)


@dash_app7.callback(
    Output("collapse", "is_open"),
    [Input("collapse-button", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open