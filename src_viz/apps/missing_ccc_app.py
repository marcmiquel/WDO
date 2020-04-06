import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app28 = Dash(__name__, server = app, url_base_pathname = webtype + '/missing_ccc_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app28.config['suppress_callback_exceptions']=True

#dash_app28.config.supress_callback_exceptions = True

dash_app28.title = 'Missing CCC'+title_addenda
dash_app28.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])

#dash_app28.config['suppress_callback_exceptions']=True

source_lang_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    source_lang_dict[lang_name] = languagecode

source_lang_list = list(sorted(source_lang_dict))
source_lang_list.insert(0,'No territorial coexistance')
source_lang_list.insert(0,'Territorial coexistance')
source_lang_list.insert(0,'All languages')

source_lang_dict['Territorial coexistance'] = 'coexist'
source_lang_dict['No territorial coexistance'] = 'nocoexist'
source_lang_dict['All languages'] = 'all'


missing_incomplete_dict = {'Incomplete articles':'incomplete','Missing articles':'missing'}

topic_dict={'All':'all','People':'people','Women':'women','Men':'men','Folk':'folk','Earth':'earth','Monuments and Buildings':'monuments_and_buildings','Music Creations and Organizations':'music_creations_and_organizations','Sports and Teams':'sport_and_teams','Food':'food','Paintings':'paintings','GLAM':'glam','Books':'books','Clothing and Fashion':'clothing_and_fashion','Industry':'industry'}

ccc_segment_dict={'All':'all','Keywords':'keywords','Geolocated':'geolocated'}

target_lang_dict = language_names

features_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Inlinks from CCC':'num_inlinks_from_original_CCC','Outlinks to CCC':'num_outlinks_to_original_CCC','Bytes Target Lang.':'num_bytes_original_lang'}

features_dict_inv= {v: k for k, v in features_dict.items()}

columns_dict = {'num':'Nº','source_lang':'Language', 'page_title':'Title', 'page_title_original_lang': 'Title Target Lang.', 'qitem':'Qitem', 'labellang':'Lang Label','Bytes Target Lang.':'num_bytes_original_lang'}
columns_dict.update(features_dict_inv)


def dash_app28_build_layout(params):
    if len(params)!=0 and params['target_lang'].lower()!='none':
   
        filename = ''
        conn = sqlite3.connect(databases_path + 'missing_ccc.db'); cur = conn.cursor()


        # TARGET LANGUAGE
        target_lang=params['target_lang'].lower()
        target_language = languages.loc[target_lang]['languagename']

        if 'target_country' in params:
            target_country = params['target_country'].upper()
            if target_country == 'NONE' or target_country == 'ALL': target_country = 'all'
        else:
            target_country = 'all'

        if 'target_region' in params:
            target_region = params['target_region'].upper()
            if target_region == 'NONE' or target_region == 'ALL': target_region = 'all'
        else:
            target_region = 'all'


        # TOPIC
        if 'type' in params:
            type_ = params['type'] # incomplete or missing
            if type_ == "None":
                type_ = "missing"
        else:
            type_ = "missing"


        if 'ccc_segment' in params:
            ccc_segment = params['ccc_segment']
        else:
            ccc_segment = 'none'

        if 'topic' in params:
            topic = params['topic']
        else:
            topic = 'none'


        # SOURCE lANGUAGE
        if 'source_lang' in params:
            source_lang=params['source_lang'].lower() #
        else:
            source_lang= 'none'

        if 'order_by' in params:
            order_by = params['order_by']
        else:
            order_by = 'none'

        if 'limit' in params:
            limit = params['limit']
        else:
            limit = 'none'


        # CREATING THE QUERY
        query = 'SELECT '


        if type_ == 'missing' or type_ == "none":
            columns = ['num','source_lang','page_title','num_editors','num_pageviews','num_interwiki','num_bytes']

            query += 'languagecode as source_lang, REPLACE(page_title,"_"," ") as page_title, '
            query += 'num_editors, num_pageviews, num_interwiki, num_bytes, '
            if order_by in ['num_edits','num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_original_CCC','num_outlinks_to_original_CCC','num_references']: 
                query += order_by+', '
                columns = columns + [order_by]

            query += '("label_lang" || " " || "label") AS labellang, qitem '
            columns = columns + ['labellang','qitem']

            query += 'FROM '+target_lang+'wiki '
            query += 'WHERE page_title_original_lang IS NULL '

        elif type_ == 'incomplete':
            columns = ['num','source_lang','page_title','page_title_original_lang','num_bytes','num_bytes_original_lang','num_editors','num_pageviews','num_inlinks']

            query += 'languagecode as source_lang, REPLACE(page_title,"_"," ") as page_title, REPLACE(page_title_original_lang,"_"," ") as page_title_original_lang, '
            query += 'num_bytes, num_bytes_original_lang, num_editors, num_pageviews, num_inlinks, '
            if order_by in ['num_edits','num_outlinks','num_interwiki','num_wdproperty','num_discussions','num_inlinks_from_original_CCC','num_outlinks_to_original_CCC','num_references']: 
                query += order_by+', '
                columns = columns + [order_by]
            query += 'qitem '
            columns = columns + ['qitem']

            query += 'FROM '+target_lang+'wiki '
            query += 'WHERE num_bytes > num_bytes_original_lang '

        if ccc_segment == 'keywords':
            query += 'AND keyword_title IS NOT NULL '

        if ccc_segment == 'geolocated':
            query += 'AND (geocoordinates IS NOT NULL OR location_wd IS NOT NULL) '

        if target_country != "none" and target_country != "all":
            query += 'AND iso3166 = "'+target_country+'" '

        if target_region != "none" and target_region != "all":
            query += 'AND iso31662 = "'+target_region+'" '


        if topic != "none" and topic != "None" and topic != "all":
            if topic == 'men': # male
                query += 'AND gender = "Q6581097" '

            elif topic == 'women': # female
                query += 'AND gender = "Q6581072" '

            elif topic == 'people':
                query += 'AND gender IS NOT NULL '

            else:
                query += 'AND '+topic+' IS NOT NULL '
    
        if source_lang == 'coexist':
            query += 'AND non_language_pairs IS NULL '

        elif source_lang == 'nocoexist':
            query += 'AND non_language_pairs == 1 '

        elif source_lang != "none" and source_lang != "all":
            query += 'AND languagecode = "'+source_lang+'" '

        query += 'AND (num_inlinks_from_original_CCC!=0 OR num_outlinks_to_original_CCC!=0) '

        if order_by == "none" or order_by == "None":
            query += 'ORDER BY num_pageviews DESC '
        else:
            query += 'ORDER BY '+order_by+' DESC '

        if limit == "none":
            query += 'LIMIT 100;'
        else:
            query += 'LIMIT '+str(limit)+';'

#        print(query)
        df = pd.read_sql_query(query, conn)#, parameters)
        df = df.fillna(0)

#        print(columns)

        for x in range(0,len(columns)):
            try:
                columns[x]=columns_dict[columns[x]]
            except:
                columns[x]=columns[x]

#        print(columns)


        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            text = '''
            In this page, you can consult a list of articles that could and might need to exist or be extended in a language CCC (i.e. part of their local content), and instead, they only exist in other Wikipedia language editions or they are longer (more Bytes).

            It is possible to query any list by changing the URL parameters or by using the following menus. You first need to select the *Target language* (where you would like to improve local content representation). 
            Additionally, if you want to aim at specific part of a language context, you can select the *target country* and *Target region* - they are optional and allow you to filter for a specific area. For instance, for target language French, whose language context encompasses several countries, Target country and Target region could be France and Québec.

            It is also possible to filter the content according to several purposes. *Type of gap* allows you decide whether you want articles ‘missing’ in the target language and existing in the source languages or simply less complete (by default the type of gap is in missing mode), *CCC segment* allows you to select some specific part of the CCC, and *Topic* allows you to select a among the following topics (all, people, women, men, folk, earth, monuments and buildings, music creations and organizations, sports and teams, food, paintings, GLAM, books, clothing and fashion and industry).

            Finally, it is necessary to select the *Source language(s)* from which you want to obtain articles. It is possible to select all languages (this is the default), those with which the source language coexists in the same territority (i.e. Welsh coexists with English), those with which the source language does not coexists with, and a specific language from all the Wikipedia language editions. The last two optional parameters are *Order by feature* and *Limit the results*, which allow you to sort the results according to a specific feature and limit the entries to a specific number (by default 100).

            There are specific combinations of parameters that usually give relevant selections of articles. By order of importance, topics/CCC segments such as geolocated articles, men, women, monuments and buildings, GLAM and Earth, present valuable articles when results are ordered by features interwiki links, incoming links from CCC, bytes, edits in talk page, pageviews and references. Topics such as food, music, paintings and sports may also give interesting results when the choosen feature to sort the results is pageviews.

            This tool is in Alpha version - you may find bugs. In this case, please e-mail us at tools.wcdo@tools.wmflabs.org.
            '''


            layout = html.Div([
                html.H3('Missing CCC Articles', style={'textAlign':'center'}),

                html.H5('Unfortunately there are not articles proposed for the local content for this language. Try another combination of parameters.'),

                html.Br(),

                dcc.Markdown(
                    text.replace('  ', '')),

                html.H5('Select the Wikipedia'),

                html.Div(
                html.P('Target language'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Target country'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Target region'),
                style={'display': 'inline-block','width': '200px'}),


                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_country',
#                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a country (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_region',
                    options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                    value='none',
                    placeholder="Select a region (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),




                html.H5('Filter by content'),

                html.Div(
                html.P('Search type'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('CCC segment'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Topic'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='type',
                    options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict, reverse=True)],
                    value='none',
                    placeholder="Type of gap",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_segment',
                    options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                    value='none',
                    placeholder="Select a CCC segment (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),


                html.H5('Choose the source of content'),

                html.Div(
                html.P('Source language(s)'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Order by feature'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Limit the results'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': source_lang_dict[i]} for i in source_lang_list],
                    value='none',
                    placeholder="Source language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),




                html.A(html.Button('Query Results!'),
                    href=''),
            ], className="container")

            return layout


        # # PREPARE THE DATA

        df=df.rename(columns=columns_dict)
        df_list = list()
        k = 0
        for index, rows in df.iterrows():            
            df_row = list()
            for col in columns:

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))


                elif col == 'Language':
                    df_row.append(html.A(rows['Language'], href='https://'+rows['Language']+'.wikipedia.org/wiki/', target="_blank", title = languages.loc[rows['Language']]['languagename'], style={'text-decoration':'none'}))

                elif col == 'Title':
                    title = rows['Title']
                    df_row.append(html.A(title.replace('_',' '), href='https://'+rows['Language']+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'WDProperties':
                    df_row.append(html.A( rows['WDProperties'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Bytes Target Lang.':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')


                elif col == 'Discussions':
                    title = rows['Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+rows['Language']+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+rows['Language']+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+rows['Language']+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))



                elif col == 'Lang Label':
                    label = rows['Lang Label']
                    if label == 0: label = ''
                    df_row.append(label)

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                else:
                    df_row.append(rows[col])

            df_list.append(df_row)

#        print (df.head())

        df1 = pd.DataFrame(df_list)
        df1.columns = columns


        if type_ == 'incomplete': typ = 'Incomplete'
        if type_ == 'missing' or type_ == "none": typ = 'Missing'
        title = typ+' CCC Articles in '+target_language+' Wikipedia'

        if ccc_segment.lower() != "none": 
            title = title + ' on CCC segment '+ccc_segment
            if topic != "none": title = title + ' and'
        if topic.lower() != "none": 
            title = title + ' on '+topic

        dash_app28.title = title+title_addenda

        # LAYOUT
        col_len = len(columns)
        # columns[1]=language_origin+' '+columns[1]
        # columns[col_len-1]=language_target+columns[col_len-1]

        order_by = order_by.lower()
        if order_by != 'none':
            order = features_dict_inv[order_by]
        else:
            order = 'pageviews'

        # RESULTS PAGE
        if type_ == 'missing': typ = 'that might need to exist in '
        if type_ == 'incomplete' or type == "none": typ = 'that might be more complete in '

        text = '''
        The following table shows the first '''+limit+''' articles '''+typ+''' '''+target_language+''' Wikipedia local content (CCC). 

        Articles are sorted by '''+order+''' - if the parameter *Order by feature* is not used, the default is pageviews. The rest of columns present complementary features that are explicative of the article relevance (editors, Bytes or Interwiki links). The column named *Lang Label* shows for each article the Wikidata related Qitem Label in the target language when it exists, otherwise in English or the source language. Finally, the last column *Qitem* provides a link to the Wikidata item.

            There are specific combinations of parameters that usually give relevant selections of articles. By order of importance, topics/CCC segments such as geolocated articles, men, women, monuments and buildings, GLAM and Earth, present valuable articles when results are ordered by features interwiki links, incoming links from CCC, bytes, edits in talk page, pageviews and references. Topics such as food, music, paintings and sports may also give interesting results when the choosen feature to sort the results is pageviews.

            This tool is in Alpha version - you may find bugs. In this case, please e-mail us at tools.wcdo@tools.wmflabs.org.

        '''


        countries_sel = language_countries[target_lang]

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', '')),
#            html.Br(),

            html.Div(
            html.A(
                '',
                id='download-link',
                download=filename+".xlsx",
                href='/downloads/missing_ccc_lists/'+filename+'.xlsx',
                target="_blank"),
                style={'float': 'right','width': '200px'}),


                html.H5('Select the Wikipedia'),

                html.Div(
                html.P('Target language'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Target country'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Target region'),
                style={'display': 'inline-block','width': '200px'}),


                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_country',
#                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a country (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_region',
#                    options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                    value='none',
                    placeholder="Select a region (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),




                html.H5('Filter by content'),

                html.Div(
                html.P('Search type'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('CCC segment'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Topic'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='type',
                    options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict, reverse=True)],
                    value='none',
                    placeholder="Type of gap",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_segment',
                    options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                    value='none',
                    placeholder="Select a CCC segment (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),


                html.H5('Choose the source of content'),

                html.Div(
                html.P('Source language(s)'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Order by feature'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Limit the results'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': source_lang_dict[i]} for i in source_lang_list],
                    value='none',
                    placeholder="Source language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


            html.A(html.Button('Query Results!'),
                href=''),

            html.Br(),
            html.Br(),

            html.H5('Results'),

            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(df_row[x]) for x in range(len(columns))
            ]) for df_row in df_list])

        ], className="container")


    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.

        text = '''
        In this page, you can consult a list of articles that could and might need to exist or be extended in a language CCC (i.e. part of their local content), and instead, they only exist in other Wikipedia language editions or they are longer (more Bytes).

        It is possible to query any list by changing the URL parameters or by using the following menus. You first need to select the *Target language* (where you would like to improve local content representation). 
        Additionally, if you want to aim at specific part of a language context, you can select the *target country* and *Target region* - they are optional and allow you to filter for a specific area. For instance, for target language French, whose language context encompasses several countries, Target country and Target region could be France and Québec.

        It is also possible to filter the content according to several purposes. *Type of gap* allows you decide whether you want articles ‘missing’ in the target language and existing in the source languages or simply less complete (by default the type of gap is in missing mode), *CCC segment* allows you to select some specific part of the CCC, and *Topic* allows you to select a among the following topics (all, people, women, men, folk, earth, monuments and buildings, music creations and organizations, sports and teams, food, paintings, GLAM, books, clothing and fashion and industry).

        Finally, it is necessary to select the *Source language(s)* from which you want to obtain articles. It is possible to select all languages (this is the default), those with which the source language coexists in the same territority (i.e. Welsh coexists with English), those with which the source language does not coexists with, and a specific language from all the Wikipedia language editions. The last two optional parameters are *Order by feature* and *Limit the results*, which allow you to sort the results according to a specific feature and limit the entries to a specific number (by default 100).

        There are specific combinations of parameters that usually give relevant selections of articles. By order of importance, topics/CCC segments such as geolocated articles, men, women, monuments and buildings, GLAM and Earth, present valuable articles when results are ordered by features interwiki links, incoming links from CCC, bytes, edits in talk page, pageviews and references. Topics such as food, music, paintings and sports may also give interesting results when the choosen feature to sort the results is pageviews.

        '''



        layout = html.Div([
            html.H3('Missing CCC Articles', style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', '')),

#            html.Br(),

            html.H5('Select the Wikipedia'),

            html.Div(
            html.P('Target language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Target country'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Target region'),
            style={'display': 'inline-block','width': '200px'}),


            html.Br(),
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_lang',
                options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                value='none',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_country',
#                options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                value='none',
                placeholder="Select a country (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_region',
#                options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                value='none',
                placeholder="Select a region (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),




            html.H5('Filter by content'),

            html.Div(
            html.P('Type of gap'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('CCC segment'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Topic'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='type',
                options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict, reverse=True)],
                value='none',
                placeholder="Type of gap",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='ccc_segment',
                options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                value='none',
                placeholder="Select a CCC segment (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='topic',
                options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                value='none',
                placeholder="Select a topic (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            html.H5('Choose the source of content'),

            html.Div(
            html.P('Source language(s)'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Order by feature'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Limit the results'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang',
                options=[{'label': i, 'value': source_lang_dict[i]} for i in source_lang_list],
                value='none',
                placeholder="Source language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='100',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.A(html.Button('Query Results!'),
                href=''),
        ], className="container")

    return layout



@dash_app28.callback(
    Output('target_country', 'options'),
    [Input('target_lang', 'value')])
def set_countries(target_lang):

    try:
        countries_sel = language_countries[target_lang]
    except:
        countries_sel = {}

    countries_list = [{'label': i, 'value': countries_sel[i]} for i in sorted(countries_sel)]

    # print (target_lang)
    # print (countries_list)
    if countries_list != None:
        return countries_list
    else:
        return


@dash_app28.callback(
    Output('target_region', 'options'),
    [Input('target_lang', 'value'),Input('target_country', 'value')])
def set_region(target_lang,target_country):
    
    if target_lang == "none": return
    subdivisions_sel = {}
    subdivisions_sel = subd[target_lang]

    if target_country != "none" and target_country != "none":
        for x,y in subdivisions_sel.copy().items():
            ct = str(y.split('-')[0])
#            print (ct)
            if ct != target_country.strip():
                del subdivisions_sel[x]

#    print (subdivisions_sel)
    subdivisions_list = [{'label': i, 'value': subdivisions_sel[i]} for i in sorted(subdivisions_sel)]
    return subdivisions_list
    # if subdivisions_list != None:
    #     return subdivisions_list
    # else:
    #     return


# callback update URL
component_ids_app28 = ['target_lang','target_country','target_region','type','ccc_segment','topic','source_lang','limit','order_by']
@dash_app28.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app28])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids_app28, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app28.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app28_build_layout(state)