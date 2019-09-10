import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app28 = Dash(__name__, server = app, url_base_pathname = webtype + '/missing_ccc_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app28.config.supress_callback_exceptions = True

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
source_lang_dict['Territorial coexistance'] = 'coexist'
source_lang_dict['No territorial coexistance'] = 'nocoexist'

missing_incomplete_dict = {'Missing articles':'missing','Incomplete articles':'incomplete'}

topic_dict={'All':'all','People':'people','Women':'women','Men':'men','Folk':'folk','Earth':'earth','Monuments and Buildings':'monuments_and_buildings','Music Creations and Organizations':'music_creations_and_organizations','Sports and Teams':'sport_and_teams','Food':'food','Paintings':'paintings','GLAM':'glam','Books':'books','Clothing and Fashion':'clothing_and_fashion','Industry':'industry'}

ccc_segment_dict={'All':'all','Keywords':'keywords','Geolocated':'geolocated'}

target_lang_dict = language_names

features_dict = {'Number of Editors':'num_editors','Number of Edits':'num_edits','Number of Pageviews':'num_pageviews','Number of Inlinks':'num_inlinks','Number of References':'num_references','Number of Bytes':'num_bytes','Number of Outlinks':'num_outlinks','Number of Interwiki':'num_interwiki','Number of WDProperties':'num_wdproperty','Number of Discussions':'num_discussions','Number of Inlinks from CCC':'num_inlinks_from_original_CCC','Number of Outlinks to CCC':'num_outlinks_to_original_CCC'}

features_dict_inv= {v: k for k, v in features_dict.items()}

columns_dict = {'num':'Nº','source_lang':'Language', 'page_title':'Title', 'page_title_original_lang': 'Title Target Lang.', 'qitem':'Qitem', 'labellang':'Label (Lang)','Number of Bytes Target Lang.':'num_bytes_original_lang'}
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
        type = params['type'] # incomplete or missing
        if type == "None":
            type = "missing"

        ccc_segment = params['ccc_segment']

        topic = params['topic']


        # SOURCE lANGUAGE
        source_lang=params['source_lang'].lower() #

        order_by = params['order_by']

        limit = params['limit']


        # CREATING THE QUERY
        query = 'SELECT '


        if type == 'missing' or type == "none":
            columns = ['num','source_lang','page_title','num_editors','num_pageviews','num_interwiki','num_bytes']

            query += 'languagecode as source_lang, REPLACE(page_title,"_"," ") as page_title, '
            query += 'num_editors, num_pageviews, num_interwiki, num_bytes, '
            if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_original_CCC','num_outlinks_to_original_CCC','num_references']: 
                query += order_by+', '
                columns = columns + [order_by]

            query += '("label_lang" || " " || "label") AS labellang, qitem '
            columns = columns + ['labellang','qitem']

            query += 'FROM '+target_lang+'wiki '
            query += 'WHERE page_title_original_lang IS NULL '

        elif type == 'incomplete':
            columns = ['num','source_lang','page_title','page_title_original_lang','num_bytes','num_bytes_original_lang','num_editors','num_pageviews','num_inlinks']

            query += 'languagecode as source_lang, REPLACE(page_title,"_"," ") as page_title, REPLACE(page_title_original_lang,"_"," ") as page_title_original_lang, '
            query += 'num_bytes, num_bytes_original_lang, num_editors, num_pageviews, num_inlinks, '
            if order_by in ['num_outlinks','num_interwiki','num_wdproperty','num_discussions','num_inlinks_from_original_CCC','num_outlinks_to_original_CCC','num_references']: 
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

        elif source_lang != "none":
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

        print(query)
        df = pd.read_sql_query(query, conn)#, parameters)
        df = df.fillna(0)

        for x in range(0,len(columns)):
            try:
                columns[x]=columns_dict[columns[x]]
            except:
                columns[x]=columns[x]

#        print(columns)


        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:
            text = ''
            # text = '''In this page, you can consult the Missing CCC articles lists... 
            # It is possible to query any list by changing the URL parameters. You need to specify.....'''

            layout = html.Div([
                html.H3('Missing CCC Articles', style={'textAlign':'center'}),

                html.H5('Unfortunately there are not articles proposed for the local content for this language. Try another combination of parameters.'),

                html.Br(),

                dcc.Markdown(
                    text.replace('  ', '')),


                html.H5('Select the Wikipedia'),

                html.Div(
                html.P('Target Language'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Country (Optional)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Region (Optional)'),
                style={'display': 'inline-block','width': '200'}),


                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_country',
                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a country (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_region',
                    options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                    value='none',
                    placeholder="Select a region (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),




                html.H5('Filter by content'),

                html.Div(
                html.P('Search type'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('CCC Segment'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Topic'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='type',
                    options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict)],
                    value='none',
                    placeholder="Type of gap",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_segment',
                    options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                    value='none',
                    placeholder="Select a CCC Segment",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),


                html.H5('Choose source of content'),

                html.Div(
                html.P('Source language(s)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Order by feature'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Limit the results'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': source_lang_dict[i]} for i in sorted(source_lang_dict)],
                    value='none',
                    placeholder="Source language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),




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

                elif col == 'Number of Interwiki':
                    df_row.append(html.A( rows['Number of Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Number of WDProperties':
                    df_row.append(html.A( rows['Number of WDProperties'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Number of Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Number of Discussions':
                    title = rows['Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+title+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Label (Lang)':
                    label = rows['Label (Lang)']
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


        # if country == 'all':
        #     title = 'Top 500 CCC articles list "'+list_dict_inv[list_name] + '" from '+language_origin+' CCC in '+language_target+' Wikipedia'
        #     country_origin = ' '
        # else:
        #     country_origin = country_names[country]
        #     title = 'Top 500 CCC articles list "'+list_dict_inv[list_name]+'" from '+country_origin+' ('+language_origin+' CCC) in '+language_target+' Wikipedia'
        #     country_origin = '('+country_origin+')'
    #    dash_app287.title = title+title_addenda


        # LAYOUT
        col_len = len(columns)
        # columns[1]=language_origin+' '+columns[1]
        # columns[col_len-1]=language_target+columns[col_len-1]

        title = 'Missing CCC Articles'
        text = ''
        # text = '''
        # The following table shows the Missing CCC articles list for '''+target_language+''' CCC '''+target_country+''' and its article availability in '''+target_language+''' Wikipedia.
        # '''

        countries_sel = language_countries[target_lang]

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', '')),

    #        html.Br(),
           html.Div(
            html.A(
                '',
                id='download-link',
                download=filename+".xlsx",
                href='/downloads/missing_ccc_lists/'+filename+'.xlsx',
                target="_blank"),
                style={'float': 'right','width': '200'}),


                html.H5('Select the Wikipedia'),

                html.Div(
                html.P('Target Language'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Country (Optional)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Region (Optional)'),
                style={'display': 'inline-block','width': '200'}),


                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_country',
                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a country (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_region',
                    options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                    value='none',
                    placeholder="Select a region (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),




                html.H5('Filter by content'),

                html.Div(
                html.P('Search Type'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('CCC Segment'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Topic'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='type',
                    options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict)],
                    value='none',
                    placeholder="Type of gap",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_segment',
                    options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                    value='none',
                    placeholder="Select a CCC Segment",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),


                html.H5('Choose source of content'),

                html.Div(
                html.P('Source language(s)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Order by feature'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Limit the results'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': source_lang_dict[i]} for i in sorted(source_lang_dict)],
                    value='none',
                    placeholder="Source language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


            html.A(html.Button('Query Results!'),
                href=''),

            html.Br(),
            html.Br(),
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

        text = ''
        # text = '''
        # In this page, you can consult .

        # '''    

        layout = html.Div([
            html.H3('Missing CCC Articles', style={'textAlign':'center'}),
            dcc.Markdown(text.replace('  ', '')),#,containerProps={'textAlign':'center'}),

    #            html.Br(),

                html.H5('Select the Wikipedia'),

                html.Div(
                html.P('Target Language'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Country (Optional)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Target Region (Optional)'),
                style={'display': 'inline-block','width': '200'}),


                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_lang',
                    options=[{'label': i, 'value': target_lang_dict[i]} for i in sorted(target_lang_dict)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_country',
                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a country (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_region',
                    options=[{'label': i, 'value': subdivisions_ISO31662_dict[i]} for i in sorted(subdivisions_ISO31662_dict)],
                    value='none',
                    placeholder="Select a region (optional)",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),




                html.H5('Filter by content'),

                html.Div(
                html.P('Type of gap'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('CCC Segment'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Topic'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='type',
                    options=[{'label': i, 'value': missing_incomplete_dict[i]} for i in sorted(missing_incomplete_dict)],
                    value='none',
                    placeholder="Type of gap",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_segment',
                    options=[{'label': i, 'value': ccc_segment_dict[i]} for i in sorted(ccc_segment_dict)],
                    value='none',
                    placeholder="Select a CCC Segment",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Br(),

                html.H5('Choose source of content'),

                html.Div(
                html.P('Source language(s)'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Order by feature'),
                style={'display': 'inline-block','width': '200'}),

                html.Div(
                html.P('Limit the results'),
                style={'display': 'inline-block','width': '200'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang',
                    options=[{'label': i, 'value': source_lang_dict[i]} for i in sorted(source_lang_dict)],
                    value='none',
                    placeholder="Source language",
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by",           
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '190'}
                 ), style={'display': 'inline-block','width': '200'}),


            html.A(html.Button('Query Results!'),
                href=''),
        ], className="container")

    return layout


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