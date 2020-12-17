import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *




### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

eg = group_labels.loc[(group_labels["lang"] == 'en') & (group_labels["category_label"] == 'ethnic_groups')][['qitem','label','page_title']]

ethnic_groups_dict = {}
for index, rows in eg.iterrows():
    qitem = rows['qitem']
    label = rows['label']
    page_title = rows['page_title']

    if page_title != '':
        value = page_title
    elif label != '':
        value = label
    else:
        continue
    ethnic_groups_dict[value+' ('+qitem+')'] = qitem



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app20 = Dash(__name__, server = app, url_base_pathname = webtype + '/ethnic_groups_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app20.config['suppress_callback_exceptions']=True


dash_app20.title = 'Ethnic Groups Articles'+title_addenda

dash_app20.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])

features_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Inlinks from CCC':'num_inlinks_from_CCC','Outlinks to CCC':'num_outlinks_to_CCC', 'Date Created':'date_created', 'Images':'num_images', 'Start Time':'start_time', 'End Time':'end_time'}

features_dict_inv = {v: k for k, v in features_dict.items()}

ethnic_groups_dict_inv = {v: k for k, v in ethnic_groups_dict.items()}


content_dict = {'All kinds of topics':'alltopics', 'Biographies':'people', 'No biographies':'not_people', 'Men':'men', 'Women':'women', 'CCC':'ccc_binary'}

show_gaps_dict = {'No language gaps':'no-gaps','At least one gap':'one-gap-min','Only gaps':'only-gaps'}


results_text = '''
The following table shows the results from the query. The columns present the title...
'''


results_text = '''
The following table shows the resulting list of articles about a specific Ethnic Group, and its availability in the target languages. 

The Qitem column provides the id and a link to the Wikidata corresponding page. The Source Lang. provides the language where this item has been identified as being related to this ethnic group (either people, geographical entity, etc.). The column Title provides the title in the source language. 
The next columns (editors, edits, pageviews, interwiki, creation date) show the value for some features in the first source language. If the content is ordered by another feature, this is added as an extra column. The column Target Langs. provides links to the article version in each of the selected target languages. The last column shows the title in the first target language.
'''    



text_default = '''In this page you can retrieve articles of ethnic groups from Wikipedia language editions, including biographies and all kinds of topics, and check its availability in a specific Wikipedia.'''    


interface_row1 = html.Div([


    html.Div(
    [
    html.P(
        [
            "Target ",
            html.Span(
                "languages",
                id="tooltip-target-language",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select the target Wikipedia language editions in which you want to check whether the resulting list of articles exist or not.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-language",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '440px'},
    ),


    html.Div([
    html.P(
        [
            "Select ",
            html.Span(
                "ethnic group",
                id="tooltip-target-country",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select an ethnic group to retrieve articles.",
        style={"width": "24rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-country",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '280px'},
    ),



    html.Div(
    [
    html.P(
        [
            "Select ",
            html.Span(
                "content type",
                id="tooltip-target-content",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    ],
    style={'display': 'inline-block','width': '250px'},
    ),

    dbc.Tooltip(
        html.Div(
            'Select a Topic to filter the resulting articles to biographies, men or women, no biographies or CCC.',
        style={"width": "43rem", 'font-size': 12, 'text-align':'left','backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-content",
        placement="bottom",
        style={'color':'black','backgroundColor':'transparent'},
    ),

    ])




interface_row2 = html.Div([
    html.Div(
    [
    html.P(
        [
            "Order by ",
            html.Span(
                "feature",
                id="tooltip-target-order_by",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature and use it to sort the list of articles.",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-order_by",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '220px'},
    ),



    html.Div(
    [
    html.P(
        [
            "Show the ",
            html.Span(
                "gaps",
                id="tooltip-target-show_gaps",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select Show the gaps to limit the results to only the articles that are missing in the target languages (Only Gaps), that are missing in at least one language (At least one gap) or that are not missing (No language gaps).",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-show_gaps",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '220px'},
    ),


    html.Div(
    [
    html.P(
        [
            "Limit the ",
            html.Span(
                "results",
                id="tooltip-target-limit",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Choose a number of results (by default 100, maximum 1000)",
        style={"width": "38rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '100px'},
    )

    ])



def dash_app20_build_layout(params):


    if len(params)!=0 and params['target_langs'].lower()!='none' and params['ethnic_group'].lower()!='none':
        
        functionstartTime = time.time()
        conn = sqlite3.connect(databases_path + 'ethnic_groups_content_production.db'); cur = conn.cursor()

        if 'ethnic_group' in params:
            ethnic_group = params['ethnic_group']
        else:
            ethnic_group = 'none'


        if 'content' in params:
            content = params['content']
            if content == 'none' or content == 'None': content = 'alltopics'
        else:
            content = 'alltopics'


        # TARGET LANGUAGES
        target_langs = params['target_langs'].lower()
        target_langs = target_langs.split(',')
        target_language = languages.loc[target_langs[0]]['languagename']


        if 'order_by' in params:
            order_by=params['order_by'].lower()
            if order_by not in features_dict_inv: order_by = 'num_editors'
        else:
            order_by='date_created'


        if 'limit' in params:
            limit = int(params['limit'])
            try:
                limit = int(limit)
                if limit == 0: limit = 500
                elif limit > 10000: limit = 10000
            except:
                limit = 500
        else:
            limit = 500

        
        try:
            show_gaps = params['show_gaps']
        except:
            show_gaps = 'none'

        # print (show_gaps)
        # print (content)


        # CREATING THE QUERY FROM THE PARAMS
        query = 'SELECT DISTINCT '
        query += 'qitem, '

#        query += 'REPLACE(page_title,"_"," ") as page_title, '
        query += 'primary_lang, page_id, page_title, '

        query += 'num_editors, num_edits, num_pageviews, num_interwiki, num_bytes, date_created, '
        columns = ['num','qitem','primary_lang','page_title','num_editors','num_edits','num_pageviews','num_interwiki','num_bytes','date_created']

        if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references']:
            query += order_by+', '
            columns = columns + [order_by]

        query += 'category_crawling_absolute_level'
        query += ' FROM ethnic_group_articles '
        query += 'WHERE ethnic_group_binary = 1 '
        query += 'AND qitem_ethnic_group = "'+ethnic_group+'" '

        if content != "none" and content != "None":
            if content == 'ccc_binary':
                query += 'AND ccc_binary = 1 '
            elif content == 'men': # male
                query += 'AND gender = 2 ' #"Q6581097"
            elif content == 'women': # female
                query += 'AND gender = 1 ' #"Q6581072" '
            elif content == 'people':
                query += 'AND gender IS NOT NULL '
            elif content == 'not_people':
                query += 'AND gender IS NULL '
            elif content == 'alltopics':
                query += ''


        if order_by == "none" or order_by == "None":
            query += 'ORDER BY num_editors ASC '

        elif order_by in ['num_outlinks','num_wdproperty','num_discussions','num_inlinks_from_group','num_outlinks_to_group','num_references','num_pageviews']: 
            query += 'ORDER BY '+order_by+' DESC '


        if limit == "none":
            query += 'LIMIT 500;'
        else:
            query += 'LIMIT 500;'
            # query += 'LIMIT '+str(limit)+';'

        columns = columns + ['target_langs']

        df = pd.read_sql_query(query, conn)#, parameters)


        # print (query)
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')
        # print (df.head(10))
        # print (len(df))
        # print ('abans page_titles')


        sourcelangs = df.primary_lang.unique()
        z = 0
        for srlang in sourcelangs:
            z+=1
            mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(srlang); mysql_cur_read = mysql_con_read.cursor()
            dfs = df.loc[(df['primary_lang'] == srlang)]
            dfs = wikilanguages_utils.get_interwikilinks_articles(srlang, target_langs, dfs, mysql_con_read)
            if z == 1: dfz = dfs
            dfz = dfz.append(dfs)
        df = dfz

        # print ('fi page_titles')
        # print (len(df))


        columns_dict = {'num':'Nº','page_title':'Title','target_langs':'Target Langs.','qitem':'Qitem', 'primary_lang':'Source Lang.'}
        columns_dict.update(features_dict_inv)



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([
                navbar,                
                html.H3('Ethnic Groups Topics Articles', style={'textAlign':'center'}),
                html.Br(),
                dcc.Markdown(text_default.replace('  ', '')),

                # HERE GOES THE INTERFACE
                # LINE 
                html.Br(),
                html.H5('Select the ethnic group'),

                interface_row1,
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_langs',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    placeholder="Select languages",
                    value=['es','ca','it','en'],
                    multi=True,
                    style={'width': '430px'}
                 ), style={'display': 'inline-block','width': '440px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ethnic_group',
                    options=[{'label': i, 'value': ethnic_groups_dict[i]} for i in ethnic_groups_dict],
                    value='none',
                    placeholder="Select the ethnic group",           
                    style={'width': '270px'}
                 ), style={'display': 'inline-block','width': '280px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='content',
                    options=[{'label': i, 'value': content_dict[i]} for i in content_dict],
                    value='people',
                    placeholder="Select the type of editors",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                # LINE 
                html.Br(),
                interface_row2,
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='order_by',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '210px'}
                 ), style={'display': 'inline-block','width': '220px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='show_gaps',
                    options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                    value='none',
                    placeholder="Show gaps (optional)",           
                    style={'width': '210px'}
                 ), style={'display': 'inline-block','width': '220px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='100',
                    style={'width': '90px'}
                 ), style={'display': 'inline-block','width': '100px'}),

                html.A(html.Button('Query Results!'),
                    href=''),

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



        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
        # # PREPARE THE DATA
        df=df.rename(columns=columns_dict)

        df = df.fillna(0)

        order_by = order_by.lower()
        if order_by != 'none':
            order = features_dict_inv[order_by]
        else:
            order = features_dict_inv['num_editors']

#        print (order)
        df = df.sort_values(order,ascending=False)


        # # PREPARE THE DATA
        df=df.rename(columns=columns_dict)
        # print (df.head(100))

        columns_ = []
        for x in columns:
            columns_.append(columns_dict[x])
        columns = columns_
        columns.append(target_language+' Title')
        # print (columns)



        df_list = list()
        k = 0
        z = 0
        for index, rows in df.iterrows():
            df_row = list()


            for col in columns:
                title = rows['Title']
                primary_lang = rows['Source Lang.']
                if not isinstance(title, str):
                    title = title.iloc[0]

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == 'Source Lang.':
                    if not isinstance(title, str):
                        title = title.iloc[0]
                    df_row.append(html.A(languages.loc[primary_lang]['languagename'], href='https://'+primary_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Title':
                    title = rows['Title']
                    if not isinstance(title, str):
                        title = title.iloc[0]
                    df_row.append(html.A(title.replace('_',' '), href='https://'+primary_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == target_language+' Title':

                    t_title = rows['page_title_1']
                    if isinstance(t_title, int): 
                        df_row.append('')

                    elif not isinstance(t_title, str):
                        t_title = t_title.iloc[0]

                    if isinstance(t_title, str):  
                        df_row.append(html.A(t_title.replace('_',' '), href='https://'+target_langs[0]+'.wikipedia.org/wiki/'+t_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Outlinks' or col == 'References' or col == 'Images':
                    title = rows['Title']
                    df_row.append(html.A( rows[col], href='https://'+target_langs[0]+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+primary_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+primary_lang+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+primary_lang+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits Last Month':
                    df_row.append(html.A( rows['Edits Last Month'], href='https://'+primary_lang+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    df_row.append(html.A( rows['Discussions'], href='https://'+primary_lang+'.wikipedia.org/wiki/Talk:'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikirank':
                    df_row.append(html.A( rows['Wikirank'], href='https://wikirank.net/'+primary_lang+'/'+rows['Title'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+primary_lang+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    title = rows['Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+primary_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Images':
                    df_row.append(str(rows[col]));

                elif col == 'Target Langs.':

                    z=0
                    target_langs_titles = []
                    for i in range(1,len(target_langs)+1):
                        if rows['page_title_'+str(i)] == '':
                            z+=1
                        target_langs_titles.append(rows['page_title_'+str(i)])
#                    print (target_langs_titles)

                    text = ''

                    for x in range(0,len(target_langs)):
                        cur_title = target_langs_titles[x]
                        if cur_title!= None and cur_title != '' and cur_title != 0:
                            if text!='': text+=', '

                            text+= '['+target_langs[x]+']'+'('+'http://'+target_langs[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'
    
                    df_row.append(dcc.Markdown(text))

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Date Created':
                    try:
                        timestamp = str(int(rows[col]))
                        df_row.append(datetime.datetime.strftime(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S'),'%Y-%m-%d'))
                    except:
                        df_row.append('')
                else:
                    df_row.append(rows[col])


            if show_gaps == 'one-gap-min' and z == 0: continue
            elif show_gaps == 'only-gaps' and z < len(target_langs_titles): continue
            elif show_gaps == 'no-gaps' and z > 0: continue

            if k <= limit:
                df_list.append(df_row)


        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')


        # RESULTS
        title = 'Ethnic Groups Topics Articles'
        df1 = pd.DataFrame(df_list)


        dash_app20.title = title+title_addenda

        # LAYOUT
        layout = html.Div([
            navbar,
           
            html.H3('Ethnic Groups Topics Articles', style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the ethnic group'),

            interface_row1,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                placeholder="Select languages",
                value=['es','ca','it','en'],
                multi=True,
                style={'width': '430px'}
             ), style={'display': 'inline-block','width': '440px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='ethnic_group',
                options=[{'label': i, 'value': ethnic_groups_dict[i]} for i in ethnic_groups_dict],
                value='none',
                placeholder="Select the ethnic group",           
                style={'width': '270px'}
             ), style={'display': 'inline-block','width': '280px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='content',
                options=[{'label': i, 'value': content_dict[i]} for i in content_dict],
                value='biographies',
                placeholder="Select the type of editors",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '210px'}
             ), style={'display': 'inline-block','width': '220px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show gaps (optional)",           
                style={'width': '210px'}
             ), style={'display': 'inline-block','width': '220px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='100',
                style={'width': '90px'}
             ), style={'display': 'inline-block','width': '100px'}),

            html.A(html.Button('Query Results!'),
                href=''),
           

            # here there is the table            
            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(results_text.replace('  ', '')),
            html.Br(),

            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(df_row[x]) for x in range(len(columns))
            ]) for df_row in df_list]),
            footbar,

        ], className="container")
#        print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' before printing')



    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            navbar,
            
            html.H3('Ethnic Groups Topics Articles', style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the ethnic group'),

            interface_row1,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                placeholder="Select languages",
                value=['es','ca','it'],
                multi=True,
                style={'width': '430px'}
             ), style={'display': 'inline-block','width': '440px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='ethnic_group',
                options=[{'label': i, 'value': ethnic_groups_dict[i]} for i in sorted(ethnic_groups_dict)],
                value='none',
                placeholder="Select the ethnic group",           
                style={'width': '270px'}
             ), style={'display': 'inline-block','width': '280px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='content',
                options=[{'label': i, 'value': content_dict[i]} for i in content_dict],
                value='biographies',
                placeholder="Select the type of editors",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='order_by',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '210px'}
             ), style={'display': 'inline-block','width': '220px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show gaps (optional)",           
                style={'width': '210px'}
             ), style={'display': 'inline-block','width': '220px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='100',
                style={'width': '90px'}
             ), style={'display': 'inline-block','width': '100px'}),

            html.A(html.Button('Query Results!'),
                href=''),
            footbar,      

        ], className="container")

    return layout



# callback update URL
component_ids_app20 = ['target_langs','ethnic_group','content','order_by','show_gaps','limit']
@dash_app20.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app20])
def update_url_state(*values):

    if not isinstance(values[0], str):
        values = ','.join(values[0]),values[1],values[2],values[3],values[4],values[5]

    state = urlencode(dict(zip(component_ids_app20, values)))
    return '?'+state



# callback update page layout
@dash_app20.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app20_build_layout(state)
