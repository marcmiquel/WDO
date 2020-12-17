import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app33 = Dash(server = app, url_base_pathname = webtype + '/common_ccc_articles/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

# dash_app33.config.supress_callback_exceptions = True
dash_app33.config['suppress_callback_exceptions']=True


dash_app33.title = 'Common CCC'+title_addenda
dash_app33.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])



source_langs_dict = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    source_langs_dict[lang_name] = languagecode

topic_dict={'All':'all','Keywords':'keywords','Geolocated':'geolocated','People':'people','Women':'women','Men':'men','Folk':'folk','Earth':'earth','Monuments and Buildings':'monuments_and_buildings','Music Creations and Organizations':'music_creations_and_organizations','Sports and Teams':'sport_and_teams','Food':'food','Paintings':'paintings','GLAM':'glam','Books':'books','Clothing and Fashion':'clothing_and_fashion','Industry':'industry'}

target_langs_dict = language_names

features_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Creation Date':'date_created','Inlinks from CCC':'num_inlinks_from_CCC','Outlinks to CCC':'num_outlinks_to_CCC'}

features_dict_inv= {v: k for k, v in features_dict.items()}


ccc_all_dict = {'CCC in all source languages':'all','CCC in first source language':'first'}

show_gaps_dict = {'No language gaps':'no-gaps','At least one gap':'one-gap-min','All gaps':'all-gaps'}

columns_dict = {'num':'Nº','source_langs':'Source Langs.', 'sl_page_title':'Title (SL1)', 'tl_page_title':'Title (TL1)','target_langs':'Target Langs.','qitem':'Qitem'}
columns_dict.update(features_dict_inv)


full_text = '''In this page, you can consult a list of articles that are related to more than one language CCC. In other words, it allows you to retrieve articles that could belong to more than one cultural context.'''


results_text = '''
The following table shows the resulting list of articles common to the source languages’ CCC, and its availability in the target languages.

The column Source Langs. provides links to the article version in each of the selected source languages. The next column shows the title in the first source language (SL1). The next columns (editors, edits, pageviews, interwiki, creation date) show the value for some features in the first source language. If the content is ordered by another feature, this is added as an extra column. The next column shows the title in the first target language (TL1). The column Target Langs. provides links to the article version in each of the selected target languages. Finally, the Qitem column provides the id and a link to the Wikidata corresponding page.

The results provided are only an approximation to common local content and may contain unrelated articles. For the best results, set the CCC requirement to 'CCC in all source languages'.
'''





interface_row1 = html.Div([
    html.Div(
    [
    html.P(
        [
            "Source ",
            html.Span(
                "Languages' CCC",
                id="tooltip-target-srclangccc",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select the Languages you want to find articles in common in their local content or CCC. The first language selected will set the reference CCC, and the other languages will be used to filter the resulting list of articles depending on how related they are to it.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-srclangccc",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '400px'},
    ),


    html.Div(
    [
    html.P(
        [
            "Target ",
            html.Span(
                "Languages",
                id="tooltip-target-targetlangs",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select the target Wikipedia language editions in which you want to check whether the resulting list of articles exist or not.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-targetlangs",
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
            html.Span(
                "Topic",
                id="tooltip-target-topic",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a Topic to filter the resulting articles to those geolocated, biographies, GLAM, among others.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-topic",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    html.Div(
    [
    html.P(
        [
            "CCC ",
            html.Span(
                "requirement",
                id="tooltip-target-ccc",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Check the CCC requirement parameter to enforce that resulting articles need also to be part of the other source languages’ CCC in addition to the first language. It is highly recommended for better results, although it is a limitation to the number of results.",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-ccc",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div(
    [
    html.P(
        [
            "% Outlinks to ",
            html.Span(
                "CCC",
                id="tooltip-target-outlinks",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a % Outlinks to CCC to set the criterion for which articles are selected: the minimal percentage of their outgoing links to their CCC. Articles with a lower percentage of Outlinks to CCC are filtered out of the results. Thus, the higher the percentage the more the easier they’d be part of their CCC (by default it is set to 20%).",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-outlinks",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])




interface_row3 = html.Div([



    html.Div(
    [
    html.P(
        [
            "Order by ",
            html.Span(
                "feature",
                id="tooltip-target-feat",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a feature to sort the results (by default it uses the number of pageviews).",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-feat",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),


    html.Div(
    [
    html.P(
        [
            "Show the ",
            html.Span(
                "gaps",
                id="tooltip-target-exclude",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select Show the gaps to limit the results to only the articles that are missing in the target languages (All Gaps), that are missing in at least one language (At least one gap) or that are not missing (No language gaps).",
        style={"width": "42rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-exclude",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
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
            "Choose a number of results (by default 100)",
        style={"width": "auto", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])






def dash_app33_build_layout(params):
   
    # http://127.0.0.1:8050/common_ccc_articles/?source_langs=ca,pt,es&target_langs=en,it,fr&ccc_all=1&percent_outlinks_ccc=20&topic=geolocated&order_by=none&limit=100&show_gaps=none

    if len(params)!=0 and params['target_langs'].lower()!='none' and params['source_langs'].lower()!='none':
   
        conn = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cur = conn.cursor()

        # SOURCE lANGUAGE
        source_langs = params['source_langs'].lower()
        source_langs = source_langs.split(',')
        source_lang1 = languages.loc[source_langs[0]]['languagename']

        # TARGET LANGUAGES
        target_langs = params['target_langs'].lower()
        target_langs = target_langs.split(',')
        target_lang1 = languages.loc[target_langs[0]]['languagename']

        # CONTENT
        if 'ccc_all' in params:
            ccc_all = params['ccc_all']
        else:
            ccc_all = 'none'

        if 'percent_outlinks_ccc' in params:
            percent_outlinks_ccc = params['percent_outlinks_ccc']
        else:
            percent_outlinks_ccc = '20'

        if 'topic' in params:
            topic = params['topic']
        else:
            topic = 'none'

        # FILTER
        if 'order_by' in params:
            order_by = params['order_by']
        else:
            order_by = 'none'

        if 'limit' in params:
            limit = params['limit']
        else:
            limit = 100

        try:
            show_gaps = params['show_gaps']
        except:
            show_gaps = 'none'


        # CREATING THE QUERY FROM THE PARAMS
        query = 'SELECT '

        query += 'r.qitem, '

#        query += 'REPLACE(r.page_title,"_"," ") as r.page_title, '
        query += 'r.page_title as sl_page_title, '

        sl_titles = ''
        i=1
        for sl in source_langs:
            if sl == source_langs[0]: continue
            sl_titles += 'o' + str(i) + '.page_title as '+' o' + str(i) +'page_title,'
            i+=1
        query += sl_titles

        query += 'r.num_editors, r.num_edits, r.num_pageviews, r.num_interwiki, r.num_bytes, r.date_created,'

        columns = ['num','source_langs','sl_page_title','num_editors','num_edits','num_pageviews','num_interwiki','num_bytes','date_created']


        if order_by in ['num_outlinks','num_inlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','num_references']: 
            query += 'r.'+order_by+', '
            columns = columns + [order_by]

        percent = 'r.percent_outlinks_to_CCC as rpercent_outlinks_to_CCC'
        i=1
        for sl in source_langs:
            if sl == source_langs[0]: continue
            percent += ', o' + str(i) + '.percent_outlinks_to_CCC as o' + str(i) + 'percent_outlinks_to_CCC'
            i+=1
        query += percent

        percent_outlinks = []
        i=1
        for sl in source_langs:
            if sl == source_langs[0]: continue
            percent_outlinks.append('o' + str(i) + 'percent_outlinks_to_CCC')
            i+=1

        query += ', p0.page_title as tl_page_title'
        columns = columns + ['tl_page_title']

        tl_titles = ''
        i=1
        for sl in target_langs[1:len(target_langs)]:
            tl_titles += ', p' + str(i) + '.page_title as p' + str(i) + 'page_title'
            i+=1
        query += tl_titles


        query += ' FROM '+source_langs[0]+'wiki r '

        i=1
        for sl in source_langs:
            if sl == source_langs[0]: continue
            query += 'INNER JOIN '+source_langs[i]+ 'wiki '+'o' + str(i) +' ON r.qitem ='+' o' + str(i) +'.qitem '
            i+=1


        i=0
        for sl in target_langs:
            query += 'LEFT JOIN '+target_langs[i]+ 'wiki '+'p' + str(i) +' USING (qitem) '
            i+=1

        query += 'WHERE r.ccc_binary = 1 '


        if ccc_all == 'all':
            i=1
            for sl in source_langs:
                if sl == source_langs[0]: continue
                query += 'AND o' + str(i) +'.ccc_binary = 1 '
                i+=1

        if percent_outlinks_ccc != 'none':
            percent_outlinks_ccc = str(float(percent_outlinks_ccc)/100)
        else:
            percent_outlinks_ccc = str(0.20)

        query += 'AND r.percent_outlinks_to_CCC >= '+percent_outlinks_ccc+' '

        i=1
        for sl in source_langs:
            if sl == source_langs[0]: continue
            query += 'AND o' + str(i) +'.percent_outlinks_to_CCC >= '+percent_outlinks_ccc+' '
            i+=1

        if topic != "none" and topic != "None" and topic != "all":
            if topic == 'keywords':
                query += 'AND r.keyword_title IS NOT NULL '
            elif topic == 'geolocated':
                query += 'AND (r.geocoordinates IS NOT NULL OR r.location_wd IS NOT NULL) '
            elif topic == 'men': # male
                query += 'AND r.gender = "Q6581097" '
            elif topic == 'women': # female
                query += 'AND r.gender = "Q6581072" '
            elif topic == 'people':
                query += 'AND r.gender IS NOT NULL '
            else:
                query += 'AND r.'+topic+' IS NOT NULL '


        # case 1: show_gaps exist in all languages
        # case 2: show_gaps exist in at least one language
        # case 3: show_gaps not exist in all languages

        # if show_gaps == 'one-exist-min': # show_gaps els que no existeixen en cap llengua

        #     # condició: que en alguna llengua hagi d'existir
        #     query += 'AND ('
        #     i=0
        #     q = ''
        #     for sl in target_langs:
        #         if i!=0: q+= ' OR'
        #         q += ' p' + str(i) +'.page_title IS NOT NULL'
        #         i+=1
        #     query += q + ') '

        if show_gaps == 'no-gaps': # show_gaps els que no existeixen a totes les llengües

            # condició: força tots a que existeixin.
            i=0
            for sl in target_langs:
                query += 'AND p' + str(i) +'.page_title IS NOT NULL '
                i+=1


        elif show_gaps == 'one-gap-min':

            # condició: que en alguna llengua no hagi d'existir
            query += 'AND ('
            i=0
            q = ''
            for sl in target_langs:
                if i!=0: q+= ' OR'
                q += ' p' + str(i) +'.page_title IS NULL'
                i+=1
            query += q + ') '



        elif show_gaps == 'all-gaps': # show_gaps existing in no language

            # condició: força que cap existeixi.
            i=0
            for sl in target_langs:
                query += 'AND p' + str(i) +'.page_title IS NULL '
                i+=1


        if order_by == "none" or order_by == "None":
#            pass
            query += 'ORDER BY r.num_pageviews DESC '

        elif order_by in ['num_outlinks','num_wdproperty','num_discussions','num_inlinks_from_CCC','num_outlinks_to_CCC','percent_outlinks_to_CCC','percent_inlinks_from_CCC','num_references']: 
            query += 'ORDER BY r.'+order_by+' DESC '

#        query += 'LIMIT 10000'
        # if limit == "none":
        #     query += 'LIMIT 50;'
        # else:
        #     query += 'LIMIT '+str(limit)+';'

        columns = columns + ['target_langs','qitem']

#        print (query)

        df = pd.read_sql_query(query, conn)#, parameters)
        df = df.fillna(0)

        if limit == 'none':
            df = df.head(100)
        else:
            df = df.head(int(limit))


        df['avg_percent_outlinks_to_CCC'] = df[percent_outlinks].mean(axis=1)
        df = df.sort_values(by='avg_percent_outlinks_to_CCC', ascending=False)

        if order_by == "none" or order_by == "None":
            df = df.sort_values(by='num_pageviews', ascending=False)
        else:
            df = df.sort_values(by=order_by, ascending=False)


#        print (df.head(50))

        for x in range(0,len(columns)):
            try:
                columns[x]=columns_dict[columns[x]]
            except:
                columns[x]=columns[x]
#        print(columns)

#        print(df.columns)

#        df['average_1_3'] = df[['salary_1', 'salary_3']].mean(axis=1)



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:
            layout = html.Div([
                navbar,

                html.H3('Common CCC Articles', style={'textAlign':'center'}),
                html.Br(),

                dcc.Markdown(full_text.replace('  ', '')),
                html.Br(),

                html.H5('Select the Languages'),

                interface_row1,


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_langs',
                    options=[{'label': i, 'value': source_langs_dict[i]} for i in sorted(source_langs_dict)],
                    value='none',
                    multi=True,
                    placeholder="Select languages",
                    style={'width': '390px'}
                 ), style={'display': 'inline-block','width': '400px'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='target_langs',
                    options=[{'label': i, 'value': target_langs_dict[i]} for i in sorted(target_langs_dict)],
                    value=['en'],
                    multi=True,
                    placeholder="Select languages",           
                    style={'width': '380px'}
                 ), style={'display': 'inline-block','width': '390px'}),


                html.Br(),


                html.H5('Filter content'),

                interface_row2,

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='topic',
                    options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                    value='none',
                    placeholder="Select a topic (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='ccc_all',
                    options=[{'label': i, 'value': ccc_all_dict[i]} for i in sorted(ccc_all_dict, reverse=True)],
                    value='all',
                    placeholder="Set requirement (optional)",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='percent_outlinks_ccc',                    
                    placeholder='Enter a value (optional)',
                    type='text',
                    value='20',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),

                interface_row3,

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
                    id='show_gaps',
                    options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                    value='none',
                    placeholder="Show the gaps",           
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

                html.Hr(),
                html.H5('Results'),
                dcc.Markdown(results_text.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately there are no common articles for these languages and these parameters.'),

                footbar,

            ], className="container")


            return layout



        # PAGE CASE: RESULTS
        df=df.rename(columns=columns_dict)
        df_list = list()
        k = 0

#        print(df.columns.tolist())


        for index, rows in df.iterrows():            
            df_row = list()
            for col in columns:

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))


                elif col == 'Source Langs.':
                    source_langs_titles = []
                    source_langs_titles.append(rows['Title (SL1)'])
                    for i in range(1,len(source_langs)):
                        source_langs_titles.append(rows['o' + str(i) + 'page_title'])

                    text = ''
                    for x in range(0,len(source_langs_titles)):
                        cur_title = source_langs_titles[x]
                        if cur_title!= 0:
                            if i!=0 and x!=0:
                                text+=', '

                            text+= '['+source_langs[x]+']'+'('+'http://'+source_langs[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'
                    df_row.append(dcc.Markdown(text))




                elif col == 'Title (SL1)':
                    title = rows['Title (SL1)']
                    df_row.append(html.A(title.replace('_',' '), href='https://'+source_langs[0]+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))




                elif col == 'Editors':
                    title = rows['Title (SL1)']
                    df_row.append(html.A( rows['Editors'], href='https://'+source_langs[0]+'.wikipedia.org/w/index.php?title='+title.replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+source_langs[0]+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Title (SL1)'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')



                elif col == 'Creation Date':
                    date = rows['Creation Date']
                    if date == 0: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)


                # FOR THE ORDER BY
#                features_dict = {'Editors':'num_editors','Edits':'num_edits','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Inlinks from CCC':'num_inlinks_from_CCC','Outlinks to CCC':'num_outlinks_to_CCC'}

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+source_langs[0]+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title (SL1)'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks from CCC':
                    df_row.append(html.A( rows['Inlinks from CCC'], href='https://'+source_langs[0]+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title (SL1)'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+source_langs[0]+'.wikipedia.org/w/index.php?title='+rows['Title (SL1)'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Target Langs.':

#                    print (target_langs)

                    target_langs_titles = []

                    target_langs_titles.append(rows['Title (TL1)'])

                    for i in range(1,len(target_langs)):
                        target_langs_titles.append(rows['p' + str(i) + 'page_title'])

                    text = ''
                    for x in range(0,len(target_langs)):
                        cur_title = target_langs_titles[x]
                        if cur_title!= 0:
                            if text!='': text+=', '

                            text+= '['+target_langs[x]+']'+'('+'http://'+target_langs[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'

 #                    print (target_langs_titles)
                    df_row.append(dcc.Markdown(text))


                elif col == 'Title (TL1)':
                    title = rows['Title (TL1)']
                    if title == 0: title = ''
                    df_row.append(html.A(title.replace('_',' '), href='https://'+target_langs[0]+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                else:
                    df_row.append(rows[col])

            df_list.append(df_row)

#        print (df.head())
        col_len = len(columns)
        columns[2]=source_lang1+' '+columns[2]
        columns[col_len-3]=target_lang1+' '+columns[col_len-3]



        df1 = pd.DataFrame(df_list)
        df1.columns = columns


        layout = html.Div([
            navbar,
            html.H3('Common CCC Articles', style={'textAlign':'center'}),

            dcc.Markdown(full_text.replace('  ', '')),
            html.Br(),

            # here there is the interface
            html.H5('Select the Languages'),

            interface_row1,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_langs',
                options=[{'label': i, 'value': source_langs_dict[i]} for i in sorted(source_langs_dict)],
                value='none',
                multi=True,
                placeholder="Select languages",
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': target_langs_dict[i]} for i in sorted(target_langs_dict)],
                value=['en'],
                multi=True,
                placeholder="Select languages",           
                style={'width': '380px'}
             ), style={'display': 'inline-block','width': '390px'}),

            html.Br(),
            html.H5('Filter content'),

            interface_row2,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='topic',
                options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                value='none',
                placeholder="Select a topic (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='ccc_all',
                options=[{'label': i, 'value': ccc_all_dict[i]} for i in sorted(ccc_all_dict, reverse=True)],
                value='all',
                placeholder="Set requirement (optional)",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='percent_outlinks_ccc',                    
                placeholder='Enter a value (optional)',
                type='text',
                value='20',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            interface_row3,

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
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show the gaps",           
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


    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            navbar,
            html.H3('Common CCC Articles', style={'textAlign':'center'}),
            dcc.Markdown(full_text.replace('  ', '')),
            html.Br(),

            # here there is the interface
            html.H5('Select the Languages'),

            interface_row1,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_langs',
                options=[{'label': i, 'value': source_langs_dict[i]} for i in sorted(source_langs_dict)],
                value='none',
                multi=True,
                placeholder="Select languages",
                style={'width': '390px'}
             ), style={'display': 'inline-block','width': '400px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='target_langs',
                options=[{'label': i, 'value': target_langs_dict[i]} for i in sorted(target_langs_dict)],
                value=['en'],
                multi=True,
                placeholder="Select languages",           
                style={'width': '380px'}
             ), style={'display': 'inline-block','width': '390px'}),


            html.Br(),


            html.H5('Filter content'),

            interface_row2,

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='topic',
                options=[{'label': i, 'value': topic_dict[i]} for i in sorted(topic_dict)],
                value='none',
                placeholder="Select a topic (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='ccc_all',
                options=[{'label': i, 'value': ccc_all_dict[i]} for i in sorted(ccc_all_dict, reverse=True)],
                value='all',
                placeholder="Set requirement (optional)",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='percent_outlinks_ccc',                    
                placeholder='Enter a value (optional)',
                type='text',
                value='20',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),

            interface_row3,

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
                id='show_gaps',
                options=[{'label': i, 'value': show_gaps_dict[i]} for i in sorted(show_gaps_dict)],
                value='none',
                placeholder="Show the gaps",           
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

            footbar,

        ], className="container")

    return layout


# callback update URL
component_ids_app33 = ['source_langs','target_langs','ccc_all','percent_outlinks_ccc','topic','order_by','show_gaps','limit']
@dash_app33.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app33])
def update_url_state(*values):
    # print ('ah')
    # print (type(values[1]))
    # print (values[1])
    # print ('eh')
    if not isinstance(values[0], str):
        values = ','.join(values[0]),values[1],values[2],values[3],values[4],values[5],values[6],values[7]

    if not isinstance(values[1], str):
        values = values[0],','.join(values[1]),values[2],values[3],values[4],values[5],values[6],values[7]

    state = urlencode(dict(zip(component_ids_app33, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app33.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app33_build_layout(state)


