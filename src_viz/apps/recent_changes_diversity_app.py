import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



####### -------------------- This is the beginning of the App.

##### METHODS SPECIFIC #####

# Get me the recent created articles or recent edits (Filter: Bot, New)
def get_recent_articles_recent_edits(languagecode, edittypes, editortypess, periodhours, resultslimit):
    functionstartTime = time.time()
    print (languagecode, edittypes, editortypess, periodhours, resultslimit)

    def conditions(s):
        if (s['rc_bot'] == 1):
            return 'bot'
        elif (s['rev_actor'] == 'NULL'):
            return 'anonymous'
        else:
            return 'editor'


    query = 'SELECT CONVERT(rc_title USING utf8mb4) as page_title, rc_cur_id as page_id, CONVERT(rc_timestamp USING utf8mb4) as rc_timestamp, rc_bot, rev_actor, rc_new_len as Bytes, CONVERT(actor_name USING utf8mb4) as actor_name FROM recentchanges rc INNER JOIN revision rv ON rc.rc_timestamp = rv.rev_timestamp INNER JOIN actor a ON rv.rev_actor = a.actor_id WHERE rc_namespace = 0 '
#    query = 'SELECT CONVERT(rc_title USING utf8mb4) as page_title, rc_cur_id as page_id, rc_new, rc_type, rc_deleted, rc_bot, CONVERT(rc_timestamp USING utf8mb4) as rc_timestamp FROM recentchanges WHERE rc_namespace = 0 '

    if edittypes == 'new_articles': 
        query += 'AND rc_new = 1 '

    if edittypes == 'wikidata_edits': 
        query += 'AND rc_type = 5 '

    if editortypess == 'no_bots':
        query+= 'AND rc_bot = 0 '

    if editortypess == 'bots_edits':
        query+= 'AND rc_bot = 1 '

    if editortypess == 'anonymous_edits':
        query+= 'AND actor_user IS NULL '

    if editortypess == 'editors_edits':
        query+= 'AND rc_bot = 0 '
        query+= 'AND actor_user IS NOT NULL '


    timelimit = datetime.datetime.now() - datetime.timedelta(hours=int(periodhours))
    timelimit_string = datetime.datetime.strftime(timelimit,'%Y%m%d%H%M%S') 
    query+= 'AND rc_timestamp > "'+timelimit_string+'" '

    query+= 'ORDER BY rc_timestamp DESC'

    query+= ' LIMIT '+str(resultslimit)

    # print (query)
    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
    df = pd.read_sql(query, mysql_con_read);

    df['Editor Edit Type'] = df.apply(conditions, axis=1)
    df=df.drop(columns=['rev_actor','rc_bot'])

    # print (df.head(100))
    # print (len(df))
    # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')
    return df


# Get me the articles that are also in the wikipedia_diversity_production.db and the diversity categories it belongs to.
def get_articles_diversity_categories_wikipedia_diversity_db(languagecode, df_rc):

    conn = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cursor = conn.cursor()
    df_rc = df_rc.set_index('page_title')

    # df_rc = df_rc.head(10)
    # print (df_rc)
    # print (len(df_rc))

    page_titles = df_rc.index.tolist()
    page_asstring = ','.join( ['?'] * len(page_titles) )
    df_categories = pd.read_sql_query('SELECT page_title, qitem, iso3166, iso31662, region, gender, ethnic_group, sexual_orientation, ccc_binary, num_editors, num_pageviews, date_created, num_inlinks, num_outlinks, num_inlinks_from_CCC, num_outlinks_to_CCC, num_inlinks_from_women, num_outlinks_to_women, num_references, num_discussions, num_wdproperty, num_interwiki, num_images, wikirank from '+languagecode+'wiki WHERE page_title IN ('+page_asstring+');', conn, params = page_titles)

    df_categories = df_categories.set_index('page_title')
    df_rc_categories = df_rc.merge(df_categories, how='left', on='page_title')

    return df_rc_categories




def get_proportion_diversity_category(df_rc_categories, limit, language):

    iso3166 = df_rc_categories['iso3166'].value_counts()#.to_dict()
    iso31662 = df_rc_categories['iso31662'].value_counts()#.to_dict()
    region = df_rc_categories['region'].value_counts()#.to_dict()
    gender = df_rc_categories['gender'].value_counts()#.to_dict()
    ethnic_group = df_rc_categories['ethnic_group'].value_counts()#.to_dict()
    sexual_orientation = df_rc_categories['sexual_orientation'].value_counts()#.to_dict()
    ccc_binary = df_rc_categories['ccc_binary'].value_counts().iloc[1:]#.to_dict()

#    print (ccc_binary); input('')


    dfx = pd.DataFrame(iso3166).rename_axis('Id').rename(columns={'iso3166':'Number of edits'}).reset_index()
    dfx['Category'] = 'ISO3166'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = dfx

    dfx = pd.DataFrame(iso31662).rename_axis('Id').rename(columns={'iso31662':'Number of edits'}).reset_index()
    dfx['Category'] = 'ISO31662'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = pd.concat([dfy, dfx],sort=True)

    dfx = pd.DataFrame(region).rename_axis('Id').rename(columns={'region':'Number of edits'}).reset_index()
    dfx['Category'] = 'Continent'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = pd.concat([dfy, dfx],sort=True)

    dfx = pd.DataFrame(gender).rename_axis('Id').rename(columns={'gender':'Number of edits'}).reset_index()
    dfx['Category'] = 'Gender'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = pd.concat([dfy, dfx],sort=True)

    dfx = pd.DataFrame(ethnic_group).rename_axis('Id').rename(columns={'ethnic_group':'Number of edits'}).reset_index()
    dfx['Category'] = 'Ethnic Group'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = pd.concat([dfy, dfx],sort=True)

    dfx = pd.DataFrame(sexual_orientation).rename_axis('Id').rename(columns={'sexual_orientation':'Number of edits'}).reset_index()
    dfx['Category'] = 'Sexual Orientation'
    dfx['Percentage'] = round(100*dfx['Number of edits']/dfx['Number of edits'].sum(),2)
    dfy = pd.concat([dfy, dfx],sort=True)

    dfx = pd.DataFrame(ccc_binary).rename_axis('Id').rename(columns={'ccc_binary':'Number of edits'}).reset_index()
    dfx['Category'] = language+' CCC'
    dfx['Percentage'] = round(100*dfx['Number of edits']/limit,2)
    dfx['Id'] = language + ' CCC'
    dfy = pd.concat([dfy, dfx],sort=True)


    return dfy




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
#dash_app18 = Dash(__name__, server = app, url_base_pathname = webtype + '/search_ccc_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)
dash_app18 = Dash(__name__, server = app, url_base_pathname= webtype + '/recent_changes_diversity/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app18.config['suppress_callback_exceptions']=True
dash_app18.title = 'Recent Changes Diversity'+title_addenda
dash_app18.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),

])



features_dict = {'Edit Timestamp':'rc_timestamp','Editors': 'num_editors', 'Edits': 'num_edits', 'Images': 'num_images', 'Wikirank': 'wikirank', 'Pageviews': 'num_pageviews', 'Inlinks': 'num_inlinks', 'References': 'num_references', 'Current Length': 'rev_len', 'Outlinks': 'num_outlinks', 'Interwiki': 'num_interwiki', 'Wikidata Properties': 'num_wdproperty', 'Discussions': 'num_discussions', 'Creation Date': 'date_created', 'Inlinks from CCC': 'num_inlinks_from_ccc', 'Outlinks to CCC': 'num_outlinks_to_ccc', 'Inlinks from Women': 'num_inlinks_from_women', 'Outlinks to Women': 'num_outlinks_to_women', 'Featured Article': 'featured_article', 'ISO3166':'iso3166', 'ISO3166-2':'iso31662', 'Continent':'region', 'Gender':'gender', 'Ethnic Group':'ethnic_group', 'Sexual Orientation':'sexual_orientation', 'CCC':'ccc_binary', 'Edit Timestamp':'timestamp'}

features_dict_inv = {v: k for k, v in features_dict.items()}

edittypes_dict = {'New articles':'new_articles','All edits':'all_edits','Wikidata Edits':'wikidata_edits'}
editortypes_dict = {'Only registered editors':'editors_edits','Only anonymous':'anonymous_edits','Only bots':'bots_edits','No bots':'no_bots','All editors':'all_editors'}

edittypes_dict_inv = {v: k for k, v in edittypes_dict.items()}

editortypes_dict_inv = {v: k for k, v in editortypes_dict.items()}

diversitycategory_dict = {'Country Code (ISO3166)':'iso3166', 'Subdivision Code (ISO3166-2)':'iso31662', 'Continent':'region', 'Gender':'gender', 'Ethnic Group':'ethnic_group', 'Sexual Orientation':'sexual_orientation', 'CCC':'ccc_binary'}
diversitycategory_dict_inv = {v: k for k, v in diversitycategory_dict.items()}
diversitycategory_dict_inv.update({'all_topics':'All Topics'})



## ----------------------------------------------------------------------------------------------------- ##



text_default = '''In this page you can retrieve the list of Recent Changes in a Wikipedia language edition according with different categories relevant to diversity (e.g. Gender, Sexual Orientation, Ethnic Group, Cultural Context Content (CCC), Country, Country Subdivisions, Continents).'''    



text_results = '''
The following graph shows bars with the number of edits and the percentage for each of the categories relevant to diversity that were detected using the project's database. The colors represent the different topics for each category. 

The table shows the list of requested Recent changes edits. The columns present the article title, timestamp, editor, article creation date, current length after the edit, number of pageviews and number of Interwiki links. When a featured is selected to sort the results (order by), it is added as a column. The remaining columns are the mentioned diversity-related categories. 

Note: The categorization according to geographical categories is based on geocoordiantes. Since the categorization is based on the last database created, some features may not be up to date. This dashboard is in alpha phase.
'''    

text_results2 = '''
The following table shows the list of requested Recent changes edits. The columns present the article title, timestamp, editor, article creation date, current length after the edit, number of pageviews and number of Interwiki links. When a featured is selected to sort the results (order by), it is added as a column. The remaining columns are the mentioned diversity-related categories. 

Note: The categorization according to geographical categories is based on geocoordiantes. Since the categorization is based on the last database created, some features may not be up to date. This dashboard is in alpha phase.
'''    


## ----------------------------------------------------------------------------------------------------- ##


interface_row1 = html.Div([

    html.Div([
    html.P(
        [
            "Source ",
            html.Span(
                "language",
                id="tooltip-target-lang",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a source language to retrieve a list of recent changes.",
        style={"width": "38rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-lang",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div([
    html.P(
        [
            "Types of ",
            html.Span(
                "edits",
                id="tooltip-target-content",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select all the edits, edits that resulted in new articles, or external edits made in Wikidata.",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-content",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),



    html.Div([
    html.P(
        [
            "Types of ",
            html.Span(
                "editors",
                id="tooltip-target-editortypes",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select or filter the edits by a specific type of editor.",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-editortypes",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),




    html.Div(
    [
    html.P(
        [
            "Filter by ",
            html.Span(
                "diversity category",
                id="tooltip-target-category",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a Topic to filter the results to show only articles about certain topic (geolocated with a ISO code, gender, CCC, etc.)",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-category",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    ),

    ])


interface_row2 = html.Div([

    html.Div([
    html.P(
        [
            "Order by ",
            html.Span(
                "feature or category",
                id="tooltip-target-orderby",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Select a relevance feature or diversity category to sort the results).",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
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
            "Timeframe in ",
            html.Span(
                "hours",
                id="tooltip-target-timeframe",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Specify the number of hours from now you to retrieve the recent changes (by default 24).",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-timeframe",
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
                "number of results",
                id="tooltip-target-limit",
                style={"textDecoration": "underline", "cursor": "pointer"},
            ),
        ]
    ),
    dbc.Tooltip(
        html.P(
            "Limit the number of results (by default 300, maximum of 5000)",
        style={"width": "40rem", 'font-size': 12, 'text-align':'left', 'backgroundColor':'#F7FBFE','padding': '12px 12px 12px 12px'}
        ),
        target="tooltip-target-limit",
        placement="bottom",
        style={'color':'black', 'backgroundColor':'transparent'},
    )],
    style={'display': 'inline-block','width': '200px'},
    )

])




def dash_app18_build_layout(params):

    if len(params)!=0 and params['lang']!='none' and params['lang']!= None:
        functionstartTime = time.time() 

        if 'lang' in params:
            languagecode = params['lang']
            if languagecode != 'none': language = languages.loc[languagecode]['languagename']
        else:
            languagecode = 'none'

        if 'edittypes' in params:
            edittypes = params['edittypes']
            if edittypes not in edittypes_dict_inv: edittypes = 'all_edits'
        else:
            edittypes = 'all_edits'

        if 'editortypes' in params:
            editortypes=params['editortypes']
            if editortypes not in editortypes_dict_inv: editortypes = 'all_editors'
        else:
            editortypes='all_editors'

        if 'diversitycategory' in params:
            category = params['diversitycategory']
            if category not in diversitycategory_dict_inv: category = 'all_topics'
        else:
            category = 'all_topics'

        if 'orderby' in params:
            orderby=params['orderby'].lower()
            if orderby not in features_dict_inv: orderby = 'timestamp'
        else:
            orderby='timestamp'

        if 'timeframe' in params:
            timeframe = params['timeframe']
            try:
                timeframe = int(timeframe)
                if timeframe == 0: timeframe = 24
                elif timeframe > 168: timeframe = 168
            except:
                timeframe = 24
        else:
            timeframe = 24

        if 'limit' in params:
            limit = int(params['limit'])
            try:
                limit = int(limit)
                if limit == 0: limit = 5000
                elif limit > 5000: limit = 5000
            except:
                limit = 5000
        else:
            limit = 5000
        

        # print (languagecode, edittypes, editortypes, category, orderby, timeframe, limit)

        # df = pd.read_csv(databases_path+'df_rc_categories_sample.csv')
        # df = df.rename_axis('position')
        # df = df.reset_index()

        # if limit != 'none':
        #     df = df.head(limit)


        df = get_recent_articles_recent_edits(languagecode, edittypes, editortypes, timeframe, limit)
        df = get_articles_diversity_categories_wikipedia_diversity_db(languagecode, df)
        df = df.reset_index()
        # print (df.head(10))
        df = df.rename_axis('position')
        df = df.reset_index()
        # print (df.head(10))

        if category.lower() != 'none' and category != None and category != 'all_topics':
            if category == 'ccc_binary':
                df = df.loc[df[category]==1]
            else:
                df = df.loc[df[category].notnull()]



        qitem_labels_lang = group_labels.loc[(group_labels["lang"] == languagecode)][['qitem','label','page_title']]
        qitem_labels_lang = qitem_labels_lang.set_index('qitem')
        qitem_labels_en = group_labels.loc[(group_labels["lang"] == "en")][['qitem','label','page_title']]
        qitem_labels_en = qitem_labels_en.set_index('qitem')



        # PAGE CASE 2: PARAMETERS WERE INTRODUCED AND THERE ARE NO RESULTS
        if len(df) == 0:

            layout = html.Div([
                navbar,          
                html.H3('Recent Changes Diversity', style={'textAlign':'center'}),
                html.Br(),

                dcc.Markdown(
                    text_default.replace('  ', '')),


                # HERE GOES THE INTERFACE
                # LINE 
                html.Br(),
                html.H5('Select the source'),
                interface_row1,
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='lang',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='ca',
                    placeholder="Select a source language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
        #        dcc.Link('Query',href=""),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='edittypes',
                    options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                    value='none',
                    placeholder="Type of edits (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='editortypes',
                    options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                    value='none',
                    placeholder="Type of editors (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='diversitycategory',
                    options=[{'label': i, 'value': diversitycategory_dict[i]} for i in diversitycategory_dict],
                    value='none',
                    placeholder="Category (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),



                # LINE 
                html.Br(),
                interface_row2,
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='orderby',
                    options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                    value='none',
                    placeholder="Order by (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='timeframe',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='24',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='500',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                ###            

                html.Div(
                html.A(html.Button('Query Results!'),
                    href=''),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Br(),

                html.Hr(),
                # html.H5('Results'),
                # dcc.Markdown(text_results.replace('  ', '')),
                html.Br(),
                html.H6('There are not results. Unfortunately this list is empty for this language.'),

                footbar,
            ], className="container")

            return layout


        # PAGE CASE 3: PARAMETERS WERE INTRODUCED AND THERE ARE RESULTS
#        print (df.columns)
        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after queries.')


        # PREPARE THE GRAPH
        dfy = get_proportion_diversity_category(df, limit, language)
        column = list()
        for index, rows in dfy.iterrows():
            Topic = rows['Id']
            cat = rows['Category']
            label = ''

            if cat == 'ISO31662':
                try:
                    label = ISO31662_subdivisions[Topic]
                except:
                    label = Topic
            
            elif cat == 'ISO3166':
                label = country_names[Topic]

            elif cat == 'Continent':
                label = Topic

            elif cat == 'CCC':
                label = 'CCC'

            else:
                try:
                    try:
                        label = qitem_labels_lang.loc[Topic]['page_title']
                        if len(label) == 2: label = label.tolist()[0]
                        if label == '': label = qitem_labels_lang.loc[Topic]['label']
                    except:
                        label = qitem_labels_lang.loc[Topic]['label']
                        if len(label) == 2: label = label.tolist()[0]

                except: 
                    try:
                        try:
                            label = qitem_labels_en.loc[Topic]['page_title']
                            if len(label) == 2: label = label.tolist()[0]
                            if label == '': label = qitem_labels_en.loc[Topic]['label']
                        except:
                            label = qitem_labels_en.loc[Topic]['label']
                            if len(label) == 2: label = label.tolist()[0]
                    except:
                        label = Topic
            column.append(label)
        dfy['Topic'] = column 




        if edittypes != 'new_articles':
            fig = px.bar(dfy, x="Category", y="Number of edits", color="Topic", title="Categories Summary", hover_data=['Id','Percentage'],text=dfy['Percentage'])
        else:
            fig = None


        # PREPARE THE DATATABLE
        columns_dict = {'position':'Nº','page_title':'Title','rc_timestamp':'Edit Timestamp','actor_name':'Editor','Bytes':'Current Length','ccc_binary':language+' CCC', 'num_inlinks_from_CCC':'Inlinks from CCC', 'num_outlinks_to_CCC':'Outlinks to CCC'}
        columns_dict.update(features_dict_inv)

        df=df.rename(columns=columns_dict)
        final_columns = ['Nº','Title','Edit Timestamp','Editor']+['Creation Date','Current Length','Pageviews','Interwiki']
        diversity = ['ISO3166','ISO3166-2','Continent','Gender','Ethnic Group','Sexual Orientation',language+' CCC']
        if orderby!='none' and features_dict_inv[orderby] not in diversity and features_dict_inv[orderby] not in final_columns:
            final_columns+= [features_dict_inv[orderby]]
        final_columns = final_columns + diversity
        if edittypes == 'wikidata_edits': 
            final_columns.remove('Editor')

        columns = final_columns

        # df1=df1.drop(columns=todelete)
        orderby = orderby.lower()
        if orderby != 'none':
            order = features_dict_inv[orderby]
        else:
            order = features_dict_inv['rc_timestamp']
        df = df.sort_values(order,ascending=False)
        df = df.fillna('')
        df_list = list()


        k = 0
        for index, rows in df.iterrows():
            if k > limit: break

            df_row = list()

            for col in columns:
                title = rows['Title']
                if not isinstance(title, str):
                    title = title.iloc[0]

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == 'Title':
                    title = rows['Title']
                    if not isinstance(title, str):
                        title = title.iloc[0]
                    df_row.append(html.A(title.replace('_',' '), href='https://'+languagecode+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Editor':
                    editor = rows['Editor']
                    df_row.append(html.Div(editor, style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    try:
                        df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    except:
                        df_row.append('')

                elif col == 'Current Length':
                    try:
                        value = round(float(int(rows[col])/1000),1)
                        df_row.append(str(value)+'k')
                    except:
                        df_row.append('0')


                elif col == 'Outlinks' or col == 'References' or col == 'Images':
                    title = rows['Title']
                    df_row.append(html.A( rows[col], href='https://'+languagecode+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+languagecode+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks from CCC':
                    df_row.append(html.A( rows['Inlinks from CCC'], href='https://'+languagecode+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Outlinks from CCC':
                    df_row.append(html.A( rows['Outlinks from CCC'], href='https://'+languagecode+'.wikipedia.org/wiki/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Inlinks from Women':
                    df_row.append(html.A( rows['Inlinks from Women'], href='https://'+languagecode+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Outlinks from Women':
                    df_row.append(html.A( rows['Outlinks from Women'], href='https://'+languagecode+'.wikipedia.org/wiki/'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+languagecode+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+languagecode+'.wikipedia.org/w/index.php?title='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    df_row.append(html.A( rows['Discussions'], href='https://'+languagecode+'.wikipedia.org/wiki/Talk:'+rows['Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikirank':
                    df_row.append(html.A( rows['Wikirank'], href='https://wikirank.net/'+languagecode+'/'+rows['Title'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+languagecode+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Discussions':
                    title = rows['Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+languagecode+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edit Timestamp':
                    timestamp = str(int(rows[col]))
                    df_row.append(datetime.datetime.strftime(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S'),'%H:%M:%S %d-%m-%Y'))

                elif col == 'Creation Date':
                    try:
                        timestamp = str(int(rows[col]))
                        df_row.append(datetime.datetime.strftime(datetime.datetime.strptime(timestamp,'%Y%m%d%H%M%S'),'%Y-%m-%d'))
                    except:
                        df_row.append('')

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))


                elif col == 'Sexual Orientation' or col == 'Ethnic Group' or col == 'Gender':
                    if pd.isna(rows[col]): 
                        df_row.append('')
                        continue
                    qit = str(rows[col])

                    if ';' in qit:
                        qlist = qit.split(';')
                    else:
                        qlist = [qit]

                    c = len(qlist)

                    text = ' '

                    i = 0
                    for ql in qlist:
                        i+= 1
                        try:
                            try:
                                label = qitem_labels_lang.loc[ql]['page_title']
                                if label == '': label = qitem_labels_lang.loc[ql]['label']
                            except:
                                label = qitem_labels_lang.loc[ql]['label']

                            text+= '['+label+']'+'('+'http://'+languagecode+'.wikipedia.org/wiki/'+ label.replace(' ','_')+')'
                        except: 
                            try:
                                try:
                                    label = qitem_labels_en.loc[ql]['page_title']
                                    if label == '': label = qitem_labels_en.loc[ql]['label']
                                except:
                                    label = qitem_labels_en.loc[ql]['label']

                                text+= '['+label+' (en)'+']'+'('+'http://en.wikipedia.org/wiki/'+ label.replace(' ','_')+')'

                            except:
                                label = ql
                                text+= '['+label+']'+'('+'https://www.wikidata.org/wiki/'+ label+')'

                        if i<c:
                            text+=', '

                    df_row.append(dcc.Markdown(text))


                elif col == language+' CCC':
#                    print (rows['Title'],rows['CCC'])
                    if rows['CCC'] == 1:
                        df_row.append('yes')
                    else:
                        df_row.append('')

                else:
                    df_row.append(rows[col])

            if k <= limit:
                df_list.append(df_row)


        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' after htmls')
        # print (len(df_list))


        # RESULTS PAGE
        title = 'Recent Changes Diversity'
        df1 = pd.DataFrame(df_list)
        dash_app18.title = title+title_addenda

        # LAYOUT
        layout = html.Div([
            navbar,           
            html.H3(title, style={'textAlign':'center'}),
#            html.Br(),

            dcc.Markdown(
                text_default.replace('  ', '')),
            html.Br(),


            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the source'),

            interface_row1,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='ca',
                placeholder="Select a source language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='edittypes',
                options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                value='none',
                placeholder="Type of edits (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='editortypes',
                options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                value='none',
                placeholder="Type of editors (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='diversitycategory',
                options=[{'label': i, 'value': diversitycategory_dict[i]} for i in diversitycategory_dict],
                value='none',
                placeholder="Category (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),



            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='orderby',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='timeframe',                    
                placeholder='Enter a value...',
                type='text',
                value='24',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='500',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.A(html.Button('Query Results!'),
                href=''),
           

            # here there is the table            
            html.Br(),
            html.Br(),

            html.Hr(),
            html.H5('Results'),
            html.Div(html.I('Recent Changes for '+language+' Wikipedia [limited by '+str(limit)+' results, last '+str(timeframe)+' hours, '+edittypes_dict_inv[edittypes]+', '+edittypes_dict_inv[edittypes]+', '+editortypes_dict_inv[editortypes]+', '+diversitycategory_dict_inv[category]+', and ordered by '+features_dict_inv[orderby]+']')),
            html.Br(),


            dcc.Markdown(text_results.replace('  ', '')),
            dcc.Graph(
                id='example-graph',
                figure=fig
            ),

            # if edittypes != 'new_articles':
            # else:
            #     dcc.Markdown(text_results2.replace('  ', '')),
    
            html.Br(),
            html.H6('Table'),


            html.Table(
            # Header
            [html.Tr([html.Th(col) for col in columns])] +
            # Body
            [html.Tr([
                html.Td(
                    (df_row[x]),
                    style={'font-size':"12px"} # 'background-color':"lightblue"}
                    )
                for x in range(len(columns))
            ]) for df_row in df_list]),

            footbar,

        ], className="container")

        # print(str(datetime.timedelta(seconds=time.time() - functionstartTime))+' before printing')
    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            navbar,           
            html.H3('Recent Changes Diversity', style={'textAlign':'center'}),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE 
            html.Br(),
            html.H5('Select the source'),

            interface_row1,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='ca',
                placeholder="Select a source language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='edittypes',
                options=[{'label': i, 'value': edittypes_dict[i]} for i in edittypes_dict],
                value='none',
                placeholder="Type of edits (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='editortypes',
                options=[{'label': i, 'value': editortypes_dict[i]} for i in editortypes_dict],
                value='none',
                placeholder="Type of editors (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='diversitycategory',
                options=[{'label': i, 'value': diversitycategory_dict[i]} for i in diversitycategory_dict],
                value='none',
                placeholder="Category (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),



            # LINE 
            html.Br(),
            interface_row2,
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='orderby',
                options=[{'label': i, 'value': features_dict[i]} for i in sorted(features_dict)],
                value='none',
                placeholder="Order by (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='timeframe',                    
                placeholder='Enter a value...',
                type='text',
                value='24',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='500',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.A(html.Button('Query Results!'),
                href=''),
           
            footbar,

        ], className="container")

    return layout



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# callback update page layout
@dash_app18.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app18_build_layout(state)

# callback update URL
component_ids_app18 = ['lang','edittypes', 'editortypes','diversitycategory','orderby','timeframe','limit']
@dash_app18.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app18])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids_app18, values)))
    return '?'+state
#    return f'?{state}'