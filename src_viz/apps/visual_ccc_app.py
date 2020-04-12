import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app32 = Dash(server = app, url_base_pathname = webtype + '/visual_ccc_articles/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
dash_app32.config['suppress_callback_exceptions']=True

dash_app32.title = 'Visual CCC Articles'+title_addenda
dash_app32.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])



text_default = '''
In this page you can look for gaps in images from different sources of content, whether it is a Top CCC list or a list of articles you introduce. 

You can simply paste the list of article titles, the URLs, or select the list and query the gallery of images. For each article, you will obtain the most used images across the different language editions in which it exists. Each image has a caption with the number of languages in which it is used and the availability in the source language. You are able to select whether you only want to see the gaps or all the images.

It is especially interesting to query excluding existing images or query articles with a low number of images.

Some queries might time more time than usual or return a blank page. This is because the table imagelinks in the replicas performance might not be optimal at the moment.

'''    

language_names_2 = language_names.copy()

showonly_dict = {'All images':'all', 'Existing images':'covered', 'Non existing images':'gaps'}
showonlyart_dict = {'Articles with zero images':'zero','Articles with one image':'one','Articles with less than five':'less_five','All articles':'all'}

features_dict = {'Number of Editors':'num_editors','Number of Edits':'num_edits','Number of images':'num_images','Wikirank':'wikirank','Number of Pageviews':'num_pageviews','Number of Inlinks':'num_inlinks','Number of References':'num_references','Number of Bytes':'num_bytes','Number of Outlinks':'num_outlinks','Number of Interwiki':'num_interwiki','Number of WDProperties':'num_wdproperty','Number of Discussions':'num_discussions','Creation Date':'date_created','Number of Inlinks from CCC':'num_inlinks_from_CCC'}

features_dict_inv = {'num_editors':'Editors', 'num_edits':'Edits', 'num_images':'Images', 'wikirank':'Wikirank', 'num_pageviews':'Pageviews', 'num_inlinks':'Inlinks', 'num_references':'References','num_bytes':'Bytes','num_outlinks':'Outlinks','num_interwiki':'Interwiki','num_wdproperty':'Wikidata Properties','num_discussions':'Discussions','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC'}


lists_dict = {'Editors':'editors','Featured':'featured','Geolocated':'geolocated','Keywords':'keywords','Women':'women','Men':'men','Created First Three Years':'created_first_three_years','Created Last Year':'created_last_year','Pageviews':'pageviews','Discussions':'discussions','Edits':'edits', 'Edited Last Month':'edited_last_month', 'Images':'images', 'WD Properties':'wdproperty_many', 'Interwiki':'interwiki_many', 'Least Interwiki Most Editors':'interwiki_editors', 'Least Interwiki Most WD Properties':'interwiki_wdproperty', 'Wikirank':'wikirank', 'Wiki Loves Earth':'earth', 'Wiki Loves Monuments':'monuments_and_buildings', 'Wiki Loves Sports':'sport_and_teams', 'Wiki Loves GLAM':'glam', 'Wiki Loves Folk':'folk', 'Wiki Loves Music':'music_creations_and_organizations', 'Wiki Loves Food':'food', 'Wiki Loves Paintings':'paintings', 'Wiki Loves Books':'books', 'Wiki Loves Clothing and Fashion':'clothing_and_fashion', 'Wiki Loves Industry':'industry'}

columns_dict = {'position':'Nº','qitem':'Qitem','page_title_original':'Article Title','page_title':'Article Title','num_images':'Images'}
columns_dict.update(features_dict_inv)


list_dict_inv = {v: k for k, v in lists_dict.items()}

def dash_app32_build_layout(params):

    if len(params)!=0 and (params['source_lang_list'].lower()!='none' or params['source_lang_text'].lower()!='none'):

        if params['list'].lower()=='none':
            list_name='editors'
        else:
            list_name=params['list'].lower()
    
        if params['source_lang_list'].lower()!='none':
            source_lang=params['source_lang_list'].lower()
            lists = 1
            text = 0
        else:
            source_lang=params['source_lang_text'].lower()
            text = 1
            lists = 0


        if 'source_country' in params:
            country=params['source_country'].upper()
            if country == 'NONE' or country == 'ALL': country = 'all'
        else:
            country = 'all'

        if 'show_only' in params:
            exclude_images=params['show_only'].lower()
        else:
            exclude_images='none'

        if 'show_only_art' in params:
            exclude_art=params['show_only_art'].lower()
        else:
            exclude_art='none'

        if 'textbox' in params:
            textbox=params['textbox'].lower()
        else:
            textbox='textbox'

        if 'order_by' in params:
            order_by=params['order_by'].lower()
        else:
            order_by='none'

        source_language = languages.loc[source_lang]['languagename']
        # print (source_lang,lists,text)

        if 'images' in params:
            try:
                number_images=int(params['images'])
            except:
                number_images=4
        else:
            number_images=4



        if lists == 1:

    #    lists = ['editors','featured','geolocated','keywords','women','men','created_first_three_years','created_last_year','pageviews','discussions']

            conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cur = conn.cursor()

            # COLUMNS
            query = 'SELECT r.qitem, f.page_title_original, f.num_images, f.num_interwiki '
            columns = ['Nº','Qitem','Article Title','Images']

            if order_by != 'none':
                feat = features_dict_inv[order_by]
                if feat not in columns:
                    columns+= [feat]
                    query+= ', f.'+order_by+' '

#            print (columns)

            # NEW LISTS

            query += 'FROM '+source_lang+'wiki_top_articles_lists r '
            query += 'INNER JOIN '+source_lang+'wiki_top_articles_features f USING (qitem) '
            query += "WHERE r.list_name = '"+list_name+"' "
            if country: query += 'AND r.country IS "'+country+'" '

            if exclude_art == 'zero':
                query+= ' AND num_images = 0 '
            elif exclude_art == 'one':
                query+= ' AND num_images = 1 '
            elif exclude_art == 'less_five':
                query+= ' AND num_images < 5 '

            if order_by != 'none':
                query += 'ORDER BY f.'+order_by+' DESC;'
            else:
                query += 'ORDER BY r.position ASC;'


            for x in range (1,number_images+1):
                columns.append('Image '+str(x))

#            print (query)

            df = pd.read_sql_query(query, conn)#, parameters)
            df = df.fillna(0)

        else:

            conn = sqlite3.connect(databases_path + 'wikipedia_diversity.db'); cursor = conn.cursor()

            # COLUMNS
            query = 'SELECT qitem, page_title, num_images, num_interwiki '
            columns = ['Qitem','Article Title','Images']

            if order_by != 'none':
                feat = features_dict_inv[order_by]
                if feat not in columns:
                    columns+= [feat]
                    query+= ', '+order_by+' '

#            print (columns)

            # NEW LISTS

            query += 'FROM '+source_lang+'wiki '

            page_titles = list(text_to_pageids_page_titles(source_lang, textbox).values())

            page_asstring = ','.join( ['?'] * len(page_titles) )
            query += 'WHERE page_title IN (%s) ' % page_asstring

            if exclude_art == 'zero':
                query+= ' AND num_images = 0 '
            elif exclude_art == 'one':
                query+= ' AND num_images = 1 '
            elif exclude_art == 'less_five':
                query+= ' AND num_images < 5 '

            if order_by != 'none':
                query += 'ORDER BY '+order_by+' DESC '

            query += 'LIMIT 100;'

            for x in range (1,number_images+1):
                columns.append('Image '+str(x))

#            print (query)

            page_titles = tuple(page_titles)
            df = pd.read_sql_query(query, conn, params = page_titles)#, parameters)
            df = df.fillna(0)



        if len(df) == 0: # there are no results.

            layout = html.Div([
                navbar,
                html.H3('Top CCC articles images', style={'textAlign':'center'}),

                html.H5('There are not results. Unfortunately this list is empty for this language. Try another language and list.'),

                html.Br(),

                dcc.Markdown(
                    text_default.replace('  ', '')),

                # html.Div([
                #     dcc.Markdown(text_default2.replace('  ', '')),
                #     ],
                #     style={'font-size': 12}),


                html.H5('Source of content'),

                html.Div(
                html.P('Option A: Select a Top CCC List'),
                style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),


                html.Br(),

                html.Div(
                html.P('Source language'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Source country'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Top CCC List'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang_list',
                    options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                    value='none',
                    placeholder="Select a source language",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_country',
                    options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                    value='none',
                    placeholder="Select a source country (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='list',
                    options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                    value='none',
                    placeholder="Select a list (Optional)",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),
                html.Br(),

                html.Div(
                html.P('Option B: Paste a list of articles titles'),
                style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),

                html.Br(),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='source_lang_text',
                    options=[{'label': i, 'value': language_names_2[i]} for i in sorted(language_names_2)],
                    value='none',
                    placeholder="Select a source language",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                dash_apps.apply_default_value(params)(dcc.Textarea)(
                    id='textbox',
                    placeholder='You can paste a list of articles titles or URL here to obtain the results.',
                    value='',
                    style={'width': '100%', 'height':'100'}
                 ), style={'display': 'inline-block','width': '590'}),

                html.Br(),

                html.H5('Filter the results'),

                html.Div(
                html.P('Order articles by'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Show articles'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),

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
                    id='show_only_art',
                    options=[{'label': i, 'value': showonlyart_dict[i]} for i in sorted(showonlyart_dict)],
                    value='all',
                    placeholder="Show only (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Br(),


                html.Div(
                html.P('Show images'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Number of images'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='show_only',
                    options=[{'label': i, 'value': showonly_dict[i]} for i in sorted(showonly_dict)],
                    value='all',
                    placeholder="Show only (optional)",           
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='images',                    
                    placeholder='By default 4, max: 6.',
                    type='text',
                    value='4',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),
            
                html.A(html.Button('Query Results!'),
                    href=''),

                footbar,

            ], className="container")

            return layout

        try:
            page_title_original = df.page_title_original.tolist()
        except:
            page_title_original = df.page_title.tolist()

        qitems_num_interwiki = df.set_index('qitem')['num_interwiki'].to_dict()

        for x in range (1,number_images+1):
            df['Image '+str(x)] = None

        page_id_images = page_titles_to_current_images(page_title_original, source_lang)

        df = page_titles_to_rank_images(df, page_id_images, qitems_num_interwiki, source_lang, number_images, exclude_images)

#        print ('page_titles_to_rank_images')

        k = 0
        df=df.rename(columns=columns_dict)

#        print(df.columns.tolist())

        df_list = list()
        for index, rows in df.iterrows():

            df_row = list()
            for col in columns:
                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Article Title':
                    title = rows['Article Title']
                    df_row.append(html.A(title.replace('_',' '), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Featured Article': 
                    fa = rows['Featured Article']
                    if fa == 0:
                        df_row.append('No')
                    else:
                        df_row.append('Yes')

                elif col == 'Interwiki Links':
                    df_row.append(html.A( rows['Interwiki Links'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))


                elif col == 'Inlinks':
                    df_row.append(html.A( rows['Inlinks'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))


                elif col == 'Inlinks from CCC':
                    df_row.append(html.A( rows['Inlinks from CCC'], href='https://'+source_lang+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'}))

                elif col == 'Editors':
                    df_row.append(html.A( rows['Editors'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Edits':
                    df_row.append(html.A( rows['Edits'], href='https://'+source_lang+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikirank':
                    df_row.append(html.A( rows['Wikirank'], href='https://wikirank.net/'+source_lang+'/'+rows['Article Title'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Pageviews':
                    df_row.append(html.A( rows['Pageviews'], href='https://tools.wmflabs.org/pageviews/?project='+source_lang+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))


                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')

                elif col == 'Creation Date':
                    date = rows[col]
                    if date == 0: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)

                elif col == 'Images':
                    title = rows['Article Title']
                    df_row.append(html.A(str(rows[col]), href='https://'+source_lang+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                else:
                    df_row.append(rows[col])

            df_list.append(df_row)

        df1 = pd.DataFrame(df_list)

        df1.columns = columns


        df_list = df1.values.tolist()

#        print (df1.head())
        
        if lists == 1:
            text = '''The following table shows the Top 500 articles list '''+list_dict_inv[list_name] + ''' from '''+source_language+''' CCC and its images. '''

            title = 'Images in the ' + source_language + ' Top CCC articles list "'+list_dict_inv[list_name]+'"'

        else:
            title = 'Images in the ' + source_language + ' Wikipedia queried articles'

            text = '''The following table shows the articles that have been queried.'''

        ## LAYOUT
        text += ''' The columns present the position in the list, the Qitem in Wikidata, the article title in the source language, the number of images, the value of a feature (in case you set order by), and the number of images you selected to see.

        Below each image there is the number of languages in which this image to illustrate their version of the article and the total number of languages in which the article exists. The color green or red means that the image either is or is not used in the article version of the language you are querying ('''+ source_language+ ''').
        '''

        countries_sel = language_countries[source_lang]



        layout = html.Div([
            html.H3(
                navbar,
                title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', '')),


            html.H5('Source of content'),

            html.Div(
            html.P('Option A: Select a Top CCC List'),
            style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),


            html.Br(),

            html.Div(
            html.P('Source language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Source country'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Top CCC List'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang_list',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a source language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_country',
                options=[{'label': i, 'value': countries_sel[i]} for i in sorted(countries_sel)],
                value='none',
                placeholder="Select a source country (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list (Optional)",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),
            html.Br(),

            html.Div(
            html.P('Option B: Paste a list of articles titles'),
            style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang_text',
                options=[{'label': i, 'value': language_names_2[i]} for i in sorted(language_names_2)],
                value='none',
                placeholder="Select a source language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Textarea)(
                id='textbox',
                placeholder='You can paste a list of articles titles or URL here to obtain the results.',
                value='',
                style={'width': '100%', 'height':'100'}
             ), style={'display': 'inline-block','width': '590'}),

            html.Br(),

            html.H5('Filter the results'),

            html.Div(
            html.P('Order articles by'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Show articles'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

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
                id='show_only_art',
                options=[{'label': i, 'value': showonlyart_dict[i]} for i in sorted(showonlyart_dict)],
                value='all',
                placeholder="Show only (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),


            html.Div(
            html.P('Show images'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Number of images'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='show_only',
                options=[{'label': i, 'value': showonly_dict[i]} for i in sorted(showonly_dict)],
                value='all',
                placeholder="Show only (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='images',                    
                placeholder='By default 4, max: 6.',
                type='text',
                value='4',
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
            ]) for df_row in df_list]),

            footbar,

        ], className="container")

    else:

        layout = html.Div([
            html.H3('Visual CCC articles', style={'textAlign':'center'}),
            dcc.Markdown(
                text_default.replace('  ', '')),


#            html.Br(),
            html.Br(),

            html.H5('Source of content'),

            html.Div(
            html.P('Option A: Select a Top CCC List'),
            style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),


            html.Br(),

            html.Div(
            html.P('Source language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Source country'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Top CCC List'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang_list',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a source language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_country',
                options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                value='none',
                placeholder="Select a source country (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list (Optional)",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.Br(),
            html.Br(),

            html.Div(
            html.P('Option B: Paste a list of articles titles'),
            style={'display': 'inline-block','fontSize':14, 'fontWeight':'bold'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='source_lang_text',
                options=[{'label': i, 'value': language_names_2[i]} for i in sorted(language_names_2)],
                value='none',
                placeholder="Select a source language",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),
            html.Div(
            dash_apps.apply_default_value(params)(dcc.Textarea)(
                id='textbox',
                placeholder='You can paste a list of articles titles or URL here to obtain the results.',
                value='',
                style={'width': '100%', 'height':'100'}
             ), style={'display': 'inline-block','width': '590px'}),

            html.Br(),

            html.H5('Filter the results'),

            html.Div(
            html.P('Order articles by'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Show articles'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

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
                id='show_only_art',
                options=[{'label': i, 'value': showonlyart_dict[i]} for i in sorted(showonlyart_dict)],
                value='all',
                placeholder="Show only (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Br(),


            html.Div(
            html.P('Show images'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Number of images'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='show_only',
                options=[{'label': i, 'value': showonly_dict[i]} for i in sorted(showonly_dict)],
                value='all',
                placeholder="Show only (optional)",           
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='images',                    
                placeholder='By default 4, max: 6.',
                type='text',
                value='4',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),


            html.A(html.Button('Query Results!'),
                href=''),

            footbar,

        ], className="container")

    return layout




def page_titles_to_current_images(page_titles, languagecode):

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    # query = 'SELECT il_to FROM imagelinks where il_from = %s;'
    page_id_images = {}

    try:
        page_asstring = ','.join( ['%s'] * len(page_titles) )
        query = 'SELECT page_id, il_to FROM imagelinks INNER JOIN page on il_from = page_id WHERE il_from_namespace = 0 AND page_title IN (%s) ORDER BY page_title;' % page_asstring

        # print (page_titles)

        mysql_cur_read.execute(query, (page_titles))
        result = mysql_cur_read.fetchall()

        list_images = set()
        old_page_id = ''
        i = 0
        for row in result:
            imagename = row[1].decode('utf-8')
            if '.ogg' in imagename: continue
            page_id = row[0]

            if page_id != old_page_id:
                page_id_images[old_page_id]=list_images
                list_images = set()
                i = 0

            old_page_id = page_id
            

            i += 1
            # if i<=20:
            list_images.add(imagename)

        page_id_images[page_id]=list_images
    except:
        print ('timeout.')
    
#    print ('page ids with images: '+str(len(page_id_images)))
    return page_id_images


def page_titles_to_rank_images(df, page_id_images, qitems_num_interwiki, languagecode, number_images, exclude_images):

    conn = sqlite3.connect(databases_path + 'images.db'); cursor = conn.cursor()

    qitems = df.qitem.tolist()

    df = df.set_index('qitem')

    page_asstring = ','.join( ['?'] * len(qitems) )
    query = 'SELECT page_id, images, qitem FROM '+languagecode+'wiki_images_count WHERE qitem IN (%s)' % page_asstring
    for row in cursor.execute(query, qitems):
        page_id = row[0]
        images = row[1]
        if images == '' or images == None: continue
        qitem = row[2]

        imageranks = images.split(';')

        try:
            images_set = page_id_images[page_id]
        except:
            images_set = set()

        # print (page_id, qitem, images_set)

        j = 1
        for imagerank in imageranks:
            imagerank = imagerank.split(':')
            image = imagerank[0]
            if '.ogg' in image: continue
            try:
                rank = imagerank[1]
            except:
                rank = 1

            is_local = None
            if image in images_set:
                is_local = 1

            image_size = 900/number_images

            URL = "https://commons.wikimedia.org/wiki/Special:FilePath/"+image+"?width=160"
            URL = "https://commons.wikimedia.org/wiki/Special:Redirect/file/"+image

            # URL = requests.get('https://commons.wikimedia.org/wiki/Special:FilePath/'+image+'?width=320')


            link_1 = html.A(html.Img(src=URL,style={'max-width': str(image_size)+ 'px', 'max-height':str(image_size)+ 'px'}), href='https://commons.wikimedia.org/wiki/File:'+image, target="_blank", style={'text-decoration':'none'})

            if is_local == 1:
                line = html.P(str(rank)+'/'+str(qitems_num_interwiki[qitem])+' Langs. ('+languagecode+')')
                color = 'green'
                # print (image)
                # print (images_set)
                # print (row)
            else:
                line = html.P(str(rank)+'/'+str(qitems_num_interwiki[qitem])+' Langs.')
                color = 'red'


            # CONDITIONS OF EXCLUSION
            if exclude_images == 'none' or exclude_images == 'all':
                df.at[qitem,'Image '+str(j)] = html.Div([
                    link_1, 
                    line,
                    ],style={'color': color})
                j+=1


            elif exclude_images == 'gaps' and image not in images_set: # case of gaps
                # line = html.P(str(rank)+'/'+str(qitems_num_interwiki[qitem])+' Langs. not including '+languagecode)
                df.at[qitem,'Image '+str(j)] = html.Div([
                    link_1, 
                    line,
                    ],style={'color': color})
                j+=1

            elif exclude_images == 'covered' and image in images_set: # case of gaps
                # line = html.P(str(rank)+'/'+str(qitems_num_interwiki[qitem])+' Langs. including '+languagecode)
                df.at[qitem,'Image '+str(j)] = html.Div([
                    link_1, 
                    line,
                    ],style={'color': color})

                j+=1

# exclude = {'Existing images':'gaps', 'Non existing images':'covered'}

            if j > number_images:
                break

    df = df.reset_index()
    return df


def text_to_pageids_page_titles(languagecode, textbox):
#    print (textbox)

    textbox = textbox.lower()
    page_titles = []

    if ('.org') in textbox:
        textbox = textbox.replace('https://'+languagecode+'.wikipedia.org/wiki/','')

    if '\n' in textbox:
        textbox = textbox.replace('\n','\t')

    if ';' in textbox:
        textbox = textbox.replace(';','\t')

    if ',' in textbox:
        textbox = textbox.replace(',','\t')

    page_titles = textbox.split('\t')

    page_titles = set(page_titles)

    params = []
    for x in page_titles:
        x = str(x)
        params.append(x.replace(' ','_'))

    page_asstring = ','.join( ['%s'] * len(params) )

    mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()

    query = 'SELECT page_id, page_title FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND CONVERT(page_title USING utf8mb4) COLLATE utf8mb4_general_ci IN (%s)' % page_asstring

    mysql_cur_read.execute(query,params)
    rows = mysql_cur_read.fetchall()

    page_dict = {}
    for row in rows:
        page_id = row[0]
        page_title = row[1].decode('utf-8')
        page_dict[page_id] = page_title

#    print (page_dict)

    return page_dict


@dash_app32.callback(
    Output('source_country', 'options'),
    [Input('source_lang_list', 'value')])
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
component_ids_app32 = ['list','source_lang_list','source_country','order_by','show_only','show_only_art','source_lang_text','textbox','images']
@dash_app32.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app32])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids_app32, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app32.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app32_build_layout(state)