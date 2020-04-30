import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app25 = Dash(__name__, server = app, url_base_pathname = webtype + '/incomplete_ccc_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app25.config['suppress_callback_exceptions']=True

dash_app25.title = 'Incomplete CCC Articles '+title_addenda
dash_app25.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])


features_dict = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Interwiki':'num_interwiki','WDProperties':'num_wdproperty','Discussions':'num_discussions','Featured Article':'featured_article','Images':'num_images'}

features_dict_lv = {'Editors':'num_editors','Edits':'num_edits','Pageviews':'num_pageviews','Inlinks':'num_inlinks','References':'num_references','Bytes':'num_bytes','Outlinks':'num_outlinks','Discussions':'num_discussions','Featured Article':'featured_article','Images':'num_images'}

columns_dict = {'position':'Nº','languagecode':'Lang.','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_bytes':'Bytes','num_references':'References','num_inlinks':'Inlinks','num_wdproperty':'Wikidata Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_discussions':'Discussions','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC','page_title':'Article Title','qitem':'Qitem'}

features_dict_inv = {'num_editors':'Editors', 'num_edits':'Edits', 'num_images':'Images', 'wikirank':'Wikirank', 'num_pageviews':'Pageviews', 'num_inlinks':'Inlinks', 'num_references':'References','num_bytes':'Bytes','num_outlinks':'Outlinks','num_interwiki':'Interwiki','num_wdproperty':'Wikidata Properties','num_discussions':'Discussions','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC','featured_article':'Featured Article'}


columns_dict.update(features_dict_inv)

language_names_2 = language_names.copy()
language_names_3 = language_names.copy()

text_base = '''In this page you can check whether the articles of a language edition you introduce manually or a Top CCC list is more complete in other language editions. In other words, you can compare each article stats (number of Bytes, number of references, number of images, number of outlinks, among others) in other languages, and then, decide whether to expand these articles or not. You can also compare engagement characteristics (e.g. number of editors, number of edits or number of pageviews) or the 'featured article' distinction.

    You need to select the *source language* for each of the two options (**Option A: Select a Top CCC List** or **Option B: Paste a list of articles' titles**). For the Option A: You need to choose a *source language* from which you want to retrieve articles. The *source country* is used to filter some part of the language context, in case it encompasses more than one country (i.e. English is used in several countries besides the United Kingdom, you can choose among them). In case no country is selected, the default is 'all'. Then, you can choose among the following *lists*. In case no list is selected, the default list is 'editors'. If you have any idea for a new list, please, ask: tools.wcdo@tools.wmflabs.org. For the Option B: you need to choose a *source language* and paste the list of articles (titles or full URL) separated by a comma, semicolon or a line feed.

    Then you can select whether you want to see only articles from other languages that are more complete in specific features (*show only feature*) - leaving it empty allows you to see all those more complete in any feature - or articles from a specific language (*show only language*). Finally you can also sort the results by a specific feature (*order articles by*) or limit the number of articles (*limit the number of articles* is 300 by default).
'''



def dash_app25_build_layout(params):

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

        if 'limit' in params:
            limit = params['limit']
        else:
            limit = 'none'

        if 'show_only' in params and params['show_only']!='none':
            show_only = set()
            a = params['show_only'].split(',')
            for x in a: show_only.add(features_dict_inv[x])
        else:
            show_only = set()
#        print (show_only)
        s_len = len(show_only)

        if 'show_only_lang' in params and params['show_only_lang'].lower()!='none':
            show_only_lang=params['show_only_lang'].lower()
#            print (show_only_lang)
        else:
            show_only_lang='none'

        conn = sqlite3.connect(databases_path + 'wikipedia_diversity_production.db'); cur = conn.cursor()

        if lists == 1:
            conn2 = sqlite3.connect(databases_path + 'top_ccc_articles_production.db'); cur2 = conn2.cursor()

            # COLUMNS
            query = 'SELECT qitem, f.num_wdproperty, f.num_interwiki, f.page_title_original as page_title, '

            query += 'f.num_inlinks, f.num_outlinks, f.num_bytes, f.num_references, f.num_images, f.num_edits, f.num_editors, f.num_discussions, f.num_pageviews, f.featured_article '

            query += 'FROM '+source_lang+'wiki_top_articles_lists r '
            query += 'INNER JOIN '+source_lang+'wiki_top_articles_features f USING (qitem) '
            query += "WHERE r.list_name = '"+list_name+"' "
            if country: query += 'AND r.country IS "'+country+'" '

            if order_by != 'none':
                query += 'ORDER BY f.'+order_by+' DESC;'
            else:
                query += 'ORDER BY r.position ASC;'

            df = pd.read_sql_query(query, conn2)#, parameters)
            df = df.fillna(0)

        else:
            # COLUMNS
            query = 'SELECT qitem, num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_images, num_edits, num_editors, num_discussions, num_pageviews, featured_article '

            query += 'FROM '+source_lang+'wiki '

            page_titles = list(text_to_pageids_page_titles(source_lang, textbox).values())
            page_asstring = ','.join( ['?'] * len(page_titles) )
            query += 'WHERE page_title IN (%s) ' % page_asstring

            if order_by != 'none':
                query += 'ORDER BY '+order_by+' DESC '

            page_titles = tuple(page_titles)
            df = pd.read_sql_query(query, conn, params = page_titles)#, parameters)
            df = df.fillna(0)

        df['languagecode'] = '_original_'


        qitems = df.qitem.tolist()
        # functionstartTime = time.time()

        for lang in closest_langs[source_lang]: 

            page_asstring = ','.join( ['?'] * len(qitems) )
            query = 'SELECT qitem, num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_images, num_edits, num_editors, num_discussions, num_pageviews, featured_article '

            query += 'FROM '+lang+'wiki WHERE qitem IN (%s);' % page_asstring

            df1 = pd.read_sql_query(query, conn, params = tuple(qitems))#, parameters)
            df1 = df1.fillna(0)
            df1['languagecode'] = lang

            df = df.append(df1)

        df = df.sort_values(['qitem', 'languagecode'], ascending=[True, True])

        # print (str(datetime.timedelta(seconds=time.time() - functionstartTime)))

        df=df.rename(columns=columns_dict)

        columns = df.columns.tolist()
        columns.insert(0,'Nº')
#        print (columns)

        # print (columns)
        # print (df.head(10))
        # print (show_only)
        # print (s_len)
        # print (show_only_lang)

        k=0
        df_list = list()
        for index, rows in df.iterrows():

            if rows['Lang.'] == '_original_':
                ref_rows = rows
                continue

            if show_only_lang != 'none' and rows['Lang.'] != show_only_lang:
                continue

            keep_row = set()

            df_row = list()
            for col in columns:
                # num_wdproperty, num_interwiki, page_title, num_inlinks, num_outlinks, num_bytes, num_references, num_edits, num_editors, num_discussions, num_pageviews, num_images, featured_article

                if col == 'Nº':
                    k+=1
                    df_row.append(str(k))   
                                 
                elif col == 'Lang.':
                    link = html.A(rows['Lang.'], href='https://'+rows['Lang.']+'.wikipedia.org/wiki/', target="_blank", style={'text-decoration':'none'})
                    df_row.append(link)

                elif col == 'Qitem':
                    df_row.append(html.A( rows['Qitem'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Interwiki':
                    df_row.append(html.A( rows['Interwiki'], href='https://www.wikidata.org/wiki/'+rows['Qitem'], target="_blank", style={'text-decoration':'none'}))

                elif col == 'Article Title':

                    abbr_label = html.Abbr(rows['Article Title'].replace('_',' '),title=source_language+' Wikipedia article title: '+ref_rows['Article Title'].replace('_',' '))
                    link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                    df_row.append(link)

                elif col == 'Bytes':
#                    print (rows[col])
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  round(float(int(value - value_original)/1000),1)

                        color = html.Div('+'+str(difference)+'k', style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(round(value_original/1000,1))+'k')
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(round(value/1000,1))+'k',title=col+' in ' + source_language+' Wikipedia article: '+str(round(value_original/1000,1))+'k')
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)


                elif col == 'Images' or col == 'References' or col == 'Outlinks':

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'Featured Article': 

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original == 0 and value != 0:
                        color = html.Div('Yes', style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(abbr_label)
                        keep_row.add(col)
                    elif value != 0:                       
                        link = html.A('Yes', href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)
                    else:
                        link = html.A('No', href='https://'+rows['Lang.']+'.wikipedia.org/wiki/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)

                elif col == 'Inlinks':

                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/wiki/Special:WhatLinksHere/'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})
                        df_row.append(link)

                elif col == 'Editors' or col == 'Edits':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+rows['Lang.']+'.wikipedia.org/w/index.php?title='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'Discussions':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://'+source_lang+'.wikipedia.org/wiki/Talk:'+rows['Article Title'].replace(' ','_'), target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

                elif col == 'Pageviews':
                    value_original = int(ref_rows[col])
                    value = int(rows[col])

                    if value_original < value:
                        difference =  int(value - value_original)

                        color = html.Div('+'+str(difference), style={'color': 'red'})
                        abbr_label = html.Abbr(color,title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://tools.wmflabs.org/pageviews/?project='+rows['Lang.']+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)
                        keep_row.add(col)
                    else:
                        abbr_label = html.Abbr(str(value),title=col+' in ' + source_language+' Wikipedia article: '+str(value_original))
                        link = html.A(abbr_label, href='https://tools.wmflabs.org/pageviews/?project='+rows['Lang.']+'.wikipedia.org&platform=all-access&agent=user&range=latest-20&pages='+rows['Article Title'].replace(' ','_')+'&action=history', target="_blank", style={'text-decoration':'none'})

                        df_row.append(link)

            if len(keep_row) > 0:              
                if show_only == None:
                    df_list.append(df_row)

                elif len(keep_row.intersection(show_only))==s_len:
                    df_list.append(df_row)
                else:
                    k=k-1
            else:
                k=k-1

        df1 = pd.DataFrame(df_list)

        # NO RESULTS PAGE
        if len(df1) == 0: # there are no results.
            layout = html.Div([
                navbar,

                html.H3('Incomplete CCC articles', style={'textAlign':'center'}),

                html.Br(),
                dcc.Markdown(text_base.replace('  ', '')),

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
                html.P('Top CCC list'),
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
                    placeholder="Select a list (Optional)",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),


                html.Br(),
                html.Br(),

                html.Div(
                html.P("Option B: Paste a list of articles' titles"),
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
                    placeholder="You can paste a list of articles' titles or URL here to obtain the results.",
                    value='',
                    style={'width': '100%', 'height':'100'}
                 ), style={'display': 'inline-block','width': '590px'}),

                html.Br(),

                html.H5('Filter the results'),


                html.Div(
                html.P('Show only feature'),
                style={'display': 'inline-block','width': '200px'}),


                html.Div(
                html.P('Show only language'),
                style={'display': 'inline-block','width': '200px'}),

                html.Br(),



                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id = 'show_only',
                    options=[{'label': i, 'value': features_dict_lv[i]} for i in sorted(features_dict_lv)],
                    value='none',
                    multi=True,
                    placeholder="Show only articles more complete in these features...",           
                    style={'width': '390pxpx'}
                 ), style={'display': 'inline-block','width': '400px'}),

                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='show_only_lang',
                    options=[{'label': i, 'value': language_names_3[i]} for i in sorted(language_names_3)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Br(),

                html.Div(
                html.P('Order articles by'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Limit the number of articles'),
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
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='300',
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
                html.H6('There are not results. Unfortunately the list of incomplete articles is empty for this language and parameters. Try another combination of parameters and query again.'),

                footbar,

            ], className="container")

            return layout


 
        ## LAYOUT
        if lists == 1:
            title = 'Top CCC articles list "'+list_dict_inv[list_name]+'" more complete in other languages'

            text = '''The following table shows the Top 500 articles list '''+list_dict_inv[list_name] + ''' from '''+source_language+''' CCC and its stats covered better by other languages. '''

        else:
            title = '' + source_language + ' Wikipedia queried articles more complete in other languages.'

            text = '''The following table shows the articles that have been queried.'''


        text_table = ''' First, it shows the language in which the article is more complete in another feature (Lang.), followed by the Qitem from Wikidata, the number of Wikidata properties, and the number of Interwiki links (i.e. the number of languages in which the article exists). The rest of columns present the article title in that language, and relevance features such as the number of Inlinks, number of Outlinks, number of Bytes, number of References, number of Images, and also engagement features such as the number of Edits, number of Editors, number of edits in Discussions, and number of Pageviews. The last column shows whether the article has the distinction "Featured article" in that language. For those features that will have a higher value in the other language version than the soruce language the difference between them will be shown in red with a + in front of it (e.g. article Pep Guardiola has +88.2k Bytes more in the English Wikipedia version than in the Catalan Wikipedia one). Otherwise the value of the feature will be shown in blue. If you hover on any value you will see the original value of the feature in the source language.
        '''    

        countries_sel = language_countries[source_lang]

        # RESULTS PAGE
        layout = html.Div([
            navbar,
            html.H3(title, style={'textAlign':'center'}),


            dcc.Markdown(text_base.replace('  ', '')),


    #        html.Br(),

            # HERE GOES THE INTERFACE
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
                html.P('Top CCC list'),
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
#                    options=[{'label': i, 'value': countries_sel[i]} for i in sorted(countries_sel)],
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
                html.P("Option B: Paste a list of articles' titles"),
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
                    placeholder="You can paste a list of articles' titles or URL here to obtain the results.",
                    value='',
                    style={'width': '100%', 'height':'100'}
                 ), style={'display': 'inline-block','width': '590px'}),

                html.Br(),

                html.H5('Filter the results'),

                html.Div(
                html.P('Show only feature'),
                style={'display': 'inline-block','width': '400px'}),

                html.Div(
                html.P('Show only language'),
                style={'display': 'inline-block','width': '200px'}),
                html.Br(),



                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id = 'show_only',
                    options=[{'label': i, 'value': features_dict_lv[i]} for i in sorted(features_dict_lv)],
                    value='none',
                    multi=True,
                    placeholder="Show only articles more complete in these features...",           
                    style={'width': '390pxpx'}
                 ), style={'display': 'inline-block','width': '400px'}),


                html.Div(
                dash_apps.apply_default_value(params)(dcc.Dropdown)(
                    id='show_only_lang',
                    options=[{'label': i, 'value': language_names_3[i]} for i in sorted(language_names_3)],
                    value='none',
                    placeholder="Select a language",
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.Br(),
                html.Div(
                html.P('Order articles by'),
                style={'display': 'inline-block','width': '200px'}),

                html.Div(
                html.P('Limit the number of articles'),
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
                dash_apps.apply_default_value(params)(dcc.Input)(
                    id='limit',                    
                    placeholder='Enter a value...',
                    type='text',
                    value='300',
                    style={'width': '190px'}
                 ), style={'display': 'inline-block','width': '200px'}),

                html.A(html.Button('Query Results!'),
                    href=''),


            html.Br(),
            html.Br(), 
            html.Hr(),
            html.H5('Results'),
            dcc.Markdown(text_table.replace('  ', '')),

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
            html.H3('Incomplete CCC articles', style={'textAlign':'center'}),
            dcc.Markdown(
                text_base.replace('  ', '')),


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
            html.P('Top CCC list'),
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
#                options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
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
            html.P("Option B: Paste a list of articles' titles"),
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
                placeholder="You can paste a list of articles' titles or URL here to obtain the results.",
                value='',
                style={'width': '100%', 'height':'100'}
             ), style={'display': 'inline-block','width': '590px'}),

            html.Br(),

            html.H5('Filter the results'),

            html.Div(
            html.P('Show only feature'),
            style={'display': 'inline-block','width': '400px'}),

            html.Div(
            html.P('Show only language'),
            style={'display': 'inline-block','width': '200px'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id = 'show_only',
                options=[{'label': i, 'value': features_dict_lv[i]} for i in sorted(features_dict_lv)],
                value='none',
                multi=True,
                placeholder="Show only articles more complete in these features...",           
                style={'width': '390pxpx'}
             ), style={'display': 'inline-block','width': '400px'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='show_only_lang',
                options=[{'label': i, 'value': language_names_3[i]} for i in sorted(language_names_3)],
                value='none',
                placeholder="Select a language",
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),
    #        dcc.Link('Query',href=""),

            html.Br(),

            html.Div(
            html.P('Order articles by'),
            style={'display': 'inline-block','width': '200px'}),

            html.Div(
            html.P('Limit the number of articles'),
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
            dash_apps.apply_default_value(params)(dcc.Input)(
                id='limit',                    
                placeholder='Enter a value...',
                type='text',
                value='300',
                style={'width': '190px'}
             ), style={'display': 'inline-block','width': '200px'}),

            html.A(html.Button('Query Results!'),
                href=''),

            footbar,


        ], className="container")


    return layout



def text_to_pageids_page_titles(languagecode, textbox):

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


    return page_dict


@dash_app25.callback(
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
component_ids_app25 = ['source_lang_list','source_country','list','source_lang_text','textbox','order_by','show_only','limit','show_only_lang']
@dash_app25.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids_app25])
def update_url_state(*values):
    if not isinstance(values[6], str):
        values = values[0],values[1],values[2],values[3],values[4],values[5],','.join(values[6]),values[7],values[8]

    # if values[4]=='None':
    #     values = values[0],values[1],values[2],values[3],'',values[5],values[6],values[7]

    state = urlencode(dict(zip(component_ids_app25, values)))
    return '?'+state
#    return f'?{state}'

# callback update page layout
@dash_app25.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = dash_apps.parse_state(href)
    return dash_app25_build_layout(state)