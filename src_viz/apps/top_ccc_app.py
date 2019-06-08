import sys
import dash_apps
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
dash_app7 = Dash(__name__, server = app, url_base_pathname = webtype + '/top_ccc_articles/', external_stylesheets=external_stylesheets ,external_scripts=external_scripts)

dash_app7.config.supress_callback_exceptions = True

dash_app7.title = 'List of Top 500 Articles Lists from Cultural Context Content'+title_addenda
dash_app7.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content') 
])

def dash_app7_build_layout(params):
    if len(params)!=0 and params['list'].lower()!='none' and params['lang_origin'].lower()!='none':
        list_name=params['list'].lower()
        lang_origin=params['lang_origin'].lower()
        lang_target=params['lang_target'].lower()
        if 'country_origin' in params:
            country=params['country_origin'].upper()
            if country == 'NONE' or country == 'ALL': country = 'all'
        else:
            country = 'all'

        language_origin = languages.loc[lang_origin]['languagename']
        language_target = languages.loc[lang_target]['languagename']

    #    lists = ['editors','featured','geolocated','keywords','women','men','created_first_three_years','created_last_year','pageviews','discussions']

        conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cur = conn.cursor()

        columns_dict = {'position':'Nº','page_title_original':'Title','num_editors':'Editors','num_edits':'Edits','num_pageviews':'Pageviews','num_bytes':'Bytes','num_references':'References','num_inlinks':'Inlinks','num_wdproperty':'Wikidata Properties','num_interwiki':'Interwiki Links','featured_article':'Featured Article','num_discussions':'Discussion Edits','date_created':'Creation Date','num_inlinks_from_CCC':'Inlinks from CCC','other_languages':'Other Languages','page_title_target':' Title'}

        # COLUMNS
        query = 'SELECT r.position, r.qitem, f.page_title_original, '
        columns = ['Nº','Title']

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
            columns+= ['Discussion Edits','Pageviews','Edits','Bytes','References','Featured Article','Wikidata Properties','Interwiki Links']

        query += 'f.num_inlinks_from_CCC, f.date_created, p.page_title_target, p.generation_method, p0.page_title_target pt0, p0.generation_method pg0, p1.page_title_target pt1, p1.generation_method pg1, p2.page_title_target pt2, p2.generation_method pg2, p3.page_title_target pt3, p3.generation_method pg3 '

        columns+= ['Inlinks from CCC','Creation Date']
        columns+= ['Other Languages',' Title']


        query += 'FROM ccc_'+lang_origin+'wiki_top_articles_lists r '
        query += 'LEFT JOIN ccc_'+lang_target+'wiki_top_articles_page_titles p USING (qitem) '
        query += 'LEFT JOIN ccc_'+closest_langs[lang_target][0]+'wiki_top_articles_page_titles p0 USING (qitem) '
        query += 'LEFT JOIN ccc_'+closest_langs[lang_target][1]+'wiki_top_articles_page_titles p1 USING (qitem) '
        query += 'LEFT JOIN ccc_'+closest_langs[lang_target][2]+'wiki_top_articles_page_titles p2 USING (qitem) '
        query += 'LEFT JOIN ccc_'+closest_langs[lang_target][3]+'wiki_top_articles_page_titles p3 USING (qitem) '
        query += 'INNER JOIN ccc_'+lang_origin+'wiki_top_articles_features f USING (qitem) '
        query += 'WHERE r.list_name = "'+list_name+'" '
        if country: query += 'AND r.country IS "'+country+'" '
        query += 'ORDER BY r.position ASC;'

        df = pd.read_sql_query(query, conn)#, parameters)
        df = df.fillna(0)

        closest_languages = closest_langs[lang_target]
        page_titles_target = ['pt0','pt1','pt2','pt3']
        generation_method_target = ['pg0','pg1','pg2','pg3']
        cl = len(closest_languages)

        # Excel file
        filename = 'top_ccc_articles_'+lang_origin+'_'+list_name+'_'+country
        workbook = xlsxwriter.Workbook('/srv/wcdo/src_viz/downloads/top_ccc_lists/'+filename+'.xlsx')
        worksheet = workbook.add_worksheet(language_target)
        columns_excel = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P']
        for pos_ex in range(0,len(columns)):
            col_ex = columns_excel[pos_ex]
            label = columns[pos_ex]
            worksheet.write(col_ex+'1', label)
        worksheet.set_column(1, 1, 30)
        len_columns = len(columns)-1
        worksheet.set_column(len_columns, len_columns, 30)
        worksheet.set_column(len_columns-2, len_columns-2, 10)

        k = 1
        df=df.rename(columns=columns_dict)
        df_list = list()
        for index, rows in df.iterrows():
            k+=1

            df_row = list()
            for col in columns:
                l = columns_excel[columns.index(col)]
                pos = l+str(k)

                if col == 'Featured Article': 
                    fa = rows['Featured Article']
                    if fa == 0:
                        df_row.append('No')
                        worksheet.write(pos, u'No')
                    else:
                        df_row.append('Yes')
                        worksheet.write(pos, u'Yes')

                elif col == 'Interwiki Links':
                    df_row.append(html.A( rows['Interwiki Links'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    worksheet.write_url(pos, 'https://www.wikidata.org/wiki/'+str(rows['qitem']), string=str(rows['Interwiki Links']))

                elif col == 'Wikidata Properties':
                    df_row.append(html.A( rows['Wikidata Properties'], href='https://www.wikidata.org/wiki/'+rows['qitem'], target="_blank", style={'text-decoration':'none'}))
                    worksheet.write_url(pos, 'https://www.wikidata.org/wiki/'+str(rows['qitem']), string=str(rows['Wikidata Properties']))

                elif col == 'Other Languages':
                    i = 0
                    text = ''
                    text_ex = ''
                    for x in range(cl):
                        cur_generation_method = rows[generation_method_target[x]]
                        if cur_generation_method != 'sitelinks': continue
                        cur_title = rows[page_titles_target[x]]
                        if cur_title!= 0:
                            if i!=0 and i!=cl:
                                text+=', '
                                text_ex+=', '
                            text+= '['+closest_languages[x]+']'+'('+'http://'+closest_languages[x]+'.wikipedia.org/wiki/'+ cur_title.replace(' ','_')+')'#+'{:target="_blank"}'
                            text_ex+= closest_languages[x]
                            i+=1
                    df_row.append(dcc.Markdown(text))
                    worksheet.write(pos, text_ex)

                elif col == 'Bytes':
                    value = round(float(int(rows[col])/1000),1)
                    df_row.append(str(value)+'k')
                    worksheet.write(pos, str(value)+'k')

                elif col == 'Creation Date':
                    date = rows[col]
                    if date == 0: 
                        date = ''
                    else:
                        date = str(time.strftime("%Y-%m-%d", time.strptime(str(int(date)), "%Y%m%d%H%M%S")))
                    df_row.append(date)
                    worksheet.write(pos, date)

                elif col == 'Title':
                    title = rows['Title']
                    df_row.append(html.A(title.replace('_',' '), href='https://'+lang_origin+'.wikipedia.org/wiki/'+title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                    worksheet.write_url(pos, 'https://'+lang_origin+'.wikipedia.org/wiki/'+title.replace(' ','_'), string=title)

                elif col == ' Title':
                    cur_title = rows[' Title']
                    if cur_title != 0:
                        cur_title = cur_title.replace('_',' ')
                        if rows['generation_method'] == 'sitelinks':
                            df_row.append(html.A(cur_title, href='https://'+lang_target+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none'}))
                            worksheet.write_url(pos, 'https://'+lang_target+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), string=cur_title)
                        else:
                            df_row.append(html.A(cur_title, href='https://'+lang_target+'.wikipedia.org/wiki/'+cur_title.replace(' ','_'), target="_blank", style={'text-decoration':'none',"color":"#ba0000"}))
                    else:
                        df_row.append('')
                else:
                    df_row.append(rows[col])
                    worksheet.write(pos,rows[col])

            df_list.append(df_row)

        df1 = pd.DataFrame(df_list)
        df1.columns = columns

        workbook.close()

        if country == 'all':
            title = 'Top 500 CCC articles list "'+list_dict_inv[list_name] + '" from '+language_origin+' CCC to '+language_target
            country_origin = ' '
        else:
            country_origin = country_names[country]
            title = 'Top 500 CCC articles list "'+list_dict_inv[list_name]+'" from '+country_origin+' ('+language_origin+' CCC) to '+language_target
            country_origin = '('+country_origin+')'
    #    dash_app7.title = title+title_addenda


        ## LAYOUT
        col_len = len(columns)
        columns[1]=language_origin+' '+columns[1]
        columns[col_len-1]=language_target+columns[col_len-1]

        text = '''
        In this page, you can consult Top 500 CCC articles list from any country or language CCC generated with the latest CCC dataset. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

        The following table shows the Top 500 articles list '''+list_dict_inv[list_name] + ''' from '''+language_origin+''' CCC '''+country_origin+''' and its article availability in '''+language_target+''' Wikipedia. Articles are sorted by the feature ***'''+list_dict_inv[list_name]+'''***. The rest of columns present complementary features that are explicative of the article rellevance (number of editors, edits, pageviews, Bytes, Wikidata properties or Interwiki links). In particular, number of Inlinks from CCC (incoming links from the CCC group of articles) highlights the article importance in terms of how much it is required by other articles. The column named Other Language present Interwiki links to the article version when available in the four languages closer to the target language (those that cover best this language and therefore it is likely their editors consult it).

        The table's last column shows the article title in its target language, in ***blue*** when it exists, in ***red*** as a proposal generated with the Wikimedia Content Translation tool or as an existing Wikidata label in the same language, and ***empty*** when the article does not exist or there is no title proposition available.

        The available Top CCC articles lists are: list of CCC articles with most pageviews during the last month (**Pageviews**), list of CCC articles with most number of editors (**Editors**), list of CCC articles created during the first three years and with most edits (**First 3Y.**), list of CCC articles created during the last year and with most edits **Last Y.**),  list of CCC articles with most edits in talk pages (**Discussions**), list of CCC articles with featured article distinction (**Featured**), most bytes and references (weights: 0.8, 0.1 and 0.1 respectively), list of CCC articles with geolocation with most links coming from CCC (**Geolocated**), list of CCC articles with keywords on title with most bytes (**Keywords**), list of CCC articles categorized in Wikidata as women with most edits (**Women**) and list of CCC articles categorized in Wikidata as men with most edits (**Men**).

        It is possible to query any list by changing the URL parameters. You need to specify the list parameter (editors, featured, geolocated, keywords, women, men, created_first_three_years, created_last_year, pageviews and discussions), the language target parameter (as lang_target and the language wikicode), the language origin (as lang_origin and the language wikicode), and, optionally to limit the scope of the selection, the country origin parameter as part of the CCC (as country_origin and the country [ISO3166 code](https://en.wikipedia.org/wiki/ISO_3166-1)). In case no country is selected, the default is 'all'.


        '''    

        layout = html.Div([
            html.H3(title, style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', ''),
            containerProps={'textAlign':'center'}),

    #        html.Br(),
           html.Div(
            html.A(
                'Download Table (Excel)',
                id='download-link',
                download=filename+".xlsx",
                href='/downloads/top_ccc_lists/'+filename+'.xlsx',
                target="_blank"),
            style={'float': 'right','width': '200'}),

            html.H5('Select the parameters'),

            html.Div(
            html.P('List'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Language origin'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Country origin'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Language target'),
            style={'display': 'inline-block','width': '200'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list",
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang_origin',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='country_origin',
                options=[{'label': i, 'value': language_countries[lang_origin][i]} for i in sorted(language_countries[lang_origin])],
                value='none',
                placeholder="Select a country (optional)",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang_target',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),
    #        dcc.Link('Query',href=""),

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
        text = '''
        In this page, you can consult Top 500 CCC articles list from any country or language CCC generated with the latest CCC dataset. It is updated on a monthly basis (between updates there may be changes and the table may not reflect them).

        It is possible to query any list by changing the URL parameters. You need to specify the list parameter (editors, featured, geolocated, keywords, women, men, created_first_three_years, created_last_year, pageviews and discussions), the language target parameter (as lang_target and the language wikicode), the language origin (as lang_origin and the language wikicode), and, optionally to limit the scope of the selection, the country origin parameter as part of the CCC (as country_origin and the country [ISO3166 code](https://en.wikipedia.org/wiki/ISO_3166-1)). In case no country is selected, the default is 'all'.

        '''    

        layout = html.Div([
            html.H3('Top CCC articles lists', style={'textAlign':'center'}),
            dcc.Markdown(
                text.replace('  ', ''),
            containerProps={'textAlign':'center'}),

    #            html.Br(),

            html.H5('Select the parameters'),

            html.Div(
            html.P('List'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Language origin'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Country origin'),
            style={'display': 'inline-block','width': '200'}),

            html.Div(
            html.P('Language target'),
            style={'display': 'inline-block','width': '200'}),

            html.Br(),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='list',
                options=[{'label': i, 'value': lists_dict[i]} for i in sorted(lists_dict)],
                value='none',
                placeholder="Select a list",
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang_origin',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='country_origin',
                options=[{'label': i, 'value': country_names_inv[i]} for i in sorted(country_names_inv)],
                value='none',
                placeholder="Select a country (optional)",           
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),

            html.Div(
            dash_apps.apply_default_value(params)(dcc.Dropdown)(
                id='lang_target',
                options=[{'label': i, 'value': language_names[i]} for i in sorted(language_names)],
                value='none',
                placeholder="Select a language",
                style={'width': '190'}
             ), style={'display': 'inline-block','width': '200'}),
    #        dcc.Link('Query',href=""),

            html.A(html.Button('Query Results!'),
                href=''),
        ], className="container")
    return layout


# callback update URL
component_ids = ['list','lang_origin','country_origin','lang_target']
@dash_app7.callback(Output('url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):
    state = urlencode(dict(zip(component_ids, values)))
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

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###