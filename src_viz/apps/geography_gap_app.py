import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


#### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

treemapoptions = ['All Geolocated Articles','Wikidata Geolocated Qitems']+language_names_list
treemapdict = language_names; treemapdict['All Geolocated Articles']='All Geolocated Articles'; treemapdict['Wikidata Geolocated Qitems']='Wikidata Geolocated Qitems'; 
language_names_inv = {v: k for k, v in language_names.items()}

ISO2_3 = {'AF':'AFG','AL':'ALB','DZ':'DZA','AS':'ASM','AD':'AND','AO':'AGO','AI':'AIA','AQ':'ATA','AG':'ATG','AR':'ARG','AM':'ARM','AW':'ABW','AU':'AUS','AT':'AUT','AZ':'AZE','BS':'BHS','BH':'BHR','BD':'BGD','BB':'BRB','BY':'BLR','BE':'BEL','BZ':'BLZ','BJ':'BEN','BM':'BMU','BT':'BTN','BO':'BOL','BQ':'BES','BA':'BIH','BW':'BWA','BV':'BVT','BR':'BRA','IO':'IOT','BN':'BRN','BG':'BGR','BF':'BFA','BI':'BDI','CV':'CPV','KH':'KHM','CM':'CMR','CA':'CAN','KY':'CYM','CF':'CAF','TD':'TCD','CL':'CHL','CN':'CHN','CX':'CXR','CC':'CCK','CO':'COL','KM':'COM','CD':'COD','CG':'COG','CK':'COK','CR':'CRI','HR':'HRV','CU':'CUB','CW':'CUW','CY':'CYP','CZ':'CZE','CI':'CIV','DK':'DNK','DJ':'DJI','DM':'DMA','DO':'DOM','EC':'ECU','EG':'EGY','SV':'SLV','GQ':'GNQ','ER':'ERI','EE':'EST','SZ':'SWZ','ET':'ETH','FK':'FLK','FO':'FRO','FJ':'FJI','FI':'FIN','FR':'FRA','GF':'GUF','PF':'PYF','TF':'ATF','GA':'GAB','GM':'GMB','GE':'GEO','DE':'DEU','GH':'GHA','GI':'GIB','GR':'GRC','GL':'GRL','GD':'GRD','GP':'GLP','GU':'GUM','GT':'GTM','GG':'GGY','GN':'GIN','GW':'GNB','GY':'GUY','HT':'HTI','HM':'HMD','VA':'VAT','HN':'HND','HK':'HKG','HU':'HUN','IS':'ISL','IN':'IND','ID':'IDN','IR':'IRN','IQ':'IRQ','IE':'IRL','IM':'IMN','IL':'ISR','IT':'ITA','JM':'JAM','JP':'JPN','JE':'JEY','JO':'JOR','KZ':'KAZ','KE':'KEN','KI':'KIR','KP':'PRK','KR':'KOR','KW':'KWT','KG':'KGZ','LA':'LAO','LV':'LVA','LB':'LBN','LS':'LSO','LR':'LBR','LY':'LBY','LI':'LIE','LT':'LTU','LU':'LUX','MO':'MAC','MG':'MDG','MW':'MWI','MY':'MYS','MV':'MDV','ML':'MLI','MT':'MLT','MH':'MHL','MQ':'MTQ','MR':'MRT','MU':'MUS','YT':'MYT','MX':'MEX','FM':'FSM','MD':'MDA','MC':'MCO','MN':'MNG','ME':'MNE','MS':'MSR','MA':'MAR','MZ':'MOZ','MM':'MMR','NA':'NAM','NR':'NRU','NP':'NPL','NL':'NLD','NC':'NCL','NZ':'NZL','NI':'NIC','NE':'NER','NG':'NGA','NU':'NIU','NF':'NFK','MP':'MNP','NO':'NOR','OM':'OMN','PK':'PAK','PW':'PLW','PS':'PSE','PA':'PAN','PG':'PNG','PY':'PRY','PE':'PER','PH':'PHL','PN':'PCN','PL':'POL','PT':'PRT','PR':'PRI','QA':'QAT','MK':'MKD','RO':'ROU','RU':'RUS','RW':'RWA','RE':'REU','BL':'BLM','SH':'SHN','KN':'KNA','LC':'LCA','MF':'MAF','PM':'SPM','VC':'VCT','WS':'WSM','SM':'SMR','ST':'STP','SA':'SAU','SN':'SEN','RS':'SRB','SC':'SYC','SL':'SLE','SG':'SGP','SX':'SXM','SK':'SVK','SI':'SVN','SB':'SLB','SO':'SOM','ZA':'ZAF','GS':'SGS','SS':'SSD','ES':'ESP','LK':'LKA','SD':'SDN','SR':'SUR','SJ':'SJM','SE':'SWE','CH':'CHE','SY':'SYR','TW':'TWN','TJ':'TJK','TZ':'TZA','TH':'THA','TL':'TLS','TG':'TGO','TK':'TKL','TO':'TON','TT':'TTO','TN':'TUN','TR':'TUR','TM':'TKM','TC':'TCA','TV':'TUV','UG':'UGA','UA':'UKR','AE':'ARE','GB':'GBR','UM':'UMI','US':'USA','UY':'URY','UZ':'UZB','VU':'VUT','VE':'VEN','VN':'VNM','VG':'VGB','VI':'VIR','WF':'WLF','EH':'ESH','YE':'YEM','ZM':'ZMB','ZW':'ZWE','AX':'ALA'}



#### COVERAGE DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

subregions_regions = {}
for x,y in subregions.items():
    subregions_regions[y]=regions[x]


### FUNCTIONS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

def params_to_df(langs, content_type, geographical_entity):

    conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 
    if isinstance(langs,str): langs = [langs]
    lass = ','.join( ['?'] * len(langs) )

    if content_type == 'zero_ill':

        query = "SELECT set1, set1descriptor, set2, set2descriptor, abs_value, rel_value, abs_value*100/rel_value as Geolocated_Articles, period FROM wdo_intersections_accumulated WHERE set2='geolocated' AND set1='"+geographical_entity+"' AND set2descriptor = 'zero_ill' AND content = 'articles' AND period = '"+last_period+"' ORDER BY abs_value DESC;"
        df_geolocated_countriessubregionsregions_noill = pd.read_sql_query(query, conn).fillna(0)

        df_geolocated_countriessubregionsregions_noill.Geolocated_Articles = df_geolocated_countriessubregionsregions_noill.Geolocated_Articles.astype(int)
        df_geolocated_countriessubregionsregions_noill = df_geolocated_countriessubregionsregions_noill.rename(columns={'abs_value':'Geolocated Articles No-IW','rel_value':'Articles No-IW (%)','Geolocated_Articles':'Geolocated Articles'})

        if geographical_entity == 'countries':
            df_geolocated_countriessubregionsregions_noill=df_geolocated_countriessubregionsregions_noill.rename(columns={'set1descriptor':'ISO 3166'})
            df_geolocated_countriessubregionsregions_noill['Country'] = df_geolocated_countriessubregionsregions_noill['ISO 3166'].map(country_names)
            df_geolocated_countriessubregionsregions_noill['Subregion'] = df_geolocated_countriessubregionsregions_noill['ISO 3166'].map(subregions)
            df_geolocated_countriessubregionsregions_noill['Region'] = df_geolocated_countriessubregionsregions_noill['ISO 3166'].map(regions)

        if geographical_entity == 'subregions':
            df_geolocated_countriessubregionsregions_noill=df_geolocated_countriessubregionsregions_noill.rename(columns={'set1descriptor':'Subregion','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki'})
            df_geolocated_countriessubregionsregions_noill['Region'] = df_geolocated_countriessubregionsregions_noill['Subregion'].map(subregions_regions)

        if geographical_entity == 'regions':
            df_geolocated_countriessubregionsregions_noill=df_geolocated_countriessubregionsregions_noill.rename(columns={'set1descriptor':'Region','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki'})

        df = df_geolocated_countriessubregionsregions_noill.round(1)


    if content_type == 'wikidata_article_qitems':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = 'geolocated' AND content = 'articles' AND period = '"+last_period+"' AND set2 = '"+geographical_entity+"' AND set1 = 'wikidata_article_qitems' ORDER BY abs_value DESC;"
        df_geolocated_extent_wikidata = pd.read_sql_query(query, conn)
        df_geolocated_extent_wikidata = df_geolocated_extent_wikidata.fillna(0).round(1)

        if geographical_entity == 'countries':
            df_geolocated_extent_wikidata = df_geolocated_extent_wikidata.rename(columns={'set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})
            df_geolocated_extent_wikidata['ISO 3166 alpha-3'] = df_geolocated_extent_wikidata['ISO 3166'].map(ISO2_3)
            df_geolocated_extent_wikidata['Country'] = df_geolocated_extent_wikidata['ISO 3166'].map(country_names)
            df_geolocated_extent_wikidata['Subregion'] = df_geolocated_extent_wikidata['ISO 3166'].map(subregions)
            df_geolocated_extent_wikidata['Region'] = df_geolocated_extent_wikidata['ISO 3166'].map(regions)

        if geographical_entity == 'subregions':
            df_geolocated_extent_wikidata = df_geolocated_extent_wikidata.rename(columns={'set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
            df_geolocated_extent_wikidata['Region'] = df_geolocated_extent_wikidata['Subregion'].map(subregions_regions)

        if geographical_entity == 'regions':
            df_geolocated_extent_wikidata = df_geolocated_extent_wikidata.rename(columns={'set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})

        df = df_geolocated_extent_wikidata.round(1)


    if content_type == 'all_wp_all_articles':

        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value FROM wdo_intersections_accumulated WHERE set1descriptor = 'geolocated' AND content = 'articles' AND period = '"+last_period+"' AND set1 = 'all_wp_all_articles' AND set2 = '"+geographical_entity+"' ORDER BY abs_value DESC;"
        df_geolocated_extent_allwparticles = pd.read_sql_query(query, conn)
        df_geolocated_extent_allwparticles = df_geolocated_extent_allwparticles.fillna(0).round(1)

        if geographical_entity == 'countries':
            df_geolocated_extent_allwparticles=df_geolocated_extent_allwparticles.rename(columns={'set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})
            df_geolocated_extent_allwparticles['ISO 3166 alpha-3'] = df_geolocated_extent_allwparticles['ISO 3166'].map(ISO2_3)
            df_geolocated_extent_allwparticles['Country'] = df_geolocated_extent_allwparticles['ISO 3166'].map(country_names)
            df_geolocated_extent_allwparticles['Subregion'] = df_geolocated_extent_allwparticles['ISO 3166'].map(subregions)
            df_geolocated_extent_allwparticles['Region'] = df_geolocated_extent_allwparticles['ISO 3166'].map(regions)

        if geographical_entity == 'subregions':
            df_geolocated_extent_allwparticles = df_geolocated_extent_allwparticles.rename(columns={'set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
            df_geolocated_extent_allwparticles['Region'] = df_geolocated_extent_allwparticles['Subregion'].map(subregions_regions)

        if geographical_entity == 'regions':
            df_geolocated_extent_allwparticles = df_geolocated_extent_allwparticles.rename(columns={'set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})

        df = df_geolocated_extent_allwparticles.round(1)



    if content_type == 'lang geographical entities':
        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value, 100*abs_value/rel_value as Total_Articles, period as Period FROM wdo_intersections_accumulated WHERE set2descriptor = 'geolocated' AND content = 'articles' AND period = '"+last_period+"' AND set1 = '"+geographical_entity+"' AND set2 IN ("+lass+") ORDER BY abs_value DESC;"
        df_languages_coverage = pd.read_sql_query(query, conn, params = langs)
        df_languages_coverage = df_languages_coverage.fillna(0)
        df_languages_coverage.Total_Articles = df_languages_coverage.Total_Articles.astype(int)


        query = "SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value, period FROM wdo_intersections_accumulated WHERE set1descriptor = 'geolocated' AND content = 'articles' AND set1 IN ("+lass+") AND set2 = '"+geographical_entity+"' AND period = '"+last_period+"' ORDER BY abs_value DESC;"
        df_geolocated_extent = pd.read_sql_query(query, conn, params = langs)
        df_geolocated_extent = df_geolocated_extent.fillna(0).round(1)


        if geographical_entity == 'countries':
            df_geolocated_coverage_countries=df_languages_coverage.rename(columns={'set1descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})
            df_geolocated_coverage_countries['Wiki (ISO 3166)'] = df_geolocated_coverage_countries['Wiki']+' ('+df_geolocated_coverage_countries['ISO 3166']+')'
            df_geolocated_coverage_countries = df_geolocated_coverage_countries.set_index('Wiki (ISO 3166)')
            df_geolocated_coverage_countries=df_geolocated_coverage_countries.drop(['Wiki','Articles','ISO 3166'],axis=1)

            df_languages_countries=df_geolocated_extent.rename(columns={'set1':'Wiki','set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})
            df_languages_countries['ISO 3166 alpha-3'] = df_languages_countries['ISO 3166'].map(ISO2_3)
            df_languages_countries['Language'] = df_languages_countries['Wiki'].map(language_names_full)
            df_languages_countries['Country'] = df_languages_countries['ISO 3166'].map(country_names)
            df_languages_countries['Subregion'] = df_languages_countries['ISO 3166'].map(subregions)
            df_languages_countries['Region'] = df_languages_countries['ISO 3166'].map(regions)
            df_languages_countries['Wiki (ISO 3166)'] = df_languages_countries['Wiki']+' ('+df_languages_countries['ISO 3166']+')'
            df_languages_countries = df_languages_countries.set_index('Wiki (ISO 3166)')

            df_lang_countries_final = pd.concat([df_languages_countries,df_geolocated_coverage_countries], axis=1, sort=False)
            df_lang_countries_final = df_lang_countries_final[['Wiki','Language', 'ISO 3166','ISO 3166 alpha-3', 'Articles', 'Coverage (%)','Extent (%)','Country', 'Subregion', 'Region','Total Articles', 'Period']].round(1)

            df = df_lang_countries_final.round(1)



        if geographical_entity == 'subregions':
            df_geolocated_coverage_subregions=df_languages_coverage.rename(columns={'set1descriptor':'Subregion','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})
            df_geolocated_coverage_subregions['Language'] = df_geolocated_coverage_subregions['Wiki'].map(language_names_full)
            df_geolocated_coverage_subregions['Wiki (Subregion)'] = df_geolocated_coverage_subregions['Wiki']+' ('+df_geolocated_coverage_subregions['Subregion']+')'
            df_geolocated_coverage_subregions = df_geolocated_coverage_subregions.set_index('Wiki (Subregion)')
            df_geolocated_coverage_subregions=df_geolocated_coverage_subregions.drop(['Wiki','Articles','Subregion'],axis=1)


            df_languages_subregions=df_geolocated_extent.rename(columns={'set1':'Wiki','set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
            df_languages_subregions['Region'] = df_languages_subregions['Subregion'].map(subregions_regions)
            df_languages_subregions['Wiki (Subregion)'] = df_languages_subregions['Wiki']+' ('+df_languages_subregions['Subregion']+')'
            df_languages_subregions = df_languages_subregions.set_index('Wiki (Subregion)')

            df_lang_subregion_final = pd.concat([df_languages_subregions,df_geolocated_coverage_subregions], axis=1, sort=False)
            df_lang_subregion_final = df_lang_subregion_final[['Wiki', 'Language', 'Subregion','Articles', 'Coverage (%)', 'Extent (%)','Region','Total Articles', 'Period']].round(1)

            df = df_lang_subregion_final.round(1)



        if geographical_entity == 'regions':
            df_geolocated_coverage_regions=df_languages_coverage.rename(columns={'set1descriptor':'Region','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})
            df_geolocated_coverage_regions['Language'] = df_geolocated_coverage_regions['Wiki'].map(language_names_full)
            df_geolocated_coverage_regions['Wiki (Region)'] = df_geolocated_coverage_regions['Wiki']+' ('+df_geolocated_coverage_regions['Region']+')'
            df_geolocated_coverage_regions = df_geolocated_coverage_regions.set_index('Wiki (Region)')
            df_geolocated_coverage_regions = df_geolocated_coverage_regions.drop(['Wiki','Articles','Region'],axis=1)

            df_languages_regions=df_geolocated_extent.rename(columns={'set1':'Wiki','set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})
            df_languages_regions['Wiki (Region)'] = df_languages_regions['Wiki']+' ('+df_languages_regions['Region']+')'
            df_languages_regions = df_languages_regions.set_index('Wiki (Region)')

            df_lang_region_final = pd.concat([df_languages_regions,df_geolocated_coverage_regions], axis=1, sort=False)
            df_lang_region_final = df_lang_region_final[['Wiki', 'Language', 'Region', 'Articles', 'Coverage (%)', 'Extent (%)','Total Articles', 'Period']].round(1)

            df = df_lang_region_final.round(1)


    return df


### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

# GEOLOCATED ARTICLES EXTENT IN WIKIPEDIA LANGUAGE EDITIONS
conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor()
query = "SELECT set1 as Wiki, abs_value as Geolocated_articles, rel_value as Geolocated_percent, abs_value*100/rel_value as Articles FROM wdo_intersections_accumulated WHERE set1descriptor = 'wp' AND set2 = 'languagecode' AND set2descriptor = 'geolocated' AND content = 'articles' AND period = '"+last_period+"' ORDER BY 1 DESC;"
df_geolocated_articles = pd.read_sql_query(query, conn)
df_geolocated_articles = df_geolocated_articles.set_index('Wiki')

query = "SELECT set1 as Wiki, abs_value as CCC_geolocated_art, rel_value as CCC_geolocated_percent, abs_value*100/rel_value as CCC_Articles FROM wdo_intersections_accumulated WHERE set1descriptor = 'wp' AND set2descriptor = 'ccc_geolocated' AND content = 'articles' AND set1=set2 AND period = '"+last_period+"' ORDER BY 1 DESC;"
df_ccc_geolocated_articles = pd.read_sql_query(query, conn)
df_ccc_geolocated_articles = df_ccc_geolocated_articles.set_index('Wiki')

df_ccc_geolocated_final = pd.concat([df_geolocated_articles,df_ccc_geolocated_articles], axis=1, sort=False)
df_ccc_geolocated_final['Region']=languages.region
for x in df_ccc_geolocated_final.index.values.tolist():
    if ';' in df_ccc_geolocated_final.loc[x]['Region']: df_ccc_geolocated_final.at[x, 'Region'] = df_ccc_geolocated_final.loc[x]['Region'].split(';')[0]

df_ccc_geolocated_final['Subregion']=languages.subregion
for x in df_ccc_geolocated_final.index.values.tolist():
    if ';' in df_ccc_geolocated_final.loc[x]['Subregion']: df_ccc_geolocated_final.at[x, 'Subregion'] = df_ccc_geolocated_final.loc[x]['Subregion'].split(';')[0]

df_ccc_geolocated_final = df_ccc_geolocated_final.fillna(0).round(1)
df_ccc_geolocated_final.CCC_geolocated_art = df_ccc_geolocated_final.CCC_geolocated_art.astype(int)
df_ccc_geolocated_final.Articles = df_ccc_geolocated_final.Articles.astype(int)
df_ccc_geolocated_final.CCC_Articles = df_ccc_geolocated_final.CCC_Articles.astype(int)

df_ccc_geolocated_final = df_ccc_geolocated_final.reset_index()
df_ccc_geolocated_final=df_ccc_geolocated_final.rename(columns={'index':'Wiki','Geolocated_articles':'Geolocated Art.', 'CCC_Articles':'CCC Articles','Geolocated_percent':'Geolocated (%)','CCC_geolocated_art':'CCC Geolocated Art.','CCC_geolocated_percent':'CCC Geolocated (%)'})
df_ccc_geolocated_final['Language'] = df_ccc_geolocated_final['Wiki'].map(language_names_full)
df_ccc_geolocated_final = df_ccc_geolocated_final.loc[(df_ccc_geolocated_final['Region']!='')]




### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app3 = Dash(__name__, server = app, url_base_pathname= webtype + '/geography_gap/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app3.config['suppress_callback_exceptions']=True


title = "Wikipedia Geolocated Articles (Geography Gap)"
dash_app3.title = title+title_addenda
text_heatmap = ''

dash_app3.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that explain how well each Wikipedia language edition covers 
        the Geolocated articles.
        They illustrate the content geography gap, that is the imbalances in representing the different geographical entities (country, subregion and world region). '''.replace('  ', '')),

    # dcc.Markdown('''

    #     The graphs answer the following questions:
    #     * What is the extent of geolocated articles in each Wikipedia language edition?
    #     * What is the extent of geolocated articles without interwiki links in each country, subregion and world region?
    #     * What is the extent of geographical entities in the sum of all geolocated articles in all languages, all unique geolocated articles (qitems), and in each Wikipedia language edition?
    #     * How well do Wikipedia language editions cover the available geolocated articles in each country?
    #     * What Wikipedia language editions accumulated more geolocated articles in each country, subregion, and region?
    #     '''),
    html.Br(),

    dcc.Tabs([
        dcc.Tab(label='Geolocated Articles Without Interwiki (Scatterplot)', children=[
		    html.Br(),

		    # html.H5('Geolocated Articles Without Interwiki Links'),

		    dcc.Markdown('''* **What is the extent of geolocated articles without Interwiki links in each country, subregion and world region?**'''),

            html.Div(
            html.P('Select a geographical entity'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

		    dcc.Dropdown(
		        id='geolocateddropdown_coverage',
		        options = [{'label': k, 'value': k} for k in ['Countries','Subregions','Regions']],
		        value = 'Countries',
		        style={'width': '190px'}
		     ),
		    dcc.Graph(id = 'scatterplot_nointerwiki'),

#		    html.Hr(),
            dcc.Markdown('''The scatterplot graph shows for a list of countries, subregions or world regions the number of articles they contain (Y-axis) and on the percentage of articles without any interwiki links (X-axis). Wikipedia language editions are colored according to the world region (continent) where the language is spoken. You can double-click the region name to see only the languages from that region.'''),

        ]),

		###
        dcc.Tab(label='Extent of Geolocated Art. in Wikipedias/CCC (Scatterplot)', children=[
		    html.Br(),

		    # html.H5('Extent of Geolocated Articles in Wikipedia Language Editions and Language CCC'),

		    dcc.Markdown('''* **What is the extent of geolocated articles in each Wikipedia language edition and in each language CCC?**'''.replace('  ', '')),

            html.Div(
            html.P('Select the entity'),
            style={'display': 'inline-block','width': '200px'}),
            html.Br(),

		    dcc.Dropdown(
		        id='geolocatedarticlesdropdown_coverage',
		        options = [{'label': k, 'value': k} for k in ['Wikipedia','CCC']],
		        value = 'Wikipedia',
		        style={'width': '190px'}
		     ),
		    dcc.Graph(id = 'scatterplot_geolocatedextent'),

            dcc.Markdown('''The scatterplot graph shows the extent of geolocated articles and CCC geolocated articles in each Wikipedia language edition. CCC geolocated articles are geolocated articles that belong to the language CCC. Wikipedia language editions are colored according to the world region (continent) where the language is spoken. You can double-click the region name to see only the languages from that region.
             '''),

        ]),

		####
        dcc.Tab(label='Extent of Geolocated Entities in Geolocated Articles (Treemap)', children=[
		    html.Br(),

		    # html.H5("Extent of Countries, Subregions, Regions in Wikipedia Content", style={'textAlign':'left'}),

		    dcc.Markdown('''
		        * **What is the extent of geographical entities in the sum of all geolocated articles in all languages, all unique geolocated articles (qitems), and in each Wikipedia language edition?**
		        '''.replace('  ', '')),

		    html.Br(),

		    html.Div(
		    html.P('Select two projects to compare'),
		    style={'display': 'inline-block','width': '200px'}),
		    html.Br(),

		    html.Div(
		    dcc.Dropdown(
		        id='sourcelangdropdown_treemapgeolocatedcoverage',
		        options = [{'label': k, 'value': k} for k in treemapoptions],
		        value = 'All Geolocated Articles',
		        style={'width': '240px'}
		     ), style={'display': 'inline-block','width': '250px'}),

		    html.Div(
		    dcc.Dropdown(
		        id='sourcelangdropdown_treemapgeolocatedcoverage2',
		        options = [{'label': k, 'value': k} for k in treemapoptions],
		        value = 'Wikidata Geolocated Qitems',
		        style={'width': '240px'}
		     ), style={'display': 'inline-block','width': '250px'}),

		    html.Br(),
		    html.Div(
		    html.P('Select a geographical entity'),
		    style={'display': 'inline-block','width': '200px'}),
		    html.Br(),

		    html.Div(
		    dcc.Dropdown(
		        id='sourceentitytype_treemap',
		        options = [{'label': k, 'value': k} for k in ['Countries','Subregions','Regions']],
		        value = 'Countries',
		        style={'width': '190px'}
		     ), style={'display': 'inline-block','width': '200px'}),

		    html.Br(),

		    dcc.Graph(id = 'treemap_geolocated_coverage'),

            dcc.Markdown('''The treemap graphs show for two selected projects both the extent and the coverage of different geographical entities (countries, subregions and world regions). The size of the tiles and the colorscale (orange-dark blue) is according to the extent the geographical entities take in the selected project, which can be the sum of all geolocated articles in all Wikipedia language editions, all the Wikidata geolocated qitems or specific Wikipedia language editions. When you hover on a tile you can read the same information regarding the coverage and extent plus the number of articles.
                '''.replace('  ', '')),
        ]),

		####
        dcc.Tab(label='Coverage of Countries Geolocated Articles (Map)', children=[
		    html.Br(),

		    # html.H5("Coverage of Countries Geolocated Articles by Wikipedia Language Editions", style={'textAlign':'left'}),

		    dcc.Markdown('''
		        * **How well do Wikipedia language editions cover the available geolocated articles in each country?**
		        '''.replace('  ', '')),

		    html.Br(),

		    html.Div(
		    html.P('Select a Wikipedia'),
		    style={'display': 'inline-block','width': '200px'}),
		    html.Br(),

		    html.Div(
		    dcc.Dropdown(
		        id='sourcelangdropdown_mapgeolocatedcoverage',
		        options = [{'label': k, 'value': k} for k in language_names_list],
		        value = 'Italian (it)',
		        style={'width': '240px'}
		     ), style={'display': 'inline-block','width': '250px'}),

		    html.Br(),
		    dcc.Graph(id = 'choropleth_map_countries_coverage'),
		    dcc.Graph(id = 'choropleth_map_countries_extent'),

            dcc.Markdown('''
                The map graphs show the coverage of the Total Articles geolocated in the following territories by a Wikipedia language edition, and the extent they take in the group of geolocated articles of this Wikipedia language edition. You can hover on each country and see the number of covered articles and total articles, the percentages of coverage and extent, as well as other information regarding the country.
                '''.replace('  ', '')),
        ]),

		####
        dcc.Tab(label='Wikipedias by Coverage of Geographical Entities (Barchart)', children=[
		    html.Br(),
	
		    # html.H5('Wikipedia Language Editions Coverage of Countries, Subregions and Regions'),
			dcc.Markdown('''* **What Wikipedia language editions cover best each country, subregion, and region geolocated articles?**'''),

		    html.Div(html.P('Select a Geographical Entity'), 
		    	style={'display': 'inline-block','width': '400px'}),

		    html.Div(
		    html.P('Select all languages or a specific group'),
		    style={'display': 'inline-block', 'width': '400px'}),

		    html.Br(),
		    html.Div(
		    dcc.Dropdown(
		        id='sourceentitytype',
		        options = [{'label': k, 'value': k} for k in ['Countries','Subregions','Regions']],
		        value = 'Countries',
		        style={'width': '190px'}
		     ), style={'display': 'inline-block','width': '200px'}),

		    html.Div(
		    dcc.Dropdown(
		        id='sourceentityspecific',
		#        options = options,
		        value = 'South Africa',
		        style={'width': '190px'}
		     ), style={'display': 'inline-block','width': '200px'}),

		    html.Div(
		    dcc.RadioItems(
		        id='specific_group_radio',
		        options=[{'label':'All languages','value':'all'},{'label':'Select a group','value':'group'}],
		        value='group',
		        labelStyle={'display': 'inline-block', "margin": "0px 5px 0px 5px"},
		        style={'width': '390px'}
		     ), style={'display': 'inline-block','width': '400px'}),

		    html.Br(),
		    html.Br(),

		    html.Div(
		    html.P('Select a group of Wikipedias'),
		    style={'display': 'inline-block','width': '200px'}),

		    html.Br(),


		    html.Div(
		    dcc.Dropdown(
		        id='grouplangdropdown',
		        options=[{'label': k, 'value': k} for k in lang_groups],
		        value='Top 10',
		        disabled = False,
		        style={'width': '190px'}
		     ), style={'display': 'inline-block','width': '200px'}),

		    html.Div(
		    dcc.Dropdown(id='sourcelangdropdown_geography_gap',
		        options = [{'label': k, 'value': k} for k in language_names_list],
		        multi=True,
		        disabled = False,
		        style={'width': '990px'}
		     ), style={'display': 'inline-block','width': '1000px'}),
		    html.Br(),

		    dcc.Graph(id = 'barchart_coverage'),


            dcc.Markdown('''The barchart graph shows for a selected geographical entity (country, world subregion or region) the degree of coverage of its geolocated articles/Qitems in them by all Wikipedia language editions or a selected group. By hovering in each Wikipedia you can see the coverage percentage as well, the extent it takes in percentage of their total number of articles, the total number of articles geolocated in that geographical entity as well as other geographical information.'''.replace('  ', '')),
        ]),
    ]),

	footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# SCATTERPLOT NO INTERWIKI
@dash_app3.callback(
    Output('scatterplot_nointerwiki', 'figure'), [Input('geolocateddropdown_coverage', 'value')])
def update_scatterplot(value):
   
    df = params_to_df([], 'zero_ill', value.lower())
    if value == 'Countries': 
        entity = 'Country'
        text = 'ISO 3166'

    if value == 'Subregions':
        entity = 'Subregion'
        text = entity

    if value == 'Regions':
        entity = 'Region'
        text = entity

    fig = px.scatter(df, x="Articles No-IW (%)", y="Geolocated Articles", color="Region", log_x=False, log_y=True,hover_data=[entity,'Geolocated Articles No-IW'],text=text) #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",

    fig.update_traces(textposition='top center')
    fig.update_layout(
        autosize=True,
#        width=700,
        height=600,
#        paper_bgcolor="White",
#        title_text='Languages CCC Extent % (Left) and Languages CCC Extent % (Right)',
        title_x=0.5,
    )

    return fig



# SCATTERPLOT GEOLOCATED ARTICLES
@dash_app3.callback(
    Output('scatterplot_geolocatedextent', 'figure'), [Input('geolocatedarticlesdropdown_coverage', 'value')])
def update_scatterplot(value):

	if value == 'Wikipedia':
		fig = px.scatter(df_ccc_geolocated_final, x="Geolocated (%)", y="Articles", color="Region", log_x=False, log_y=True,hover_data=['Subregion','Geolocated Art.'],text="Wiki")


	if value == 'CCC':
		fig = px.scatter(df_ccc_geolocated_final, x="CCC Geolocated (%)", y="CCC Articles", color="Region", log_x=False, log_y=True,hover_data=['Subregion','CCC Geolocated Art.'],text="Wiki")

	fig.update_traces(textposition='top center')

	fig.update_layout(
	    autosize=True,
	#        width=700,
	    height=600,
	#        paper_bgcolor="White",
	#        title_text='Languages CCC Extent % (Left) and Languages CCC Extent % (Right)',
	    title_x=0.5,
	)

	return fig



# Geographical Entity Type to Geographical Entity
@dash_app3.callback(
    Output('sourceentityspecific', 'options'), [Input('sourceentitytype', 'value')])
def update_scatterplot(value):

	if value == 'Countries': options = countries_list
	if value == 'Subregions': options = subregions_list
	if value == 'Regions': options = regions_list

	return [{'label': i, 'value': i} for i in options]



@dash_app3.callback(
    [Output('sourcelangdropdown_geography_gap', 'disabled'), Output('grouplangdropdown', 'disabled'), Output('grouplangdropdown','value')],
    [Input('specific_group_radio','value')])
def set_radio_languages(radio_value):

	if radio_value == "all":
		return True, True, []
	else:
		return False, False, "Top 10"

@dash_app3.callback(
    dash.dependencies.Output('sourcelangdropdown_geography_gap', 'value'),
    [dash.dependencies.Input('grouplangdropdown', 'value'), Input('specific_group_radio','value')])
def set_langs_options_geography(selected_group, radio_value):
    langolist, langlistnames = wikilanguages_utils.get_langs_group(selected_group, None, None, None, wikipedialanguage_numberarticles, territories, languages)
    available_options = [{'label': i, 'value': i} for i in langlistnames.keys()]
    list_options = []
    for item in available_options:
        list_options.append(item['label'])
    re = sorted(list_options,reverse=False)

    if radio_value == "all":
	    re = []

    return re





@dash_app3.callback(Output('barchart_coverage', 'figure'),
    [Input('sourceentityspecific', 'value'), Input('sourceentitytype', 'value'), Input('sourcelangdropdown_geography_gap', 'value')])
def update_barchart(entityspecific, entitytype, languages):

    langs = []
    if type(languages) == list and len(languages) != 0:
    	for x in languages: 
    		langs.append(language_names[x])

    elif type(languages) == str:
    	langs.append(language_names[languages])

    else:
    	langs = wikilanguagecodes


    if entitytype == 'Countries':
        df = params_to_df(langs, 'lang geographical entities', 'countries')
        df = df.loc[(df['Country'] == entityspecific)]
        df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

        hover_data = ['Country','ISO 3166','Subregion','Region','Coverage (%)','Country Total Articles','Covered Articles','Language','Period']
        fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)

    if entitytype == 'Subregions':
        # df = params_to_df(langs, 'lang geographical entities', 'subregions')
        df = params_to_df(langs, 'lang geographical entities', 'countries')
        df = df.loc[(df['Subregion'] == entityspecific)]
        df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

        df['Subregion Covered Articles'] = df.groupby('Language')['Covered Articles'].transform('sum')
        df['Subregion Total Articles'] = df.groupby('Language')['Country Total Articles'].transform('sum')
        df['Subregion Coverage (%)'] = (100*df['Subregion Covered Articles']/df['Subregion Total Articles']).round(1)
        hover_data = ['Country','ISO 3166','Subregion','Region','Country Total Articles','Covered Articles','Coverage (%)','Subregion Covered Articles','Subregion Total Articles','Subregion Coverage (%)']

        df['Region Covered Articles'] = df.groupby('Language')['Articles'].transform('sum')
        df['Region Total Articles'] = df.groupby('Language')['Articles'].transform('sum')

        fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)


    if entitytype == 'Regions':
        # df = params_to_df(langs, 'lang geographical entities', 'regions')
        df = params_to_df(langs, 'lang geographical entities', 'countries')
        df = df.loc[(df['Region'] == entityspecific)]
        df = df.rename(columns={'Total Articles':'Country Total Articles', 'Articles':'Covered Articles'})

        df['Region Covered Articles'] = df.groupby('Language')['Covered Articles'].transform('sum')
        df['Region Total Articles'] = df.groupby('Language')['Country Total Articles'].transform('sum')
        df['Region Coverage (%)'] = (100*df['Region Covered Articles']/df['Region Total Articles']).round(1)
        hover_data = ['Country','ISO 3166','Subregion','Region','Country Total Articles','Covered Articles','Coverage (%)','Region Covered Articles','Region Total Articles','Region Coverage (%)']

        fig = px.bar(df, x='Language', y='Covered Articles', hover_data=hover_data, color='Coverage (%)', height=400)

    fig.update_layout(
        barmode='stack', 
        xaxis={'categoryorder':'total descending','titlefont_size':12, 'tickfont_size':12
        },
        yaxis=dict(
            title='Covered Articles',
            titlefont_size=12,
            tickfont_size=10,
        ))
    return fig





# TREEMAP WIKIPEDIA LANGUAGES - COUNTRIES, SUBREGIONS, REGIONS COVERAGE
@dash_app3.callback(
    Output('treemap_geolocated_coverage', 'figure'),
    [Input('sourcelangdropdown_treemapgeolocatedcoverage','value'),Input('sourcelangdropdown_treemapgeolocatedcoverage2', 'value'),Input('sourceentitytype_treemap','value')])
def update_treemap_coverage(project,project2,entitytype):

    projectname = project; projectname2 = project2
    if '(' in projectname: projectname+= ' Geolocated Articles'
    if '(' in projectname2: projectname2+= ' Geolocated Articles'
    project = treemapdict[project]
    project2 = treemapdict[project2]
    #	print (project,project2,entitytype)


    if project == "All Geolocated Articles":
    	texttemplate = "<b>%{label} </b><br>Extent: %{value}%"
    	hovertemplate = '<b>%{label} </b><br>ISO 3166: %{text}<br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    elif project == "Wikidata Geolocated Qitems":
    	texttemplate = "<b>%{label} </b><br>Extent: %{value}%"
    	hovertemplate = '<b>%{label} </b><br>ISO 3166: %{text}<br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    else:
    	text = 'Coverage (%)'
    	texttemplate = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%"
    	hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>'


    if project2 == "All Geolocated Articles":
    	texttemplate2 = "<b>%{label} </b><br>Extent: %{value}%"
    	hovertemplate2 = '<b>%{label} </b><br>ISO 3166: %{text}<br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    elif project2 == "Wikidata Geolocated Qitems":
    	texttemplate2 = "<b>%{label} </b><br>Extent: %{value}%"
    	hovertemplate2 = '<b>%{label} </b><br>ISO 3166: %{text}<br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    else:
    	text2 = 'Coverage (%)'
    	texttemplate2 = "<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%"
    	hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Cov.: %{text}%<br>Art.: %{customdata}<br><extra></extra>'


    if entitytype == 'Countries':
        labels = "Country"
        if project == "All Geolocated Articles":
            text = 'ISO 3166'
            df = params_to_df(project, 'all_wp_all_articles', 'countries')

        elif project == "Wikidata Geolocated Qitems":
            text = 'ISO 3166'
            df = params_to_df(project, 'wikidata_article_qitems', 'countries')
        else:
            df = params_to_df(project, 'lang geographical entities', 'countries')

        if project2 == "All Geolocated Articles":
            text2 = 'ISO 3166'
            df2 = params_to_df(project2, 'all_wp_all_articles', 'countries')
        if project2 == "Wikidata Geolocated Qitems":
            text2 = 'ISO 3166'
            df2 = params_to_df(project2, 'wikidata_article_qitems', 'countries')
        else:
            df2 = params_to_df(project2, 'lang geographical entities', 'countries')


    if entitytype == 'Subregions':
    	labels = "Subregion"

    	if project == "All Geolocated Articles":
            text = 'Region'
            df = params_to_df(project, 'all_wp_all_articles', 'subregions')
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    	elif project == "Wikidata Geolocated Qitems":
            text = 'Region'
            df = params_to_df(project, 'wikidata_article_qitems', 'subregions')
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    	else:
            df = params_to_df(project, 'lang geographical entities', 'subregions')

    	if project2 == "All Geolocated Articles":
            text2 = 'Region'
            df2 = params_to_df(project2, 'all_wp_all_articles', 'subregions')
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    	elif project2 == "Wikidata Geolocated Qitems":
            text2 = 'Region'
            df2 = params_to_df(project2, 'wikidata_article_qitems', 'subregions')
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'
    	else:
            df2 = params_to_df(project2, 'lang geographical entities', 'subregions')


    if entitytype == 'Regions':
    	labels = "Region"

    	if project == "All Geolocated Articles":
            text = 'Region'
            df = params_to_df(project, 'all_wp_all_articles', 'regions')
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    	elif project == "Wikidata Geolocated Qitems":
            text = 'Region'
            df = params_to_df(project, 'wikidata_article_qitems', 'regions')
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'
    	else:
            df = params_to_df(project, 'lang geographical entities', 'regions')

    	if project2 == "All Geolocated Articles":
            df2 = params_to_df(project2, 'all_wp_all_articles', 'regions')
            text2 = 'Region'
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

    	elif project2 == "Wikidata Geolocated Qitems":
            df2 = params_to_df(project2, 'wikidata_article_qitems', 'regions')
            text2 = 'Region'
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'
    	else:
            df2 = params_to_df(project2, 'lang geographical entities', 'regions')


    parents = list()
    for x in df.index: parents.append('')

    parents2 = list()
    for x in df2.index: parents2.append('')

    fig = make_subplots(
	    cols = 2, rows = 1,
	    column_widths = [0.45, 0.45],
	    # subplot_titles = ('CCC Coverage % (Size)<br />&nbsp;<br />', 'CCC Extent % (Size)<br />&nbsp;<br />'),
	    specs = [[{'type': 'treemap', 'rowspan': 1}, {'type': 'treemap'}]]
	)

    fig.add_trace(go.Treemap(
	    parents = parents,
	    labels = df[labels],
	    customdata = df['Articles'],
	    values = df['Extent (%)'],
	    text = df[text],
	    texttemplate = texttemplate,
	    hovertemplate= hovertemplate,
	    marker_colorscale = 'RdBu',
	    ),
	        row=1, col=1)

    fig.add_trace(go.Treemap(
	    parents = parents,
	    labels = df2[labels],
	    customdata = df2['Articles'],
	    values = df2['Extent (%)'],
	    text = df2[text2],
	    texttemplate = texttemplate,
	    hovertemplate= hovertemplate2,
	    marker_colorscale = 'RdBu',
	    ),
	        row=1, col=2)

    fig.update_layout(
	    autosize=True,
	#        width=700,
	    height=900,
        paper_bgcolor="White",
        title_text=projectname+' (Left) and '+projectname2+' (Right)',
        title_font_size=12,
	    title_x=0.5,

	)

    return fig




# MAP WIKIPEDIA LANGUAGES - COUNTRIES
@dash_app3.callback(
    Output('choropleth_map_countries_coverage', 'figure'),
    [Input('sourcelangdropdown_mapgeolocatedcoverage','value')])
def update_map_coverage(project):

    df = params_to_df(language_names[project], 'lang geographical entities', 'countries')
    df = df.rename(columns={'Articles':'Covered Articles'})

    fig = px.choropleth(df, locations="ISO 3166 alpha-3",
	                    color="Coverage (%)",
	                    hover_data=['Country', 'Subregion', 'Region', 'ISO 3166', 'Coverage (%)', 'Extent (%)', 'Covered Articles', 'Total Articles'], # column to add to hover information
	                    color_continuous_scale=px.colors.sequential.Plasma)

    fig.update_layout(
        autosize=True,
        height=600,
        paper_bgcolor="White",
        title_text=project+' Wikipedia Coverage of Total Geolocated Articles by Country',
        title_font_size=12,
        title_x=0.5,
    )

    return fig


@dash_app3.callback(
    Output('choropleth_map_countries_extent', 'figure'),
    [Input('sourcelangdropdown_mapgeolocatedcoverage','value')])
def update_map_extent(project):

    df = params_to_df(language_names[project], 'lang geographical entities', 'countries')
    df = df.rename(columns={'Articles':'Covered Articles'})

    fig2 = px.choropleth(df, locations="ISO 3166 alpha-3",
	                    color="Extent (%)",
	                    hover_data=['Country', 'Subregion', 'Region', 'ISO 3166', 'Coverage (%)', 'Extent (%)', 'Covered Articles', 'Total Articles'], # column to add to hover information
	                    color_continuous_scale=px.colors.sequential.Plasma)

    fig2.update_layout(
	    autosize=True,
	#        width=700,
	    height=800,
        paper_bgcolor="White",
        title_font_size=12,
        title_text='Geolocated Articles by Country Extent in '+project+' Wikipedia',
	    title_x=0.5,
	)

    return fig2