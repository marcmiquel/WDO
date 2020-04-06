import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


treemapoptions = ['All Geolocated Articles','Wikidata Geolocated Qitems']+language_names_list
treemapdict = language_names; treemapdict['All Geolocated Articles']='All Geolocated Articles'; treemapdict['Wikidata Geolocated Qitems']='Wikidata Geolocated Qitems'; 
language_names_inv = {v: k for k, v in language_names.items()}

ISO2_3 = {'AF':'AFG','AL':'ALB','DZ':'DZA','AS':'ASM','AD':'AND','AO':'AGO','AI':'AIA','AQ':'ATA','AG':'ATG','AR':'ARG','AM':'ARM','AW':'ABW','AU':'AUS','AT':'AUT','AZ':'AZE','BS':'BHS','BH':'BHR','BD':'BGD','BB':'BRB','BY':'BLR','BE':'BEL','BZ':'BLZ','BJ':'BEN','BM':'BMU','BT':'BTN','BO':'BOL','BQ':'BES','BA':'BIH','BW':'BWA','BV':'BVT','BR':'BRA','IO':'IOT','BN':'BRN','BG':'BGR','BF':'BFA','BI':'BDI','CV':'CPV','KH':'KHM','CM':'CMR','CA':'CAN','KY':'CYM','CF':'CAF','TD':'TCD','CL':'CHL','CN':'CHN','CX':'CXR','CC':'CCK','CO':'COL','KM':'COM','CD':'COD','CG':'COG','CK':'COK','CR':'CRI','HR':'HRV','CU':'CUB','CW':'CUW','CY':'CYP','CZ':'CZE','CI':'CIV','DK':'DNK','DJ':'DJI','DM':'DMA','DO':'DOM','EC':'ECU','EG':'EGY','SV':'SLV','GQ':'GNQ','ER':'ERI','EE':'EST','SZ':'SWZ','ET':'ETH','FK':'FLK','FO':'FRO','FJ':'FJI','FI':'FIN','FR':'FRA','GF':'GUF','PF':'PYF','TF':'ATF','GA':'GAB','GM':'GMB','GE':'GEO','DE':'DEU','GH':'GHA','GI':'GIB','GR':'GRC','GL':'GRL','GD':'GRD','GP':'GLP','GU':'GUM','GT':'GTM','GG':'GGY','GN':'GIN','GW':'GNB','GY':'GUY','HT':'HTI','HM':'HMD','VA':'VAT','HN':'HND','HK':'HKG','HU':'HUN','IS':'ISL','IN':'IND','ID':'IDN','IR':'IRN','IQ':'IRQ','IE':'IRL','IM':'IMN','IL':'ISR','IT':'ITA','JM':'JAM','JP':'JPN','JE':'JEY','JO':'JOR','KZ':'KAZ','KE':'KEN','KI':'KIR','KP':'PRK','KR':'KOR','KW':'KWT','KG':'KGZ','LA':'LAO','LV':'LVA','LB':'LBN','LS':'LSO','LR':'LBR','LY':'LBY','LI':'LIE','LT':'LTU','LU':'LUX','MO':'MAC','MG':'MDG','MW':'MWI','MY':'MYS','MV':'MDV','ML':'MLI','MT':'MLT','MH':'MHL','MQ':'MTQ','MR':'MRT','MU':'MUS','YT':'MYT','MX':'MEX','FM':'FSM','MD':'MDA','MC':'MCO','MN':'MNG','ME':'MNE','MS':'MSR','MA':'MAR','MZ':'MOZ','MM':'MMR','NA':'NAM','NR':'NRU','NP':'NPL','NL':'NLD','NC':'NCL','NZ':'NZL','NI':'NIC','NE':'NER','NG':'NGA','NU':'NIU','NF':'NFK','MP':'MNP','NO':'NOR','OM':'OMN','PK':'PAK','PW':'PLW','PS':'PSE','PA':'PAN','PG':'PNG','PY':'PRY','PE':'PER','PH':'PHL','PN':'PCN','PL':'POL','PT':'PRT','PR':'PRI','QA':'QAT','MK':'MKD','RO':'ROU','RU':'RUS','RW':'RWA','RE':'REU','BL':'BLM','SH':'SHN','KN':'KNA','LC':'LCA','MF':'MAF','PM':'SPM','VC':'VCT','WS':'WSM','SM':'SMR','ST':'STP','SA':'SAU','SN':'SEN','RS':'SRB','SC':'SYC','SL':'SLE','SG':'SGP','SX':'SXM','SK':'SVK','SI':'SVN','SB':'SLB','SO':'SOM','ZA':'ZAF','GS':'SGS','SS':'SSD','ES':'ESP','LK':'LKA','SD':'SDN','SR':'SUR','SJ':'SJM','SE':'SWE','CH':'CHE','SY':'SYR','TW':'TWN','TJ':'TJK','TZ':'TZA','TH':'THA','TL':'TLS','TG':'TGO','TK':'TKL','TO':'TON','TT':'TTO','TN':'TUN','TR':'TUR','TM':'TKM','TC':'TCA','TV':'TUV','UG':'UGA','UA':'UKR','AE':'ARE','GB':'GBR','UM':'UMI','US':'USA','UY':'URY','UZ':'UZB','VU':'VUT','VE':'VEN','VN':'VNM','VG':'VGB','VI':'VIR','WF':'WLF','EH':'ESH','YE':'YEM','ZM':'ZMB','ZW':'ZWE','AX':'ALA'}

conn = sqlite3.connect(databases_path + 'stats.db'); cursor = conn.cursor() 
conn2 = sqlite3.connect(databases_path + 'wikipedia_diversity.db'); cursor2 = conn2.cursor() 


#### COVERAGE DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

subregions_regions = {}
for x,y in subregions.items():
    subregions_regions[y]=regions[x]


# GEOLOCATED ARTICLES EXTENT IN WIKIPEDIA LANGUAGE EDITIONS
query = 'SELECT set1 as Wiki, abs_value as Geolocated_articles, rel_value as Geolocated_percent, abs_value*100/rel_value FROM wcdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2 = "languagecode" AND set2descriptor = "geolocated" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY 1 DESC;'
df_geolocated_articles = pd.read_sql_query(query, conn)
df_geolocated_articles = df_geolocated_articles.set_index('Wiki')
df_geolocated_articles = df_geolocated_articles.rename(columns={'abs_value*100/rel_value':'Articles'})

query = 'SELECT set1 as Wiki, abs_value as CCC_geolocated_art, rel_value as CCC_geolocated_percent, abs_value*100/rel_value FROM wcdo_intersections_accumulated WHERE set1descriptor = "wp" AND set2descriptor = "ccc_geolocated" AND content = "articles" AND set1=set2 AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY 1 DESC;'
df_ccc_geolocated_articles = pd.read_sql_query(query, conn)
df_ccc_geolocated_articles = df_ccc_geolocated_articles.set_index('Wiki')
df_ccc_geolocated_articles = df_ccc_geolocated_articles.rename(columns={'abs_value*100/rel_value':'CCC_Articles'})

df_ccc_geolocated_final = pd.concat([df_geolocated_articles,df_ccc_geolocated_articles], axis=1, sort=False)

df_ccc_geolocated_final['Region']=languages.region
for x in df_ccc_geolocated_final.index.values.tolist():
    if ';' in df_ccc_geolocated_final.loc[x]['Region']: df_ccc_geolocated_final.at[x, 'Region'] = df_ccc_geolocated_final.loc[x]['Region'].split(';')[0]

df_ccc_geolocated_final['Subregion']=languages.subregion
for x in df_ccc_geolocated_final.index.values.tolist():
    if ';' in df_ccc_geolocated_final.loc[x]['Subregion']: df_ccc_geolocated_final.at[x, 'Subregion'] = df_ccc_geolocated_final.loc[x]['Subregion'].split(';')[0]

df_ccc_geolocated_final = df_ccc_geolocated_final.fillna(0)
df_ccc_geolocated_final = df_ccc_geolocated_final.round(1)
df_ccc_geolocated_final.CCC_geolocated_art = df_ccc_geolocated_final.CCC_geolocated_art.astype(int)
df_ccc_geolocated_final.Articles = df_ccc_geolocated_final.Articles.astype(int)
df_ccc_geolocated_final.CCC_Articles = df_ccc_geolocated_final.CCC_Articles.astype(int)

df_ccc_geolocated_final = df_ccc_geolocated_final.reset_index()


df_ccc_geolocated_final=df_ccc_geolocated_final.rename(columns={'index':'Wiki','Geolocated_articles':'Geolocated Art.', 'CCC_Articles':'CCC Articles','Geolocated_percent':'Geolocated (%)','CCC_geolocated_art':'CCC Geolocated Art.','CCC_geolocated_percent':'CCC Geolocated (%)'})
df_ccc_geolocated_final['Language'] = df_ccc_geolocated_final['Wiki'].map(language_names_full)
df_ccc_geolocated_final = df_ccc_geolocated_final.loc[(df_ccc_geolocated_final['Region']!='')]



# # EXTENT GENERAL DATA
query = 'SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value, period FROM wcdo_intersections_accumulated WHERE set1descriptor = "geolocated" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY abs_value DESC;'
df_geolocated_extent = pd.read_sql_query(query, conn)
df_geolocated_extent = df_geolocated_extent.fillna(0).round(1)


# COUNTRIES, SUBREGIONS, REGIONS EXTENT IN WIKIDATA GEOLOCATED QITEMS
df_geolocated_extent_wikidata = df_geolocated_extent[df_geolocated_extent['set1']=="wikidata_article_qitems"]
df_countries_wikidata = df_geolocated_extent_wikidata[df_geolocated_extent_wikidata['set2']=="countries"]
df_subregions_wikidata = df_geolocated_extent_wikidata[df_geolocated_extent_wikidata['set2']=="subregions"]
df_regions_wikidata = df_geolocated_extent_wikidata[df_geolocated_extent_wikidata['set2']=="regions"]

df_subregions_wikidata=df_subregions_wikidata.rename(columns={'set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
df_regions_wikidata=df_regions_wikidata.rename(columns={'set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})
df_countries_wikidata=df_countries_wikidata.rename(columns={'set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})
df_countries_wikidata['ISO 3166 alpha-3'] = df_countries_wikidata['ISO 3166'].map(ISO2_3)
df_countries_wikidata['Country'] = df_countries_wikidata['ISO 3166'].map(country_names)
df_countries_wikidata['Subregion'] = df_countries_wikidata['ISO 3166'].map(subregions)
df_countries_wikidata['Region'] = df_countries_wikidata['ISO 3166'].map(regions)
df_subregions_wikidata['Region'] = df_subregions_wikidata['Subregion'].map(subregions_regions)

# print (df_countries_wikidata.head(10))
# print (df_subregions_wikidata.head(10))
# print (df_regions_wikidata.head(10))


# # COUNTRIES, SUBREGIONS, REGIONS EXTENT IN THE SUM OF ALL WIKIPEDIA LANGUAGE EDITIONS GEOLOCATED ARTICLES
df_geolocated_extent_all_wp_geolocated_articles = df_geolocated_extent[df_geolocated_extent['set1']=="all_wp_all_articles"]
df_countries_allwparticles = df_geolocated_extent_all_wp_geolocated_articles[df_geolocated_extent_all_wp_geolocated_articles['set2']=="countries"]
df_subregions_allwparticles = df_geolocated_extent_all_wp_geolocated_articles[df_geolocated_extent_all_wp_geolocated_articles['set2']=="subregions"]
df_regions_allwparticles = df_geolocated_extent_all_wp_geolocated_articles[df_geolocated_extent_all_wp_geolocated_articles['set2']=="regions"]

df_subregions_allwparticles=df_subregions_allwparticles.rename(columns={'set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
df_regions_allwparticles=df_regions_allwparticles.rename(columns={'set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})
df_countries_allwparticles=df_countries_allwparticles.rename(columns={'set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})
df_countries_allwparticles['ISO 3166 alpha-3'] = df_countries_allwparticles['ISO 3166'].map(ISO2_3)
df_countries_allwparticles['Country'] = df_countries_allwparticles['ISO 3166'].map(country_names)
df_countries_allwparticles['Subregion'] = df_countries_allwparticles['ISO 3166'].map(subregions)
df_countries_allwparticles['Region'] = df_countries_allwparticles['ISO 3166'].map(regions)
df_subregions_allwparticles['Region'] = df_subregions_allwparticles['Subregion'].map(subregions_regions)




# print (df_countries_allwparticles.head(10))
# print (df_subregions_allwparticles.head(10))
# print (df_regions_allwparticles.head(10))



# COUNTRIES, SUBREGIONS, REGIONS EXTENT IN EVERY WIKIPEDIA LANGUAGE EDITIONS
df_geolocated_extent = df_geolocated_extent.loc[df_geolocated_extent['set1'].isin(wikilanguagecodes)]
df_languages_countries = df_geolocated_extent[df_geolocated_extent['set2']=="countries"]
df_languages_subregions = df_geolocated_extent[df_geolocated_extent['set2']=="subregions"]
df_languages_regions = df_geolocated_extent[df_geolocated_extent['set2']=="regions"]

df_languages_subregions=df_languages_subregions.rename(columns={'set1':'Wiki','set2descriptor':'Subregion','abs_value':'Articles','rel_value':'Extent (%)'})
df_languages_regions=df_languages_regions.rename(columns={'set1':'Wiki','set2descriptor':'Region','abs_value':'Articles','rel_value':'Extent (%)'})
df_languages_countries=df_languages_countries.rename(columns={'set1':'Wiki','set2descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Extent (%)'})

df_languages_countries['ISO 3166 alpha-3'] = df_languages_countries['ISO 3166'].map(ISO2_3)
df_languages_countries['Language'] = df_languages_countries['Wiki'].map(language_names_full)
df_languages_countries['Country'] = df_languages_countries['ISO 3166'].map(country_names)
df_languages_countries['Subregion'] = df_languages_countries['ISO 3166'].map(subregions)
df_languages_countries['Region'] = df_languages_countries['ISO 3166'].map(regions)
df_languages_subregions['Region'] = df_languages_subregions['Subregion'].map(subregions_regions)


df_languages_subregions['Wiki (Subregion)'] = df_languages_subregions['Wiki']+' ('+df_languages_subregions['Subregion']+')'
df_languages_subregions = df_languages_subregions.set_index('Wiki (Subregion)')

df_languages_regions['Wiki (Region)'] = df_languages_regions['Wiki']+' ('+df_languages_regions['Region']+')'
df_languages_regions = df_languages_regions.set_index('Wiki (Region)')

df_languages_countries['Wiki (ISO 3166)'] = df_languages_countries['Wiki']+' ('+df_languages_countries['ISO 3166']+')'
df_languages_countries = df_languages_countries.set_index('Wiki (ISO 3166)')



# # WIKIPEDIA LANGUAGE EDITIONS COVERAGE OF COUNTRIES, SUBREGIONS, REGIONS
query = 'SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value, period FROM wcdo_intersections_accumulated WHERE set2descriptor = "geolocated" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY abs_value DESC;'
df_languages_coverage = pd.read_sql_query(query, conn)
df_languages_coverage = df_languages_coverage.fillna(0)
df_languages_coverage = df_languages_coverage.loc[df_languages_coverage['set2'].isin(wikilanguagecodes)]

df_geolocated_coverage_countries = df_languages_coverage[df_languages_coverage['set1']=="countries"]
df_geolocated_coverage_subregions = df_languages_coverage[df_languages_coverage['set1']=="subregions"]
df_geolocated_coverage_regions = df_languages_coverage[df_languages_coverage['set1']=="regions"]

df_geolocated_coverage_countries['Total_Articles'] = df_geolocated_coverage_countries['abs_value']/(df_geolocated_coverage_countries['rel_value']/100)
df_geolocated_coverage_subregions['Total_Articles'] = df_geolocated_coverage_subregions['abs_value']/(df_geolocated_coverage_subregions['rel_value']/100)
df_geolocated_coverage_regions['Total_Articles'] = df_geolocated_coverage_regions['abs_value']/(df_geolocated_coverage_regions['rel_value']/100)

df_geolocated_coverage_countries.Total_Articles = df_geolocated_coverage_countries.Total_Articles.astype(int)
df_geolocated_coverage_subregions.Total_Articles = df_geolocated_coverage_subregions.Total_Articles.astype(int)
df_geolocated_coverage_regions.Total_Articles = df_geolocated_coverage_regions.Total_Articles.astype(int)

df_geolocated_coverage_subregions=df_geolocated_coverage_subregions.rename(columns={'set1descriptor':'Subregion','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})
df_geolocated_coverage_regions=df_geolocated_coverage_regions.rename(columns={'set1descriptor':'Region','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})
df_geolocated_coverage_countries=df_geolocated_coverage_countries.rename(columns={'set1descriptor':'ISO 3166','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki','Total_Articles':'Total Articles'})


# df_geolocated_coverage_countries['Language'] = df_geolocated_coverage_countries['Wiki'].map(language_names_full)
df_geolocated_coverage_subregions['Language'] = df_geolocated_coverage_subregions['Wiki'].map(language_names_full)
df_geolocated_coverage_regions['Language'] = df_geolocated_coverage_regions['Wiki'].map(language_names_full)

# df_geolocated_coverage_subregions['Region'] = df_geolocated_coverage_subregions['Subregion'].map(subregions_regions)
# df_geolocated_coverage_countries['Country'] = df_geolocated_coverage_countries['ISO 3166'].map(country_names)
# df_geolocated_coverage_countries['Subregion'] = df_geolocated_coverage_countries['ISO 3166'].map(subregions)
# df_geolocated_coverage_countries['Region'] = df_geolocated_coverage_countries['ISO 3166'].map(regions)

df_geolocated_coverage_subregions['Wiki (Subregion)'] = df_geolocated_coverage_subregions['Wiki']+' ('+df_geolocated_coverage_subregions['Subregion']+')'
df_geolocated_coverage_subregions = df_geolocated_coverage_subregions.set_index('Wiki (Subregion)')

df_geolocated_coverage_regions['Wiki (Region)'] = df_geolocated_coverage_regions['Wiki']+' ('+df_geolocated_coverage_regions['Region']+')'
df_geolocated_coverage_regions = df_geolocated_coverage_regions.set_index('Wiki (Region)')

df_geolocated_coverage_countries['Wiki (ISO 3166)'] = df_geolocated_coverage_countries['Wiki']+' ('+df_geolocated_coverage_countries['ISO 3166']+')'
df_geolocated_coverage_countries = df_geolocated_coverage_countries.set_index('Wiki (ISO 3166)')


df_geolocated_coverage_countries=df_geolocated_coverage_countries.drop(['Wiki','Articles','ISO 3166'],axis=1)
df_geolocated_coverage_subregions=df_geolocated_coverage_subregions.drop(['Wiki','Articles','Subregion'],axis=1)
df_geolocated_coverage_regions=df_geolocated_coverage_regions.drop(['Wiki','Articles','Region'],axis=1)


# MERGED COVERAGE EXTENT LANGUAGES / COUNTRIES, SUBREGIONS, REGIONS 
df_lang_countries_final = pd.concat([df_languages_countries,df_geolocated_coverage_countries], axis=1, sort=False)
df_lang_subregion_final = pd.concat([df_languages_subregions,df_geolocated_coverage_subregions], axis=1, sort=False)
df_lang_region_final = pd.concat([df_languages_regions,df_geolocated_coverage_regions], axis=1, sort=False)

df_lang_countries_final = df_lang_countries_final[['Wiki','Language', 'ISO 3166','ISO 3166 alpha-3', 'Articles', 'Coverage (%)','Extent (%)','Country', 'Subregion', 'Region','Total Articles']].round(1)
df_lang_subregion_final = df_lang_subregion_final[['Wiki', 'Language', 'Subregion','Articles', 'Coverage (%)', 'Extent (%)','Region','Total Articles']].round(1)
df_lang_region_final = df_lang_region_final[['Wiki', 'Language', 'Region', 'Articles', 'Coverage (%)', 'Extent (%)','Total Articles']].round(1)

#df_lang_countries_final = df_lang_countries_final[df_lang_countries_final['ISO 3166 alpha-3']==""]
#print (df_lang_countries_final.head(10))


# GEOLOCATED ARTICLES WITHOUT INTERWIKI LINKS
query = 'SELECT set1,set1descriptor,set2,set2descriptor, abs_value, rel_value, abs_value*100/rel_value as Geolocated_Articles, period FROM wcdo_intersections_accumulated WHERE set2="geolocated" AND set2descriptor = "zero_ill" AND content = "articles" AND period IN (SELECT MAX(period) FROM wcdo_intersections_accumulated) ORDER BY abs_value DESC;'
df_geolocated_countriessubregionsregions_noill = pd.read_sql_query(query, conn)
df_geolocated_countriessubregionsregions_noill = df_geolocated_countriessubregionsregions_noill.fillna(0)
df_geolocated_countriessubregionsregions_noill.Geolocated_Articles = df_geolocated_countriessubregionsregions_noill.Geolocated_Articles.astype(int)
df_geolocated_countriessubregionsregions_noill = df_geolocated_countriessubregionsregions_noill.rename(columns={'abs_value':'Geolocated Articles No-IW','rel_value':'Articles No-IW (%)','Geolocated_Articles':'Geolocated Articles'})

df_wikidata_noill = df_geolocated_countriessubregionsregions_noill[df_geolocated_countriessubregionsregions_noill['set1']=="wikidata_article_qitems"]
df_countries_wikidata_noill = df_geolocated_countriessubregionsregions_noill[df_geolocated_countriessubregionsregions_noill['set1']=="countries"]
df_subregions_wikidata_noill = df_geolocated_countriessubregionsregions_noill[df_geolocated_countriessubregionsregions_noill['set1']=="subregions"]
df_regions_wikidata_noill = df_geolocated_countriessubregionsregions_noill[df_geolocated_countriessubregionsregions_noill['set1']=="regions"]

df_countries_wikidata_noill=df_countries_wikidata_noill.rename(columns={'set1descriptor':'ISO 3166'})
df_countries_wikidata_noill['Country'] = df_countries_wikidata_noill['ISO 3166'].map(country_names)
df_countries_wikidata_noill['Subregion'] = df_countries_wikidata_noill['ISO 3166'].map(subregions)
df_countries_wikidata_noill['Region'] = df_countries_wikidata_noill['ISO 3166'].map(regions)

df_subregions_wikidata_noill=df_subregions_wikidata_noill.rename(columns={'set1descriptor':'Subregion','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki'})
df_subregions_wikidata_noill['Region'] = df_subregions_wikidata_noill['Subregion'].map(subregions_regions)

df_regions_wikidata_noill=df_regions_wikidata_noill.rename(columns={'set1descriptor':'Region','abs_value':'Articles','rel_value':'Coverage (%)','set2':'Wiki'})




### FUNCTIONS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app3 = Dash(__name__, server = app, url_base_pathname= webtype + '/geography_gap/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

#dash_app3.config['suppress_callback_exceptions']=True

title = "Wikipedia Geolocated Articles (Geography Gap)"
dash_app3.title = title+title_addenda
text_heatmap = ''

dash_app3.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that explain how well each Wikipedia language edition covers 
        the Geolocated articles.
        They illustrate the content geography gap, that is the imbalances in representing the different world regions, subregions and countries. They answer the following questions:
        * What is the extent of geolocated articles without interwiki links in each country, subregion and world region?
        * What is the extent of geolocated articles in each Wikipedia language edition?
        * What is the extent of geolocated articles in countries, subregions and regions in the sum of all geolocated articles in all languages, all unique geolocated articles (qitems), and in each Wikipedia language edition?
        * How well do Wikipedia language editions cover the available geolocated articles in each country?
        * What Wikipedia language editions cover best each country, subregion, and region geolocated articles?
        '''),

    html.Hr(),


###

    html.H5('Geolocated Wikidata Qitems Without Interwiki Links Scatterplot'),

    dcc.Markdown('''* **What is the extent of geolocated articles without Interwiki links in each country, subregion and world region?**

        The following scatterplot graph shows for a list of countries, subregions or world regions the number of articles they contain (Y-axis) and on the percentage of articles without any interwiki links (X-axis). Wikipedia language editions are colored according to the world region (continent) where the language is spoken. You can double-click the region name to see only the languages from that region.
     '''.replace('  ', '')),

    dcc.Dropdown(
        id='geolocateddropdown_coverage',
        options = [{'label': k, 'value': k} for k in ['Countries','Subregions','Regions']],
        value = 'Countries',
        style={'width': '190px'}
     ),
    dcc.Graph(id = 'scatterplot_nointerwiki'),

    html.Hr(),


###

    html.H5('Extent of Geolocated Articles in Wikipedia Language Editions and Language CCC Scatterplot'),

    dcc.Markdown('''* **What is the extent of geolocated articles in each Wikipedia language edition?**

        The following scatterplot graph shows the extent of geolocated articles and CCC geolocated articles in each Wikipedia language edition. CCC geolocated articles are geolocated articles that belong to the language CCC. Wikipedia language editions are colored according to the world region (continent) where the language is spoken. You can double-click the region name to see only the languages from that region.
     '''.replace('  ', '')),

    dcc.Dropdown(
        id='geolocatedarticlesdropdown_coverage',
        options = [{'label': k, 'value': k} for k in ['Wikipedia','CCC']],
        value = 'Wikipedia',
        style={'width': '190px'}
     ),
    dcc.Graph(id = 'scatterplot_geolocatedextent'),

   html.Hr(),


####

    html.H5("Extent of Countries, Subregions, Regions in Wikipedia Content Treemap", style={'textAlign':'left'}),

    dcc.Markdown('''
        * **What is the extent of geolocated articles in countries, subregions and regions in the sum of all geolocated articles in all languages, all unique geolocated articles (qitems), and in each Wikipedia language edition?**
        '''.replace('  ', '')),
    dcc.Markdown('''
        The following treemap graphs show for two selected projects both the extent and the coverage of different geographical entities (countries, subregions and world regions). The size of the tiles and the colorscale (orange-dark blue) is according to the extent the geographical entities take in the selected project, which can be the sum of all geolocated articles in all Wikipedia language editions, all the Wikidata geolocated qitems or specific Wikipedia language editions. When you hover on a tile you can read the same information regarding the coverage and extent plus the number of articles.
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

    html.Hr(),

####

    html.H5("Coverage of Countries Geolocated Articles by Wikipedia Language Editions Map", style={'textAlign':'left'}),

    dcc.Markdown('''
        * **How well do Wikipedia language editions cover the available geolocated articles in each country?**
        '''.replace('  ', '')),
    dcc.Markdown('''
        The following map graphs show the coverage of the Total Articles geolocated in the following territories by a Wikipedia language edition, and the extent they take in the group of geolocated articles of this Wikipedia language edition. You can hover on each country and see the number of covered articles and total articles, the percentages of coverage and extent, as well as other information regarding the country.
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

    html.Hr(),

####

    html.H5('Wikipedia Language Editions Coverage of Countries, Subregions and Regions Barchart'),
    dcc.Markdown('''* **What Wikipedia language editions cover best each country, subregion, and region geolocated articles?**

        The following barchart graph shows for a selected geographical entity (country, world subregion or region) the coverage by Wikipedia language editions of the total available number of geolocated articles/qitems in them. By hovering in each Wikipedia you can see the coverage percentage as well, the extent it takes in percentage of their total number of articles, the total number of articles geolocated in that geographical entity as well as other geographical information.'''.replace('  ', '')),


    html.Div(html.P('Select a Geographical Entity'), style={'display': 'inline-block','width': '200px'}),

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

    dcc.Graph(id = 'barchart_coverage')

#    html.Hr()

	], className="container")


###



### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 


# SCATTERPLOT NO INTERWIKI
@dash_app3.callback(
    Output('scatterplot_nointerwiki', 'figure'), [Input('geolocateddropdown_coverage', 'value')])
def update_scatterplot(value):

    if value == 'Countries': 
        df = df_countries_wikidata_noill
        fig = px.scatter(df, x="Articles No-IW (%)", y="Geolocated Articles", color="Region", log_x=False, log_y=True,hover_data=['Country','Geolocated Articles No-IW'],text="ISO 3166") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",

    if value == 'Subregions': 
        df = df_subregions_wikidata_noill
        fig = px.scatter(df, x="Articles No-IW (%)", y="Geolocated Articles", color="Region", log_x=False, log_y=True,hover_data=['Subregion','Geolocated Articles No-IW'],text="Subregion") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",

    if value == 'Regions': 
        df = df_regions_wikidata_noill
        fig = px.scatter(df, x="Articles No-IW (%)", y="Geolocated Articles", color="Region", log_x=False, log_y=True,hover_data=['Region','Geolocated Articles No-IW'],text="Region") #text="Wiki",size='Percentage of Sum of All CCC Articles',text="Wiki",

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
    if value == None: return

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
    if value == None: return

    if value == 'Countries':
        options = df_lang_countries_final['Country'].unique().tolist()

    if value == 'Subregions':
        options = df_lang_subregion_final['Subregion'].unique().tolist()

    if value == 'Regions':
        options = df_lang_region_final['Region'].unique().tolist()

    return [{'label': i, 'value': i} for i in options]



# BARCHART CCC SPREAD
@dash_app3.callback(
    Output('barchart_coverage', 'figure'),
    [Input('sourceentityspecific', 'value'),Input('sourceentitytype', 'value')])
def update_barchart(entityspecific,entitytype):
#   print (entityspecific,entitytype)
    if entityspecific == None or entitytype == None: return

    if entitytype == 'Countries':
#  Wiki Language ISO 3166  Articles  Coverage (%)  Extent (%)                   Country                        Subregion    Region
        df = df_lang_countries_final.loc[df_lang_countries_final['Country'] == entityspecific]
        fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','ISO 3166','Country','Subregion','Region'], color='Articles', height=400)

    if entitytype == 'Subregions':
# Wiki Language                        Subregion  Articles  Coverage (%)  Extent (%)    Region
        df = df_lang_subregion_final.loc[df_lang_subregion_final['Subregion'] == entityspecific]
        fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','Subregion','Region'], color='Articles', height=400)

    if entitytype == 'Regions':
# Wiki        Language    Region  Articles  Coverage (%)  Extent (%)
        df = df_lang_region_final.loc[df_lang_region_final['Region'] == entityspecific]
        fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','Region'], color='Articles', height=400)

    return fig



# TREEMAP WIKIPEDIA LANGUAGES - COUNTRIES, SUBREGIONS, REGIONS COVERAGE
@dash_app3.callback(
    Output('treemap_geolocated_coverage', 'figure'),
    [Input('sourcelangdropdown_treemapgeolocatedcoverage','value'),Input('sourcelangdropdown_treemapgeolocatedcoverage2', 'value'),Input('sourceentitytype_treemap','value')])
def update_treemap_coverage(project,project2,entitytype):
    if project == None or project2 == None or entitytype == None: return

    projectname = project; projectname2 = project2
    if '(' in projectname: projectname+= ' Geolocated Articles'
    if '(' in projectname2: projectname2+= ' Geolocated Articles'
    project = treemapdict[project]
    project2 = treemapdict[project2]
#   print (project,project2,entitytype)



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
            df = df_countries_allwparticles

        elif project == "Wikidata Geolocated Qitems":
            text = 'ISO 3166'
            df = df_countries_wikidata
        else:
            df = df_lang_countries_final.loc[df_lang_countries_final['Wiki'] == project]

        if project2 == "All Geolocated Articles":
            text2 = 'ISO 3166'
            df2 = df_countries_allwparticles
        if project2 == "Wikidata Geolocated Qitems":
            text2 = 'ISO 3166'
            df2 = df_countries_wikidata
        else:
            df2 = df_lang_countries_final.loc[df_lang_countries_final['Wiki'] == project2]

    if entitytype == 'Subregions':
        labels = "Subregion"

        if project == "All Geolocated Articles":
            text = 'Region'
            df = df_subregions_allwparticles
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        elif project == "Wikidata Geolocated Qitems":
            text = 'Region'
            df = df_subregions_wikidata
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        else:
            df = df_lang_subregion_final.loc[df_lang_subregion_final['Wiki'] == project]

        if project2 == "All Geolocated Articles":
            text2 = 'Region'
            df2 = df_subregions_allwparticles
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        elif project2 == "Wikidata Geolocated Qitems":
            text2 = 'Region'
            df2 = df_subregions_wikidata
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        else:
            df2 = df_lang_subregion_final.loc[df_lang_subregion_final['Wiki'] == project2]

    if entitytype == 'Regions':
        labels = "Region"

        if project == "All Geolocated Articles":
            text = 'Region'
            df = df_regions_allwparticles
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        elif project == "Wikidata Geolocated Qitems":
            text = 'Region'
            df = df_regions_wikidata
            hovertemplate = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'
        else:
            df = df_lang_region_final.loc[df_lang_region_final['Wiki'] == project]

        if project2 == "All Geolocated Articles":
            df2 = df_regions_allwparticles
            text2 = 'Region'
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'

        elif project2 == "Wikidata Geolocated Qitems":
            df2 = df_regions_wikidata
            text2 = 'Region'
            hovertemplate2 = '<b>%{label} </b><br>Extent: %{value}%<br>Art.: %{customdata}<br><extra></extra>'
        else:
            df2 = df_lang_region_final.loc[df_lang_region_final['Wiki'] == project2]

#       fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','ISO 3166','Country','Subregion','Region'], color='Articles', height=400)

#       fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','Subregion','Region'], color='Articles', height=400)

#       fig = px.bar(df.head(20), x='Language', y='Coverage (%)', hover_data=['Language','Articles','Total Articles','Coverage (%)','Extent (%)','Region'], color='Articles', height=400)
    
    # print (df.head(10))
    # print (df.columns.tolist())
    # print (df2.head(10))
    # print (df2.columns.tolist())
#   input('')


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
        parents = parents2,
        labels = df2[labels],
        customdata = df2['Articles'],
        values = df2['Extent (%)'],
        text = df2[text2],
        texttemplate = texttemplate2,
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
        title_x=0.5,
    )



    return fig


# MAP WIKIPEDIA LANGUAGES - COUNTRIES
@dash_app3.callback(
    Output('choropleth_map_countries_coverage', 'figure'),
    [Input('sourcelangdropdown_mapgeolocatedcoverage','value')])
def update_map_coverage(project):
    if project == None: return

    df = df_lang_countries_final.loc[df_lang_countries_final['Wiki'] == language_names[project]]
    df=df.rename(columns={'Articles':'Covered Articles'})

    fig = px.choropleth(df, locations="ISO 3166 alpha-3",
                        color="Coverage (%)",
                        hover_data=['Country', 'Subregion', 'Region', 'ISO 3166', 'Coverage (%)', 'Extent (%)', 'Covered Articles', 'Total Articles'], # column to add to hover information
                        color_continuous_scale=px.colors.sequential.Plasma)

    fig.update_layout(
        autosize=True,
    #        width=700,
        height=600,
        paper_bgcolor="White",
        title_text=project+' Wikipedia Coverage of Total Geolocated Articles by Country',
        title_x=0.5,
    )

    return fig

@dash_app3.callback(
    Output('choropleth_map_countries_extent', 'figure'),
    [Input('sourcelangdropdown_mapgeolocatedcoverage','value')])
def update_map_extent(project):
    if project == None: return

    df = df_lang_countries_final.loc[df_lang_countries_final['Wiki'] == language_names[project]]
    df=df.rename(columns={'Articles':'Covered Articles'})

    fig2 = px.choropleth(df, locations="ISO 3166 alpha-3",
                        color="Extent (%)",
                        hover_data=['Country', 'Subregion', 'Region', 'ISO 3166', 'Coverage (%)', 'Extent (%)', 'Covered Articles', 'Total Articles'], # column to add to hover information
                        color_continuous_scale=px.colors.sequential.Plasma)

    fig2.update_layout(
        autosize=True,
    #        width=700,
        height=600,
        paper_bgcolor="White",
        title_text='Country Extent in '+project+' Wikipedia Geolocated Articles',
        title_x=0.5,
    )

    return fig2
