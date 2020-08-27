# -*- coding: utf-8 -*-

# flash dash
import flask
from flask import Flask, request, render_template
from flask import send_from_directory
from flask_caching import Cache
from dash import Dash
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_table
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
# dash bootstrap components
import dash_bootstrap_components as dbc

# viz
import plotly
import chart_studio.plotly as py
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# data
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import pandas as pd
import sqlite3
import xlsxwriter

# other
import numpy as np
import random
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time
import requests
import subprocess


# script
from update_top_diversity_interwiki import *
sys.path.insert(0, '/srv/wcdo/src_data')
import wikilanguages_utils

setting_up_time = time.time()



##### DATA RESOURCES FOR ALL APPS #####
databases_path = '/srv/wcdo/databases/'

last_period = wikilanguages_utils.get_last_accumulated_period_year_month()
#last_period = '2019-05' # CHANGE WHEN THE NEXT stats.db IS COMPUTED.


territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
languages = wikilanguages_utils.load_wiki_projects_information();

wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'')
wikilanguagecodes = list(wikipedialanguage_numberarticles.keys())

for i in (set(languages.index.tolist()) - set(list(wikipedialanguage_numberarticles.keys()))):
    try: languages.drop(i, inplace=True); territories.drop(i, inplace=True)
    except: pass
print (wikilanguagecodes)

# Only those with a geographical context
languageswithoutterritory=list(set(languages.index.tolist()) - set(list(territories.index.tolist())))
for languagecode in languageswithoutterritory: wikilanguagecodes.remove(languagecode)

language_names_list = []
language_names = {}
language_names_full = {}
language_name_wiki = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
    language_name_wiki[lang_name]=languages.loc[languagecode]['languagename']
    language_names_full[languagecode]=languages.loc[languagecode]['languagename']
    language_names[lang_name] = languagecode
    language_names_list.append(lang_name)
language_names_inv = {v: k for k, v in language_names.items()}

lang_groups = list()
lang_groups += ['Top 5','Top 10', 'Top 20', 'Top 30', 'Top 40']#, 'Top 50']
lang_groups += territories['region'].unique().tolist()
lang_groups += territories['subregion'].unique().tolist()
lang_groups.remove('')

closest_langs = wikilanguages_utils.obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, 4)


country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()
country_names_inv = {v: k for k, v in country_names.items()}

countries_list = list(set(country_names.values()))
subregions_list = list(set(subregions.values()))
regions_list = list(set(regions.values()))

ISO31662_subdivisions_dict, subdivisions_ISO31662_dict = wikilanguages_utils.load_iso_31662_to_subdivisions()
subregions_regions = {}
for x,y in subregions.items():
    subregions_regions[y]=regions[x]

language_countries = {}
for languagecode in wikilanguagecodes:
    countries = wikilanguages_utils.load_countries_from_language(languagecode,territories)
    countries_from_lang = {}
    for country in countries: countries_from_lang[country_names[country]+' ('+country.lower()+')']=country.lower()
    language_countries[languagecode] = countries_from_lang

language_subdivisions = {}
for languagecode in wikilanguagecodes:
    subdivisions = wikilanguages_utils.load_countries_subdivisions_from_language(languagecode,territories)
    subdivisions_from_lang = {}
    for subdivision in subdivisions:
        if subdivision!=None and subdivision!='':
            subdivisions_from_lang[ISO31662_subdivisions_dict[subdivision]+' ('+subdivision.lower()+')']=subdivision.lower()
    language_subdivisions[languagecode] = subdivisions_from_lang


group_labels = wikilanguages_utils.get_group_identities_labels()


##### WEB RESOURCES #####

title_addenda = ' - Wikipedia Diversity Observatory (WDO)'
#external_stylesheets = ['https://wcdo.wmflabs.org/assets/bWLwgP.css'] 
external_stylesheets = [dbc.themes.BOOTSTRAP]
external_scripts = ['https://wcdo.wmflabs.org/assets/gtag.js']
webtype = ''


##### NAVBAR #####
#LOGO = "https://wcdo.wmflabs.org/assets/logo.png"
LOGO = "./assets/logo.png"
LOGO_foot = "./assets/wikimedia-logo.png"
# LOGO = app.get_asset_url('logo.png') # this would have worked. 

navbar = html.Div([
    html.Br(),
    dbc.Navbar(
        [ dbc.Collapse(
                dbc.Nav(
                    [
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Top CCC Articles ", href="https://wcdo.wmflabs.org/top_ccc_articles/"),
                        dbc.DropdownMenuItem("Missing CCC ", href="https://wcdo.wmflabs.org/missing_ccc_articles/"),
                        dbc.DropdownMenuItem("Common CCC", href="https://wcdo.wmflabs.org/common_ccc_articles"),
                        dbc.DropdownMenuItem("Incomplete CCC", href="https://wcdo.wmflabs.org/incomplete_ccc_articles/"),
                        dbc.DropdownMenuItem("Search CCC ", href="https://wcdo.wmflabs.org/search_ccc_articles/"),
                        dbc.DropdownMenuItem("Visual CCC ", href="https://wcdo.wmflabs.org/visual_ccc_articles/"),
                        ],
                        label="Tools",
                        nav=True,
                    ),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Local Content / CCC", href="https://wcdo.wmflabs.org/cultural_context_content/"),
                        dbc.DropdownMenuItem("Cultural Gap (CCC Coverage)", href="https://wcdo.wmflabs.org/ccc_coverage/"),
                        dbc.DropdownMenuItem("Cultural Gap (CCC Spread)", href="https://wcdo.wmflabs.org/ccc_spread/"),
                        dbc.DropdownMenuItem("Geography Gap", href="https://wcdo.wmflabs.org/geography_gap/"),
                        dbc.DropdownMenuItem("Gender Gap", href="http://wcdo.wmflabs.org/gender_gap/"),
                        dbc.DropdownMenuItem("Topical Coverage", href="https://wcdo.wmflabs.org/topical_coverage/"),
                        dbc.DropdownMenuItem("Last Month Pageviews", href="https://wcdo.wmflabs.org/last_month_pageviews/"),
                        dbc.DropdownMenuItem("Diversity Over Time", href="https://wcdo.wmflabs.org/diversity_over_time/"),
                        dbc.DropdownMenuItem("Languages Top CCC Articles Coverage", href="https://wcdo.wmflabs.org/languages_top_ccc_articles_coverage/"),
                        dbc.DropdownMenuItem("Countries Top CCC Articles Coverage", href="https://wcdo.wmflabs.org/countries_top_ccc_articles_coverage/"),
                        dbc.DropdownMenuItem("Languages Top CCC Articles Spread", href="https://wcdo.wmflabs.org/languages_top_ccc_articles_spread/")],
                        label="Visualizations",
                        nav=True,
                    ),
                    html.A(
                    # Use row and col to control vertical alignment of logo / brand
                    dbc.Row(
                        [
                            dbc.Col(html.Img(src=LOGO, height="35px")),
    #                        dbc.Col(dbc.NavbarBrand("Wikipedia Diversity Observatory", className="mb-5")),
                        ],
                        align="center",
                        no_gutters=True,
                    ),
                    href="https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory", target= "_blank",
                style = {'margin-left':"5px"}),
                ], className="ml-auto", navbar=True),
                id="navbar-collapse2",
                navbar=True,
            ),
        ],
        color="white",
        dark=False,
        className="ml-2",
    ),
    ])


##### FOOTBAR #####
footbar = html.Div([
        html.Br(),
        html.Br(),
        html.Hr(),

        html.Div(
            dbc.Nav(
                [
                    dbc.NavLink("Diversity Observatory Meta-Wiki Page", href="https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory", target="_blank", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("View Source", href="https://github.com/marcmiquel/wcdo", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("Datasets/Databases", href="https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content#Datasets", style = {'color': '#8C8C8C'}),
                    dbc.NavLink("Research", href="https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory/Cultural_Context_Content#References", style = {'color': '#8C8C8C'}),
                ], className="ml-2"), style = {'textAlign': 'center', 'display':'inline-block' , 'width':'60%'}),

        html.Div(id = 'current_data', children=[        
            'Updated with dataset from: ',
            html.B(last_period)],
#            html.B(current_dataset_period_stats)],
            style = {'textAlign':'right','display': 'inline-block', 'width':'40%'}),
        html.Br(),
        html.Div([
            html.P('Hosted with ♥ on ',style = {'display':'inline-block'}),
            html.A('Wikimedia Cloud VPS',href='https://wikitech.wikimedia.org/wiki/Portal:Cloud_VPS', target="_blank", style = {'display':'inline-block'}),
            html.P('.',style = {'display':'inline-block', 'margin-right':"5px"}),
            html.A(html.Img(src=LOGO_foot, height="35px"),href='https://wikitech.wikimedia.org/wiki/Help:Cloud_Services_Introduction', target="_blank", style = {'display':'inline-block'}),

            ], style = {'textAlign':'right'}
            ),
        html.Br(),
    ])




##### FLASK APP #####
app = flask.Flask(__name__)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', threaded=True, debug=True)

# @app.route('/')
# def main():
#     return flask.redirect('https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory')

@app.route('/list_of_language_territories_by_cultural_context_content')
def ccc_territories():
    return flask.redirect('https://wcdo.wmflabs.org/cultural_context_content')

@app.route('/list_of_wikipedias_by_cultural_context_content')
def ccc():
    return flask.redirect('https://wcdo.wmflabs.org/cultural_context_content')

@app.errorhandler(404)
def handling_page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404






##### DASH APPS #####

# visualizations
from apps.language_territories_mapping_app import *
from apps.cultural_context_content_app import *
from apps.ccc_coverage_spread_apps import *
from apps.top_ccc_coverage_spread_apps import *
from apps.geography_gap_app import *
from apps.last_month_pageviews_app import *
from apps.topical_coverage_app import *
from apps.gender_gap_app import *
from apps.diversity_over_time import *

# tools
from apps.top_ccc_app import *
from apps.missing_ccc_app import *
from apps.common_ccc_app import *
from apps.visual_ccc_app import *
from apps.search_ccc_app import *
from apps.incomplete_ccc_app import *

# others
from apps.home_app import *
from apps.data_status_app import *
from apps.stubs_app import *





##### FUNCTIONS #####
# parse
def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
#    print (state)
    return state

# layout
def apply_default_value(params):
    def wrapper(func):
        def apply_value(*args, **kwargs):
            if 'id' in kwargs and kwargs['id'] in params:
                kwargs['value'] = params[kwargs['id']]
            return func(*args, **kwargs)
        return apply_value
    return wrapper




##### EXECUTING FUNCTIONS #####

print ('* dash_apps loaded after: '+str(datetime.timedelta(seconds=time.time() - setting_up_time)))
print ('\n\n\n*** START WCDO APP:'+str(datetime.datetime.now()))



# # UPDATE TOP CCC INTERWIKIS AND INTERSECTIONS
# while True:
#     # DAILY
#     # print ('hi!')
#     time.sleep(84600)
#     print ("Good morning. Update the Top Diversity Lists Interwiki and Intersections: "+str(datetime.datetime.now()))
#     update_top_diversity_articles_interwiki()
#     # update_top_diversity_interwiki.update_top_diversity_articles_intersections() # this needs to be tested with the new database.
#     print ("done.")
#     # time.sleep(84600)



