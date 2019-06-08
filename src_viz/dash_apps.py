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
import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
# viz
import plotly
import plotly.plotly as py
import plotly.figure_factory as ff
# data
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import pandas as pd
import sqlite3
import xlsxwriter
# other
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time
# script
sys.path.insert(0, '/srv/wcdo/src_data')
import wikilanguages_utils


##### RESOURCES #####
databases_path = '/srv/wcdo/databases/'

territories = wikilanguages_utils.load_languageterritories_mapping()
languages = wikilanguages_utils.load_wiki_projects_information(territories);

wikilanguagecodes = languages.index.tolist()
wikipedialanguage_numberarticles = wikilanguages_utils.load_wikipedia_language_editions_numberofarticles(wikilanguagecodes,'old')
for languagecode in wikilanguagecodes:
    if languagecode not in wikipedialanguage_numberarticles: wikilanguagecodes.remove(languagecode)

languageswithoutterritory=['eo','got','ia','ie','io','jbo','lfn','nov','vo']
# Only those with a geographical context
for languagecode in languageswithoutterritory: wikilanguagecodes.remove(languagecode)

language_names = {}
for languagecode in wikilanguagecodes:
	lang_name = languages.loc[languagecode]['languagename']+' ('+languagecode+')'
	language_names[lang_name] = languagecode

lists_dict = {'Editors':'editors','Featured':'featured','Geolocated':'geolocated','Keywords':'keywords','Women':'women','Men':'men','Created First Three Years':'created_first_three_years','Created Last Year':'created_last_year','Pageviews':'pageviews','Discussions':'discussions'}
list_dict_inv = {v: k for k, v in lists_dict.items()}

closest_langs = wikilanguages_utils.obtain_closest_for_all_languages(wikipedialanguage_numberarticles, wikilanguagecodes, 4)

country_names, regions, subregions = wikilanguages_utils.load_iso_3166_to_geographical_regions()

country_names_inv = {v: k for k, v in country_names.items()}

#print (country_names)

language_countries = {}
for languagecode in wikilanguagecodes:
	countries = wikilanguages_utils.load_countries_from_language(languagecode,territories)
	countries_from_lang = {}
#	countries_from_lang['all']='all'
	for country in countries:
		countries_from_lang[country_names[country]+' ('+country.lower()+')']=country.lower()
	language_countries[languagecode] = countries_from_lang


conn = sqlite3.connect(databases_path + 'top_ccc_articles.db'); cursor = conn.cursor()
# countries Top CCC article lists totals
query = 'SELECT set1, set1descriptor, abs_value FROM wcdo_intersections WHERE set1 LIKE "%(" ||  set2 || ")%" AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1;'
df_countries = pd.read_sql_query(query, conn)
df_countries = df_countries.set_index('set1')

# languages Top CCC article lists totals
query = 'SELECT set1, set1descriptor, abs_value FROM wcdo_intersections WHERE set1 = set2 AND set2descriptor = "wp" AND measurement_date IN (SELECT MAX(measurement_date) FROM wcdo_intersections) ORDER BY set1;'    
df_langs = pd.read_sql_query(query, conn)
df_langs = df_langs.set_index('set1')

# web
title_addenda = ' - Wikipedia Cultural Diversity Observatory (WCDO)'
external_stylesheets = ['https://wcdo.wmflabs.org/assets/bWLwgP.css']
external_scripts = ['https://wcdo.wmflabs.org/assets/gtag.js']
webtype = ''



##### FLASK APP #####
app = flask.Flask(__name__)

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', threaded=True, debug=True)

@app.route('/')
def main():
    return flask.redirect('https://meta.wikimedia.org/wiki/Wikipedia_Cultural_Diversity_Observatory')

@app.errorhandler(404)
def handling_page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


##### DASH APPS #####
from apps.language_territories_mapping_app import *
from apps.list_of_wikipedias_territories_by_ccc_apps import *
from apps.ccc_coverage_spread_apps import *
from apps.ccc_pageviews_apps import *
from apps.top_ccc_app import *
from apps.top_ccc_coverage_spread_apps import *

print ('\n\n\n*** START WCDO APP:'+str(datetime.datetime.now()))


##### METHODS #####
# parse
def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    print (state)
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