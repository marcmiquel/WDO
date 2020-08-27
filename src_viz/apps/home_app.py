# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
home_app = Dash(__name__, server = app, url_base_pathname= webtype + "/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
home_app.config['suppress_callback_exceptions']=True

title = "Home"
home_app.title = title+title_addenda
home_app.layout = html.Div([
    navbar,

    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.H2(html.B('Wikipedia Diversity Observatory'), style={'textAlign':'center', 'font-weight':'bold'}),
    html.Div(
    dcc.Markdown('''
    Providing data, visualizations and tools to work towards more diversity within Wikipedia language editions.'''.replace('  ', '')), style={'textAlign':'center'},),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),

    footbar,

], className="container")