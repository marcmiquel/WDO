import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *



### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

conn = sqlite3.connect(databases_path + 'stats_production.db'); cursor = conn.cursor() 



### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app16 = Dash(__name__, server = app, url_base_pathname= webtype + '/time_gap/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

dash_app16.config['suppress_callback_exceptions']=True

title = "Time Gap"
dash_app16.title = title+title_addenda

dash_app16.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown('''
        This page shows stastistics and graphs that illustrate the Time gap in Wikipedia language editions content. It is not ready yet.
        '''),
    footbar,

], className="container")


### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
