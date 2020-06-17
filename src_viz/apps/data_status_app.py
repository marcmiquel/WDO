# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *


databases_path = '/srv/wcdo/databases/'
data_scripts_path = '/srv/wcdo/src_data/'
apps_scripts_path = '/srv/wcdo/src_viz/'
n_lines_default = 100



### FUNCTIONS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 

def check_current_cycle():
    return '2019-07'
#    return wikilanguages_utils.get_last_accumulated_period_year_month()

def check_running_processes():
    cmd = "ps aux | grep py | grep root"
#     try:
#         processes = str(subprocess.check_output(cmd, shell=True).decode('utf-8'))
# #    processes = str(subprocess.check_output("ps aux | grep py | grep root", shell=True).decode('utf-8'))
#     except:
#         processes = ''

    os.system(cmd+' > tmp1')
    processes = open('tmp1', 'r').read()

    return processes

def read_files_from_directory(path, filetype):
    cmd = "ls -lt "+path+" | grep "+filetype
    # try: 
    #     files = str(subprocess.check_output(cmd, shell=True).decode('utf-8')).replace('marcmiquel','').replace('staff','')
    # except:
    #     files = ''

    os.system(cmd+' > tmp2')
    files = open('tmp2', 'r').read()

    return files

def read_from_log_file(path, filename, numlines):
#    print ("tail -"+str(numlines)+" "+path+filename)
    cmd = "tail -"+str(numlines)+" "+path+filename
    # try:
    #     log = str(subprocess.check_output(cmd, shell=True).decode('utf-8'))
    # except:
    #     log = ''

    os.system(cmd+' > tmp_'+filename)
    log = open('tmp_'+filename, 'r').read()

    return log

def read_from_function_account(script_name):
    query = 'SELECT function_name, year_month, finish_time, duration FROM function_account LIMIT 10;'
    if script_name == 'Wikipedia Diversity': c = sqlite3.connect(databases_path + 'wikipedia_diversity.db')
    if script_name == 'Content Selection': c = sqlite3.connect(databases_path + 'wikipedia_diversity.db')
    if script_name == 'Top CCC Selection': c = sqlite3.connect(databases_path + 'top_ccc_articles.db');
    if script_name == 'Stats Generation': c = sqlite3.connect(databases_path + 'stats.db');
    if script_name == 'Relevance Features': c = sqlite3.connect(databases_path + 'wikipedia_diversity.db')
    if script_name == 'Missing CCC Selection': c = sqlite3.connect(databases_path + 'missing_ccc.db');

    try:
        df = pd.read_sql_query(query, c)
        fa = df.to_csv(index=False)
    except:
        fa = ''
    return fa

### DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
scripts_names = ['Content Selection','History Features','Wikipedia Diversity','Top CCC Selection','Stats Generation','Relevance Features','Missing CCC Selection']
script_names_to_files = {'Wikipedia Diversity':'wikipedia_diversity','Content Selection':'content_selection','Top CCC Selection':'top_ccc_selection','Stats Generation':'stats_generation','Relevance Features':'relevance_features','Missing CCC Selection':'missing_content_selection','History Features':'history_features'}
script_names_to_files_inv = {v: k for k, v in script_names_to_files.items()}


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_status = Dash(__name__, server = app, url_base_pathname= webtype + '/data_status/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# dash_app13 = Dash()
dash_app_status.config['suppress_callback_exceptions']=True


title = "Data Status and Active Processes"
dash_app_status.title = title+title_addenda

dash_app_status.layout = html.Div([
    navbar,
    html.H3(title, style={'textAlign':'center'}),
    dcc.Location(id='url', refresh=False),

    html.Div(id = 'current_cycle', children='Current cycle:', style = {'width':'30%','display': 'inline-block'}),

    html.Br(),
    html.H6('Database files:', style = {'width':'37%','display': 'inline-block'}),
    html.H6('Src_data logs:', style = {'width':'33%','display': 'inline-block'}),
    html.H6('Active processes:', style = {'width':'27%','display': 'inline-block'}),

    dcc.Textarea(
            id='databases', style = {'width':'37%', 'height':'100px'}),
    dcc.Textarea(
            id='src_data', style = {'width':'33%', 'height':'100px'}),
    dcc.Textarea(
            id='running_processes', style = {'width':'27%', 'height':'100px'}),
    html.Br(),

    html.Button('Update', id='button_general_info'),

    html.H6('Scripts:'),


    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script0',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[0],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script0',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script0')], style = {'width':'50%','display': 'inline-block'}),

    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script1',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[1],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script1',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script1')], style = {'width':'50%','display': 'inline-block'}),

    dcc.Textarea(
        id='script0', style = {'width':'50%', 'height':'300px'}),
    dcc.Textarea(
        id='script1', style = {'width':'50%', 'height':'300px'}),
    html.Br(),
    html.Br(),


    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script2',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[2],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script2',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script2')], style = {'width':'50%','display': 'inline-block'}),

    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script3',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[3],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script3',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script3')], style = {'width':'50%','display': 'inline-block'}),

    dcc.Textarea(
        id='script2', style = {'width':'50%', 'height':'300px'}),
    dcc.Textarea(
        id='script3', style = {'width':'50%', 'height':'300px'}),
    html.Br(),
    html.Br(),


    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script4',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[4],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script4',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script4')], style = {'width':'50%','display': 'inline-block'}),

    html.Div([
    html.Div(
    dcc.Dropdown(
        id='dropdown_script5',
        options = [{'label': k, 'value': k} for k in scripts_names],
        value = scripts_names[5],
        style={'width': '190px'}
     ), style={'display': 'inline-block','width': '200px'}),
    dcc.Input(
        id='input_script5',
        placeholder='Enter a value...',
        type='text',
        value=''
    ),
    html.Button('Update', id='button_script5')], style = {'width':'50%','display': 'inline-block'}),

    dcc.Textarea(
        id='script4', style = {'width':'50%', 'height':'300px'}),
    dcc.Textarea(
        id='script5', style = {'width':'50%', 'height':'300px'}),

    html.Br(),

    html.H6('Processes conditions:'),
    dcc.Markdown('''
        Each of the following scripts need other scripts to be finished to start:

        * wikipedia_diversity: none
        * content_selection: wikipedia_diversity
        * stats_generation: content_selection
        * relevance_features: content_selection
        * history_features: content_selection
        * top_ccc_selection: content_selection, relevance_features
        * missing_content_selection: content_selection
     '''.replace('  ', '')),

    footbar,

], className="container")

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###




#### CALLBACKS ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
@dash_app_status.callback(
    dash.dependencies.Output('current_cycle', 'children'),[Input('url', 'pathname')])
def current_cycle(url):
    return 'Current cycle: '+check_current_cycle()

@dash_app_status.callback(
    dash.dependencies.Output('running_processes', 'value'),[dash.dependencies.Input('button_general_info', 'n_clicks')])
def running_processes(url):
    return check_running_processes()

@dash_app_status.callback(
    dash.dependencies.Output('databases', 'value'),[dash.dependencies.Input('button_general_info', 'n_clicks')])
def databases(url):
    return read_files_from_directory(databases_path,'db')

@dash_app_status.callback(
    dash.dependencies.Output('src_data', 'value'),[dash.dependencies.Input('button_general_info', 'n_clicks')])
def src_data(url):
    return read_files_from_directory(data_scripts_path,'log')

@dash_app_status.callback(
    dash.dependencies.Output('script0', 'value'),
    [dash.dependencies.Input('dropdown_script0', 'value'), dash.dependencies.Input('input_script0', 'value'),dash.dependencies.Input('button_script0', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'


@dash_app_status.callback(
    dash.dependencies.Output('script1', 'value'),
    [dash.dependencies.Input('dropdown_script1', 'value'), dash.dependencies.Input('input_script1', 'value'),dash.dependencies.Input('button_script1', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'

@dash_app_status.callback(
    dash.dependencies.Output('script2', 'value'),
    [dash.dependencies.Input('dropdown_script2', 'value'), dash.dependencies.Input('input_script2', 'value'), dash.dependencies.Input('button_script2', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'

@dash_app_status.callback(
    dash.dependencies.Output('script3', 'value'),
    [dash.dependencies.Input('dropdown_script3', 'value'), dash.dependencies.Input('input_script3', 'value'), dash.dependencies.Input('button_script3', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'

@dash_app_status.callback(
    dash.dependencies.Output('script4', 'value'),
    [dash.dependencies.Input('dropdown_script4', 'value'), dash.dependencies.Input('input_script4', 'value'), dash.dependencies.Input('button_script4', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'

@dash_app_status.callback(
    dash.dependencies.Output('script5', 'value'),
    [dash.dependencies.Input('dropdown_script5', 'value'), dash.dependencies.Input('input_script5', 'value'), dash.dependencies.Input('button_script5', 'n_clicks'), Input('url', 'pathname')])
def script0(script_name, nlines, n_clicks, url_path):
    if url_path == None: url_path = ''
    if nlines == '': nlines = n_lines_default
    db_lines = read_from_function_account(script_name)
    log_data = read_from_log_file(data_scripts_path,script_names_to_files[script_name]+'.log', nlines)
    return '* Script Log:\n'+str(log_data)+'\n* Database Function_Account:\n'+str(db_lines)+'\n'

