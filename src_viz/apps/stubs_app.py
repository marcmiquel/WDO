import sys
sys.path.insert(0, '/srv/wcdo/src_viz')
from dash_apps import *

### DASH APP ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 
dash_app_viquiestirada = Dash(__name__, server = app, url_base_pathname= webtype + '/viquiestirada/', external_stylesheets=external_stylesheets, external_scripts=external_scripts)

df = pd.read_csv('/srv/wcdo/other/cawiki_relevant_stubs.csv')

# Renaming the columns
columns_dict = {'page_title':'Títol','num_bytes':'Octets','column_byte_diff':'Dif.Oct.','column_byte_diff_lang':'Lleng.Ref.','num_pageviews':'Vistes','num_interwiki':'Interwiki','num_wdproperty':'WDProp.','num_inlinks':'Inlinks','date_created':'Creació','iso3166':'Geo.','gender':'Gènere','ccc_binary':'CCC','category_name':'Categories','cur_preferred_languages':'Lleng.Props.'}

df = df.rename(columns=columns_dict)
df = df.reset_index()

df = df.fillna('')


title = "Viquiestirada: llista d'articles rellevants amb menys de 1500 octets de la Viquipèdia en català"
dash_app_viquiestirada.title = title+title_addenda
dash_app_viquiestirada.layout = html.Div([
    html.H3(title, style={'textAlign':'center'}),
    dcc.Markdown(
    '''A l'entorn d'una mica més d'un quart de la Viquipèdia en català són articles de menys de 1500 octets, és a dir, poc més d'un paràgraf.

    **Aquesta pàgina conté una taula amb articles que poden ser rellevants i que tenen menys de 1500 octets**. Per obtenir-los s'han agafat dades de la tardor del 2018 i s'ha fet una cerca dels 30,000 articles d'aquesta mida amb més interwiki links (interès entre llengües) i una altra dels 30,000 amb més vistes durant el mes d'octubre de 2018. Els articles que tenen en comú i apareixen a la llista són 8176.

    Cada article és una fila i les columnes corresponen a diferents característiques. La columna **Dif.Oct.** marca el nombre d'octets que té de més la versió més llarga de l'article en una altra llengua, que seria la de la columna següent (**Lleng.Ref.**). **Vistes** seria el nombre de visites. **Interwiki** el nombre de vincles a d'altres llengües on l'article existeix. **WDProp.** el nombre de propietats que aquest ítem té a Wikidata. **Inlinks** el nombre de vincles entrants que té aquest article des de la Viquipèdia en català, de manera que també n'incrementa la probabilitat de rebre una visita. **Geo.** inclou el codi ISO3166 de país, en el cas que aquests articles siguin geolocalitzats. Gènere si l'article es defineix com a home o dona a Wikidata. **Categories** serien les categories les quals té assignades l'article. **Lleng.Props**. mostra si l'article és disponible en castellà, anglès, francès, italià i alemany. **Creació** mostra la data de creació a la Viquipèdia en català, és a dir, ens permet veure el temps que porta com a esborrany.

    Us animo a provar la interfície d'aquesta taula que, a mode similar a l'Excel, permet ordenar les columnes de menor a major (molt útil per explorar quins són els que tenen més valor de qualsevol característica) i filtrar (FILTER ROWS) per qualsevol característica.
    Per exemple, si volem els articles referents a Austràlia, podem clicar FILTER ROWS i anar a Geo. i escriure AU. Si volem els articles que tenen entre 100 o més 100 interwiki links podem escriure 100 o > 100 en la seva casella. D'aquesta manera podem centrar-nos a explorar grups concrets.

    A sota de la taula s'hi poden veure tres gràfics que mostren la distribució d'articles d'acord amb tres característiques (el nombre d'octets de diferència amb la versió més llarga de l'article, el nombre de visites obtingudes i el nombre d'inlinks o vincles entrants des d'altres articles de la mateixa viquipèdia). Si ordeneu les columnes d'acord amb alguna d'aquestes característiques veureu que el gràfic també canvia. Aquests gràfics estan limitats a un nombre d'articles, per tant, només seran visibles quan feu una selecció d'una part de la taula.

    **Que tinguem una bona Viquiestirada!**
    '''.replace('  ', '')),

    dt.DataTable(
        rows=df.to_dict('records'),
        columns = ['Títol','Octets','Dif.Oct.','Lleng.Ref.','Vistes','Interwiki','WDProp.','Inlinks','Geo.','Gènere','Categories','Lleng.Props.','Creació'],
        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-viquiestirada'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-viquiestirada'
    )

], className="container")



@dash_app_viquiestirada.callback(
    Output('datatable-viquiestirada', 'selected_row_indices'),
    [Input('graph-viquiestirada', 'clickData')],
    [State('datatable-viquiestirada', 'selected_row_indices')])
def app2_update_selected_row_indices(clickData, selected_row_indices):
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@dash_app_viquiestirada.callback(
    Output('graph-viquiestirada', 'figure'),
    [Input('datatable-viquiestirada', 'rows'),
     Input('datatable-viquiestirada', 'selected_row_indices')])
def app2_update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    fig = plotly.tools.make_subplots(
        rows=3, cols=1,
        subplot_titles=("Articles per octets de diferència amb la seva versió lingüística més llarga.", "Articles per nombre de visites obtingudes.", "Articles per nombre d'inlinks (vincles entrants)",),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'name':'',
        'hovertext':'',
        'x': dff['Títol'],
        'y': dff['Dif.Oct.'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'',
        'x': dff['Títol'],
        'y': dff['Vistes'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig.append_trace({
        'name':'',
        'hovertext':'',
        'x': dff['Títol'],
        'y': dff['Inlinks'],
        'type': 'bar',
        'marker': marker
    }, 3, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

