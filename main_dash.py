from dash import Dash, html, Input, Output, callback_context, dcc
import plotly.express as px
import pandas as pd

file_csv_path_cube_data ="./cube_data.csv"

data = pd.read_csv(file_csv_path_cube_data)

selected_as = 'AVG_PRCP'

list_COUNTRY_GLOBAL = data.loc[~data['COUNTRY_ISO'].duplicated(), 'COUNTRY_ISO'].tolist()
list_CITY_GLOBAL = data.loc[~data['CITY'].duplicated(), 'CITY'].tolist()
list_YEAR_GLOBAL = data.loc[~data['Year'].duplicated(), 'Year'].tolist()
list_SEASON_GLOBAL = data.loc[~data['Season'].duplicated(), 'Season'].tolist()

def select_data(_data, list_country,list_CITY, list_Season, years):
    dff = _data.copy()
    #
    if (list_country == [] or list_country == None):
        list_country = list_COUNTRY_GLOBAL
    dff = dff[dff['COUNTRY_ISO'].isin(list_country)]
    #
    if (list_CITY == [] or list_CITY == None):
        list_CITY = list_CITY_GLOBAL
    dff = dff[dff['CITY'].isin(list_CITY)]
    #
    if (years == [] or years == None):
        years = list_YEAR_GLOBAL
    dff = dff[dff['Year'].isin(years)]
    #
    if (list_Season == [] or list_Season == None):
        list_Season = list_SEASON_GLOBAL
    dff = dff[dff['Season'].isin(list_Season)]
    #
    return  dff

# ---------------------------------------------------------------------------

def get_map01(_data,_msg):
    dff = _data.copy()
    dff = dff.groupby(['COUNTRY_ISO', 'COUNTRY']).mean()
    dff.reset_index(inplace=True)
    #
    fig = px.choropleth(data_frame=dff,
                        locations="COUNTRY_ISO",
                        color=selected_as,
                        scope="africa",
                        hover_name="COUNTRY",
                        template='plotly_white',
                        color_continuous_scale=px.colors.sequential.Viridis_r)

    # fig.write_html("get_map01.html")
    fig.update_geos(
        center=dict(lon=0, lat=36),
        projection_rotation=dict(lon=0, lat=36, roll=0),
        lataxis_range=[-15, 15],
        lonaxis_range=[-15, 15]
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return html.Div([
        html.H1(_msg, style={'text-align': 'center'}),
        dcc.Graph(figure=fig)
    ], style={'width': '47%', 'border-style': 'solid', 'float': 'left', 'border-color': '#0000FF'}
    )
# ---------------------------------------------------------------------------
def get_map02(_data, _msg):
    dff = _data.copy()
    dff = dff.groupby(['STATION_ID']).mean()
    dff.reset_index(inplace=True)

    fig = px.scatter_geo(dff,
                         lat='LATITUDE',
                         lon='LONGITUDE',
                         scope="africa",
                         size=selected_as,
                         color=selected_as,
                         template='plotly_white'
                         )
    fig.update_geos(
        center=dict(lon=0, lat=36),
        projection_rotation=dict(lon=0, lat=36, roll=0),
        lataxis_range=[-15, 15],
        lonaxis_range=[-15, 15]
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return html.Div([
        html.H1(_msg, style={'text-align': 'center'}),
        dcc.Graph(figure=fig)
    ], style={'width': '47%', 'border-style': 'solid', 'float': 'left', 'border-color': '#0000FF'}
    )
# ---------------------------------------------------------------------------

def get_map03(_data,_msg):
    dff = _data.copy()
    dff = dff.groupby(['STATION_ID', 'CITY']).mean()
    dff.reset_index(inplace=True)

    fig = px.histogram(dff, x="CITY", y=selected_as, histfunc='avg')
    return html.Div([
        html.H1(_msg, style={'text-align': 'center'}),
        dcc.Graph(figure=fig)
    ], style={'width': '47%', 'border-style': 'solid', 'float': 'left', 'border-color': '#0000FF'}
    )

# ---------------------------------------------------------------------------

app = Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.Button('PRCP', id='btn-nclicks-1', n_clicks=0),
        html.Button('TAVG', id='btn-nclicks-2', n_clicks=0),
        html.Button('TMAX', id='btn-nclicks-3', n_clicks=0),
        html.Button('TMIN', id='btn-nclicks-4', n_clicks=0),
        # --------------------------------------------------------------
        html.H1('slect county', style={'text-align': 'center'}),
        dcc.Dropdown(id='dropdown_1',
                     options={'DZA': 'Algeria','MAR': 'Morocco','TUN': 'Tunisia'},
                     multi=True, style={'text-align': 'center'}
                     ),
        # --------------------------------------------------------------
        html.H1('slect city', style={'text-align': 'center'}),
        dcc.Dropdown(id='dropdown_2', options=[{'label': f'{x}', 'value': f'{x}'} for x in list_CITY_GLOBAL],
                     multi=True, style={'text-align': 'center'}),
        # --------------------------------------------------------------
        html.H1('slect season', style={'text-align': 'center'}),
        dcc.Dropdown(id='dropdown_3',
                     options=list_SEASON_GLOBAL, multi=True, style={'text-align': 'center'}
                     ),
        # --------------------------------------------------------------
        html.H1('slect year', style={'text-align': 'center'}),
        dcc.Dropdown(id='dropdown_4', options=[{'label': f'{x}', 'value': x} for x in list_YEAR_GLOBAL],
                     multi=True, style={'text-align': 'center'}),
        # --------------------------------------------------------------
    ], style={'width': '47%', 'height': '200px', 'border-style': 'solid', 'float': 'left'}),
    html.Div(id='container-button-timestamp')
], style={'width': '100%', 'height': '100%'})


# ---------------------------------------------------------------------------

@app.callback(
    Output('container-button-timestamp', 'children'),
    Input('btn-nclicks-1', 'n_clicks'),
    Input('btn-nclicks-2', 'n_clicks'),
    Input('btn-nclicks-3', 'n_clicks'),
    Input('btn-nclicks-4', 'n_clicks'),
    Input('dropdown_1', 'value'),
    Input('dropdown_2', 'value'),
    Input('dropdown_3', 'value'),
    Input('dropdown_4', 'value')

)
# ---------------------------------------------------------------------------

def dis(b1, b2, b3, b4, v1, v2, v3,v4):
    global data,selected_as
    msg = 'precipitation rate'
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    if 'btn-nclicks-1' in changed_id:
        selected_as = 'AVG_PRCP'
        msg = 'precipitation rate'
    elif 'btn-nclicks-2' in changed_id:
        selected_as = 'AVG_TAVG'
        msg = 'average temperature rate'
    elif 'btn-nclicks-3' in changed_id:
        selected_as = 'AVG_TMAX'
        msg = 'maximum temperature rate'
    elif 'btn-nclicks-4' in changed_id:
        selected_as = 'AVG_TMIN'
        msg = 'minimum temperature rate'

    data_selected = select_data(data, v1,v2, v3, v4)

    return html.Div([
        get_map01(data_selected,msg),
        get_map02(data_selected,msg),
        get_map03(data_selected,msg)
    ])
# ---------------------------------------------------------------------------

    
if __name__ == '__main__':
    app.run_server(debug=False)
