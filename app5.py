from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, timedelta
from datetime import datetime as dtime

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
import plotly as plt
import plotly.figure_factory as ff
import plotly.graph_objects as go
import psycopg2
from dash.dash import no_update
from dash.dependencies import Input, Output

import mkdf
import pgconn
from server import server

graph=defaultdict(list)

x=[]

lines=['LZ-01','LZ-02','LZ-03','LZ-04','LZ-05']

#print([{'label':line,'value':line} for line in lines])

app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,)
#    url_base_pathname='/realtime/')


app.layout= html.Div([
    dcc.Dropdown(
        id='lines',
        options=[{'label':line,'value':line} for line in lines],
        placeholder='Select Line',
        multi=True,
        style={
            'width':'500px'
        }
    ),
    dcc.DatePickerSingle(
        id='TablesDPS',
        min_date_allowed=dtime(2017, 1, 1),
        max_date_allowed=(
            dtime.today()),
        initial_visible_month=(
            dtime.today()-timedelta(days=1)),
        date=dtime.today()-timedelta(days=1),
        display_format='DD-MM-YYYY',
    ),
]

)


if __name__ == '__main__':
    app.run_server(debug=True)
#    app.run_server(host='10.81.50.14', port=5000, debug=False
    #serve(app, port=8050)


    