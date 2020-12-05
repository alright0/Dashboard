from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date
from datetime import datetime as dtime
from datetime import timedelta
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import dash_table
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


path = Path(__file__).parents[0]
lines=['LZ-01','LZ-02','LZ-03','LZ-04','LZ-05']

#print(mkdf.ibea_orders())

orders_dict = mkdf.ibea_orders()
#orders_dict = OrderedDict(sorted(mkdf.ibea_orders()))
#pth= path / r'.\ibea_test.csv'
#df_test = pd.read_csv(pth,sep=';')


#print([{'label':line,'value':line} for line in lines])

app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,)
#    url_base_pathname='/realtime/')


    dcc.Dropdown(
        id='orders',
        options=[{'label':'{}: {}'.format(order, orders_dict.get(order)), 'value':str(order)} 
            for order in sorted(orders_dict.keys(),reverse=True) if len(order) == 5],
        placeholder='Select Order',
        style={
            'width':'500px'
        }
    ),

    
    html.Div(
        id='table',
        style={
            'width':'35%'
            },
            )
],
)


@app.callback(
    [dash.dependencies.Output('table', 'children')],
    [dash.dependencies.Input('orders', 'value')]
)
def fill_table(orderno):
    


    df=mkdf.ibea_stat(orderno)
    df['Date Start'] = pd.to_datetime(df['Date Start'], format='%d.%m.%Y').astype(str)
    
    if not df.empty:

        df = df.append(
            {'Date Start':'Total',
            'Shift':'',
            'Order':'',
            'Description':'',
            'line':'',
            'Percent':df['Percent'].mean(),
            'Rejected':df['Rejected'].sum(),
            'Total':df['Total'].sum(),
            },
            ignore_index=True)

    fig = go.Figure(
        data=[go.Table(
            columnwidth=[25,10,20,50,20,15,20,20],
            header=dict(
                values=df.columns.values,
                height=30),
            cells=dict(
                values=df.transpose(),
                align=['left', 'left','left','left','right','right','right','right',],
                format=[None, None, None,None,None,'.2f',',d',',d'],
                height=20
                )
            )],
        layout=go.Layout(
            margin=dict(t=0, l=0, b=0, r=0)))

    


    #fig.show()
    return [dcc.Graph(
            id='tablea',
            figure=fig,
            #style={'height':'280px'},
            config = dict(displayModeBar=False))]
            





if __name__ == '__main__':
    app.run_server(debug=True)
#    app.run_server(host='10.81.50.14', port=5000, debug=False
    #serve(app, port=8050)


    