from datetime import date, datetime, timedelta
from collections import deque
import time

import pandas as pd
import plotly as plt
import plotly.figure_factory as ff
import plotly.graph_objects as go
import psycopg2

import pgconn
import mkdf
from server import server

import dash_daq as daq
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import pandas as pd
from dash.dash import no_update
from dash.dependencies import Input, Output
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

graph=defaultdict(list)

x=[]

lines=['LZ-01','LZ-02','LZ-03','LZ-04','LZ-05','LN-01','LN-03', 'LL-01', 'LL-02', 'LP-01']

app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
    url_base_pathname='/realtime/')

UPDADE_INTERVAL=5


def update():

    # список DIV форм с упакованными графиками
    graphs = []

    # df получаемый из обращения в таблицу up_line_def
    line_info=mkdf.get_df_con()
    
    # список накапливает временные метки для оси Х
    x.append(datetime.now())

    # предел значений графика. Какое количество отсчетов будет отображаться на графике
    list_lim = 100

    #print(x)

    # если значений Х больше предела, то удалить 0-й элемект
    if len(x) > list_lim:
        x.pop(0)

    # накопление графика по линиям
    for line in lines:

        color, label, description, order, order_description, ln_input, ln_output, shift = mkdf.get_df_bar_indicat(line_info,line)
        
        # если значений Y больше предела, то удалить 0-й элемент 
        if len(graph[line]) > list_lim:
            graph[line].pop(0)

        
        # наполнение defaultdict. graph[line] добавление по ключу(создаваемому) на месте 
        graph[line].append(ln_output)

        # если смена открыта, то передать параметры значения X и Y в график,
        # также передать label_indicator, order, ln_input, ln_output, desription, line
        if shift:

            fig = go.Figure(
                data = go.Scatter(
                    y=list(graph[line]),
                    x=list(x),
                    name='input'
                ),
                layout=go.Layout(
                    margin=dict(t=25, l=70, b=10, r=20),
                ),
            )

            fig.update_layout(
                xaxis=dict(range=[x[0],max(x)+timedelta(minutes=1)]),
                yaxis=dict(range=[min(graph[line]),max(graph[line])+1000]),
                xaxis_tickformat='%H:%M',
                annotations=[
                    {
                        'text':'''<b>Input: </b> {:14,}<br><b>Output:</b> {:9,}<br>'''.format(ln_input, ln_output).replace(',',' '),
                        'align':'left',
                        "xref": "paper",
                        "yref": "paper",
                        'x':0.02,
                        'y':1,
                        "showarrow": False,
                        "font": {
                            "size": 10
                        }
                    },
                    {
                        'text':'''<b>Line status:<br>
                            <br>
                            {:>10}<br>{}<br>{}: {} </b>'''.format(label, description, order,order_description),
                        "xref": "paper",
                        "yref": "paper",
                        'align':'right',
                        'x':0.98,
                        'y':1,
                        "showarrow": False,
                        'font':{
                            'color':color,
                            "size": 11
                        },
                    },
                     {
                        'text':'<b>{}</b>'.format(line),
                        "xref": "paper",
                        "yref": "paper",
                        'opacity':0.1,
                        "showarrow": False,
                        "font": {
                            "size": 72
                        }
                    },
                ]
            )

        else:

            fig = go.Figure(
                data = go.Scatter(
                    x=[],
                    y=[],
                    name='input'
                    ),
                layout=go.Layout(
                    margin=dict(t=15, l=70, b=10, r=20),
                    ),
                )

            fig.update_layout(
            xaxis={'visible':False},
            yaxis={'visible':False},
            plot_bgcolor='white',
            annotations=[
                {
                'text':'LINE STOPPED',
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 18
                    }
                },
                {
                'text':'<b>{}</b>'.format(line),
                "xref": "paper",
                "yref": "paper",
                'opacity':0.1,
                "showarrow": False,
                "font": {
                    "size": 72
                    }
                }
            ]
        )
        

        graphs.append(
            html.Div(
                [
                
                dcc.Graph(
                    id='{} live'.format(line),
                    figure=fig,
                    #animate=True,
                    config = dict(displayModeBar=False),
                    style={'height':'450px'}
                    )
                ],
                    style={
                        'display':'inline-block',
                        'width':'20%',
                        
                        }
            )
        )


    return [graph for graph in graphs]


def layout_update():
    return html.Div(
        children=[dbc.Row(
            id='cont_line_graph',
            children=update(),
            style={
                'width':'99%',
                'margin-top':'1%',
            }
        ),
        html.Script(
            type="text/javascript",
            children='''    
                setInterval("my_function();",5000); 
                function my_function(){
                $('#refresh').load(location.href + ' #time');
                }'''
        ),
        ])

app.layout= layout_update


def get_update(period=UPDADE_INTERVAL):
    """Update the data every 'period' seconds"""
    while True:
        update()
        print("data updated")
        time.sleep(period)
"""
executor = ThreadPoolExecutor(max_workers=1)
executor.submit(get_update)
"""


if __name__ == '__main__':
    app.run_server(debug=True)
#    app.run_server(host='10.81.50.14', port=5000, debug=False
