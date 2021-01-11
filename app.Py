from calendar import monthrange
from datetime import datetime as dtime
from datetime import timedelta

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
from dash.dash import no_update
from dash.dependencies import Input, Output

import mkdf
from server import server

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
    url_base_pathname="/production_plan/",
)

lines = ["LZ-01", "LZ-02", "LZ-03", "LZ-04", "LZ-05", "LN-01", "LN-03"]
g = 1


def get_bar_graph():

    indicat_df = mkdf.get_df_con()

    dtime.today()

    year = dtime.today().year
    month = str.zfill(str(dtime.today().month), 2)
    month2 = str.zfill(str(dtime.today().month + 1), 2)

    dt = "{}{}01".format(year, month)

    dt2 = "{}{}01".format(year, month2)

    df_bar_lvl_0 = mkdf.get_df_lvl_0(dt, dt2)

    bargraph = []

    for ln in lines:

        # эта переменная получает df нулевого уровня и возвращает df для графика
        ln_graph = mkdf.get_df_line_lvl_1(df_bar_lvl_0, ln)

        # эта переменная получает df нулевого уровня и возращает df для таблицы
        # ln_table=mkdf.get_df_bar_lvl_2(ln_graph,lines.get(ln))
        graph, table = mkdf.make_bar(ln_graph, indicat_df, ln)

        # создание html блоков с графиками и таблицами
        bargraph.append(
            html.Div(
                [
                    # этот dcc.graph получает df из ln_graph и создает из него таблицу
                    dcc.Graph(
                        id=ln,
                        figure=graph,
                        style={"height": "280px"},
                        config=dict(displayModeBar=False),
                    ),
                    # этот dcc.graph принимает первый элемент lninfo - таблицу
                    dcc.Graph(
                        id=ln + "table",
                        figure=table,
                        config=dict(displayModeBar=False),
                    ),
                ],
                style={"width": "24%", "margin-left": "5px"},
            )
        )

    return [ln for ln in bargraph]


app.layout = html.Div(
    [
        # основная страница
        dcc.Interval(id="live_updated", interval=7000, n_intervals=0),
        html.Div(
            children=[
                html.Div(
                    [
                        html.Div(
                            id="title",
                            children=[
                                html.H1(
                                    children="План производства",
                                    style={"fontsize": 18, "color": "#002F6C"},
                                )
                            ],
                            style={
                                "text-align": "center",
                            },
                        ),
                        dbc.Row(
                            id="bar_row",
                            children=get_bar_graph(),
                            style={
                                "width": "99%",
                                "height": "100%",
                                "margin-left": "1%",
                            },
                        ),
                    ],
                ),
            ]
        ),
    ]
)


@app.callback(
    dash.dependencies.Output("bar_row", "children"),
    [dash.dependencies.Input("live_updated", "n_intervals")],
)
def update_graph(*args):

    return get_bar_graph()


if __name__ == "__main__":
    app.run_server(debug=True)
    # app.run_server(host='10.81.50.131', port=5000, debug=False)
