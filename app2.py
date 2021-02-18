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
from dateutil import parser

import mkdf
from server import server

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
    url_base_pathname="/daily_report/",
)

lines = [
    "LZ-01",
    "LZ-02",
    "LZ-03",
    "LZ-04",
    "LZ-05",
    "LN-01",
    "LN-03",
    "LL-01",
    "LL-02",
    "LP-01",
]


app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H4(
                    id="title",
                    children="Daily report",
                    style={
                        "textAlign": "center",
                        "margin-top": "1%",
                    },
                ),
                html.Div(
                    children=[
                        html.H1(
                            children="Select Date: ",
                            style={
                                "margin-left": "4%",
                                "margin-right": "2%",
                                "display": "inline-block",
                                "font-size": "15pt",
                                "vertical-align": "sub",
                            },
                        ),
                        # календарь для выбора дня
                        dcc.DatePickerSingle(
                            id="TablesDPS",
                            min_date_allowed=dtime(2020, 1, 1),
                            initial_visible_month=(dtime.today() - timedelta(days=1)),
                            date=dtime.today() - timedelta(days=1),
                            display_format="DD-MM-YYYY",
                        ),
                    ],
                ),
                dbc.Row(
                    id="LinesTable",
                    style={
                        "width": "100%",
                        "margin-left": "0%",
                        "height": "100%",
                    },
                ),
            ]
        )
    ]
)


def makedate(dt):

    dt = str(dt)[:10].replace("-", "")
    dt2 = dtime.strptime(dt, "%Y%m%d")

    dt2 += timedelta(days=1)
    dt2 = str(dt2)[:10].replace("-", "")

    return dt, dt2


@app.callback(
    [
        Output("LinesTable", "children"),
        Output("title", "children"),
    ],
    [Input("TablesDPS", "date")],
)
def get_line_graph(dt):
    """Эта функция форматирует начало и конец дат
    выборки так, чтобы они подходили для SQL"""

    date_start, date_end = makedate(dt)

    df_line_lvl_0 = mkdf.get_df_lvl_0(date_start, date_end)

    linegraph = []

    for line in lines:

        ln_line = mkdf.get_df_line_lvl_1(df_line_lvl_0, line)

        linegraph.append(
            html.Div(
                children=[
                    # это заголовок первого уровня с назавнием линии
                    html.H1(
                        line,
                        style={
                            "textAlign": "center",
                            "font-size": "11pt",
                            "margin-top": "5%",
                        },
                    ),
                    # этот dcc.graph получает график линии
                    dcc.Graph(
                        id=line + " line",
                        figure=mkdf.make_line(ln_line, date_start, date_end),
                        style={"height": "450px"},
                        config={
                            "editable": True,
                            "edits": {
                                "axisTitleText": False,
                                "titleText": False,
                            },
                            "displayModeBar": False,
                        },
                    ),
                    # этот dcc.graph принимает [0] элемент ln_line - график
                    dcc.Graph(
                        id=line + " line table",
                        figure=mkdf.make_line_table(ln_line, date_start, date_end)[0],
                        config={
                            "displayModeBar": False,
                        },
                    ),
                ],
                style={"width": "50%"},
            )
        )

    cache_status = ".(cached)" if "cache" in df_line_lvl_0.columns else ""

    new_title = "Report for the date: {} - {}{}".format(
        dtime.strftime(parser.parse(date_start) + timedelta(hours=8), "%d.%m.%Y %H:%M"),
        dtime.strftime(parser.parse(date_end) + timedelta(hours=8), "%d.%m.%Y %H:%M"),
        cache_status,
    )

    return [ln for ln in linegraph], new_title


if __name__ == "__main__":
    app.run_server(debug=True, threaded=False, processes=3)
    # app.run_server(host='10.81.50.14', port=5000, debug=False)
