import calendar
from calendar import monthrange
from datetime import datetime as dtime
from datetime import timedelta
from time import sleep

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_daq as daq
import dash_html_components as html
import pandas as pd
from dash.dash import no_update
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import mkdf
import settings
from server import server
from pathlib import Path

path = Path(__file__).parents[0]

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
    url_base_pathname="/settings/",
)

lines = ["LZ-01", "LZ-02", "LZ-03", "LZ-04", "LZ-05", "LN-01", "LN-03"]
letters = ["A", "B", "C", "D"]

plan_settings = []
month_names = []


plan_path = path / r".\Plan.csv"
plan_letter = path / r".\Shift.csv"


letter_id = [["{} {}".format(line, letter) for letter in letters] for line in lines]
line_letters = []
for letter in letter_id:
    for let in letter:
        line_letters.append(let)


def args_check(*args, **kwargs):
    args = list(args)

    for i in range(len(args)):
        if args[i] == None or args[i] < 0:
            args[i] = 0

    return args


for line in lines:
    # здесь создается набор графиков и таблиц по линиям.

    # этот массив собирает блоки input для вкладки settings
    plan_settings.append(
        html.Div(
            [
                # подпись названия линии напротив блока input
                html.P(
                    "{} ({}K pcs/shift): ".format(
                        line, int(settings.LINE_OUTPUT.get(line) / 1000)
                    ),
                    style={
                        "display": "inline-block",
                        "margin-right": "10px",
                        "margin-bottom": "10px",
                        "font-weight": "bold",
                    },
                ),
                # блок input
                dbc.Input(
                    id="{} plan value".format(line),
                    placeholder="{} Plan".format(line),
                    value=0,
                    type="number",
                    style={
                        "margin-right": "5px",
                        "width": "500px",
                    },
                ),
                # блоки с названием буквы смен для ввода количества рабочих смен
                html.Div(
                    [
                        html.Div(
                            children=[
                                html.P(
                                    children="{}".format(letter),
                                    style={
                                        "textAlign": "center",
                                        "margin-bottom": "0%",
                                    },
                                ),
                                dbc.Input(
                                    id="{} {}".format(line, letter),
                                    placeholder="input {} shifts".format(letter),
                                    value=0,
                                    min=0,
                                    type="number",
                                    style={"width": "80px", "margin-right": "5px"},
                                ),
                                html.P(
                                    id="{} {} quantity".format(line, letter),
                                    style={
                                        "display": "inline-block",
                                        "textAlign": "center",
                                        "width": "80px",
                                        "margin-bottom": "10px",
                                    },
                                ),
                            ],
                            style={"display": "inline-block"},
                        )
                        for letter in letters
                    ],
                    style={"display": "contents"},
                ),
                html.P(
                    id="{} sum".format(line),
                    style={
                        "display": "inline-block",
                        "margin-right": "20px",
                        "width": "60px",
                        "textAlign": "center",
                        "margin-bottom": "10px",
                    },
                ),
            ],
            style={
                "display": "inline-block",
                "margin": "5px",
            },
        )
    )


for i in range(1, 13):
    month_names.append({"label": calendar.month_name[i], "value": i})

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                dcc.Dropdown(
                    id="setyear",
                    options=[
                        {"label": "2020", "value": 2020},
                        {"label": "2021", "value": 2021},
                    ],
                    value=dtime.today().year if dtime.today().year <= 2021 else 2021,
                    clearable=False,
                    style={
                        "display": "inline-block",
                        "width": "150px",
                        "margin-right": "5px",
                    },
                ),
                dcc.Dropdown(
                    id="setmonth",
                    options=[month for month in month_names],
                    value=dtime.today().month,
                    clearable=False,
                    style={
                        "display": "inline-block",
                        "width": "150px",
                    },
                ),
            ],
            style={
                "margin": "5px",
            },
        ),
        html.Div(
            children=[
                dbc.Button(
                    "Save",
                    id="save",
                    n_clicks=0,
                    style={
                        "width": "150px",
                        "margin-right": "5px",
                    },
                ),
                dbc.Button(
                    "Load",
                    id="load",
                    n_clicks=0,
                    style={
                        "width": "150px",
                    },
                ),
            ],
            style={
                "margin-left": "5px",
            },
        ),
        html.Div(
            children=[
                html.P(id="plans_text", style={"display": "none"}),
                html.Div(
                    id="plans",
                    children=[field for field in plan_settings],
                    style={"textAlign": "left", "align": "center", "width": "1200px"},
                ),
            ],
            style={"textAlign": "left", "display": "table-cell", "width": "40%"},
        ),
    ],
)


def rd_plan_letters_csv():

    df_letter = pd.read_csv(plan_letter, sep=";")
    df_letter.reset_index(inplace=True)

    return df_letter


@app.callback(
    [Output("{} quantity".format(letter), "children") for letter in line_letters],
    [Output("{} sum".format(line), "children") for line in lines],
    [Input(letter, "value") for letter in line_letters],
)
def quantity(*args, **kwargs):

    args = args_check(*args)
    line_dict = dict.fromkeys(lines, 0)
    line_dict_sh = dict.fromkeys(lines, 0)
    res = []

    rt = list(zip(line_letters, args))

    for line in lines:
        for arg in rt:
            if line in arg[0]:
                line_dict[line] += arg[1] * settings.LINE_OUTPUT.get(line)
                line_dict_sh[line] += arg[1]
        # print(line_dict)

    line_list = [
        "TOTAL: {}/{:,d}".format(val, val2)
        for val, val2 in zip(line_dict_sh.values(), line_dict.values())
    ]

    for lnl in rt:
        res.append(lnl[1] * settings.LINE_OUTPUT.get(lnl[0][:5]))

    res = ["{:,d}".format(rs) for rs in res]

    res = [*res, *line_list]

    return res


@app.callback(
    [Output("{} plan value".format(line), "value") for line in lines],
    [Output(letter, "value") for letter in line_letters],
    [
        Input("load", "n_clicks"),
        Input("setyear", "value"),
        Input("setmonth", "value"),
    ],
)
def load_plan(n_clicks, year, month):

    # df_plan = rd_plan_csv()
    df_letter = rd_plan_letters_csv()

    df_plan = df_letter.loc[(df_letter["year"] == year) & (df_letter["month"] == month)]

    df_letter = df_letter.loc[
        (df_letter["year"] == year) & (df_letter["month"] == month)
    ]

    del df_letter["index"]
    del df_letter["line"]
    del df_letter["year"]
    del df_letter["month"]
    del df_letter["plan"]

    df_letter = df_letter.stack()

    # print(df_letter)

    letterlist = df_letter.to_list()

    plan_list = df_plan["plan"].to_list()

    # print(plan_list)

    res = *plan_list, *letterlist

    return res


@app.callback(
    Output("plans_text", "children"),
    [
        Input("save", "n_clicks"),
    ],
    [
        State("setyear", "value"),
        State("setmonth", "value"),
    ],
    [State("{} plan value".format(line), "value") for line in lines],
    [State(letter, "value") for letter in line_letters],
)
def save_plan(n_clicks, year, month, *args, **kwargs):

    if n_clicks == 0:
        raise PreventUpdate
    else:

        args = args_check(*args)
        # df_plan = rd_plan_csv()

        df_letter = rd_plan_letters_csv()

        # del df_plan['index']
        del df_letter["index"]

        line_dict = dict(zip(list(lines), (args[0:8])))

        ln = []
        i = 7
        # print(df_letter.loc[('line' == line), df_letter['plan']].iloc[0])

        for line in lines:
            ln.append([line, year, month, line_dict.get(line), *args[i : i + 4]])
            i += 4

        df_letter.reset_index(drop=True, inplace=True)

        df_letter.loc[(df_letter["year"] == year) & (df_letter["month"] == month)] = ln

        # print(df_letter)

        # df_plan.to_csv(plan_path, index=0, sep=';')
        df_letter.to_csv(plan_letter, index=0, sep=";")

        return "Saved!"


if __name__ == "__main__":
    app.run_server(debug=True)
    # app.run_server(host="10.81.50.14", port=5000, debug=False)
