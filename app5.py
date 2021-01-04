from pathlib import Path
from datetime import datetime, timedelta

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from dash.dependencies import Input, Output

import ibea
import pgconn
from settings import IBEA_ADDRESS
from server import server

path = Path(__file__).parents[0]

orders_dict = ibea.ibea_orders()


app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
)
#    url_base_pathname='/realtime/')

app.layout = html.Div(
    [
        # выпадающий список заказов
        dcc.Dropdown(
            id="orders",
            options=[
                {
                    "label": "{}: {}".format(order, orders_dict.get(order)),
                    "value": str(order),
                }
                for order in sorted(orders_dict.keys(), reverse=True)
                if len(order) == 5
            ],
            placeholder="Select Order",
            style={"width": "600px", "margin-bottom": "5px"},
        ),
        html.Div(
            id="table",
            style={"width": "800px"},
        ),
        dcc.DatePickerRange(
            id="datepicker",
            month_format="X",
            end_date_placeholder_text="X",
            start_date=datetime.today() - timedelta(days=3),
            end_date=datetime.today(),
        ),
        html.Div(
            id="timetable",
            style={"width": "800px"},
        ),
    ],
)


def mk_table(df):

    df["Date End"] = pd.to_datetime(df["Date End"], format="%d.%m.%Y").astype(str)

    df.style.format({"Percent": "{:.2%}"})
    # print(df)
    # добавление последней строки с итогами
    if not df.empty:

        df = df.append(
            {
                "Date End": "",
                "Shift": "",
                "Order": "",
                "Description": "",
                "line": "<b>Total",
                "Percent": df["Rejected"].sum() / df["Total"].sum() * 100,
                "Rejected": df["Rejected"].sum(),
                "Total": df["Total"].sum(),
            },
            ignore_index=True,
        )
    else:
        # показ примечания по статитстике, так как всегда передается пустой df
        df = pd.DataFrame(["You have to select order to see the statistic"])
        df.columns = ["information"]

    # таблица
    fig = go.Figure(
        data=[
            go.Table(
                columnwidth=[20, 10, 15, 70, 15, 15, 20, 20],
                header=dict(values=df.columns.values, height=30),
                cells=dict(
                    values=df.transpose(),
                    align=[
                        "left",  # Date Start
                        "center",  # Shift
                        "center",  # Order
                        "left",  # Description
                        "center",  # line
                        "right",  # Percent
                        "right",  # Rejected
                        "right",  # Total
                    ],
                    format=[None, None, None, None, None, ".2f", ",d", ",d"],
                    height=20,
                ),
            )
        ],
        layout=go.Layout(margin=dict(t=0, l=0, b=0, r=0)),
    )

    # возврат таблицы должен осуществляться в div
    return [
        dcc.Graph(
            id="table_",
            figure=fig,
            config=dict(displayModeBar=False),
        )
    ]


# формирование таблицы. на вход принимает номер заказа, на выход дает таблицу
@app.callback(
    [Output("table", "children")],
    [Input("orders", "value")],
)
def fill_table(orderno):

    return mk_table(ibea.ibea_stat(orderno))


@app.callback(
    [Output("timetable", "children")],
    [Input("datepicker", "start_date"), Input("datepicker", "end_date")],
)
def table_by_date(dt, dt2):

    df = ibea.ibea_date(dt, dt2)

    print(df[df["line"].str.contains(r"1\b") == True])

    # return html.Div(children=[mk_table(ibea.ibea_date(dt, dt2, line))]) for line in IBEA_ADDRESS


if __name__ == "__main__":
    app.run_server(debug=True)
